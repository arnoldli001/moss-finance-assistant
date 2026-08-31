"""
Layer 3 - Harness Engineering: SLO 监控聚合器（SLO Monitor）。

聚合以下可靠性组件的运行时指标，供 /slo/status 端点暴露：
    1. circuit_breaker.py — 各依赖熔断器状态（CLOSED/OPEN/HALF_OPEN、累计失败/拒绝次数）
    2. degradation_chain.py — 降级链执行分布（各 Tier 命中次数、平均耗时、硬上限触发次数）
    3. hallucination_guard.py — 幻觉防护通过率、置信度分布
    4. error_classifier.py — 错误四象限分布统计

SLO 定义（与 SKILL.md 第 6.2 节对齐）：
    可用性 SLO  = 成功请求数 / 总请求数 ≥ 99.0%
    延迟 SLO    = P95 延迟 ≤ 30s（单任务硬上限 150s）
    幻觉率 SLO  = 幻觉未通过率 ≤ 5%
    错误预算    = 1 - SLO，按 30 天滚动窗口计算
"""
from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

from agent.circuit_breaker import get_circuit_registry

# ===== 全局常量集中引用（替代魔鬼数字，统一修改一处即全局生效）=====
from config.constants import (
    SLO_AVAILABILITY_TARGET,
    SLO_LATENCY_P95_SEC,
    SLO_HALLUCINATION_PASS_RATE,
    SLO_MAX_TASK_SEC,
    SLO_MAX_TOKENS,
    SLO_ERROR_BUDGET_WINDOW_SEC,
    SLO_MONITOR_MEMORY_WINDOW_SIZE,
)


# ======================================================================
# SLO 目标定义
# ======================================================================

SLO_TARGETS = {
    "availability": SLO_AVAILABILITY_TARGET,
    "latency_p95_sec": SLO_LATENCY_P95_SEC,
    "hallucination_pass_rate": SLO_HALLUCINATION_PASS_RATE,
    "max_task_sec": SLO_MAX_TASK_SEC,
    "max_tokens": SLO_MAX_TOKENS,
}

ERROR_BUDGET_WINDOW_SEC = SLO_ERROR_BUDGET_WINDOW_SEC


# ======================================================================
# 单次事件记录
# ======================================================================

@dataclass
class SLOEvent:
    """单次可靠性事件。"""
    session_id: str
    timestamp: float
    success: bool
    latency_sec: float
    token_count: int = 0
    final_tier: int = 1                 # 降级链最终命中层级
    hit_hard_limit: bool = False
    hallucination_passed: Optional[bool] = None
    hallucination_confidence: Optional[float] = None
    error_quadrant: Optional[str] = None  # A/B/C/D
    circuit_open: bool = False           # 是否触发了任一熔断器 OPEN


# ======================================================================
# SLO 监控器
# ======================================================================

class SLOMonitor:
    """
    SLO 监控聚合器：内存滑动窗口 + 可选 SQLite 持久化。

    使用方式：
        monitor = get_slo_monitor()
        monitor.record_event(SLOEvent(...))
        snapshot = monitor.snapshot()  # 供 /slo/status 端点返回
    """

    def __init__(self, persist_db: Optional[Path] = None,
                 window_size: int = SLO_MONITOR_MEMORY_WINDOW_SIZE):
        """
        Args:
            persist_db: 可选的 SQLite 路径，用于跨重启持久化事件。
                        为 None 时仅保留内存窗口。
            window_size: 内存中保留的最近事件数（滑动窗口）。
        """
        self._events: Deque[SLOEvent] = deque(maxlen=window_size)
        self._tier_hits: Dict[int, int] = defaultdict(int)
        self._hard_limit_hits: int = 0
        self._circuit_open_hits: int = 0
        self._quadrant_counts: Dict[str, int] = defaultdict(int)
        self._hallucination_pass: int = 0
        self._hallucination_fail: int = 0
        self._lock = threading.Lock()
        self._persist_db = persist_db
        if persist_db is not None:
            self._init_db()

    # ------------------------------------------------------------------
    # 事件记录
    # ------------------------------------------------------------------
    def record_event(self, event: SLOEvent) -> None:
        with self._lock:
            self._events.append(event)
            self._tier_hits[event.final_tier] += 1
            if event.hit_hard_limit:
                self._hard_limit_hits += 1
            if event.circuit_open:
                self._circuit_open_hits += 1
            if event.error_quadrant:
                self._quadrant_counts[event.error_quadrant] += 1
            if event.hallucination_passed is True:
                self._hallucination_pass += 1
            elif event.hallucination_passed is False:
                self._hallucination_fail += 1
            if self._persist_db is not None:
                self._persist_event(event)

    # ------------------------------------------------------------------
    # 快照
    # ------------------------------------------------------------------
    def snapshot(self) -> Dict[str, Any]:
        """返回当前 SLO 状态快照，供监控端点暴露。"""
        with self._lock:
            events = list(self._events)
        now = time.time()
        # 仅统计窗口内事件（30 天）
        window_events = [
            e for e in events
            if now - e.timestamp <= ERROR_BUDGET_WINDOW_SEC
        ]
        total = len(window_events)
        successes = sum(1 for e in window_events if e.success)

        # 可用性
        availability = (successes / total) if total > 0 else 1.0
        # 延迟 P95
        latencies = sorted(e.latency_sec for e in window_events)
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0.0
        # 幻觉通过率
        hall_total = self._hallucination_pass + self._hallucination_fail
        hall_pass_rate = (
            self._hallucination_pass / hall_total if hall_total > 0 else 1.0
        )
        # 错误预算消耗（按可用性计算）
        error_budget_total = 1.0 - SLO_TARGETS["availability"]
        error_budget_used = max(0.0, error_budget_total - (1.0 - availability))
        error_budget_pct = (
            (error_budget_used / error_budget_total * 100)
            if error_budget_total > 0 else 0.0
        )

        # 熔断器状态
        cb_snapshots = get_circuit_registry().snapshot_all()

        # 降级链分布
        tier_dist = {
            f"tier_{k}": v for k, v in sorted(self._tier_hits.items())
        }
        total_tier = sum(self._tier_hits.values()) or 1
        tier_pct = {
            f"tier_{k}_pct": round(v / total_tier * 100, 2)
            for k, v in sorted(self._tier_hits.items())
        }

        return {
            "timestamp": now,
            "window_events": total,
            "slo_targets": SLO_TARGETS,
            "metrics": {
                "availability": round(availability, 4),
                "latency_p95_sec": round(p95, 3),
                "hallucination_pass_rate": round(hall_pass_rate, 4),
                "hard_limit_hits": self._hard_limit_hits,
                "circuit_open_hits": self._circuit_open_hits,
            },
            "error_budget": {
                "total_budget": round(error_budget_total, 4),
                "used": round(error_budget_used, 4),
                "consumed_pct": round(error_budget_pct, 2),
                "remaining_pct": round(max(0.0, 100 - error_budget_pct), 2),
            },
            "degradation_chain": {
                "tier_hits": tier_dist,
                "tier_pct": tier_pct,
            },
            "error_quadrant_distribution": dict(self._quadrant_counts),
            "circuit_breakers": cb_snapshots,
            "slo_violations": self._detect_violations(
                availability, p95, hall_pass_rate,
            ),
        }

    @staticmethod
    def _detect_violations(
        availability: float, p95: float, hall_pass_rate: float,
    ) -> List[str]:
        """检测当前是否违反任一 SLO 目标。"""
        violations: List[str] = []
        if availability < SLO_TARGETS["availability"]:
            violations.append(
                f"可用性 {availability:.2%} < SLO {SLO_TARGETS['availability']:.2%}"
            )
        if p95 > SLO_TARGETS["latency_p95_sec"]:
            violations.append(
                f"P95 延迟 {p95:.1f}s > SLO {SLO_TARGETS['latency_p95_sec']}s"
            )
        if hall_pass_rate < SLO_TARGETS["hallucination_pass_rate"]:
            violations.append(
                f"幻觉通过率 {hall_pass_rate:.2%} < SLO "
                f"{SLO_TARGETS['hallucination_pass_rate']:.2%}"
            )
        return violations

    # ------------------------------------------------------------------
    # SQLite 持久化（可选）
    # ------------------------------------------------------------------
    def _init_db(self) -> None:
        if self._persist_db is None:
            return
        self._persist_db.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(self._persist_db)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS slo_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    timestamp REAL,
                    success INTEGER,
                    latency_sec REAL,
                    token_count INTEGER,
                    final_tier INTEGER,
                    hit_hard_limit INTEGER,
                    hallucination_passed INTEGER,
                    hallucination_confidence REAL,
                    error_quadrant TEXT,
                    circuit_open INTEGER
                )
            """)
            conn.commit()

    def _persist_event(self, event: SLOEvent) -> None:
        if self._persist_db is None:
            return
        try:
            with sqlite3.connect(str(self._persist_db)) as conn:
                conn.execute(
                    """INSERT INTO slo_events
                    (session_id, timestamp, success, latency_sec, token_count,
                     final_tier, hit_hard_limit, hallucination_passed,
                     hallucination_confidence, error_quadrant, circuit_open)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        event.session_id, event.timestamp,
                        int(event.success), event.latency_sec,
                        event.token_count, event.final_tier,
                        int(event.hit_hard_limit),
                        int(event.hallucination_passed)
                            if event.hallucination_passed is not None else None,
                        event.hallucination_confidence,
                        event.error_quadrant,
                        int(event.circuit_open),
                    ),
                )
                conn.commit()
        except Exception as e:
            print(f"[SLOMonitor] 持久化失败（不致命）: {e}")


# ======================================================================
# 全局单例
# ======================================================================

_monitor: Optional[SLOMonitor] = None
_monitor_lock = threading.Lock()


def get_slo_monitor() -> SLOMonitor:
    global _monitor
    if _monitor is None:
        with _monitor_lock:
            if _monitor is None:
                _project_root = Path(__file__).resolve().parents[1]
                db_path = _project_root / "data" / "slo_events.db"
                _monitor = SLOMonitor(persist_db=db_path)
    return _monitor
