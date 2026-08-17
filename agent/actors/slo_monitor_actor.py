"""
Actor 实现 #4 —— SLOMonitorActor（SLO 监控）。

替换 agent/slo_monitor.py 中的全局 SLOMonitor 单例可变状态：
  - _events: Deque[SLOEvent]
  - _tier_hits: Dict[int, int]
  - _hard_limit_hits / _circuit_open_hits / ...
  - _hallucination_pass / _hallucination_fail

原问题：
  record_event() 被 main_agent.py 的 finally 块调用，
  同时 /slo/status 端点调用 snapshot() 读取聚合，
  虽然用了 threading.Lock，但修改点和读取点散落在多个模块，
  无法保证"状态转换的确定性顺序"。

Actor 化后：
  所有事件记录、状态聚合查询都走消息邮箱，串行处理。
"""
from __future__ import annotations

import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

from agent.actor_base import Actor, Envelope

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
# SLO 目标（与 slo_monitor.py 保持一致）
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
# 消息类型常量
# ======================================================================

class SLOMsg:
    RECORD_EVENT = "record_event"         # 记录一次 SLO 事件（send 模式）
    SNAPSHOT = "snapshot"                 # 请求 SLO 状态快照（ask 模式）


# ======================================================================
# 事件结构（简化版，避免循环依赖）
# ======================================================================

@dataclass
class _SLOEvent:
    session_id: str
    timestamp: float
    success: bool
    latency_sec: float
    token_count: int = 0
    final_tier: int = 1
    hit_hard_limit: bool = False
    hallucination_passed: Optional[bool] = None
    hallucination_confidence: Optional[float] = None
    error_quadrant: Optional[str] = None
    circuit_open: bool = False


# ======================================================================
# 私有状态
# ======================================================================

@dataclass
class _SLOState:
    events: Deque[_SLOEvent] = field(default_factory=lambda: deque(maxlen=SLO_MONITOR_MEMORY_WINDOW_SIZE))
    tier_hits: Dict[int, int] = field(default_factory=lambda: defaultdict(int))
    hard_limit_hits: int = 0
    circuit_open_hits: int = 0
    quadrant_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    hallucination_pass: int = 0
    hallucination_fail: int = 0


# ======================================================================
# Actor 实现
# ======================================================================

class SLOMonitorActor(Actor[_SLOState]):
    """
    SLO 监控 Actor。聚合可靠性指标并提供快照。

    外部调用示例：
        slo = actor_system.get("slo_monitor")

        # 记录事件（send，不阻塞主流程）
        await slo.send(SLOMsg.RECORD_EVENT, {
            "session_id": sid,
            "timestamp": time.time(),
            "success": True,
            "latency_sec": 12.5,
            "final_tier": 1,
            "hallucination_passed": True,
        })

        # 取快照（ask，供 /slo/status 端点）
        snap = await slo.ask(SLOMsg.SNAPSHOT, {})
    """

    def __init__(self, name: str, *, persist_db: Optional[Path] = None, **kwargs):
        super().__init__(name, **kwargs)
        self._persist_db = persist_db
        if persist_db is not None:
            self._init_db()

    # ------------------------------------------------------------------
    # 生命周期辅助：持久化 DB
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        if self._persist_db is None:
            return
        self._persist_db.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(self._persist_db)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS slo_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT, timestamp REAL, success INTEGER,
                    latency_sec REAL, token_count INTEGER, final_tier INTEGER,
                    hit_hard_limit INTEGER, hallucination_passed INTEGER,
                    hallucination_confidence REAL, error_quadrant TEXT,
                    circuit_open INTEGER
                )
            """)
            conn.commit()

    def _persist_event(self, ev: _SLOEvent) -> None:
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
                        ev.session_id, ev.timestamp,
                        int(ev.success), ev.latency_sec, ev.token_count,
                        ev.final_tier, int(ev.hit_hard_limit),
                        int(ev.hallucination_passed)
                            if ev.hallucination_passed is not None else None,
                        ev.hallucination_confidence,
                        ev.error_quadrant, int(ev.circuit_open),
                    ),
                )
                conn.commit()
        except Exception as e:
            print(f"[SLOActor] 持久化失败（不致命）: {e}")

    # ------------------------------------------------------------------
    # initial_state
    # ------------------------------------------------------------------

    def initial_state(self) -> _SLOState:
        return _SLOState()

    # ------------------------------------------------------------------
    # 聚合计算（纯函数，基于当前 state 计算快照）
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_snapshot(state: _SLOState,
                          cb_snapshots: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        events = list(state.events)
        now = time.time()
        window_events = [
            e for e in events
            if now - e.timestamp <= ERROR_BUDGET_WINDOW_SEC
        ]
        total = len(window_events)
        successes = sum(1 for e in window_events if e.success)
        availability = (successes / total) if total > 0 else 1.0

        latencies = sorted(e.latency_sec for e in window_events)
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0.0

        hall_total = state.hallucination_pass + state.hallucination_fail
        hall_pass_rate = (
            state.hallucination_pass / hall_total if hall_total > 0 else 1.0
        )

        error_budget_total = 1.0 - SLO_TARGETS["availability"]
        error_budget_used = max(0.0, error_budget_total - (1.0 - availability))
        error_budget_pct = (
            (error_budget_used / error_budget_total * 100)
            if error_budget_total > 0 else 0.0
        )

        tier_dist = {f"tier_{k}": v for k, v in sorted(state.tier_hits.items())}
        total_tier = sum(state.tier_hits.values()) or 1
        tier_pct = {
            f"tier_{k}_pct": round(v / total_tier * 100, 2)
            for k, v in sorted(state.tier_hits.items())
        }

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

        return {
            "timestamp": now,
            "window_events": total,
            "slo_targets": SLO_TARGETS,
            "metrics": {
                "availability": round(availability, 4),
                "latency_p95_sec": round(p95, 3),
                "hallucination_pass_rate": round(hall_pass_rate, 4),
                "hard_limit_hits": state.hard_limit_hits,
                "circuit_open_hits": state.circuit_open_hits,
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
            "error_quadrant_distribution": dict(state.quadrant_counts),
            "circuit_breakers": cb_snapshots or {},
            "slo_violations": violations,
        }

    # ------------------------------------------------------------------
    # handle_message
    # ------------------------------------------------------------------

    async def handle_message(self, state: _SLOState, env: Envelope):
        msg = env.msg_type
        p = env.payload

        # ==============================================================
        # 1. RECORD_EVENT —— 记录一次 SLO 事件
        # ==============================================================
        if msg == SLOMsg.RECORD_EVENT:
            # 构造事件对象
            ev = _SLOEvent(
                session_id=p["session_id"],
                timestamp=p.get("timestamp", time.time()),
                success=bool(p.get("success", False)),
                latency_sec=float(p.get("latency_sec", 0.0)),
                token_count=int(p.get("token_count", 0)),
                final_tier=int(p.get("final_tier", 1)),
                hit_hard_limit=bool(p.get("hit_hard_limit", False)),
                hallucination_passed=p.get("hallucination_passed"),
                hallucination_confidence=p.get("hallucination_confidence"),
                error_quadrant=p.get("error_quadrant"),
                circuit_open=bool(p.get("circuit_open", False)),
            )
            # 持久化（副作用，但不会影响 state 计算结果）
            self._persist_event(ev)
            # 构造新状态：所有计数器 +1 / append
            new_events = deque(state.events, maxlen=state.events.maxlen)
            new_events.append(ev)
            new_tier: Dict[int, int] = defaultdict(int, state.tier_hits)
            new_tier[ev.final_tier] += 1
            new_quadrant: Dict[str, int] = defaultdict(int, state.quadrant_counts)
            if ev.error_quadrant:
                new_quadrant[ev.error_quadrant] += 1
            new_hp = state.hallucination_pass
            new_hf = state.hallucination_fail
            if ev.hallucination_passed is True:
                new_hp += 1
            elif ev.hallucination_passed is False:
                new_hf += 1

            new_state = _SLOState(
                events=new_events,
                tier_hits=new_tier,
                hard_limit_hits=state.hard_limit_hits + (1 if ev.hit_hard_limit else 0),
                circuit_open_hits=state.circuit_open_hits + (1 if ev.circuit_open else 0),
                quadrant_counts=new_quadrant,
                hallucination_pass=new_hp,
                hallucination_fail=new_hf,
            )
            return new_state, None

        # ==============================================================
        # 2. SNAPSHOT —— 查询 SLO 状态快照（只读，state 不变）
        # ==============================================================
        if msg == SLOMsg.SNAPSHOT:
            cb_snapshots = p.get("circuit_breakers_snapshot")
            snap = self._compute_snapshot(state, cb_snapshots)
            return state, snap

        return state, None
