"""
Layer 3 Harness Engineering —— 可观测性 / Trace 系统。

记录每一轮交互的完整轨迹：输入、输出、工具调用、Token 估算、延迟、决策推理，
并持久化到 SQLite（data/trace.db），用于调试与审计。

设计要点：
1. 每轮一次写入（log_turn），开销极低，不阻塞主流程。
2. Token 估算采用 char/4 粗略公式（与 OpenAI 经验值接近，无需 tiktoken 依赖）。
3. 延迟统计支持 avg / p50 / p95，便于定位慢查询。
4. 通过 TRACE_ENABLED 环境变量可整体关闭写入（读接口仍可用，便于排查历史数据）。
5. PTD（Progressive Tool Disclosure）披露的工具列表一并记录，用于复盘工具路由效果。
"""
from __future__ import annotations

import json
import sqlite3
import aiosqlite
import asyncio
import datetime
import time
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

from config.constants import TRACE_RECENT_DEFAULT_LIMIT

# ======================================================================
# 配置项（可通过 .env 覆盖）
# ======================================================================
# 是否启用 Trace 写入：置为 0/false/False/off 即关闭
TRACE_ENABLED = os.getenv("TRACE_ENABLED", "1").strip() not in ("0", "false", "False")


# ======================================================================
# 数据结构
# ======================================================================
@dataclass
class TraceEntry:
    """一轮交互的完整轨迹记录。"""
    session_id: str
    turn_id: str
    user_input: str
    assistant_output: str
    tool_calls: List[Dict]              # 工具调用明细（name / args / result 摘要）
    token_estimate: int                 # char/4 粗略估算
    latency_ms: int                     # 本轮端到端延迟（毫秒）
    decision_reason: str                # 决策推理摘要（为何选这些工具 / 为何这样回答）
    timestamp: str                      # ISO 时间戳
    ptd_tools_disclosed: List[str]      # PTD 本轮披露给模型的工具 ID 列表
    memory_stats: Dict                  # 记忆系统状态快照（轮数 / 关键决策数等）


# ======================================================================
# 核心记录器
# ======================================================================
class TraceLogger:
    """
    Trace 记录器，单例。

    使用方式：
        logger = get_trace_logger()
        await logger.log_turn(
            session_id="thread-1",
            user_input="贵州茅台最新股价",
            assistant_output="...",
            tool_calls=[...],
            latency_ms=820,
            ptd_tools=["task"],
            memory_stats={"turn_count": 5},
            decision_reason="命中股票代码 → 调用 database_query_agent",
        )
        recent = await logger.get_recent_traces("thread-1")
        stats = await logger.get_latency_stats("thread-1")
    """

    def __init__(self):
        project_root = Path(__file__).resolve().parents[1]
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = data_dir / "trace.db"
        self._sqlite_conn: Optional[aiosqlite.Connection] = None
        self._sync_conn: Optional[sqlite3.Connection] = None
        # 懒初始化：先同步建表，异步连接在首次 async 方法中建立
        self._init_sync_schema()

    # --------------------------------------------------------------
    # Schema & 连接
    # --------------------------------------------------------------
    def _init_sync_schema(self):
        """同步建表（模块加载时调用，幂等）。"""
        self._sync_conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._sync_conn.execute("PRAGMA journal_mode=WAL")
        cur = self._sync_conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_traces (
                session_id           TEXT NOT NULL,
                turn_id              TEXT NOT NULL,
                user_input           TEXT NOT NULL,
                assistant_output     TEXT NOT NULL,
                tool_calls           TEXT NOT NULL DEFAULT '[]',
                token_estimate       INTEGER NOT NULL DEFAULT 0,
                latency_ms           INTEGER NOT NULL DEFAULT 0,
                decision_reason      TEXT NOT NULL DEFAULT '',
                timestamp            TEXT NOT NULL,
                ptd_tools_disclosed  TEXT NOT NULL DEFAULT '[]',
                memory_stats         TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (session_id, turn_id)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_agent_traces_session
            ON agent_traces(session_id, timestamp DESC)
        """)
        self._sync_conn.commit()

    async def _ensure_async_conn(self) -> aiosqlite.Connection:
        if self._sqlite_conn is None:
            self._sqlite_conn = await aiosqlite.connect(str(self._db_path))
            self._sqlite_conn.row_factory = aiosqlite.Row
            await self._sqlite_conn.execute("PRAGMA journal_mode=WAL")
        return self._sqlite_conn

    @staticmethod
    def _now() -> str:
        return datetime.datetime.now().isoformat(timespec="seconds")

    @staticmethod
    def _estimate_tokens(*texts: str) -> int:
        """粗略估算：总字符数 / 4（OpenAI 经验值，中文略偏低但够用）。"""
        total_chars = sum(len(t or "") for t in texts)
        return total_chars // 4

    @staticmethod
    def _new_turn_id() -> str:
        """生成全局唯一的 turn_id：时间戳 + 短随机后缀。"""
        return f"turn-{int(time.time() * 1000)}-{os.urandom(3).hex()}"

    # --------------------------------------------------------------
    # 写：记录一轮轨迹
    # --------------------------------------------------------------
    async def log_turn(
        self,
        session_id: str,
        user_input: str,
        assistant_output: str,
        tool_calls: list,
        latency_ms: int,
        ptd_tools: list,
        memory_stats: dict,
        decision_reason: str = "",
    ) -> None:
        """
        在一轮交互完成后调用，持久化完整轨迹。

        Args:
            session_id:       会话 ID（与 LangGraph thread_id 相同）
            user_input:       用户本轮原始提问
            assistant_output: 助手本轮最终回答
            tool_calls:       本轮发生的工具调用明细列表
            latency_ms:       端到端延迟（毫秒）
            ptd_tools:        PTD 本轮披露给模型的工具 ID 列表
            memory_stats:     记忆系统状态快照
            decision_reason:  决策推理摘要
        """
        if not TRACE_ENABLED:
            return
        if not session_id:
            return

        user_input = user_input or ""
        assistant_output = assistant_output or ""
        tool_calls = tool_calls or []
        ptd_tools = ptd_tools or []
        memory_stats = memory_stats or {}

        # Token 估算：综合输入 / 输出 / 工具调用 JSON
        tool_calls_json = json.dumps(tool_calls, ensure_ascii=False)
        token_estimate = self._estimate_tokens(
            user_input, assistant_output, tool_calls_json
        )

        turn_id = self._new_turn_id()
        timestamp = self._now()

        conn = await self._ensure_async_conn()
        await conn.execute(
            """INSERT INTO agent_traces
               (session_id, turn_id, user_input, assistant_output, tool_calls,
                token_estimate, latency_ms, decision_reason, timestamp,
                ptd_tools_disclosed, memory_stats)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                session_id, turn_id, user_input, assistant_output,
                tool_calls_json, token_estimate, int(latency_ms),
                decision_reason or "", timestamp,
                json.dumps(ptd_tools, ensure_ascii=False),
                json.dumps(memory_stats, ensure_ascii=False),
            ),
        )
        await conn.commit()

    # --------------------------------------------------------------
    # 读：最近轨迹（调试用）
    # --------------------------------------------------------------
    async def get_recent_traces(self, session_id: str, limit: int = TRACE_RECENT_DEFAULT_LIMIT) -> List[Dict]:
        """
        查询某会话最近的 N 条轨迹，按时间倒序返回。
        每条记录中的 JSON 字段（tool_calls / ptd_tools_disclosed / memory_stats）
        会被反序列化为原生 Python 对象，便于直接消费。
        """
        conn = await self._ensure_async_conn()
        cur = await conn.execute(
            """SELECT session_id, turn_id, user_input, assistant_output, tool_calls,
                      token_estimate, latency_ms, decision_reason, timestamp,
                      ptd_tools_disclosed, memory_stats
               FROM agent_traces
               WHERE session_id = ?
               ORDER BY timestamp DESC
               LIMIT ?""",
            (session_id, limit),
        )
        rows = await cur.fetchall()
        results: List[Dict] = []
        for r in rows:
            try:
                tool_calls = json.loads(r["tool_calls"]) if r["tool_calls"] else []
            except (json.JSONDecodeError, TypeError):
                tool_calls = []
            try:
                ptd_tools = json.loads(r["ptd_tools_disclosed"]) if r["ptd_tools_disclosed"] else []
            except (json.JSONDecodeError, TypeError):
                ptd_tools = []
            try:
                memory_stats = json.loads(r["memory_stats"]) if r["memory_stats"] else {}
            except (json.JSONDecodeError, TypeError):
                memory_stats = {}
            results.append({
                "session_id": r["session_id"],
                "turn_id": r["turn_id"],
                "user_input": r["user_input"],
                "assistant_output": r["assistant_output"],
                "tool_calls": tool_calls,
                "token_estimate": int(r["token_estimate"]),
                "latency_ms": int(r["latency_ms"]),
                "decision_reason": r["decision_reason"],
                "timestamp": r["timestamp"],
                "ptd_tools_disclosed": ptd_tools,
                "memory_stats": memory_stats,
            })
        return results

    # --------------------------------------------------------------
    # 读：延迟统计
    # --------------------------------------------------------------
    async def get_latency_stats(self, session_id: str) -> Dict:
        """
        返回某会话的延迟统计：avg / p50 / p95 / count / max / min。
        p50、p95 采用 nearest-rank 法计算。
        """
        conn = await self._ensure_async_conn()
        cur = await conn.execute(
            "SELECT latency_ms FROM agent_traces WHERE session_id = ? ORDER BY latency_ms ASC",
            (session_id,),
        )
        rows = await cur.fetchall()
        latencies = [int(r["latency_ms"]) for r in rows]
        count = len(latencies)
        if count == 0:
            return {
                "session_id": session_id,
                "count": 0,
                "avg_ms": 0,
                "p50_ms": 0,
                "p95_ms": 0,
                "min_ms": 0,
                "max_ms": 0,
            }

        avg = sum(latencies) / count
        # nearest-rank 百分位：index = ceil(p/100 * n) - 1，夹到 [0, n-1]
        def _percentile(sorted_vals: List[int], p: float) -> int:
            import math
            if not sorted_vals:
                return 0
            idx = math.ceil(p / 100.0 * len(sorted_vals)) - 1
            idx = max(0, min(idx, len(sorted_vals) - 1))
            return sorted_vals[idx]

        return {
            "session_id": session_id,
            "count": count,
            "avg_ms": round(avg, 2),
            "p50_ms": _percentile(latencies, 50),
            "p95_ms": _percentile(latencies, 95),
            "min_ms": latencies[0],
            "max_ms": latencies[-1],
        }

    # --------------------------------------------------------------
    # 清理
    # --------------------------------------------------------------
    async def clear_session(self, session_id: str) -> None:
        """删除某会话的全部轨迹记录。"""
        conn = await self._ensure_async_conn()
        await conn.execute(
            "DELETE FROM agent_traces WHERE session_id = ?",
            (session_id,),
        )
        await conn.commit()


# ======================================================================
# 全局单例
# ======================================================================
_logger: Optional[TraceLogger] = None


def get_trace_logger() -> TraceLogger:
    """获取 TraceLogger 全局单例。"""
    global _logger
    if _logger is None:
        _logger = TraceLogger()
    return _logger
