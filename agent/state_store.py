"""
Layer 4 - Loop Engineering: 跨运行状态持久化模块。

记录智能体跨会话/运行的任务状态：已完成 / 进行中 / 待处理，
供下一轮运行感知"做到哪了、下一步做什么"，形成闭环。

状态数据持久化到 data/state.db（SQLite），按 session_id 隔离。
"""
from __future__ import annotations

import json
import sqlite3
import aiosqlite
import asyncio
import datetime
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


@dataclass
class TaskState:
    """一个任务的状态记录。"""
    task_id: str
    session_id: str
    status: str          # pending / in_progress / completed / failed
    description: str
    created_at: str
    updated_at: str
    result_summary: str = ""
    priority: str = "medium"   # high / medium / low


class StateStore:
    """
    跨运行状态存储器。

    使用方式：
        store = get_state_store()
        task_id = await store.create_task(session_id, "分析A股盘前热点")
        await store.update_task_status(task_id, "in_progress")
        summary = await store.build_state_summary(session_id)
    """

    def __init__(self):
        project_root = Path(__file__).resolve().parents[1]
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = data_dir / "state.db"
        self._async_conn: Optional[aiosqlite.Connection] = None
        self._sync_conn: Optional[sqlite3.Connection] = None
        self._init_sync_schema()

    # --------------------------------------------------------------
    # Schema & 连接
    # --------------------------------------------------------------
    def _init_sync_schema(self) -> None:
        """同步建表（模块加载时调用，幂等）。"""
        self._sync_conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._sync_conn.execute("PRAGMA journal_mode=WAL")
        cur = self._sync_conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS task_states (
                task_id         TEXT PRIMARY KEY,
                session_id      TEXT NOT NULL,
                status          TEXT NOT NULL,
                description     TEXT NOT NULL,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                result_summary  TEXT NOT NULL DEFAULT '',
                priority        TEXT NOT NULL DEFAULT 'medium'
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_task_states_session
            ON task_states(session_id, status)
        """)
        self._sync_conn.commit()

    async def _ensure_async_conn(self) -> aiosqlite.Connection:
        if self._async_conn is None:
            self._async_conn = await aiosqlite.connect(str(self._db_path))
            self._async_conn.row_factory = aiosqlite.Row
            await self._async_conn.execute("PRAGMA journal_mode=WAL")
        return self._async_conn

    @staticmethod
    def _now() -> str:
        return datetime.datetime.now().isoformat(timespec="seconds")

    @staticmethod
    def _new_task_id() -> str:
        """生成唯一 task_id：时间戳前缀 + 随机后缀。"""
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "-" + os.urandom(4).hex()

    # --------------------------------------------------------------
    # 写：创建 / 更新
    # --------------------------------------------------------------
    async def create_task(self, session_id: str, description: str,
                          priority: str = "medium") -> str:
        """创建一个新任务（初始状态 pending），返回 task_id。"""
        conn = await self._ensure_async_conn()
        task_id = self._new_task_id()
        now = self._now()
        await conn.execute(
            """INSERT INTO task_states
               (task_id, session_id, status, description, created_at, updated_at,
                result_summary, priority)
               VALUES (?,?,?,?,?,?,?,?)""",
            (task_id, session_id, "pending", description, now, now, "", priority),
        )
        await conn.commit()
        return task_id

    async def update_task_status(self, task_id: str, status: str,
                                 result_summary: str = "") -> None:
        """更新任务状态（pending/in_progress/completed/failed），可选附带结果摘要。"""
        conn = await self._ensure_async_conn()
        now = self._now()
        if result_summary:
            await conn.execute(
                """UPDATE task_states
                   SET status = ?, result_summary = ?, updated_at = ?
                   WHERE task_id = ?""",
                (status, result_summary, now, task_id),
            )
        else:
            await conn.execute(
                """UPDATE task_states
                   SET status = ?, updated_at = ?
                   WHERE task_id = ?""",
                (status, now, task_id),
            )
        await conn.commit()

    # --------------------------------------------------------------
    # 读：查询任务
    # --------------------------------------------------------------
    async def get_pending_tasks(self, session_id: str) -> List[Dict]:
        """获取某会话下所有 pending / in_progress 任务（按创建时间升序）。"""
        conn = await self._ensure_async_conn()
        cur = await conn.execute(
            """SELECT * FROM task_states
               WHERE session_id = ? AND status IN ('pending', 'in_progress')
               ORDER BY created_at ASC""",
            (session_id,),
        )
        rows = await cur.fetchall()
        return [self._row_to_dict(r) for r in rows]

    async def get_completed_tasks(self, session_id: str) -> List[Dict]:
        """获取某会话下所有 completed 任务（按完成时间倒序）。"""
        conn = await self._ensure_async_conn()
        cur = await conn.execute(
            """SELECT * FROM task_states
               WHERE session_id = ? AND status = 'completed'
               ORDER BY updated_at DESC""",
            (session_id,),
        )
        rows = await cur.fetchall()
        return [self._row_to_dict(r) for r in rows]

    async def get_next_task(self, session_id: str) -> Optional[Dict]:
        """获取最高优先级的待处理任务（high > medium > low，同优先级按创建时间升序）。"""
        conn = await self._ensure_async_conn()
        cur = await conn.execute(
            """SELECT * FROM task_states
               WHERE session_id = ? AND status = 'pending'
               ORDER BY CASE priority
                          WHEN 'high'   THEN 0
                          WHEN 'medium' THEN 1
                          WHEN 'low'    THEN 2
                          ELSE 3
                        END,
                        created_at ASC
               LIMIT 1""",
            (session_id,),
        )
        row = await cur.fetchone()
        return self._row_to_dict(row) if row else None

    # --------------------------------------------------------------
    # 状态摘要
    # --------------------------------------------------------------
    async def build_state_summary(self, session_id: str) -> str:
        """
        构建当前会话状态摘要：
        【进行中任务】...
        【已完成任务】...
        【待处理任务】...
        """
        conn = await self._ensure_async_conn()

        # 进行中
        in_prog_cur = await conn.execute(
            """SELECT description, result_summary FROM task_states
               WHERE session_id = ? AND status = 'in_progress'
               ORDER BY updated_at ASC""",
            (session_id,),
        )
        in_prog = await in_prog_cur.fetchall()

        # 已完成
        done_cur = await conn.execute(
            """SELECT description, result_summary FROM task_states
               WHERE session_id = ? AND status = 'completed'
               ORDER BY updated_at DESC""",
            (session_id,),
        )
        done = await done_cur.fetchall()

        # 待处理（按优先级排序）
        pend_cur = await conn.execute(
            """SELECT description, priority FROM task_states
               WHERE session_id = ? AND status = 'pending'
               ORDER BY CASE priority
                          WHEN 'high'   THEN 0
                          WHEN 'medium' THEN 1
                          WHEN 'low'    THEN 2
                          ELSE 3
                        END,
                        created_at ASC""",
            (session_id,),
        )
        pend = await pend_cur.fetchall()

        parts: List[str] = []

        # 进行中
        if in_prog:
            items = [
                f"- {r['description']}" + (f"（{r['result_summary']}）" if r['result_summary'] else "")
                for r in in_prog
            ]
            parts.append("【进行中任务】\n" + "\n".join(items))
        else:
            parts.append("【进行中任务】无")

        # 已完成
        if done:
            items = [
                f"- {r['description']}" + (f"（{r['result_summary']}）" if r['result_summary'] else "")
                for r in done
            ]
            parts.append("【已完成任务】\n" + "\n".join(items))
        else:
            parts.append("【已完成任务】无")

        # 待处理
        if pend:
            items = [f"- [{r['priority']}] {r['description']}" for r in pend]
            parts.append("【待处理任务】\n" + "\n".join(items))
        else:
            parts.append("【待处理任务】无")

        return "\n".join(parts)

    # --------------------------------------------------------------
    # 清理
    # --------------------------------------------------------------
    async def clear_session(self, session_id: str) -> None:
        """删除某会话下的全部任务记录。"""
        conn = await self._ensure_async_conn()
        await conn.execute("DELETE FROM task_states WHERE session_id = ?", (session_id,))
        await conn.commit()

    # --------------------------------------------------------------
    # 工具
    # --------------------------------------------------------------
    @staticmethod
    def _row_to_dict(row: Any) -> Dict:
        if row is None:
            return {}
        return {
            "task_id": row["task_id"],
            "session_id": row["session_id"],
            "status": row["status"],
            "description": row["description"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "result_summary": row["result_summary"],
            "priority": row["priority"],
        }


# ======================================================================
# 全局单例
# ======================================================================
_store: Optional[StateStore] = None


def get_state_store() -> StateStore:
    global _store
    if _store is None:
        _store = StateStore()
    return _store
