"""
Layer 3 Harness Engineering —— 用户质疑 / 反驳处理 + 错误学习模块。

当用户对 Agent 上一轮结果提出质疑或反驳时：
1. detect_challenge: 检测用户消息是否包含质疑关键词
2. extract_correction: 从质疑消息中剥离情绪词，提取事实性纠正信息
3. learn_error: 把"出错查询 + 纠正信息"作为错误模式持久化到 SQLite
4. check_known_errors / build_error_avoidance_prompt: 后续相似查询时主动规避已知错误
5. build_retry_context: 组装带用户纠正信息的重搜上下文，供 Agent 重新检索分析

错误学习数据独立持久化到 data/feedback.db，与会话（session_id = thread_id）一一绑定。
"""
from __future__ import annotations

import re
import json
import sqlite3
import aiosqlite
import asyncio
import datetime
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv, find_dotenv

# 运行时提示词模板访问器（错误规避/重搜上下文提示词抽取到 prompts.yml）
from agent.prompts import format_prompt
load_dotenv(find_dotenv())

# ======================================================================
# 质疑 / 反驳检测模式
# ======================================================================
_CHALLENGE_PATTERNS = [
    r"不对",
    r"错误",
    r"质疑",
    r"反驳",
    r"不准确",
    r"不同意",
    r"数据有误",
    r"结果有误",
    r"你说的",
    r"不对吧",
    r"不是这样",
    r"查错了",
    r"信息过时",
    r"漏了",
]
_CHALLENGE_REGEX = re.compile("|".join(_CHALLENGE_PATTERNS), re.IGNORECASE)


# ======================================================================
# 数据结构
# ======================================================================
@dataclass
class ErrorLearning:
    """一条错误学习记录：记录曾经出错的查询及用户纠正。"""
    session_id: str
    error_pattern: str          # 出错查询的模式（用 original_query 充当，用于后续匹配）
    original_query: str         # 当时出错时的原始查询
    corrected_info: str         # 用户给出的纠正信息
    error_type: str             # 错误类型：factual / stale / missing / other
    created_at: str             # ISO 时间戳


# ======================================================================
# 核心处理器
# ======================================================================
class FeedbackHandler:
    """
    用户质疑处理 + 错误学习器，单例。

    使用方式：
        fh = get_feedback_handler()
        if fh.detect_challenge(user_msg):
            correction = fh.extract_correction(user_msg)
            await fh.learn_error(session_id, last_query, correction, "factual")
            retry_ctx = await fh.build_retry_context(session_id, last_query, user_msg)
            # 把 retry_ctx 拼到重搜 prompt 中
        # 任意查询前注入规避提示
        prefix = await fh.build_error_avoidance_prompt(session_id, current_query)
    """

    def __init__(self):
        project_root = Path(__file__).resolve().parents[1]
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = data_dir / "feedback.db"
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
            CREATE TABLE IF NOT EXISTS error_learning (
                session_id       TEXT NOT NULL,
                error_pattern    TEXT NOT NULL,
                original_query   TEXT NOT NULL,
                corrected_info   TEXT NOT NULL,
                error_type       TEXT NOT NULL DEFAULT 'factual',
                created_at       TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_error_learning_session
            ON error_learning(session_id, created_at DESC)
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

    # --------------------------------------------------------------
    # 质疑检测 & 纠正信息提取
    # --------------------------------------------------------------
    def detect_challenge(self, user_message: str) -> bool:
        """检查用户消息是否包含质疑 / 反驳关键词。"""
        if not user_message:
            return False
        return _CHALLENGE_REGEX.search(user_message) is not None

    def extract_correction(self, user_message: str) -> str:
        """
        从用户质疑消息中剥离质疑关键词，保留事实性纠正信息。

        策略：
        1. 切句（按标点 / 换行）
        2. 丢弃整句都是质疑关键词的句子（如"你说的不对"）
        3. 其余句子去掉残留的质疑词，拼接为纠正信息
        """
        if not user_message:
            return ""
        # 切句
        sentences = [s.strip() for s in re.split(r"[。！!？?\n；;]", user_message) if s.strip()]
        kept: List[str] = []
        for sent in sentences:
            # 整句都是质疑词（去掉质疑词后几乎没有实质内容）则丢弃
            stripped = _CHALLENGE_REGEX.sub("", sent).strip(" ，,。.！!？?的了吗呢吧啊呀")
            if not stripped or len(stripped) < 2:
                continue
            # 去掉句中残留质疑词，保留事实部分，并清理首尾残留标点
            cleaned = _CHALLENGE_REGEX.sub("", sent).strip(" ，,。.！!？?；;的了吗呢吧啊呀")
            if cleaned:
                kept.append(cleaned)
        # 若所有句子都被判定为纯质疑，则回退返回原文（避免误丢用户信息）
        if not kept:
            return user_message.strip()
        return "；".join(kept)

    # --------------------------------------------------------------
    # 写：错误学习
    # --------------------------------------------------------------
    async def learn_error(
        self,
        session_id: str,
        original_query: str,
        user_correction: str,
        error_type: str = "factual",
    ) -> None:
        """
        存储一条错误学习记录，供后续相似查询规避。

        Args:
            session_id:      会话 ID
            original_query:  当时出错的原始查询
            user_correction: 用户给出的纠正信息
            error_type:      错误类型（factual / stale / missing / other）
        """
        if not session_id or not original_query:
            return
        user_correction = user_correction or ""
        # error_pattern 直接用原始查询充当：后续用关键词重叠匹配相似查询
        error_pattern = original_query.strip()
        now = self._now()
        conn = await self._ensure_async_conn()
        await conn.execute(
            """INSERT INTO error_learning
               (session_id, error_pattern, original_query, corrected_info, error_type, created_at)
               VALUES (?,?,?,?,?,?)""",
            (session_id, error_pattern, original_query.strip(),
             user_correction.strip(), error_type or "factual", now),
        )
        await conn.commit()

    # --------------------------------------------------------------
    # 读：错误模式检索
    # --------------------------------------------------------------
    async def get_error_patterns(self, session_id: str) -> List[Dict]:
        """检索某会话的全部错误学习记录，按时间倒序返回。"""
        conn = await self._ensure_async_conn()
        cur = await conn.execute(
            """SELECT session_id, error_pattern, original_query, corrected_info,
                      error_type, created_at
               FROM error_learning
               WHERE session_id = ?
               ORDER BY created_at DESC""",
            (session_id,),
        )
        rows = await cur.fetchall()
        return [
            {
                "session_id": r["session_id"],
                "error_pattern": r["error_pattern"],
                "original_query": r["original_query"],
                "corrected_info": r["corrected_info"],
                "error_type": r["error_type"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    async def check_known_errors(self, session_id: str, current_query: str) -> List[Dict]:
        """
        检查当前查询是否命中任何已知错误模式，返回匹配的错误记录列表。

        匹配策略（关键词重叠）：
        - 从 current_query 与 error_pattern 中各抽取长度 ≥ 2 的关键词
        - 若重叠关键词数 ≥ 1（且覆盖率 ≥ 30%），视为命中
        """
        if not current_query:
            return []
        known = await self.get_error_patterns(session_id)
        if not known:
            return []

        query_keywords = self._extract_keywords(current_query)
        if not query_keywords:
            return []

        matched: List[Dict] = []
        for rec in known:
            pattern_keywords = self._extract_keywords(rec["error_pattern"])
            if not pattern_keywords:
                continue
            overlap = query_keywords & pattern_keywords
            if not overlap:
                continue
            # 覆盖率：重叠词占 pattern 关键词的比例
            coverage = len(overlap) / len(pattern_keywords)
            if coverage >= 0.3:
                matched.append(rec)
        return matched

    @staticmethod
    def _extract_keywords(text: str) -> set:
        """从文本中抽取长度 ≥ 2 的关键词（中文按 2-gram，英文按单词）。"""
        if not text:
            return set()
        kws: set = set()
        # 英文 / 数字单词
        for w in re.findall(r"[A-Za-z0-9]+", text):
            if len(w) >= 2:
                kws.add(w.lower())
        # 中文 2-gram
        chinese = re.findall(r"[\u4e00-\u9fff]+", text)
        for seg in chinese:
            for i in range(len(seg) - 1):
                kws.add(seg[i:i + 2])
            # 单字段也保留（长度恰为 1 的中文段）
            if len(seg) == 1:
                kws.add(seg)
        return kws

    # --------------------------------------------------------------
    # 读：构建规避 / 重搜提示
    # --------------------------------------------------------------
    async def build_error_avoidance_prompt(self, session_id: str, current_query: str) -> str:
        """
        构建错误规避提示前缀，提醒 Agent 在相似查询中避免重复已知错误。

        格式：
            【已知错误规避】
            1. 之前在查询xxx时出错：xxx，用户纠正为：xxx，请避免重复此错误
            2. ...
        """
        matched = await self.check_known_errors(session_id, current_query)
        if not matched:
            return ""
        lines = ["【已知错误规避】"]
        for i, rec in enumerate(matched, start=1):
            # 每条错误规避提示从 prompts.yml runtime_prompts 段加载模板
            lines.append(format_prompt(
                "feedback_handler.error_avoidance_line",
                i=i,
                original_query=rec['original_query'],
                error_pattern=rec['error_pattern'],
                corrected_info=rec['corrected_info'],
            ))
        return "\n".join(lines)

    async def build_retry_context(
        self,
        session_id: str,
        original_query: str,
        user_feedback: str,
    ) -> str:
        """
        构建重搜上下文字符串，供 Agent 基于用户纠正信息重新检索分析。

        格式：
            用户对之前的结果提出质疑，补充信息如下：{correction}。
            请基于这些新信息重新搜索和分析。之前可能出错的点：{known_errors}
        """
        correction = self.extract_correction(user_feedback)
        # 学习本轮错误，便于后续规避
        await self.learn_error(session_id, original_query, correction, "factual")
        # 检索与本次查询相关的已知错误
        matched = await self.check_known_errors(session_id, original_query)
        if matched:
            known_errors = "；".join(
                f"查询「{r['original_query']}」→ 正确应为「{r['corrected_info']}」"
                for r in matched
            )
        else:
            known_errors = "无"
        # 重搜上下文提示词从 prompts.yml runtime_prompts 段加载模板
        return format_prompt(
            "feedback_handler.retry_context",
            correction=correction,
            known_errors=known_errors,
        )

    # --------------------------------------------------------------
    # 清理
    # --------------------------------------------------------------
    async def clear_session(self, session_id: str) -> None:
        """删除某会话的全部错误学习记录。"""
        conn = await self._ensure_async_conn()
        await conn.execute(
            "DELETE FROM error_learning WHERE session_id = ?",
            (session_id,),
        )
        await conn.commit()


# ======================================================================
# 全局单例
# ======================================================================
_handler: Optional[FeedbackHandler] = None


def get_feedback_handler() -> FeedbackHandler:
    """获取 FeedbackHandler 全局单例。"""
    global _handler
    if _handler is None:
        _handler = FeedbackHandler()
    return _handler
