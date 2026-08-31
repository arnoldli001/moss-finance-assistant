"""
Context Engineering 记忆管理模块。

三大核心策略：
1. 滑动窗口（Sliding Window）：默认保留最近 10 轮完整对话
2. 摘要压缩（Summarization Compression）：超 20 轮后，旧对话压缩成 3 段摘要
3. 优先级排序（Priority Ranking）：关键决策永久保留，闲聊优先丢弃

记忆数据独立持久化到 data/memory.db，与会话 (session_id = thread_id) 一一绑定。
"""
from __future__ import annotations

import re
import json
import asyncio
import sqlite3
import aiosqlite
import threading
import datetime
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os

# ===== 全局常量集中引用（替代魔鬼数字，统一修改一处即全局生效）=====
from config.constants import (
    MEMORY_WINDOW_KEEP_LAST_N,
    MEMORY_SUMMARY_TRIGGER_TURNS,
    MEMORY_SUMMARY_SEGMENTS,
    MEMORY_SUMMARY_MAX_CHARS_PER_SEG,
    MEMORY_KEY_DECISION_MAX_KEEP,
    MEMORY_RELEVANCE_THRESHOLD,
    MEMORY_MAX_RELEVANT_TURNS,
    MEMORY_CONTEXT_TOTAL_MAX_CHARS,
    MEMORY_KEY_DECISION_LINES_TOPK,
    MEMORY_KEY_DECISION_TEXT_MAX_CHARS,
    MEMORY_SUMMARY_SENTENCE_LIMIT_PER_SEG,
    MEMORY_SUMMARY_MIN_SENTENCE_CHARS,
    MEMORY_PRIORITY_BASE,
    MEMORY_PRIORITY_KEYWORD_HIT_BONUS_EACH,
    MEMORY_PRIORITY_KEYWORD_HIT_BONUS_CAP,
    MEMORY_PRIORITY_SMALLTALK_PENALTY,
    MEMORY_PRIORITY_SHORT_TEXT_PENALTY,
    MEMORY_PRIORITY_SHORT_TEXT_THRESHOLD_CHARS,
    MEMORY_SUMMARY_KEY_DECISION_SCORE_BONUS,
    MEMORY_SUMMARY_KEYWORD_REGEX_HIT_BONUS,
    MEMORY_RELEVANCE_CODE_MATCH_BONUS,
    TOKEN_ESTIMATE_CHINESE_COEF,
    TOKEN_ESTIMATE_OTHER_COEF,
)

# ===== CancellationToken: 跨层级取消联动检查点 =====
from agent.request_context import check_cancelled, current_token

# ======================================================================
# 配置项（已统一迁移到 config/constants.py，按 .env 环境变量覆盖后重新赋值）
# 保留变量名以兼容外部直接 from agent.memory_manager import WINDOW_KEEP_LAST_N 的使用方式
# ======================================================================
# 滑窗：最近 N 轮完整保留原始内容
WINDOW_KEEP_LAST_N = MEMORY_WINDOW_KEEP_LAST_N
# 超过多少轮触发摘要压缩
SUMMARY_TRIGGER_TURNS = MEMORY_SUMMARY_TRIGGER_TURNS
# 压缩后的摘要段数（早期 / 中期 / 近期）
SUMMARY_SEGMENTS = MEMORY_SUMMARY_SEGMENTS
# 单条摘要最大字符数
SUMMARY_MAX_CHARS_PER_SEG = MEMORY_SUMMARY_MAX_CHARS_PER_SEG
# 关键决策最大保留条数（防止无限膨胀）
KEY_DECISION_MAX_KEEP = MEMORY_KEY_DECISION_MAX_KEEP

# ======================================================================
# 启发式：优先级分类关键词
#   - 金融/投研场景下，命中以下关键词则提高优先级并标记为"关键决策"
#   - 纯闲聊类关键词则降低优先级，压缩时优先丢弃
# ======================================================================
_KEY_DECISION_PATTERNS = [
    # 股票/代码/指数
    r"\b\d{6}\b", r"\b[A-Z]{1,5}\.\w{2,4}\b",
    r"上证|深证|创业板|科创板|北证|沪深|恒生|纳斯达克|道琼斯|标普|标普500|S&P|NASDAQ|Dow",
    r"股票|个股|A股|港股|美股|股价|涨停|跌停|收盘|开盘|盘中",
    # 投资决策
    r"买入|卖出|建仓|清仓|加仓|减仓|止盈|止损|做多|做空|看多|看空|持有|观望",
    r"推荐|建议|评级|目标价|估值|PE|PB|EPS|ROE|ROI|收益率",
    r"仓位|配置|组合|风险|收益|波动|回撤|夏普|beta|alpha",
    # 宏观/政策
    r"央行|加息|降息|降准|放水|通胀|CPI|PPI|GDP|PMI|M2|社融|财政|货币政策",
    r"证监会|交易所|监管|政策|新规|处罚|立案|调查|退市|ST",
    # 公司/财报
    r"财报|年报|季报|中报|业绩|营收|净利润|同比|环比|预增|预减|亏损|盈利",
    r"并购|重组|收购|拆分|分拆|上市|IPO|定增|配股|分红|回购|增持|减持",
    # 用户明确标记的决策
    r"决定|结论|最终|确认|记住|重要|必记|存档|笔记",
]
_KEY_REGEX = re.compile("|".join(_KEY_DECISION_PATTERNS), re.IGNORECASE)

_SMALLTALK_PATTERNS = [
    r"^(你好|您好|hi|hello|hey|嗨|早|早上好|下午好|晚上好|晚安|谢谢|感谢|多谢|3q|ok|好的|明白|了解|收到|嗯|哦|啊|哈|哈哈|嘿嘿|嘻嘻|呢|吧|啦)$",
    r"^(再见|拜拜|bye|88|回见|下次聊|有空再聊|晚安|安)$",
    r"你是(谁|什么)|介绍一下|你能做什么|有什么功能",
    r"^(天气|时间|几点|日期|今天周几)",
]
_SMALLTALK_REGEX = re.compile("|".join(_SMALLTALK_PATTERNS), re.IGNORECASE)


# ======================================================================
# 数据结构
# ======================================================================
@dataclass
class MemoryTurn:
    """一轮对话记忆单元（一问一答为一轮）。"""
    session_id: str
    turn_index: int           # 自增轮次序号，从 1 开始
    user_content: str
    assistant_content: str
    created_at: str
    priority: int             # 0-100，越大越重要
    is_key_decision: bool     # 关键决策：压缩时尽量保留
    token_estimate: int       # 粗略 token 估算（中文按字符 × 1.5）


@dataclass
class MemorySummary:
    """一段摘要（覆盖一段历史区间）。"""
    session_id: str
    segment_index: int        # 0=早期, 1=中期, 2=近期
    content: str
    covered_from_turn: int    # 覆盖的起始 turn_index
    covered_to_turn: int      # 覆盖的结束 turn_index
    created_at: str
    updated_at: str


# ======================================================================
# 核心管理器
# ======================================================================
class MemoryManager:
    """
    Context Engineering 记忆管理器。
    
    使用方式：
        mm = MemoryManager()
        await mm.add_turn(session_id, user_text, assistant_text)
        context_str = await mm.build_prompt_context(session_id, current_query)
    """

    _instance_lock = threading.Lock()
    _instance: Optional["MemoryManager"] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return
        self._initialized = True
        project_root = Path(__file__).resolve().parents[1]
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = data_dir / "memory.db"
        self._sqlite_conn: Optional[aiosqlite.Connection] = None
        self._sync_conn: Optional[sqlite3.Connection] = None
        # ===== 并发隔离：按 session_id 的压缩锁，防止同一会话的压缩任务并发执行 =====
        # 竞态场景（修复前）：
        #   1) 用户A发消息1 → add_turn 触发 asyncio.create_task(_compress_summaries(A))
        #   2) 用户A立即发消息2 → add_turn 再次触发 asyncio.create_task(_compress_summaries(A))
        #   3) 两个压缩任务并发执行：
        #       task1: DELETE FROM memory_summaries WHERE session_id='A'
        #       task2: DELETE FROM memory_summaries WHERE session_id='A'（已空）
        #       task1: INSERT segment 1,2,3
        #       task2: INSERT segment 1,2,3（重复！）
        #       → memory_summaries 出现重复 segment，build_prompt_context 读到双倍内容
        # 修复：每个 session_id 一把 asyncio.Lock，压缩任务必须排队执行
        self._compress_locks: Dict[str, "asyncio.Lock"] = {}
        self._compress_locks_guard = asyncio.Lock()  # 保护 _compress_locks 字典本身
        # ===== 并发隔离：按 session_id 的写入锁，防止 add_turn 的 turn_index 读-改-写竞态 =====
        # 竞态场景（修复前）：
        #   1) 后台盘前小作文任务调用 add_turn(A, ...) → SELECT MAX(turn_index)+1 = 21
        #   2) 主聊天流程几乎同时调用 add_turn(A, ...) → SELECT MAX(turn_index)+1 = 21（A 尚未 INSERT）
        #   3) 后台任务 INSERT (A, 21) 成功
        #   4) 主聊天 INSERT (A, 21) → PRIMARY KEY 冲突 IntegrityError → 该轮对话丢失！
        #      即"后执行的旧请求覆盖新状态"：两个并发写入交叉，其中一个丢失更新。
        # 修复：每个 session_id 一把写入锁，turn_index 的 SELECT + INSERT 串行执行
        self._add_turn_locks: Dict[str, "asyncio.Lock"] = {}
        self._add_turn_locks_guard = asyncio.Lock()  # 保护 _add_turn_locks 字典本身
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
            CREATE TABLE IF NOT EXISTS memory_turns (
                session_id         TEXT NOT NULL,
                turn_index         INTEGER NOT NULL,
                user_content       TEXT NOT NULL,
                assistant_content  TEXT NOT NULL,
                created_at         TEXT NOT NULL,
                priority           INTEGER NOT NULL DEFAULT 50,
                is_key_decision    INTEGER NOT NULL DEFAULT 0,
                token_estimate     INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (session_id, turn_index)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_memory_turns_session
            ON memory_turns(session_id, turn_index DESC)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS memory_summaries (
                session_id          TEXT NOT NULL,
                segment_index       INTEGER NOT NULL,
                content             TEXT NOT NULL,
                covered_from_turn   INTEGER NOT NULL,
                covered_to_turn     INTEGER NOT NULL,
                created_at          TEXT NOT NULL,
                updated_at          TEXT NOT NULL,
                PRIMARY KEY (session_id, segment_index)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS memory_key_decisions (
                session_id         TEXT NOT NULL,
                decision_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                turn_index         INTEGER NOT NULL,
                content            TEXT NOT NULL,
                created_at         TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_key_decisions_session
            ON memory_key_decisions(session_id, decision_id DESC)
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
    def _estimate_tokens(text: str) -> int:
        """粗略估算：中文 1.5 token/字，英文 0.25 token/字符。"""
        if not text:
            return 0
        chinese_chars = len(re.findall(r"[\u4e00-\u9fff]", text))
        others = len(text) - chinese_chars
        return int(chinese_chars * TOKEN_ESTIMATE_CHINESE_COEF + others * TOKEN_ESTIMATE_OTHER_COEF)

    # --------------------------------------------------------------
    # 优先级分类（启发式）
    # --------------------------------------------------------------
    @classmethod
    def classify_priority(cls, user_content: str, assistant_content: str) -> Tuple[int, bool]:
        """
        根据关键词启发式分类优先级 + 是否关键决策。
        返回 (priority: 0-100, is_key_decision: bool)
        """
        text = f"{user_content or ''}\n{assistant_content or ''}"
        base = MEMORY_PRIORITY_BASE
        is_key = False

        # 关键决策命中
        key_hits = _KEY_REGEX.findall(text)
        if key_hits:
            base += min(len(key_hits) * MEMORY_PRIORITY_KEYWORD_HIT_BONUS_EACH,
                       MEMORY_PRIORITY_KEYWORD_HIT_BONUS_CAP)
            is_key = True

        # 闲聊命中 → 降权
        if _SMALLTALK_REGEX.search(user_content.strip()):
            base -= MEMORY_PRIORITY_SMALLTALK_PENALTY
            is_key = False

        # 长度惩罚：非常短的纯响应降权
        if len(text.strip()) < MEMORY_PRIORITY_SHORT_TEXT_THRESHOLD_CHARS and not is_key:
            base -= MEMORY_PRIORITY_SHORT_TEXT_PENALTY

        # 夹到 [0, 100]
        priority = max(0, min(100, base))
        return priority, is_key

    # --------------------------------------------------------------
    # 写：添加一轮对话
    # --------------------------------------------------------------
    async def add_turn(self, session_id: str, user_content: str, assistant_content: str) -> None:
        """
        在一轮交互完成后调用：存储这一轮的问答，维护摘要压缩。

        Args:
            session_id:  会话ID（与 LangGraph thread_id 相同）
            user_content: 用户本轮原始提问（不含【工作环境指令】）
            assistant_content: 助手本轮最终回答（不含 tool_calls）
        """
        if not session_id or not user_content:
            return
        # ===== [Cancellation Check] 写入记忆前检查（取消时跳过DB写入，防止半截内容写库）=====
        check_cancelled("memory.add_turn.entry")
        user_content = user_content or ""
        assistant_content = assistant_content or ""

        conn = await self._ensure_async_conn()
        # ===== 并发隔离：获取/创建当前 session 的写入锁 =====
        # 防止后台任务与主聊天流程同时为同一会话分配 turn_index，
        # 导致 PRIMARY KEY(session_id, turn_index) 冲突而丢失更新。
        async with self._add_turn_locks_guard:
            lock = self._add_turn_locks.get(session_id)
            if lock is None:
                lock = asyncio.Lock()
                self._add_turn_locks[session_id] = lock

        async with lock:
            # 1. 查询下一个 turn_index（在锁内：SELECT + INSERT 必须原子串行）
            cur = await conn.execute(
                "SELECT COALESCE(MAX(turn_index), 0) + 1 AS next_idx FROM memory_turns WHERE session_id = ?",
                (session_id,),
            )
            row = await cur.fetchone()
            turn_index = int(row["next_idx"])

            # 2. 分类
            priority, is_key = self.classify_priority(user_content, assistant_content)
            tokens = self._estimate_tokens(user_content + assistant_content)
            now = self._now()

            # 3. 插入 memory_turns
            await conn.execute(
                """INSERT INTO memory_turns
                   (session_id, turn_index, user_content, assistant_content, created_at,
                    priority, is_key_decision, token_estimate)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (session_id, turn_index, user_content, assistant_content, now,
                 priority, 1 if is_key else 0, tokens),
            )

            # 4. 关键决策单独存档（从助手中抽取关键句）
            if is_key:
                decision_text = self._extract_key_decision_text(user_content, assistant_content)
                await conn.execute(
                    """INSERT INTO memory_key_decisions (session_id, turn_index, content, created_at)
                       VALUES (?,?,?,?)""",
                    (session_id, turn_index, decision_text, now),
                )
                # 超量清理：保留最新的 KEY_DECISION_MAX_KEEP 条
                await conn.execute(
                    """DELETE FROM memory_key_decisions
                       WHERE session_id = ? AND decision_id NOT IN (
                           SELECT decision_id FROM memory_key_decisions
                           WHERE session_id = ? ORDER BY decision_id DESC LIMIT ?
                       )""",
                    (session_id, session_id, KEY_DECISION_MAX_KEEP),
                )

            await conn.commit()

            # 5. 检查是否触发摘要压缩（计数须在锁内读取，避免与并发 add_turn 交叉）
            total_row = await conn.execute(
                "SELECT COUNT(*) AS cnt FROM memory_turns WHERE session_id = ?",
                (session_id,),
            )
            total_row = await total_row.fetchone()
            total_turns = int(total_row["cnt"])

        # 锁外触发后台压缩（_compress_summaries 自带按 session 的锁，不会与本锁互锁死）
        if total_turns > SUMMARY_TRIGGER_TURNS:
            # 后台压缩（不阻塞当前调用）
            comp_task = asyncio.create_task(self._compress_summaries(session_id))
            # ===== CancellationToken: 登记为当前令牌的子任务（父取消 → 子压缩任务也取消）=====
            try:
                tok = current_token()
                if tok is not None:
                    tok.register_child_task(comp_task)
            except Exception:
                pass

    @staticmethod
    def _extract_key_decision_text(user: str, assistant: str) -> str:
        """从助手中抽取包含关键词的关键句，限制长度。"""
        text = assistant.strip() or user.strip()
        lines = [l.strip() for l in re.split(r"[。\n；;]", text) if l.strip()]
        key_lines = [l for l in lines if _KEY_REGEX.search(l)]
        chosen = key_lines or lines[:3]
        result = "；".join(chosen[:MEMORY_KEY_DECISION_LINES_TOPK])
        if len(result) > MEMORY_KEY_DECISION_TEXT_MAX_CHARS:
            result = result[:MEMORY_KEY_DECISION_TEXT_MAX_CHARS] + "…"
        return result

    # --------------------------------------------------------------
    # 关键词提取 + 相关度评分（用于过滤与当前问题无关的历史对话）
    # --------------------------------------------------------------
    @staticmethod
    def _extract_keywords(text: str) -> set:
        """从文本中提取关键词：股票代码、股票名、金融术语等。"""
        if not text:
            return set()
        keywords = set()
        # 1. 提取 6 位股票代码
        for m in re.finditer(r"(?<![A-Za-z0-9.])\d{6}(?![A-Za-z0-9.])", text):
            keywords.add(m.group())
        # 2. 提取带后缀的代码（600519.SH 等）
        for m in re.finditer(r"\d{4,6}\.(?:SH|SZ|HK|US)", text, re.IGNORECASE):
            keywords.add(m.group().upper())
        # 3. 提取金融关键词（2-4 字中文词组，简单 bigram 方式）
        # 去除标点和空白后，取连续中文字符的 2-gram
        chinese_chars = re.findall(r"[\u4e00-\u9fff]{2,}", text)
        for chunk in chinese_chars:
            # 整块作为一个关键词（如"贵州茅台"、"宁德时代"）
            if len(chunk) >= 2:
                keywords.add(chunk)
            # 同时取 bigram（如"茅台"、"宁德"）
            for i in range(len(chunk) - 1):
                keywords.add(chunk[i:i+2])
        # 4. 提取英文金融缩写（不用 \b，因中文字符不构成 word boundary）
        for m in re.finditer(r"(?<![A-Za-z])(PE|PB|ROE|EPS|GDP|CPI|PPI|PMI|M2|IPO|ETF)(?![A-Za-z])", text, re.IGNORECASE):
            keywords.add(m.group().upper())
        return keywords

    @classmethod
    def _compute_relevance(cls, query_text: str, history_text: str) -> float:
        """计算当前问题与历史对话的相关度（0.0-1.0）。

        基于 Jaccard 关键词重叠度 + 股票代码精确匹配加权。
        """
        if not query_text or not history_text:
            return 0.0
        q_kw = cls._extract_keywords(query_text)
        h_kw = cls._extract_keywords(history_text)
        if not q_kw or not h_kw:
            return 0.0
        overlap = q_kw & h_kw
        union = q_kw | h_kw
        jaccard = len(overlap) / len(union) if union else 0.0
        # 股票代码精确匹配加权（如果有共同代码，相关度大幅提升）
        q_codes = {k for k in q_kw if re.match(r"^\d{4,6}", k)}
        h_codes = {k for k in h_kw if re.match(r"^\d{4,6}", k)}
        code_match_bonus = MEMORY_RELEVANCE_CODE_MATCH_BONUS if (q_codes and h_codes and (q_codes & h_codes)) else 0.0
        score = min(1.0, jaccard + code_match_bonus)
        return score

    # --------------------------------------------------------------
    # 读：构建 prompt 上下文（记忆 → 字符串）
    # --------------------------------------------------------------
    async def build_prompt_context(self, session_id: str, current_query: str) -> str:
        """
        组装三段式记忆上下文字符串，可直接拼入 system prompt 或作为前置上下文。

        **相关性过滤**：只保留与当前问题相关度 >= RELEVANCE_THRESHOLD 的历史对话，
        跳过无关内容以减少 token 消耗。

        返回形如：
        ===== 历史摘要 =====
        [早期] ...
        [中期] ...
        [近期] ...
        ===== 关键决策记录 =====
        1. ...
        2. ...
        ===== 最近对话（滑窗）=====
        User: ...
        Assistant: ...
        """
        # 相关度阈值：低于此值的历史对话不放入 context
        RELEVANCE_THRESHOLD = MEMORY_RELEVANCE_THRESHOLD
        # 最多保留几条相关历史对话
        MAX_RELEVANT_TURNS = MEMORY_MAX_RELEVANT_TURNS

        conn = await self._ensure_async_conn()
        parts: List[str] = []

        # 1) 摘要段（摘要已压缩，保留全部，但可选择性过滤）
        summaries_cur = await conn.execute(
            """SELECT segment_index, content FROM memory_summaries
               WHERE session_id = ? ORDER BY segment_index ASC""",
            (session_id,),
        )
        summary_rows = await summaries_cur.fetchall()
        if summary_rows:
            seg_labels = ["早期摘要", "中期摘要", "近期摘要"]
            summary_parts = []
            for r in summary_rows:
                idx = int(r["segment_index"])
                label = seg_labels[idx] if idx < len(seg_labels) else f"摘要{idx+1}"
                # 摘要段也做相关度过滤：与当前问题完全无关的摘要段不放入
                seg_content = r['content']
                if current_query and seg_content:
                    rel = self._compute_relevance(current_query, seg_content)
                    if rel < RELEVANCE_THRESHOLD and len(summary_rows) > 1:
                        # 多段摘要时跳过低相关段；只有一段时保留（避免完全无上下文）
                        continue
                summary_parts.append(f"[{label}] {seg_content}")
            if summary_parts:
                parts.append("===== 历史摘要 =====" + "\n" + "\n".join(summary_parts))

        # 2) 关键决策 —— 按相关度过滤，只保留与当前问题相关的决策
        key_cur = await conn.execute(
            """SELECT content FROM memory_key_decisions
               WHERE session_id = ? ORDER BY decision_id DESC LIMIT ?""",
            (session_id, KEY_DECISION_MAX_KEEP),
        )
        key_rows = await key_cur.fetchall()
        if key_rows:
            key_parts = []
            for r in key_rows:
                decision_text = r['content']
                if current_query and decision_text:
                    rel = self._compute_relevance(current_query, decision_text)
                    if rel < RELEVANCE_THRESHOLD:
                        continue  # 与当前问题无关的决策跳过
                key_parts.append(f"{len(key_parts)+1}. {decision_text}")
            if key_parts:
                parts.append("===== 关键决策记录 =====" + "\n" + "\n".join(key_parts))

        # 3) 滑窗：最近 WINDOW_KEEP_LAST_N 轮完整对话 —— 按相关度过滤
        turns_cur = await conn.execute(
            """SELECT turn_index, user_content, assistant_content FROM memory_turns
               WHERE session_id = ? ORDER BY turn_index DESC LIMIT ?""",
            (session_id, WINDOW_KEEP_LAST_N),
        )
        turn_rows = await turns_cur.fetchall()
        # 反转回时间正序
        turn_rows = list(reversed(turn_rows))
        if turn_rows:
            dialog_parts = []
            relevant_count = 0
            skipped_count = 0
            for r in turn_rows:
                u = r["user_content"]
                a = r["assistant_content"]
                # 用 user_content 计算相关度（用户的问题是相关度的核心指标）
                history_text = f"{u}\n{a}"
                if current_query and u:
                    rel = self._compute_relevance(current_query, history_text)
                    if rel < RELEVANCE_THRESHOLD:
                        skipped_count += 1
                        continue  # 与当前问题无关的历史对话跳过
                if relevant_count >= MAX_RELEVANT_TURNS:
                    break  # 已达到相关对话上限
                if u:
                    dialog_parts.append(f"User: {u}")
                if a:
                    dialog_parts.append(f"Assistant: {a}")
                relevant_count += 1
            if dialog_parts:
                if skipped_count > 0:
                    parts.append(f"（已过滤 {skipped_count} 轮无关历史对话）")
                parts.append("===== 最近对话（滑窗）=====" + "\n" + "\n".join(dialog_parts))

        if not parts:
            return ""  # 无相关上下文，返回空字符串（不拼接任何记忆前缀）

        result = "\n\n".join(parts)
        # 防超长：总体上限
        if len(result) > MEMORY_CONTEXT_TOTAL_MAX_CHARS:
            result = result[:MEMORY_CONTEXT_TOTAL_MAX_CHARS] + "\n…[记忆上下文已截断]"
        return result

    # --------------------------------------------------------------
    # 摘要压缩算法
    # --------------------------------------------------------------
    async def _compress_summaries(self, session_id: str) -> None:
        """
        将超过 WINDOW_KEEP_LAST_N 的旧对话压缩成 SUMMARY_SEGMENTS 段摘要。

        策略（无 LLM 依赖，纯启发式抽取，性能稳定）：
        1. 把"将被滑出窗口"的历史 turn（即除了最近 WINDOW_KEEP_LAST_N 以外的）
           按时间均分成 SUMMARY_SEGMENTS 段
        2. 每段中按优先级（高→低）+ 是否关键决策排序，取 Top 句子
        3. 拼接成单段摘要，控制在 SUMMARY_MAX_CHARS_PER_SEG 字符内

        **并发隔离**：使用按 session_id 的 asyncio.Lock 确保同一会话的压缩任务串行执行，
        防止"DELETE + INSERT"被两个并发任务交叉执行导致重复 segment。
        """
        # ===== [Cancellation Check] 压缩任务入口（父任务已取消时，不要做无用压缩）=====
        check_cancelled("memory.compress.entry")
        # ===== 并发隔离：获取/创建当前 session 的压缩锁 =====
        async with self._compress_locks_guard:
            lock = self._compress_locks.get(session_id)
            if lock is None:
                lock = asyncio.Lock()
                self._compress_locks[session_id] = lock

        async with lock:
            try:
                conn = await self._ensure_async_conn()
                # 取出需要压缩的轮（除最近 WINDOW_KEEP_LAST_N 轮以外）
                cur = await conn.execute(
                    """SELECT turn_index, user_content, assistant_content,
                              priority, is_key_decision
                       FROM memory_turns
                       WHERE session_id = ?
                       ORDER BY turn_index ASC""",
                    (session_id,),
                )
                all_turns = list(await cur.fetchall())
                if len(all_turns) <= WINDOW_KEEP_LAST_N:
                    return

                to_compress = all_turns[:-WINDOW_KEEP_LAST_N]
                if not to_compress:
                    return

                # 均分成 SUMMARY_SEGMENTS 段
                seg_count = min(SUMMARY_SEGMENTS, len(to_compress))
                seg_size = len(to_compress) // seg_count
                segments = []
                for i in range(seg_count):
                    start = i * seg_size
                    end = (i + 1) * seg_size if i < seg_count - 1 else len(to_compress)
                    segments.append(to_compress[start:end])

                now = self._now()
                # 清除旧摘要后重新生成
                await conn.execute(
                    "DELETE FROM memory_summaries WHERE session_id = ?",
                    (session_id,),
                )

                for seg_idx, seg_turns in enumerate(segments):
                    covered_from = seg_turns[0]["turn_index"]
                    covered_to = seg_turns[-1]["turn_index"]
                    summary_text = self._build_segment_summary(seg_turns, SUMMARY_MAX_CHARS_PER_SEG)
                    if not summary_text:
                        continue
                    await conn.execute(
                        """INSERT INTO memory_summaries
                           (session_id, segment_index, content, covered_from_turn, covered_to_turn,
                            created_at, updated_at)
                           VALUES (?,?,?,?,?,?,?)""",
                        (session_id, seg_idx, summary_text,
                         int(covered_from), int(covered_to), now, now),
                    )
                await conn.commit()
                print(f"[MemoryManager] 会话 {session_id} 摘要压缩完成：{len(segments)} 段，"
                      f"覆盖 turn {segments[0][0]['turn_index']} ~ {segments[-1][-1]['turn_index']}")
            except Exception as e:
                print(f"[MemoryManager] 摘要压缩失败（不致命，跳过）: {e}")

    @classmethod
    def _build_segment_summary(cls, turns: list, max_chars: int) -> str:
        """将一段 turns 压缩成摘要文字：按优先级取 Top 句子拼接。"""
        scored_sentences: List[Tuple[int, str]] = []
        for t in turns:
            prio = int(t["priority"])
            is_key = bool(t["is_key_decision"])
            # 关键决策额外加权
            score = prio + (MEMORY_SUMMARY_KEY_DECISION_SCORE_BONUS if is_key else 0)
            texts = [t["user_content"], t["assistant_content"]]
            for txt in texts:
                if not txt:
                    continue
                # 切成句子（按句号/换行/分号）
                sents = [s.strip() for s in re.split(r"[。\n；;!?！？]", txt) if s.strip()]
                for s in sents:
                    if len(s) < MEMORY_SUMMARY_MIN_SENTENCE_CHARS:
                        continue
                    # 命中关键词额外加分
                    extra = MEMORY_SUMMARY_KEYWORD_REGEX_HIT_BONUS if _KEY_REGEX.search(s) else 0
                    scored_sentences.append((score + extra, s))

        # 分数降序
        scored_sentences.sort(key=lambda x: x[0], reverse=True)
        result_parts: List[str] = []
        total = 0
        for _, s in scored_sentences:
            s_len = len(s)
            if total + s_len + 3 > max_chars:
                continue
            result_parts.append(s)
            total += s_len + 1
            if len(result_parts) >= MEMORY_SUMMARY_SENTENCE_LIMIT_PER_SEG:
                break
        if not result_parts:
            # 降级：取第一条非空 assistant 文本
            for t in turns:
                if t["assistant_content"]:
                    c = t["assistant_content"][:max_chars]
                    return c if len(c) < len(t["assistant_content"]) else c + "…"
            return ""
        # 结果按出现顺序重排（保留时间逻辑）——用句子的原顺序
        order_map = {}
        for i, (_, s) in enumerate(scored_sentences):
            if s not in order_map:
                order_map[s] = i
        ordered = sorted(result_parts, key=lambda s: order_map.get(s, 999))
        joined = "；".join(ordered)
        if len(joined) > max_chars:
            joined = joined[:max_chars] + "…"
        return joined

    # --------------------------------------------------------------
    # 工具：导出/清空（调试与管理用）
    # --------------------------------------------------------------
    async def get_stats(self, session_id: str) -> Dict:
        conn = await self._ensure_async_conn()
        turn_cnt = await conn.execute(
            "SELECT COUNT(*) AS c FROM memory_turns WHERE session_id = ?",
            (session_id,),
        )
        turn_cnt = await turn_cnt.fetchone()
        key_cnt = await conn.execute(
            "SELECT COUNT(*) AS c FROM memory_key_decisions WHERE session_id = ?",
            (session_id,),
        )
        key_cnt = await key_cnt.fetchone()
        sum_cnt = await conn.execute(
            "SELECT COUNT(*) AS c FROM memory_summaries WHERE session_id = ?",
            (session_id,),
        )
        sum_cnt = await sum_cnt.fetchone()
        return {
            "session_id": session_id,
            "turn_count": int(turn_cnt["c"]),
            "key_decision_count": int(key_cnt["c"]),
            "summary_segment_count": int(sum_cnt["c"]),
            "window_size": WINDOW_KEEP_LAST_N,
            "summary_trigger_turns": SUMMARY_TRIGGER_TURNS,
        }

    async def clear_session(self, session_id: str) -> None:
        conn = await self._ensure_async_conn()
        await conn.execute("DELETE FROM memory_turns WHERE session_id = ?", (session_id,))
        await conn.execute("DELETE FROM memory_summaries WHERE session_id = ?", (session_id,))
        await conn.execute("DELETE FROM memory_key_decisions WHERE session_id = ?", (session_id,))
        await conn.commit()

    # --------------------------------------------------------------
    # 写：删除单轮对话（右键删除消息）
    # --------------------------------------------------------------
    async def delete_turn(self, session_id: str, turn_index: int) -> bool:
        """
        删除指定会话的某一轮对话（一问一答对应一轮 turn_index）。

        语义：
          - turn_index 必须 > 0 且该 session 存在该 turn；否则返回 False。
          - 删除后，turn_index 之后的所有 turn_index 统一减 1，保持索引连续（无间隙），
            避免后续 add_turn 的 MAX(turn_index)+1 出现间隙、与 summary 的 covered_turn
            范围语义不一致。
          - 同步删除 memory_key_decisions 中该 turn 的关键决策记录。
          - 删除后，若存在 summaries 且覆盖区间涉及被删 turn/后段 turns，
            直接清空 summaries（下次 add_turn 或 _compress_summaries 会重新生成）。
          - 全程在 session 写入锁下执行，防止与并发 add_turn 交叉。
        返回 True 表示成功删除，False 表示没找到。
        """
        if not session_id or turn_index <= 0:
            return False
        check_cancelled("memory.delete_turn.entry")

        conn = await self._ensure_async_conn()

        # 复用 add_turn 的 session 级写入锁：确保与 add_turn 的读-改-写不交叉
        async with self._add_turn_locks_guard:
            lock = self._add_turn_locks.get(session_id)
            if lock is None:
                lock = asyncio.Lock()
                self._add_turn_locks[session_id] = lock

        async with lock:
            # 1. 检查目标 turn 是否存在
            cur = await conn.execute(
                "SELECT 1 FROM memory_turns WHERE session_id = ? AND turn_index = ?",
                (session_id, turn_index),
            )
            exists = await cur.fetchone()
            if not exists:
                return False

            now = self._now()

            # 2. 删除目标 turn + 对应的 key_decision 记录
            await conn.execute(
                "DELETE FROM memory_turns WHERE session_id = ? AND turn_index = ?",
                (session_id, turn_index),
            )
            await conn.execute(
                "DELETE FROM memory_key_decisions WHERE session_id = ? AND turn_index = ?",
                (session_id, turn_index),
            )

            # 3. 后段 turns 的 turn_index 统一 -1，保持索引连续
            #    （对 covered_turn 语义最重要，否则下次 add_turn 会把 turn_index 填回空位）
            cur_max = await conn.execute(
                "SELECT COALESCE(MAX(turn_index), 0) AS m FROM memory_turns WHERE session_id = ?",
                (session_id,),
            )
            max_idx = int((await cur_max.fetchone())["m"])
            if max_idx >= turn_index:
                # 受影响区间 [turn_index + 1, max_idx] → 各自 -1
                # SQLite 允许 UPDATE ... SET turn_index = turn_index - 1 WHERE ...
                # 但为了兼容主键冲突的顺序，我们按升序逐条更新（从最小的开始减）
                for idx in range(turn_index + 1, max_idx + 1):
                    # 先临时改成负/大数以避免 PK 冲突，再写回 -1 的目标值
                    # 但这里是连续递推，每个 idx 的目标 = idx-1，而 idx-1 已经处理完毕，
                    # 所以直接 UPDATE WHERE = idx → idx-1 是安全的（idx-1 行已被处理）
                    await conn.execute(
                        "UPDATE memory_turns SET turn_index = turn_index - 1 "
                        "WHERE session_id = ? AND turn_index = ?",
                        (session_id, idx),
                    )
                # 同步 shift key_decisions
                for idx in range(turn_index + 1, max_idx + 1):
                    await conn.execute(
                        "UPDATE memory_key_decisions SET turn_index = turn_index - 1 "
                        "WHERE session_id = ? AND turn_index = ?",
                        (session_id, idx),
                    )

            # 4. 摘要重建策略：只要 summary 的 covered_turn 区间包含被删段（>= turn_index）
            #    就直接清空，避免 stale。下一轮 add_turn / 手动 compress 会重算。
            sm_cur = await conn.execute(
                "SELECT COUNT(*) AS c FROM memory_summaries "
                "WHERE session_id = ? AND covered_to_turn >= ?",
                (session_id, turn_index),
            )
            stale_sums = int((await sm_cur.fetchone())["c"])
            if stale_sums > 0:
                # 简单起见，清空当前 session 的所有摘要（不会丢 turns，下轮压缩自动重建）
                await conn.execute(
                    "DELETE FROM memory_summaries WHERE session_id = ?", (session_id,),
                )

            await conn.commit()
            print(f"[MemoryManager] 会话 {session_id} 删除第 {turn_index} 轮完成；"
                  f"已将后续 turn_index({turn_index+1}..{max_idx}) 统一前移 1；"
                  f"清除了 {stale_sums} 段过期摘要")
            return True

    # --------------------------------------------------------------
    # 写：删除单条消息（拆分删除：只删 user 或只删 assistant）
    # --------------------------------------------------------------
    async def delete_message(self, session_id: str, turn_index: int, role: str) -> str:
        """
        删除指定会话某一轮中的单条消息（user 或 assistant），保留另一条。

        Args:
            session_id: 会话ID
            turn_index: 轮次序号（1-based）
            role: "user" 或 "assistant"

        返回值：
          - "ok"           : 单条删除成功，另一条仍在
          - "row_removed"  : 删除后另一条也已为空，整行被删除并前移后续 turn_index
          - "not_found"    : 未找到目标 turn 或 role 非法
        """
        if not session_id or turn_index <= 0:
            return "not_found"
        if role not in ("user", "assistant"):
            return "not_found"
        check_cancelled("memory.delete_message.entry")

        conn = await self._ensure_async_conn()

        # 复用 session 写入锁，与 add_turn / delete_turn 互斥
        async with self._add_turn_locks_guard:
            lock = self._add_turn_locks.get(session_id)
            if lock is None:
                lock = asyncio.Lock()
                self._add_turn_locks[session_id] = lock

        async with lock:
            # 1. 检查目标 turn 是否存在
            cur = await conn.execute(
                "SELECT user_content, assistant_content FROM memory_turns "
                "WHERE session_id = ? AND turn_index = ?",
                (session_id, turn_index),
            )
            row = await cur.fetchone()
            if not row:
                return "not_found"

            # 2. 把目标字段置空
            field = "user_content" if role == "user" else "assistant_content"
            await conn.execute(
                f"UPDATE memory_turns SET {field} = '' "
                f"WHERE session_id = ? AND turn_index = ?",
                (session_id, turn_index),
            )

            # 3. 检查另一条是否也空：若两条都空，整行删除并前移后续 turn_index
            #    重新读取检查
            cur2 = await conn.execute(
                "SELECT user_content, assistant_content FROM memory_turns "
                "WHERE session_id = ? AND turn_index = ?",
                (session_id, turn_index),
            )
            row2 = await cur2.fetchone()
            u_empty = not (row2["user_content"] or "").strip()
            a_empty = not (row2["assistant_content"] or "").strip()

            if u_empty and a_empty:
                # 两条都空：删整行 + 前移后续 turn_index
                await conn.execute(
                    "DELETE FROM memory_turns WHERE session_id = ? AND turn_index = ?",
                    (session_id, turn_index),
                )
                await conn.execute(
                    "DELETE FROM memory_key_decisions WHERE session_id = ? AND turn_index = ?",
                    (session_id, turn_index),
                )
                # 前移后段
                cur_max = await conn.execute(
                    "SELECT COALESCE(MAX(turn_index), 0) AS m FROM memory_turns WHERE session_id = ?",
                    (session_id,),
                )
                max_idx = int((await cur_max.fetchone())["m"])
                for idx in range(turn_index + 1, max_idx + 1):
                    await conn.execute(
                        "UPDATE memory_turns SET turn_index = turn_index - 1 "
                        "WHERE session_id = ? AND turn_index = ?",
                        (session_id, idx),
                    )
                    await conn.execute(
                        "UPDATE memory_key_decisions SET turn_index = turn_index - 1 "
                        "WHERE session_id = ? AND turn_index = ?",
                        (session_id, idx),
                    )
                # 清理过期摘要
                sm_cur = await conn.execute(
                    "SELECT COUNT(*) AS c FROM memory_summaries "
                    "WHERE session_id = ? AND covered_to_turn >= ?",
                    (session_id, turn_index),
                )
                stale_sums = int((await sm_cur.fetchone())["c"])
                if stale_sums > 0:
                    await conn.execute(
                        "DELETE FROM memory_summaries WHERE session_id = ?", (session_id,),
                    )
                await conn.commit()
                print(f"[MemoryManager] 会话 {session_id} 第 {turn_index} 轮 {role} 删除后两条都空，整行删除+前移")
                return "row_removed"

            await conn.commit()
            print(f"[MemoryManager] 会话 {session_id} 第 {turn_index} 轮 {role} 消息已删除（另一条保留）")
            return "ok"

    # --------------------------------------------------------------
    # 写：批量删除多条消息
    # --------------------------------------------------------------
    async def batch_delete_messages(
        self, session_id: str, items: list,
    ) -> dict:
        if not session_id or not items:
            return {"total": 0, "ok": 0, "row_removed": 0, "not_found": 0, "details": []}

        # 按 turn_index 升序处理（从小到大删，避免前移后索引错乱）
        # row_removed 时后续 turn_index会前移，所以批量删同一会话时按turn_index降序处理更安全（先删大的，前移不影响小的）
        sorted_items = sorted(items, key=lambda x: -x["turn_index"])

        results = []
        ok_count = 0
        row_removed_count = 0
        not_found_count = 0

        for item in sorted_items:
            ti = int(item.get("turn_index", 0))
            role = item.get("role", "all")
            if role == "all":
                # 整轮删除
                r = await self.delete_turn(session_id, ti)
                if r:
                    ok_count += 1
                    row_removed_count += 1
                    results.append({"turn_index": ti, "role": role, "result": "row_removed"})
                else:
                    not_found_count += 1
                    results.append({"turn_index": ti, "role": role, "result": "not_found"})
            else:
                r = await self.delete_message(session_id, ti, role)
                if r == "ok":
                    ok_count += 1
                    results.append({"turn_index": ti, "role": role, "result": "ok"})
                elif r == "row_removed":
                    ok_count += 1
                    row_removed_count += 1
                    results.append({"turn_index": ti, "role": role, "result": "row_removed"})
                else:
                    not_found_count += 1
                    results.append({"turn_index": ti, "role": role, "result": "not_found"})

        return {
            "total": len(items),
            "ok": ok_count,
            "row_removed": row_removed_count,
            "not_found": not_found_count,
            "details": results,
        }

# 全局单例
_manager: Optional[MemoryManager] = None

def get_memory_manager() -> MemoryManager:
    global _manager
    if _manager is None:
        _manager = MemoryManager()
    return _manager
