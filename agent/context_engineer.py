"""
Context Engineering Layer 2 —— 金融资讯上下文工程模块。

在 Layer 1（MemoryManager 对话记忆管理）之上，针对"检索得到的金融资讯"
做上下文工程处理。四大核心策略：

1. 基于时间的去重：相似资讯仅保留最近 2 条并比对，情感冲突则标记需 DeepSeek 核验
2. 来源可靠性评估：官方公告/交易所/监管机构/权威媒体 = reliable；其余（论坛/博客/自媒体）= unreliable
3. 2000 字符阈值裁剪：超出阈值时按查询相关度排序，裁剪低相关内容
4. 动态上下文加载：依据查询类型（个股/宏观/基本面/通用）动态决定加载哪些上下文，
   而非静态配置——查询类型驱动相关度评分，评分驱动裁剪与加载顺序

使用方式：
    ce = get_context_engineer()
    context_str = ce.build_context(raw_search_results, user_query)
"""
from __future__ import annotations

import re
import json
import datetime
import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Set

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ===== 全局常量集中引用（替代魔鬼数字，统一修改一处即全局生效）=====
from config.constants import (
    CE_TOTAL_MAX_CHARS,
    CE_IRRELEVANT_DROP_THRESHOLD_CHARS,
    CE_NEWS_SNIPPET_MAX_CHARS,
    CE_ITEM_MAX_CHARS,
    CE_TAIL_KEEP_LAST_CHARS,
    CE_TIME_DECAY_HALF_LIFE_DAYS,
    CE_OVERHEAD_RESERVE_CHARS,
)

# ======================================================================
# 配置项（可通过 .env 覆盖）
# ======================================================================
CONTEXT_MAX_CHARS = CE_TOTAL_MAX_CHARS
IRRELEVANT_DROP_THRESHOLD_CHARS = CE_IRRELEVANT_DROP_THRESHOLD_CHARS
NEWS_SNIPPET_MAX_CHARS = CE_NEWS_SNIPPET_MAX_CHARS
ITEM_MAX_CHARS = CE_ITEM_MAX_CHARS
TAIL_KEEP_LAST_CHARS = CE_TAIL_KEEP_LAST_CHARS
# 去重时每组相似资讯保留的最近条目数（规格要求保留 2 条用于比对）
DEDUP_KEEP_RECENT = 2
# 关键词 Jaccard 相似度阈值，超过则视为相似资讯归为同组
DEDUP_SIMILARITY_THRESHOLD = 0.4
# 情感冲突时打在内容首部的核验标记
_VERIFY_PREFIX = "[需DeepSeek核验]"

# ======================================================================
# 来源可靠性正则：官方公告 / 交易所 / 监管机构 / 权威媒体
#   - 命中即视为 reliable；未命中一律视为 unreliable（金融信息保守策略）
# ======================================================================
_RELIABLE_SOURCE_REGEX = re.compile(
    r"sse\.com\.cn|szse\.cn|sse\.com|szse\.com|hkex\.com\.hk|hkex\.com|sec\.gov|"
    r"csrc\.gov\.cn|pbc\.gov\.cn|ndrc\.gov\.cn|"
    r"reuters|bloomberg|xinhua(?:net)?|cnstock|stcn\.com|stcn|yicai|21jingji|caixin|wallstreetcn|"
    r"上海证券交易所|深圳证券交易所|上交所|深交所|香港交易所|港交所|"
    r"证监会|央行|中国人民银行|银保监会|国家发改委|发改委|"
    r"路透|彭博|新华社|新华网|中国证券报|证券时报|第一财经|21世纪经济报道|财新|华尔街见闻",
    re.IGNORECASE,
)

# ======================================================================
# 股票代码正则：A股6位 / HK·US·SH·SZ 后缀格式
#   注意：用 ASCII 限定 lookaround 替代 \b——Python 中 \b 视中文字符为单词字符，
#   会导致"茅台600519"这类紧贴中文的代码无法匹配。
# ======================================================================
_STOCK_CODE_REGEX = re.compile(
    r"(?<![A-Za-z0-9.])\d{6}\.(?:SH|SZ)(?![A-Za-z0-9.])"
    r"|(?<![A-Za-z0-9.])\d{4,5}\.HK(?![A-Za-z0-9.])"
    r"|(?<![A-Za-z0-9.])[A-Z]{1,5}\.US(?![A-Za-z0-9.])"
    r"|(?<![A-Za-z0-9.])\d{6}(?![A-Za-z0-9.])",
    re.IGNORECASE,
)

# ======================================================================
# 情感关键词（用于冲突检测，简单关键词匹配）
# ======================================================================
_POSITIVE_KEYWORDS: Set[str] = {
    "利好", "利多", "上涨", "涨停", "大涨", "飙升", "走强", "看多", "做多",
    "买入", "增持", "预增", "盈利", "突破", "反弹", "回暖", "走高", "创新高", "放量上涨",
}
_NEGATIVE_KEYWORDS: Set[str] = {
    "利空", "利淡", "下跌", "跌停", "大跌", "暴跌", "走弱", "看空", "做空",
    "卖出", "减持", "预减", "亏损", "跌破", "新低", "重挫", "下挫", "放量下跌", "闪崩",
}

# ======================================================================
# 查询类型识别（驱动动态上下文加载）
# ======================================================================
_QUERY_TYPE_PATTERNS: Dict[str, "re.Pattern[str]"] = {
    "stock": re.compile(
        r"(?<![A-Za-z0-9.])\d{6}(?:\.(?:SH|SZ))?(?![A-Za-z0-9.])"
        r"|(?<![A-Za-z0-9.])\d{4,5}\.HK(?![A-Za-z0-9.])"
        r"|(?<![A-Za-z0-9.])[A-Z]{1,5}\.US(?![A-Za-z0-9.])"
        r"|个股|股票|股价|持仓|A股|港股|美股|涨停|跌停|买入|卖出|建仓|清仓|目标价|评级",
        re.IGNORECASE,
    ),
    "macro": re.compile(
        r"大盘|指数|沪深|政策|央行|加息|降息|降准|通胀|CPI|PPI|GDP|PMI|M2|社融|财政|货币政策|宏观经济",
        re.IGNORECASE,
    ),
    "fundamentals": re.compile(
        r"财报|年报|季报|中报|业绩|营收|净利润|同比|环比|预增|预减|亏损|盈利|估值|PE|PB|EPS|ROE|毛利率",
        re.IGNORECASE,
    ),
}

# ======================================================================
# 分词停用词（中文单字，用于过滤无信息量的 bigram）
# ======================================================================
_STOPWORDS: Set[str] = {
    "的", "了", "是", "在", "和", "与", "对", "等", "也", "都", "就", "这", "那",
    "一", "个", "们", "中", "上", "下", "不", "为", "有", "由", "及", "以", "或",
    "并", "之", "其", "而", "则", "可", "要", "会", "能", "到", "地", "着", "过",
    "被", "把", "让", "向", "从", "据", "称", "说", "日", "年", "月",
}

# 时间戳解析候选格式
_TS_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%Y年%m月%d日 %H:%M:%S",
    "%Y年%m月%d日",
    "%Y.%m.%d %H:%M:%S",
    "%Y.%m.%d",
)


# ======================================================================
# 数据结构
# ======================================================================
@dataclass
class ContextEntry:
    """单条资讯上下文单元。"""
    content: str = ""
    source: str = ""
    source_type: str = "unreliable"          # reliable / unreliable
    timestamp: str = ""
    relevance_score: float = 0.0              # 0-1，由 score_relevance 填充
    stock_codes: List[str] = field(default_factory=list)


# ======================================================================
# 核心管理器
# ======================================================================
class ContextEngineer:
    """
    Context Engineering Layer 2 处理器。

    使用方式：
        ce = get_context_engineer()
        ctx = ce.build_context(raw_results, query)
    """

    def __init__(self) -> None:
        # 模块级正则已预编译；此处仅持有配置，便于后续扩展
        self.max_chars: int = CONTEXT_MAX_CHARS

    # ------------------------------------------------------------------
    # 来源可靠性评估
    # ------------------------------------------------------------------
    def assess_source_reliability(self, source_text: str) -> str:
        """
        判断来源是否可靠。

        可靠来源：交易所官网、监管机构、权威媒体（见 _RELIABLE_SOURCE_REGEX）。
        其余一律标记为 unreliable（金融信息采取保守策略）。

        Returns:
            "reliable" 或 "unreliable"
        """
        s = source_text or ""
        if not s.strip():
            return "unreliable"
        if _RELIABLE_SOURCE_REGEX.search(s):
            return "reliable"
        return "unreliable"

    # ------------------------------------------------------------------
    # 股票代码抽取
    # ------------------------------------------------------------------
    def _extract_stock_codes(self, text: str) -> List[str]:
        """
        抽取股票代码：
          - A股6位数字：600519、000001
          - 后缀格式：0700.HK、AAPL.US、600519.SH、000001.SZ
        去重并保持出现顺序。
        """
        if not text:
            return []
        seen: Set[str] = set()
        codes: List[str] = []
        for m in _STOCK_CODE_REGEX.findall(text):
            code = m.upper()
            if code not in seen:
                seen.add(code)
                codes.append(code)
        return codes

    # ------------------------------------------------------------------
    # 情感冲突检测
    # ------------------------------------------------------------------
    @staticmethod
    def _sentiment_of(content: str) -> Tuple[bool, bool]:
        """返回 (has_positive, has_negative)。"""
        c = content or ""
        has_pos = any(kw in c for kw in _POSITIVE_KEYWORDS)
        has_neg = any(kw in c for kw in _NEGATIVE_KEYWORDS)
        return has_pos, has_neg

    def _detect_sentiment_conflict(self, entries: List[ContextEntry]) -> bool:
        """
        检测条目间是否存在情感冲突：存在至少一条偏正面且至少一条偏负面。
        （一条内容同时含正反面关键词视为中性自洽，不与自身冲突。）
        """
        any_pos = False
        any_neg = False
        for e in entries:
            pos, neg = self._sentiment_of(e.content)
            if pos:
                any_pos = True
            if neg:
                any_neg = True
            if any_pos and any_neg:
                return True
        return False

    # ------------------------------------------------------------------
    # 分词与相似度
    # ------------------------------------------------------------------
    @staticmethod
    def _tokenize(text: str) -> Set[str]:
        """
        简单分词：英文/数字 token（长度>=2）+ 中文 bigram（过滤停用词）。
        用于关键词重叠度与 Jaccard 相似度计算。
        """
        if not text:
            return set()
        lowered = text.lower()
        tokens: Set[str] = set(re.findall(r"[a-z0-9]{2,}", lowered))
        for seg in re.findall(r"[\u4e00-\u9fff]+", lowered):
            for i in range(len(seg) - 1):
                a, b = seg[i], seg[i + 1]
                if a in _STOPWORDS or b in _STOPWORDS:
                    continue
                tokens.add(a + b)
        return tokens

    @staticmethod
    def _is_similar(
        toks_a: Set[str], codes_a: Set[str],
        toks_b: Set[str], codes_b: Set[str],
    ) -> bool:
        """两条资讯是否相似：共享股票代码，或关键词 Jaccard 达标。"""
        if codes_a and codes_b and (codes_a & codes_b):
            return True
        union = toks_a | toks_b
        if not union:
            return False
        return (len(toks_a & toks_b) / len(union)) >= DEDUP_SIMILARITY_THRESHOLD

    def _group_similar(self, entries: List[ContextEntry]) -> List[List[ContextEntry]]:
        """贪心聚类：相似条目归为同组（连通分量，传递性合并）。"""
        n = len(entries)
        toks = [self._tokenize(e.content) for e in entries]
        codes = [set(e.stock_codes) for e in entries]
        visited = [False] * n
        groups: List[List[ContextEntry]] = []
        for i in range(n):
            if visited[i]:
                continue
            member_idx: List[int] = [i]
            visited[i] = True
            changed = True
            while changed:
                changed = False
                for j in range(n):
                    if visited[j]:
                        continue
                    if any(
                        self._is_similar(toks[j], codes[j], toks[m], codes[m])
                        for m in member_idx
                    ):
                        member_idx.append(j)
                        visited[j] = True
                        changed = True
            groups.append([entries[k] for k in member_idx])
        return groups

    # ------------------------------------------------------------------
    # 基于时间的去重
    # ------------------------------------------------------------------
    def deduplicate_news(self, entries: List[ContextEntry]) -> List[ContextEntry]:
        """
        相似资讯分组后，每组仅保留最近 DEDUP_KEEP_RECENT 条。
        若保留的 2 条存在情感冲突（一正一负），则给两者内容打上核验标记。
        """
        if not entries:
            return []
        groups = self._group_similar(entries)
        result: List[ContextEntry] = []
        for group in groups:
            # 按时间倒序（最近在前）
            group_sorted = sorted(
                group,
                key=lambda e: self._parse_timestamp(e.timestamp),
                reverse=True,
            )
            kept = group_sorted[:DEDUP_KEEP_RECENT]
            if len(kept) == DEDUP_KEEP_RECENT and self._detect_sentiment_conflict(kept):
                for e in kept:
                    if not e.content.startswith(_VERIFY_PREFIX):
                        e.content = f"{_VERIFY_PREFIX} {e.content}"
            result.extend(kept)
        return result

    # ------------------------------------------------------------------
    # 相关度评分
    # ------------------------------------------------------------------
    def _parse_timestamp(self, ts: str) -> datetime.datetime:
        """解析时间戳为 datetime；失败返回 datetime.min（排序时沉底）。"""
        if not ts:
            return datetime.datetime.min
        s = ts.strip()
        try:
            return datetime.datetime.fromisoformat(s)
        except Exception:
            pass
        for fmt in _TS_FORMATS:
            try:
                return datetime.datetime.strptime(s, fmt)
            except Exception:
                continue
        return datetime.datetime.min

    def _recency_score(self, ts: str) -> float:
        """时间新近度评分 0-1：30天内线性衰减，更久为 0；无法解析给 0.3。"""
        dt = self._parse_timestamp(ts)
        if dt == datetime.datetime.min:
            return 0.3
        now = datetime.datetime.now()
        if dt > now:
            return 1.0  # 未来时间（时钟偏移/解析异常）按最新处理
        days = (now - dt).days
        return max(0.0, 1.0 - days / CE_TIME_DECAY_HALF_LIFE_DAYS)

    def _classify_query_type(self, text: str) -> str:
        """识别文本所属查询类型：stock / macro / fundamentals / general。"""
        text = text or ""
        for qtype, pat in _QUERY_TYPE_PATTERNS.items():
            if pat.search(text):
                return qtype
        return "general"

    def _content_matches_type(self, content: str, q_type: str) -> bool:
        """内容是否与查询类型一致（用于相关度加分）。"""
        if q_type == "general":
            return False
        return self._classify_query_type(content) == q_type

    def score_relevance(self, entry: ContextEntry, query: str) -> float:
        """
        相关度评分 0-1，综合：
          - 关键词重叠度（0.45）
          - 股票代码命中（0.30）
          - 时间新近度（0.25）
          - 查询类型匹配加分（+0.10，封顶1.0）
        """
        q_terms = self._tokenize(query)
        c_terms = self._tokenize(entry.content)
        overlap = (len(q_terms & c_terms) / len(q_terms)) if q_terms else 0.0

        q_codes = set(self._extract_stock_codes(query))
        e_codes = set(entry.stock_codes)
        stock_match = 1.0 if (q_codes and e_codes and (q_codes & e_codes)) else 0.0

        recency = self._recency_score(entry.timestamp)

        q_type = self._classify_query_type(query)
        type_boost = 0.1 if self._content_matches_type(entry.content, q_type) else 0.0

        score = 0.45 * overlap + 0.30 * stock_match + 0.25 * recency + type_boost
        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------
    # 2000 字符阈值裁剪
    # ------------------------------------------------------------------
    def trim_context(
        self,
        entries: List[ContextEntry],
        query: str,
        max_chars: int = CONTEXT_MAX_CHARS,
    ) -> List[ContextEntry]:
        """
        按相关度降序累加条目至字符上限。
        可靠来源条目优先排在前面（先入先保留），保证权威信息优先占用预算。
        """
        # 计算每个条目的相关度
        for e in entries:
            e.relevance_score = self.score_relevance(e, query)

        # 可靠来源在前（按相关度降序），其次非可靠来源（按相关度降序）
        reliable = sorted(
            [e for e in entries if e.source_type == "reliable"],
            key=lambda e: e.relevance_score,
            reverse=True,
        )
        unreliable = sorted(
            [e for e in entries if e.source_type != "reliable"],
            key=lambda e: e.relevance_score,
            reverse=True,
        )
        ordered = reliable + unreliable

        kept: List[ContextEntry] = []
        total = 0
        # 每条格式化开销的粗略估算（来源/时间/标签等）
        overhead = CE_OVERHEAD_RESERVE_CHARS
        for e in ordered:
            cost = len(e.content) + len(e.source) + overhead
            # 超出预算且已有内容则跳过；但至少保留第一条（避免空上下文）
            if total + cost > max_chars and kept:
                continue
            kept.append(e)
            total += cost
        return kept

    # ------------------------------------------------------------------
    # 主入口：构建上下文字符串
    # ------------------------------------------------------------------
    def build_context(self, raw_results: List[Dict], query: str) -> str:
        """
        主入口：原始检索结果 -> 去重 -> 评分裁剪 -> 带来源标注的上下文字符串。

        Args:
            raw_results: 检索结果列表，每项为 dict，含
                         'content' / 'source' / 'timestamp' / 'title' 键
            query: 用户当前查询

        Returns:
            格式化上下文字符串（含来源可靠性标注、相关度、核验标记）
        """
        if not raw_results:
            return ""

        # 1. 原始结果 -> ContextEntry（动态评估来源可靠性、抽取股票代码）
        entries: List[ContextEntry] = []
        for r in raw_results:
            if not isinstance(r, dict):
                continue
            title = (r.get("title") or "").strip()
            content = (r.get("content") or "").strip()
            source = r.get("source") or ""
            timestamp = r.get("timestamp") or ""
            full_content = f"{title}\n{content}" if title else content
            stock_codes = self._extract_stock_codes(f"{title} {content}")
            source_type = self.assess_source_reliability(source)
            entries.append(ContextEntry(
                content=full_content,
                source=source,
                source_type=source_type,
                timestamp=timestamp,
                stock_codes=stock_codes,
            ))

        if not entries:
            return ""

        # 2. 基于时间去重 + 情感冲突标记
        entries = self.deduplicate_news(entries)

        # 3. 动态加载：查询类型驱动相关度评分 -> 裁剪至字符阈值
        #    （查询类型在 score_relevance 内部识别，无需静态配置）
        kept = self.trim_context(entries, query, self.max_chars)

        # 4. 格式化输出
        return self._format_context(kept, query)

    def _format_context(self, entries: List[ContextEntry], query: str) -> str:
        """将保留条目格式化为带标注的上下文字符串。"""
        if not entries:
            return ""
        q_type = self._classify_query_type(query)
        lines: List[str] = []
        lines.append("===== 金融资讯上下文（Context Engineering Layer 2）=====")
        lines.append(f"[查询类型] {q_type}")
        lines.append(f"[保留条目] {len(entries)}")
        for i, e in enumerate(entries, 1):
            rel_tag = "可靠来源" if e.source_type == "reliable" else "非权威来源·需谨慎"
            needs_verify = e.content.startswith(_VERIFY_PREFIX)
            verify_tag = " [需DeepSeek核验：疑似情感冲突]" if needs_verify else ""
            # 展示时剥离核验前缀（已用 verify_tag 标注）
            display = e.content[len(_VERIFY_PREFIX):].strip() if needs_verify else e.content
            codes_str = json.dumps(e.stock_codes, ensure_ascii=False) if e.stock_codes else "无"
            lines.append("")
            lines.append(f"--- 条目 {i} ---")
            lines.append(f"[来源] {e.source or '未知'} [{rel_tag}]{verify_tag}")
            lines.append(f"[时间] {e.timestamp or '未知'}")
            lines.append(f"[关联股票] {codes_str}")
            lines.append(f"[相关度] {e.relevance_score:.2f}")
            lines.append(f"[内容] {display}")
        lines.append("===== 上下文结束 =====")
        return "\n".join(lines)


# ======================================================================
# 全局单例
# ======================================================================
_engineer: Optional[ContextEngineer] = None


def get_context_engineer() -> ContextEngineer:
    """获取 ContextEngineer 全局单例。"""
    global _engineer
    if _engineer is None:
        _engineer = ContextEngineer()
    return _engineer
