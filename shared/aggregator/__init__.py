# shared/aggregator: 信息池共享模块
#
# 职责（重构.md §针对5个数据源的工作流设计 第2步 / §4步骤4）：
#   1. 接收所有数据源（web_search / zsxq / ima / local_sql / 股票缓存）的原始文本 + 结构化 _structured_items
#   2. 规范化：统一转成 shared.models.RetrievalItem
#   3. 去重：基于正文 Jaccard 相似度 + 时间近因（每组最多保留2条供对比，不同源观点不一致标记需核验）
#   4. 可靠性初评：按 source_type / URL 正则匹配官方/监管/权威媒体
#   5. 输出：(a) 共享记忆池 dict，供多 Agent 读取；(b) 供 Prompt 注入的精简上下文段（≤2000字）；(c) _citation_meta 池
#
# 设计原则：
#   - 保守迁移：尽量复用原 agent/context_engineer.py 的去重/裁剪/可靠性正则逻辑，只做壳包装。
#   - 不直接依赖 LLM：纯 Python 字符串与统计操作，避免聚合环节引入推理成本。
#   - 幂等：同一批 RetrievalItem 多次调用 aggregate() 结果相同。
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

# 可靠性 & Jaccard & 时效性正则：直接沿用原 agent/context_engineer.py 定义的规则
try:
    # 优先尝试新路径
    from shared.models import RetrievalItem, SourceReliability
except Exception:  # pragma: no cover
    # 兼容旧路径（若有的话）
    RetrievalItem = Dict[str, Any]  # type: ignore
    SourceReliability = str  # type: ignore


# ======================================================================
# 与原 context_engineer.py 对齐的基础规则（可被后续精细化替换，不影响对外 API）
# ======================================================================

# 来源可靠性判定正则：官方公告/交易所/监管机构/权威媒体
_RELIABLE_SOURCE_REGEX = re.compile(
    r"sse\.com\.cn|szse\.cn|sse\.com|szse\.com|hkex\.com\.hk|hkex\.com|sec\.gov|"
    r"csrc\.gov\.cn|pbc\.gov\.cn|ndrc\.gov\.cn|"
    r"reuters|bloomberg|xinhua(?:net)?|cnstock|stcn\.com|stcn|yicai|21jingji|caixin|wallstreetcn|"
    r"上海证券交易所|深圳证券交易所|上交所|深交所|香港交易所|港交所|"
    r"证监会|央行|中国人民银行|银保监会|国家发改委|发改委|"
    r"路透|彭博|新华社|新华网|中国证券报|证券时报|第一财经|21世纪经济报道|财新|华尔街见闻",
    re.IGNORECASE,
)

# 低相关丢弃阈值
_IRRELEVANT_DROP_CHARS = 30
# 相似度阈值（Jaccard）：超过视为同组去重
_DEDUP_SIMILARITY_THRESHOLD = 0.4
# 每组相似资讯保留最近的条目数（与重构设计一致：2 条对比）
_DEDUP_KEEP_RECENT = 2
# Prompt 上下文段落输出硬上限（字）——与原 Context Engineer 2000 字阈值对齐
_CONTEXT_OUTPUT_MAX_CHARS = 2000


# ======================================================================
# 对外数据结构
# ======================================================================

@dataclass
class AggregateResult:
    """Aggregator.aggregate() 统一返回。"""
    # 规范化+去重后的统一条目列表（按可靠性+时间排序，最相关靠前）
    items: List[RetrievalItem] = field(default_factory=list)
    # 共享记忆池：thread_id -> items 快照；跨 Agent 轮次同一 thread_id 可叠加
    shared_pool: Dict[str, List[RetrievalItem]] = field(default_factory=dict)
    # 用于 Prompt 注入的上下文段（已格式化，≤ _CONTEXT_OUTPUT_MAX_CHARS）
    prompt_context_block: str = ""
    # 统计信息
    stats: Dict[str, Any] = field(default_factory=lambda: {
        "input_total": 0,
        "after_dedup": 0,
        "reliable_count": 0,
        "unverified_count": 0,
        "deduplicated_groups": 0,
        "conflict_flagged": 0,  # 同组内观点相反，标记需 DeepSeek 核验
    })


# ======================================================================
# 辅助函数
# ======================================================================

def _jaccard_tokens(a: str, b: str) -> float:
    """字符级 2-gram Jaccard 相似度。保守迁移：不依赖 NLP 库。"""
    if not a or not b:
        return 0.0
    def _ngrams(s: str, n: int = 2):
        return set(s[i:i + n] for i in range(len(s) - n + 1))
    sa, sb = _ngrams(a), _ngrams(b)
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union > 0 else 0.0


def _parse_published_at_to_tuple(published_at: str) -> Tuple[int, int, int, int, int, int]:
    """尽量把 published_at 转成 (Y, M, D, h, m, s) 元组，无法解析返回 (0,0,0,0,0,0)。"""
    if not published_at:
        return (0, 0, 0, 0, 0, 0)
    # 格式尝试：fromisoformat / 自定义
    import datetime as _dt
    s = published_at.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d", "%Y年%m月%d日"):
        try:
            dt = _dt.datetime.strptime(s, fmt)
            return (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
        except Exception:
            continue
    try:
        dt = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
    except Exception:
        return (0, 0, 0, 0, 0, 0)


def _infer_reliability(item: RetrievalItem) -> SourceReliability:
    """按 source_type / url / channel 推断可靠性。"""
    raw = getattr(item, "reliability", None)
    if isinstance(raw, SourceReliability) and raw != SourceReliability.UNKNOWN:
        return raw
    # 可靠源：ima / db（SQL 本地数据）默认可靠
    st = str(getattr(item, "source_type", "") or "")
    if st in ("ima", "sql", "db", "cache"):
        return SourceReliability.RELIABLE
    # URL / 通道命中可靠正则
    text = f"{getattr(item, 'url', '') or ''} {getattr(item, 'channel', '') or ''}"
    if _RELIABLE_SOURCE_REGEX.search(text):
        return SourceReliability.RELIABLE
    # zsxq 知识星球小作文默认待验证
    if st in ("zsxq", "forum"):
        return SourceReliability.UNVERIFIED
    return SourceReliability.UNVERIFIED


def normalize_item(raw: Any) -> Optional[RetrievalItem]:
    """
    把任意来源的原始条目（dict/RetrievalItem/带_content的对象）归一为 RetrievalItem。
    完全丢弃"空正文+空标题"的无效条目。
    """
    if raw is None:
        return None

    # 已经是 RetrievalItem → 只补可靠性
    if isinstance(raw, RetrievalItem):
        if not raw.content and not raw.title:
            return None
        raw.reliability = _infer_reliability(raw)
        return raw

    # dict：尽力从结构化 _structured_items / tavily 原始字典抽字段
    if isinstance(raw, dict):
        content = str(
            raw.get("content") or raw.get("text") or raw.get("snippet") or raw.get("raw_content") or raw.get("summary") or ""
        )
        title = str(raw.get("title") or "")
        if not content and not title:
            return None
        return RetrievalItem(
            title=title[:200],
            content=content[:4000],
            url=str(raw.get("url") or raw.get("link") or ""),
            source_type=str(raw.get("source_type") or raw.get("source") or "other"),
            channel=str(raw.get("channel") or raw.get("site_name") or raw.get("author_channel") or ""),
            published_at=str(raw.get("published_at") or raw.get("publishedAt") or raw.get("pub_date") or raw.get("date") or ""),
            reliability=_infer_reliability(RetrievalItem(
                content=content,
                url=str(raw.get("url") or ""),
                source_type=str(raw.get("source_type") or ""),
                channel=str(raw.get("channel") or ""),
            )),
            author=str(raw.get("author") or ""),
            sentiment=str(raw.get("sentiment") or "中性"),
            raw_dict={k: v for k, v in raw.items() if isinstance(k, str)},
        )

    # 纯字符串：视作正文内容，source_type=unknown
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        return RetrievalItem(
            content=s[:4000],
            source_type="unknown",
            reliability=SourceReliability.UNVERIFIED,
        )

    # 其它对象：取 __str__ 兜底
    s = str(raw).strip()
    if not s:
        return None
    return RetrievalItem(content=s[:4000], source_type="other", reliability=SourceReliability.UNVERIFIED)


def _format_prompt_context_block(items: List[RetrievalItem], max_chars: int) -> str:
    """把规范化条目拼成 Prompt 中"检索上下文段"，硬截断到 max_chars（按条目整段裁剪不拆开）。"""
    lines: List[str] = []
    acc = 0
    lines.append("【检索结果汇总（已去重+可靠性标注）】")
    acc += len(lines[-1])
    for i, it in enumerate(items, 1):
        # 格式：[N][可靠/待验证] 标题 | 来源 日期\n    正文片段
        rel = it.reliability.value if isinstance(it.reliability, SourceReliability) else str(it.reliability)
        head = f"[{i}][{rel}]"
        if it.title:
            head += f" {it.title}"
        meta_parts = []
        if it.channel:
            meta_parts.append(it.channel)
        if it.published_at:
            meta_parts.append(it.published_at[:16])
        if meta_parts:
            head += f" （{' · '.join(meta_parts)}）"
        body = (it.content or "")[:500].rstrip()
        entry = f"{head}\n    {body}"
        if acc + len(entry) + 1 > max_chars:
            break
        lines.append(entry)
        acc += len(entry) + 1
    if len(lines) == 1:
        lines.append("    （无有效检索条目）")
    return "\n".join(lines)


# ======================================================================
# 对外 Aggregator 类（单例）
# ======================================================================

class Aggregator:
    """信息池共享 Aggregator。全局单例 get_aggregator()。"""

    _instance: Optional["Aggregator"] = None

    def __init__(self):
        # thread_id -> list[RetrievalItem]：跨 Agent 轮次累加（共享记忆池）
        self._shared_pool: Dict[str, List[RetrievalItem]] = {}
        # 同 thread_id 已 seen 的正文指纹（用于增量去重）
        self._seen_fingerprints: Dict[str, set] = {}

    @classmethod
    def get_instance(cls) -> "Aggregator":
        if cls._instance is None:
            cls._instance = Aggregator()
        return cls._instance

    # ------------------- 工具 -------------------

    def clear_thread(self, thread_id: str) -> None:
        """清理某一会话的共享池，用于新任务开始（幂等）。"""
        if not thread_id:
            return
        self._shared_pool.pop(thread_id, None)
        self._seen_fingerprints.pop(thread_id, None)

    # ------------------- 核心 -------------------

    def aggregate(
        self,
        raw_items: Iterable[Any],
        *,
        thread_id: Optional[str] = None,
        append_to_shared_pool: bool = True,
        context_max_chars: int = _CONTEXT_OUTPUT_MAX_CHARS,
        dedup_threshold: float = _DEDUP_SIMILARITY_THRESHOLD,
        keep_recent: int = _DEDUP_KEEP_RECENT,
    ) -> AggregateResult:
        """
        对一批原始条目做规范化 → 去重 → 排序 → 上下文段落输出 → 可选写入共享池。

        参数:
            raw_items: 任意条目混合（RetrievalItem / dict / 字符串 / TavilyResult 等）
            thread_id: 若提供，会把结果追加到该会话的共享记忆池（后续轮次可叠加）
            append_to_shared_pool: 是否写入共享池（某些一次性场景如盘前汇总可设 False）
            context_max_chars: 输出 prompt_context_block 的上限（默认 2000）
            dedup_threshold: 去重 Jaccard 阈值（默认 0.4）
            keep_recent: 每组相似条目最多保留 N 条（默认 2，供对比观点）
        """
        result = AggregateResult()
        inputs = list(raw_items or [])
        result.stats["input_total"] = len(inputs)

        # 1) 规范化 + 丢弃无效
        normalized: List[RetrievalItem] = []
        for raw in inputs:
            item = normalize_item(raw)
            if item is not None:
                normalized.append(item)

        # 2) 增量去重（若有 thread_id，则剔除与之前 seen 完全重复的指纹）
        seen_fp_this: set = set()
        if thread_id:
            seen_fp_this = self._seen_fingerprints.setdefault(thread_id, set())

        dedup_groups: List[List[RetrievalItem]] = []  # 每组：相似条目的列表
        group_repr_tokens: List[set] = []  # 每组代表的 2-gram 集合（用于组间匹配）

        def _token_set(s: str, n: int = 2) -> set:
            if len(s) < n:
                return set()
            return set(s[i:i + n] for i in range(len(s) - n + 1))

        for item in normalized:
            body = (item.content or "") + " " + (item.title or "")
            if len(body) < 10:
                # 极短句跳过分组（直接保留）
                dedup_groups.append([item])
                group_repr_tokens.append(set())
                continue
            # 指纹：用 content 前 200 字哈希，增量去重用
            fp = hash(body[:200])
            if thread_id and fp in seen_fp_this:
                # 完全重复 → 丢弃
                continue
            seen_fp_this.add(fp)

            tok = _token_set(body)
            placed = False
            # 匹配到已有组 → 加进去
            for i, grp_tok in enumerate(group_repr_tokens):
                if not grp_tok:
                    continue
                inter = len(tok & grp_tok)
                union = len(tok | grp_tok)
                sim = inter / union if union > 0 else 0.0
                if sim >= dedup_threshold:
                    dedup_groups[i].append(item)
                    # 代表 tokens：取组最新的那个（避免代表过时导致后续无法聚合）
                    group_repr_tokens[i] = tok
                    placed = True
                    break
            if not placed:
                dedup_groups.append([item])
                group_repr_tokens.append(tok)

        # 3) 每组：按 published_at 倒序 → 保留 keep_recent 条；观点相反标记冲突
        final_items: List[RetrievalItem] = []
        for grp in dedup_groups:
            if not grp:
                continue
            result.stats["deduplicated_groups"] += 1
            # 按时间倒序
            grp_sorted = sorted(
                grp,
                key=lambda it: _parse_published_at_to_tuple(it.published_at),
                reverse=True,
            )
            kept = grp_sorted[: max(1, keep_recent)]
            # 冲突检测：同组内 sentiment 含"利多"+"利空" → 标记 [需DeepSeek核验]
            sents = {(it.sentiment or "").strip() for it in kept if it.sentiment}
            has_bull = any("利多" in s or "利好" in s or "上涨" in s for s in sents)
            has_bear = any("利空" in s or "下跌" in s or "利空" in s for s in sents)
            if has_bull and has_bear and len(kept) >= 2:
                for it in kept:
                    if not it.title.startswith("[需核验]"):
                        object.__setattr__(it, "title", "[需核验]" + it.title)
                    if "[观点冲突]" not in it.content:
                        object.__setattr__(it, "content", "[观点冲突-需核验] " + it.content)
                result.stats["conflict_flagged"] += 1
            final_items.extend(kept)

        # 4) 组间整体排序：可靠源优先 → 新的优先 → 长正文优先
        def _sort_key(it: RetrievalItem):
            rel_score = 0 if it.reliability == SourceReliability.RELIABLE else (1 if it.reliability == SourceReliability.UNVERIFIED else 2)
            return (rel_score, -_parse_published_at_to_tuple(it.published_at)[0], -len(it.content or ""))
        final_items.sort(key=_sort_key)

        result.items = final_items
        result.stats["after_dedup"] = len(final_items)
        result.stats["reliable_count"] = sum(1 for it in final_items if it.reliability == SourceReliability.RELIABLE)
        result.stats["unverified_count"] = sum(1 for it in final_items if it.reliability == SourceReliability.UNVERIFIED)

        # 5) Prompt 上下文段落
        result.prompt_context_block = _format_prompt_context_block(final_items, max_chars=context_max_chars)

        # 6) （可选）写入共享记忆池
        if thread_id and append_to_shared_pool:
            pool = self._shared_pool.setdefault(thread_id, [])
            pool.extend(final_items)
            # 写回 AggregateResult.shared_pool 的快照（对外只读）
            result.shared_pool[thread_id] = list(pool)

        return result


def get_aggregator() -> Aggregator:
    """获取全局 Aggregator 单例。"""
    return Aggregator.get_instance()


# ======================================================================
# 便捷包装：从原 _extract_structured_items_from_tool_texts() 抽出的 list[dict] → RetrievalItem list
# ======================================================================
def aggregate_from_tool_texts(tool_texts: List[str], **kwargs) -> AggregateResult:
    """
    原 main_agent 里 _extract_structured_items_from_tool_texts 返回的 list[dict]
    与整段文本混合场景的便捷入口。内部会从每段文本里抽取可能的结构化 JSON，再聚合。
    """
    import json as _json
    raw_items: List[Any] = []

    _STRUCT_RE = re.compile(r"<structured>\s*(\{.*?\})\s*</structured>", re.I | re.S)
    _JSON_FIELD_RE = re.compile(r'"_structured_items"\s*:\s*(\[[^\]]*\])', re.S)

    for txt in (tool_texts or []):
        if not isinstance(txt, str):
            continue
        # <structured>...</structured>
        for wrapped in _STRUCT_RE.findall(txt):
            try:
                obj = _json.loads(wrapped)
                arr = obj.get("_structured_items") or []
                if isinstance(arr, list):
                    raw_items.extend(arr)
            except Exception:
                pass
        # "_structured_items": [...] 字段（Tavily/ZSXQ 模式）
        for m in _JSON_FIELD_RE.finditer(txt):
            try:
                arr = _json.loads(m.group(1))
                if isinstance(arr, list):
                    raw_items.extend(arr)
            except Exception:
                pass
        # 未结构化的文本也作为一条正文
        stripped = txt.strip()
        if stripped and len(stripped) >= 50:
            raw_items.append(stripped)

    return get_aggregator().aggregate(raw_items, **kwargs)
