# -*- coding: utf-8 -*-
"""
§3 协议适配与归一化层（adapter.stream_adapters）

核心职责：把三种"异构流式来源"（DeepSeek/本地 Ollama Qwen/IMA+ZSXQ 检索结果结构化）
统一转换为应用层 NormalizedChunk，便于编排路由层消费，同时：

  1) 把 <think>...</think> 跨 chunk 切分后的标签正确剥离，分路由为"reasoning token"
     和"正文 token"（依据 Experience 401403 教训：跨 chunk 状态机，不用单 chunk replace）。
  2) 把结构化检索结果（Tavily/IMA/ZSXQ）转为带 [citation:N] 标记的上下文，便于后续模型
     回答时自动 [N] 引用，后端再 1:1 映射 citation_meta。
  3) 把模型回答流中出现的 [N] / [citation:N] / [[N]] 三种写法统一为 [N] 角标，同时
     返回被引用的编号，便于 bus 触发 citation_meta 增量下发。

本文件零依赖除了标准库，可被 tools/* / agent/* / api/* 任意位置复用。
"""
from __future__ import annotations

import re
import hashlib
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


# ======================================================================
# §3.0 时效性窗口（RECENCY）工具：解析 published_at + 按天窗口过滤
#   所有通道（Tavily / IMA / ZSXQ）产出 items 后都走本处同一份逻辑，
#   避免三处各自重复实现造成阈值 / 解析格式不一致。
# ======================================================================

def _get_recency_constants() -> Tuple[int, int, Tuple[str, ...], Tuple[str, ...], int]:
    """懒加载常量（避免循环 import / 让工具测试在不装 config 时仍有兜底值）。"""
    try:
        from config.constants import (
            RECENCY_PREFER_DAYS as _p,
            RECENCY_FALLBACK_DAYS as _f,
            RECENCY_PARSE_FORMATS as _fmts,
            RECENCY_KEEP_ON_PARSE_FAIL_CHANNELS as _keep_chs,
            RECENCY_TIMEZONE_OFFSET_HOURS as _tz,
        )
    except Exception:
        _p, _f, _fmts, _keep_chs, _tz = 30, 90, ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y年%m月%d日"), ("ima",), 8
    return int(_p), int(_f), tuple(_fmts), tuple(_keep_chs), int(_tz)


def _now_beijing(prefer_days_from: Optional[datetime] = None, tz_offset_hours: int = 8) -> datetime:
    """返回"当前北京时间"，用于时间窗口向前推 days 判断。
    单测可传 prefer_days_from 作为伪时钟；该时间必须是带 offset 的 aware datetime，
    与 parsed published_at 相减时类型一致，避免 TypeError: can't subtract offset-naive 。
    """
    if prefer_days_from is not None:
        # 若调用者传了"基准时间"（如某条单元测试），强制确保它是 aware
        if prefer_days_from.tzinfo is None:
            return prefer_days_from.replace(tzinfo=timezone(timedelta(hours=tz_offset_hours)))
        return prefer_days_from.astimezone(timezone(timedelta(hours=tz_offset_hours)))
    return datetime.now(timezone(timedelta(hours=tz_offset_hours)))


def parse_published_at(s: Any) -> Optional[datetime]:
    """健壮地解析 published_at 字符串为带 UTC+8 tzinfo 的 datetime，解析失败返回 None。
    兼容：RFC3339（2025-01-03T10:00:00Z / ±08:00）、YYYY-MM-DD[ HH:MM:SS]、
    YYYY/MM/DD[ HH:MM:SS]、yyyy年M月d日；空白串直接返回 None。"""
    if s is None:
        return None
    if isinstance(s, datetime):
        dt = s
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
        return dt.astimezone(timezone(timedelta(hours=8)))
    raw = str(s).strip()
    if not raw:
        return None
    # 1) 优先 RFC3339 / ISO-8601
    try:
        iso_s = raw
        # 把末尾 'Z' 替换成 Python fromisoformat 能识别的 '+00:00'
        if iso_s.endswith("Z") or iso_s.endswith("z"):
            iso_s = iso_s[:-1] + "+00:00"
        dt = datetime.fromisoformat(iso_s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
        return dt.astimezone(timezone(timedelta(hours=8)))
    except (ValueError, TypeError):
        pass
    # 2) 依次尝试 RECENCY_PARSE_FORMATS
    _, _, formats, _, tz_off = _get_recency_constants()
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone(timedelta(hours=tz_off)))
        except (ValueError, TypeError):
            continue
    return None


def filter_items_by_recency(
    items: List[Dict[str, Any]],
    *,
    prefer_days: Optional[int] = None,
    fallback_days: Optional[int] = None,
    prefer_days_from: Optional[datetime] = None,
    channel: Optional[str] = None,
    auto_fallback: bool = True,
) -> Tuple[List[Dict[str, Any]], int, bool]:
    """对 items 列表（每项含 published_at / channel 字段）按「近 prefer_days 优先；
    若 0 条命中且 auto_fallback=True 则降级 fallback_days」过滤。

    返回 (kept_items, applied_days, did_fallback)：
      - kept_items 是保留下来的 items（顺序与原列表一致）；
      - applied_days 是实际生效的阈值天数（prefer 或 fallback）；
      - did_fallback=True 表示 prefer 过滤后空，触发了 fallback 扩大窗口。

    解析策略（与 §30 常量 RECENCY_KEEP_ON_PARSE_FAIL_CHANNELS 对齐）：
      - item.channel 在 keep_on_parse_fail_channels 中（即 IMA 静态 PDF）：
        published_at 无法解析 → 保留不删；超期 → 也保留（入库时间不是新闻时效）。
      - 其它通道（Tavily/ZSXQ）：无法解析的 published_at 按"过期处理"删除。
    """
    if not items:
        return [], 0, False
    _p_def, _f_def, _fmts, keep_channels, tz_off = _get_recency_constants()
    if prefer_days is None:
        prefer_days = _p_def
    if fallback_days is None:
        fallback_days = _f_def
    # 防止误配：fallback 必须 ≥ prefer
    if fallback_days < prefer_days:
        fallback_days = prefer_days

    now = _now_beijing(prefer_days_from=prefer_days_from, tz_offset_hours=tz_off)

    def _keep(it: Dict[str, Any], days: int) -> bool:
        ch = str(channel or it.get("channel") or "").lower()
        is_keep_ch = ch in keep_channels
        pub_raw = it.get("published_at") or it.get("date") or ""
        pub_dt = parse_published_at(pub_raw) if pub_raw else None
        if pub_dt is None:
            # 解析失败：IMA 静态库保留；其它通道视为过期 → 删除
            return bool(is_keep_ch)
        delta = now - pub_dt
        age_days = delta.total_seconds() / 86400.0
        if age_days <= max(0, days) + 1e-6:
            return True
        # 超期：IMA 仍保留（created_at/updated_at 不是新闻发布时间）
        return bool(is_keep_ch)

    kept_prefer = [it for it in items if _keep(it, prefer_days)]
    if kept_prefer:
        return kept_prefer, prefer_days, False
    if not auto_fallback:
        return [], prefer_days, False
    kept_fallback = [it for it in items if _keep(it, fallback_days)]
    return kept_fallback, fallback_days, True


def recency_rule_notice_line(applied_days: int, did_fallback: bool) -> str:
    """生成写入 citation 摘要块首行 / 正文前导的规则提示句。"""
    if did_fallback:
        return (
            "业务规则：检索默认只保留最近 1 个月发布/更新的新闻；"
            f"近 1 个月无结果，已自动扩大到最近 {applied_days} 天（约 3 个月）；"
            "超期条目已被剔除，不要根据已删除内容输出观点。"
        )
    return (
        f"业务规则：检索只保留最近 {applied_days} 天（约 1 个月）发布/更新的新闻；"
        "超期条目已被剔除，不要根据已删除内容输出观点。"
    )

# ======================================================================
# §3.1 统一流式 Chunk（NormalizedChunk）—— 应用内流式真源
# ======================================================================

@dataclass
class NormalizedChunk:
    """所有 LLM / 工具 / 检索 输出的归一化块。

    用法举例（流式消费循环）：
        async for chunk in deepseek_stream:
            for n in adapter.ingest_raw_text(chunk.text, is_final=chunk.done):
                if n.type == "reasoning":  await bus.ev_delta(..., is_reasoning=True)
                if n.type == "delta":      await bus.ev_delta(..., text=n.text)
                if n.type == "reasoning_end":
                    await bus.ev_reasoning(..., content=n.text, stage='model_coT')
    """
    type: str                       # "reasoning" / "delta" / "reasoning_start" / "reasoning_end"
    text: str = ""                  # 增量文本（reasoning_start/end 可能为空）
    # 本段识别到的"引用编号"列表（[N] 角标或 [citation:N]）——每 chunk 增量识别
    citations: List[int] = field(default_factory=list)


# ======================================================================
# §3.2 跨 chunk <think> 推理文本剥离（状态机）
# ======================================================================

# 兼容 4 种真实形态：<think> / </think> / <think/> / </think/> / 残缺如 '<thi'
_THINK_OPEN = ("<think>", "<think/>")
_THINK_CLOSE = ("</think>", "</think/>")
_THINK_PREFIXES = ("<think", "</think")  # 用于判断边界切分残片


class ThinkTagSplitter:
    """
    流式 <think>...</think> 状态机。

    经验 401403 教训：严禁 "if '<think>' in chunk: replace"。用状态机 + buffer 回看。
    算法：
      - 维护 state = {"text","think"}（当前这段正在积累正文还是思考）
      - 维护 buffer（可能残留上一段残缺标签，长度 <= max(open/close tag 尾字符)）
      - 每轮 buffer+incoming 按贪心切：优先匹配 open/close 完整标签，其次不完整跳过
      - 每次切到一段 (kind, payload) 后立即 yield NormalizedChunk。
    """

    def __init__(self) -> None:
        self._in_think = False
        # 尾部残缺缓冲：最多保留最长 tag 前缀 - 1 个字符
        self._tail_buffer = ""

    def ingest(self, incoming: str, *, is_final: bool = False) -> List[NormalizedChunk]:
        if not incoming:
            return []

        s = self._tail_buffer + incoming
        out: List[NormalizedChunk] = []
        i = 0
        n = len(s)

        def push(kind: str, payload: str) -> None:
            if not payload:
                return
            if kind == "think":
                out.append(NormalizedChunk(type="reasoning", text=payload))
            else:
                out.append(NormalizedChunk(type="delta", text=payload))

        while i < n:
            # 1) 当前 state 内贪婪扫描最近的"切换标签"
            if self._in_think:
                idx = -1
                used_tag = ""
                for close_tag in _THINK_CLOSE:
                    k = s.find(close_tag, i)
                    if k != -1 and (idx == -1 or k < idx):
                        idx = k
                        used_tag = close_tag
                if idx != -1:
                    push("think", s[i:idx])
                    out.append(NormalizedChunk(type="reasoning_end", text=""))
                    i = idx + len(used_tag)
                    self._in_think = False
                    continue
            else:
                idx = -1
                used_tag = ""
                for open_tag in _THINK_OPEN:
                    k = s.find(open_tag, i)
                    if k != -1 and (idx == -1 or k < idx):
                        idx = k
                        used_tag = open_tag
                if idx != -1:
                    push("text", s[i:idx])
                    out.append(NormalizedChunk(type="reasoning_start", text=""))
                    i = idx + len(used_tag)
                    self._in_think = True
                    continue

            # 2) 没找到切换标签。若是流末尾（is_final）或最后一段不会是残缺标签 → 整段输出
            tail = s[i:]
            if is_final:
                push("think" if self._in_think else "text", tail)
                i = n
                self._tail_buffer = ""
                continue

            # 不完整标签判定：
            #   末尾前缀匹配任何 tag 前缀 => 可能下一个 chunk 把它补齐
            max_keep = max(7, max(len(t) for t in _THINK_PREFIXES))  # 约 7 字符
            keep = 0
            # 看 tail 的后 k 字符，是否是某个 THINK_ 标签/前缀的前缀
            #   经验：直接从 max_keep 向前回溯
            for k in range(min(max_keep, len(tail)), 0, -1):
                candidate = tail[-k:]
                # candidate 是否是任一 open/close 标签的真前缀（不完全相等）
                for tag in _THINK_OPEN + _THINK_CLOSE:
                    if tag.startswith(candidate) and tag != candidate:
                        keep = k
                        break
                if keep:
                    break
            if keep == 0:
                push("think" if self._in_think else "text", tail)
                i = n
                self._tail_buffer = ""
            else:
                push("think" if self._in_think else "text", tail[:-keep])
                self._tail_buffer = tail[-keep:]
                i = n

        return out

    @property
    def in_think_state(self) -> bool:
        return self._in_think

    def reset(self) -> None:
        self._in_think = False
        self._tail_buffer = ""


# ======================================================================
# §3.3 引用归一化 & 抽取（[N] / [citation:N] / [[N]] 统一）
# ======================================================================

# 匹配 [N] / (N) 角标或 [citation:N] 或 [[N]]
#   - 注意：必须排除 markdown 链接格式 [text](http://url)，避免误伤
#   - 为了排除这种误匹配，我们做"负向前瞻：后面没有紧接着 ("
_CIT_NORM_PATTERNS: Tuple[Tuple[re.Pattern, str], ...] = (
    # 优先级 1：显式 [citation:N]
    (re.compile(r"\[citation:\s*(\d{1,4})\s*\]", re.I), lambda m: (int(m.group(1)), True)),
    # 优先级 2：[[N]]
    (re.compile(r"\[\[\s*(\d{1,4})\s*\]\]"), lambda m: (int(m.group(1)), True)),
    # 优先级 3：[N] / (N)，但"后面不紧接 (url" —— markdown 链接规避
    (re.compile(r"[\[(]\s*(\d{1,4})\s*[\])](?!\s*\()"), lambda m: (int(m.group(1)), True)),
)

# 模型回答中的引用角标"写法不规范"统一：所有格式都替换成 [N]
_DENOISE_REPLACEMENTS: Tuple[Tuple[re.Pattern, str], ...] = (
    (re.compile(r"\[citation:\s*(\d{1,4})\s*\]", re.I), r"[\1]"),
    (re.compile(r"\[\[\s*(\d{1,4})\s*\]\]"), r"[\1]"),
    # 只对 "(N)" 做替换，确保不把 "(1)" 这类序号变成乱码（数字最多 4 位）
    (re.compile(r"\(\s*(\d{1,4})\s*\)(?!\s*\()"), r"[\1]"),
)


def normalize_citation_markers(text: str) -> Tuple[str, List[int]]:
    """把回答文本中的引用角标统一为 [N] 格式，返回 (new_text, cited_indices)。

    注意：
      - 不破坏 markdown 链接 [link](url)。
      - cited_indices 按出现顺序，去重前的原始顺序保留（方便后续按首次出现排序）。
    """
    out = text
    for pat, repl in _DENOISE_REPLACEMENTS:
        out = pat.sub(repl, out)
    # 再提取出出现过的 N
    seen_order: List[int] = []
    for pat, _extract in _CIT_NORM_PATTERNS[:2]:  # 显式格式 + [[N]]（已被上面替换为 [N] 不补）
        pass
    # 统一匹配 [N]（替换后的版本）
    for m in re.finditer(r"\[\s*(\d{1,4})\s*\](?!\s*\()", out):
        try:
            seen_order.append(int(m.group(1)))
        except Exception:
            pass
    return out, seen_order


def extract_citations_from_delta(delta_text: str) -> Tuple[str, List[int]]:
    """流式单 chunk 引用归一化：调用 normalize_citation_markers，再加上"截断处残留
    [N/ 半角标"不做补齐（下一个 chunk 合并后再处理），避免误替换。"""
    return normalize_citation_markers(delta_text)


# ======================================================================
# §3.4 检索上下文注入器：检索文档 → 带 [citation:N] 的上下文段落
# ======================================================================

@dataclass
class CitationDocument:
    """文档条目，给模型注入上下文时带 [citation:N] 标记。"""
    index: int                      # 1..N，统一编号
    title: str
    url: str = ""
    content: str = ""               # 已被截断的正文片段（≤ 2000 字）
    source_type: str = "web"        # "web" / "knowledge_base" / "forum" / "pdf" / "db"
    reliability: str = "待验证"
    channel: str = ""               # "tavily" / "ima" / "zsxq" / "db"
    published_at: str = ""
    # 关键字哈希：用于"模型完全没打引用标记"时做关键词匹配 fallback
    _keywords: List[str] = field(default_factory=list)
    score: float = 0.0


def _doc_has_effective_content(raw: Dict[str, Any]) -> bool:
    """硬约束：单条检索结果"有有效内容"才算数。
    内容（content/snippet）是空、占位字符（"暂无"/"无"/"未找到"/"N/A"）、纯空白、<4 字短文 → 直接剔除，不要塞给模型。
    标题（title）只用于显示，**不能**作为"有内容"的判定依据——用户说"某条结果为空或无法提取有效内容"指的是正文。
    """
    content = (str(raw.get("content") or raw.get("snippet") or "")).strip()
    if not content:
        return False
    EMPTY_HINTS_EXACT = ("暂无", "无", "未找到", "未检索到", "无相关", "n/a", "na", "null", "none", "—", "-", "[]", "{}")
    ct_norm = content.lower()
    if content in EMPTY_HINTS_EXACT or ct_norm in EMPTY_HINTS_EXACT:
        return False
    # 常见"整句占位"模板（例如"未找到相关数据"、"暂无相关信息"、"无搜索结果"）：整段内容 100% 由这些提示构成
    EMPTY_PREFIX_HINTS = ("未找到相关", "暂无相关", "无相关", "无搜索结果", "没找到相关", "未检索到相关", "n/a")
    ct_stripped = re.sub(r"[\s。，、,.!！？?；;：:【】\[\]（）()\"'“”‘’\-—_]+", "", ct_norm)
    if len(ct_stripped) <= 18:
        # 短文本里命中占位关键词（且不含数字/股票代码等实质字段）→ 视为空条
        if any(p in ct_norm for p in EMPTY_PREFIX_HINTS):
            # 允许数字，但若仅数字也不视为有效（比如"未找到 600519 相关" → 也空）
            only_digits_left = re.sub(r"未找到相关|暂无相关|无相关|无搜索结果|没找到相关|未检索到相关|数据|信息|结果|内容|文档|记录", "", ct_stripped)
            only_digits_left = re.sub(r"[0-9a-zsh]+", "", only_digits_left)  # 剩余数字/代码直接忽略
            if not only_digits_left:
                return False
    # 内容过短（< 4 个中文字符）视为无法提取有效信息（比如一个"好"字的帖子顶帖）
    if len(content) < 4:
        return False
    return True


def _trim_summary_block_to_limit(block: str, limit: int) -> Tuple[str, int, bool]:
    """硬约束：把最终拼给模型的"检索结果汇总"正文块强制截到 limit 字。
    返回 (trimmed, removed_count, was_truncated)，removed_count 是被裁掉的"条"数。
    """
    if not block:
        return block, 0, False
    if len(block) <= limit:
        return block, 0, False
    # 按 "— 文档 [citation:N] —" 作为条目边界，优先整条移除末尾的文档，而不是把单文档砍半
    import re as _re
    parts = _re.split(r"(?=— 文档 \[citation:\d+\] —)", block)
    total_parts = [p for p in parts if p]
    kept: List[str] = []
    total_len = 0
    for p in total_parts:
        if total_len + len(p) <= limit:
            kept.append(p)
            total_len += len(p)
        else:
            break
    if kept:
        trimmed = "".join(kept)
        if len(trimmed) > limit:
            trimmed = trimmed[:limit]
    else:
        # 连一条都装不下 → 兜底按字硬截（仍保留第一条头部）
        first = total_parts[0] if total_parts else block
        trimmed = first[:limit]
    was_truncated = len(trimmed) < len(block)
    removed = len(total_parts) - len(kept)
    return trimmed, max(0, removed), was_truncated


def build_citation_context(
    docs: List[Dict[str, Any]],
    *,
    start_index: int = 1,
    doc_title_max: int = 200,
    doc_content_max: int = 1200,
    skip_empty_docs: bool = True,
    summary_block_max: int = 400,   # 用户硬约束：检索汇总精简，最多 400 字
    omit_default_meta: Optional[bool] = None,  # None → 读常量 CITATION_PROMPT_OMIT_DEFAULT_META
    prompt_doc_content_cap: Optional[int] = None,  # None → 读常量 CITATION_PROMPT_DOC_CONTENT_MAX
) -> Tuple[List[CitationDocument], str]:
    """把任意检索通道返回的 docs Dict 规范化为 CitationDocument 列表，
    并生成一段可直接拼到 Prompt 中的上下文字符串（每段开头含 [citation:idx]）。

    【用户新增约束】（软 + 硬双层保证）：
      (A) prompt 明确要求——空/无效检索结果**不要**写入正文；
      (B) 代码硬约束——先 `_doc_has_effective_content` 剔除空结果；
          其次拼出来的 block 超过 400 字时，整条整段删除末尾文档，绝不突破字数上限。
      (C) §29 新增：单文档正文上限（常量 CITATION_PROMPT_DOC_CONTENT_MAX 默认 800 字）；
          默认元数据（可靠性=待验证/类型=web）不写入 prompt，进一步压缩单文档 token 占比。

    docs 字段规范（至少需满足一种可渲染形态）：
      - Tavily：{title, url, content}
      - IMA   ：{title, url(可选=知识库名+页名拼接), content, knowledge_base(可选)}
      - ZSXQ  ：{title=帖子标题或首句摘要, url(可选=帖子链接), content=正文片段, published_at(可选)}
      - DB    ：{title=来源表+主键, content=行内容, source_type="db"}

    返回：
      (docs_list, prompt_context_block)
    """
    # 读 §29 常量（不存在就兜底）
    try:
        from config.constants import (
            CITATION_PROMPT_OMIT_DEFAULT_META as _DEFAULT_OMIT,
            CITATION_PROMPT_DOC_CONTENT_MAX as _DEFAULT_CONTENT_CAP,
            CITATION_TITLE_MAX_CHARS as _DEFAULT_TITLE_CAP,
        )
    except Exception:
        _DEFAULT_OMIT, _DEFAULT_CONTENT_CAP, _DEFAULT_TITLE_CAP = True, 800, 80
    if omit_default_meta is None:
        omit_default_meta = bool(_DEFAULT_OMIT)
    if prompt_doc_content_cap is None:
        prompt_doc_content_cap = int(_DEFAULT_CONTENT_CAP or doc_content_max)
    # 标题也做上限裁剪（省 token，避免 200 字巨长标题）
    doc_title_max = min(doc_title_max, int(_DEFAULT_TITLE_CAP or doc_title_max))
    # step 1: 空条/无效条剔除（保持原逻辑不变）
    filtered: List[Dict[str, Any]] = []
    for raw in docs or []:
        if skip_empty_docs and not _doc_has_effective_content(raw):
            continue
        filtered.append(raw)
    # step 2: 时效性窗口过滤 —— 只保留最近 1 个月，空则自动降级近 3 个月
    #   本处是"主 Agent 汇总三通道 + ZSXQ 本地模型上下文"的统一入口，
    #   模型最终只能看到这里 kept 下来的 docs，过期条目不会进入 prompt。
    recency_kept, recency_applied_days, recency_fell_back = filter_items_by_recency(
        filtered, auto_fallback=True
    )
    # 若三通道拼起来过滤后仍为空，后续正文保持空串，并在 header 写明"近 3 个月无相关检索结果"
    filtered = recency_kept
    recency_notice = recency_rule_notice_line(recency_applied_days or 90, recency_fell_back)
    all_empty_after_recency = (len(filtered) == 0)

    norm_docs: List[CitationDocument] = []
    blocks: List[str] = []
    for i, raw in enumerate(filtered):
        idx = start_index + i
        title = (str(raw.get("title") or f"文档{idx}")).strip()[:doc_title_max]
        url = str(raw.get("url") or "")
        content = (str(raw.get("content") or raw.get("snippet") or "")).strip()
        # 全局正文 prompt 注入上限：CITATION_PROMPT_DOC_CONTENT_MAX（默认 800）
        doc_content_cap = min(int(doc_content_max or 10**9), int(prompt_doc_content_cap or 10**9))
        # 叠加：单条按"汇总 400 ÷ 文档数"的 per_doc_cap 做更短截断（防 1 条吃光预算）
        per_doc_cap = min(doc_content_cap, max(120, summary_block_max // max(1, min(len(filtered), 5))))
        final_content_cap = min(doc_content_cap, per_doc_cap)
        content = (content[:final_content_cap] + "…" if len(content) > final_content_cap else content)
        src_type = str(raw.get("source_type") or _guess_source_type(url, raw))
        rel = str(raw.get("reliability") or _guess_reliability(url, src_type, raw))
        channel = str(raw.get("channel") or "")
        pub = str(raw.get("published_at") or raw.get("date") or "")
        score = float(raw.get("score") or 0.0)
        doc = CitationDocument(
            index=idx, title=title, url=url, content=content,
            source_type=src_type, reliability=rel, channel=channel,
            published_at=pub, score=score,
            _keywords=_extract_keywords(title, content),
        )
        norm_docs.append(doc)
        # Prompt 块：明确要求"引用使用 [N] 角标"，避免模型输出 [citation:N]
        lines = [f"— 文档 [citation:{idx}] —", f"标题：{title or '（无标题）'}"]
        if url:
            lines.append(f"链接：{url}")
        meta_extra = []
        # §29：omit_default_meta=True 时，默认值（web / 待验证 / 空通道 / 空发布时间）不写，
        # 单文档省 ~40 token；3 条合计省 ~120 token。
        _DEFAULT_REL = "待验证"
        _DEFAULT_SRC = "web"
        if channel:
            # 空通道 = 默认来源 Tavily，省
            meta_extra.append(f"通道={channel}")
        if (not omit_default_meta) or (src_type != _DEFAULT_SRC):
            meta_extra.append(f"类型={src_type}")
        if (not omit_default_meta) or (rel != _DEFAULT_REL):
            # 可靠性=待验证 是默认值（论坛/网页默认），omit 时不刷
            meta_extra.append(f"可靠性={rel}")
        if pub:
            meta_extra.append(f"发布时间={pub}")
        if meta_extra:
            lines.append("元数据：" + "；".join(meta_extra))
        lines.append(f"正文：{content}")
        blocks.append("\n".join(lines))
    # --- 用户新增硬约束：空结果不输出 + 汇总≤400字 + 时效性窗口 ---
    raw_body = "\n\n".join(blocks)
    trimmed_body, removed_doc_count, was_truncated = _trim_summary_block_to_limit(
        raw_body, limit=summary_block_max
    )
    extra_notice_lines = []
    if was_truncated and removed_doc_count > 0:
        extra_notice_lines.append(
            '（注：为遵守「摘要≤{}字」约束，已省略末尾 {} 条参考文档；'
            '正文回答仅引用上方保留的文档编号 [N]，不要引用已省略的文档。）'.format(
                summary_block_max, removed_doc_count
            )
        )
    elif was_truncated:
        extra_notice_lines.append('（注：参考文档正文已精简至 ≤{} 字。）'.format(summary_block_max))
    if all_empty_after_recency:
        extra_notice_lines.append("近 3 个月无相关检索结果。")
    # 【检索汇总写作规则】4 条 → 在 第 4 条追加"时效性窗口"
    header_lines = [
        "",
        "",
        "===== 检索到的参考文档（空结果/无效内容已剔除；正文块严格 ≤{} 字；".format(summary_block_max)
        + "请在正文中严格使用 [N] 角标引用，不要使用任何其他形式）=====",
        "【检索汇总写作规则（违反作废）】：",
        f"  0) {recency_notice}",   # 用户规则 p3(b)：在正文摘要块首行输出本规则
        "  1) 若某条结果为空、内容是「暂无/未找到/N/A」、或无法提取有效正文 → 不要在回答中提及该条。",
        "  2) 检索结果汇总是**摘要**，不要复述原文：每文档要点 ≤1 句，整体中文正文块（不含"
        "标题/链接/元数据标签）严格 ≤{} 字。".format(summary_block_max),
        "  3) （提示：[N] 对应的具体来源会在服务端映射为来源卡片；引用越多越可信。）",
    ]
    header = "\n".join(header_lines) + "\n"
    footer = (
        "\n"
        + ("\n".join(extra_notice_lines))
        + f"\n===== 参考文档结束（合计 {len(norm_docs)} 条，保留 {len(trimmed_body)} 字）；"
        "回答正文时请用 [N] 角标引用上方保留的文档。注意：上方未出现的编号不得引用。 =====\n\n"
    )
    ctx = (header + trimmed_body + footer) if (trimmed_body or all_empty_after_recency) else ""
    return norm_docs, ctx


def _guess_source_type(url: str, raw: Dict[str, Any]) -> str:
    kb_name = raw.get("knowledge_base") or raw.get("kb_name")
    if kb_name:
        return "knowledge_base"
    if raw.get("source_type"):
        return str(raw["source_type"])
    if "zsxq.com" in url or "wx.zsxq.com" in url or raw.get("channel") == "zsxq":
        return "forum"
    return "web"


def _guess_reliability(url: str, source_type: str, raw: Dict[str, Any]) -> str:
    if raw.get("reliability"):
        return str(raw["reliability"])
    if source_type == "knowledge_base":
        return "可靠"
    if source_type == "db":
        return "可靠"
    # 权威域名 → 可靠
    trusted_domains = (
        "sse.com.cn", "szse.cn", "csrc.gov.cn", "chinaclear.cn",
        "pbc.gov.cn", "stats.gov.cn", "gov.cn",
        "cninfo.com.cn",  # 巨潮资讯
        "news.cn", "xinhuanet.com", "people.com.cn",
    )
    for d in trusted_domains:
        if d in url:
            return "可靠"
    if source_type == "forum":
        return "待验证"
    return "待验证"


def _extract_keywords(title: str, content: str) -> List[str]:
    """简单关键词（停用词按中文常见单字/双字剔除）+ 4/6/8-gram 候选。

    用于 fallback：当模型完全没打 [N] 标记时，用关键词 overlap 动态分配引用。
    不依赖 jieba（怕额外重依赖未安装），退化为基于 4 字滑窗 + 去标点。
    """
    text = f"{title}\n{content}"
    # 去标点
    cleaned = re.sub(r"[\s，。！？、；：“”‘’（）《》【】…—\-,.!?;:\'\"()<>\[\]/\\]+", " ", text)
    tokens: List[str] = []
    # 2~6 字滑窗取候，去重按首次序
    seen = set()
    for size in (2, 3, 4):
        for m in re.finditer(r"[A-Za-z0-9\u4e00-\u9fff]+", cleaned):
            seg = m.group(0)
            if len(seg) < size:
                continue
            for i in range(0, len(seg) - size + 1):
                tok = seg[i:i + size]
                if tok in seen:
                    continue
                seen.add(tok)
                tokens.append(tok)
                if len(tokens) >= 64:
                    return tokens
    return tokens


# ======================================================================
# §3.5 引用 Fallback：模型回答完全没打角标时，按关键词/词法重叠动态分配
# ======================================================================
def build_focused_snippet(
    doc_content: str,
    *,
    focus_sentences: Optional[List[str]] = None,
    focus_keywords: Optional[List[str]] = None,
    max_chars: int = 100,
    halo_chars: int = 50,
    doc_title: str = "",
) -> str:
    """按「答案命中句中心窗口 ± halo_chars」抽取文档核心 snippet，硬上限 max_chars。
    - 优先：用 focus_sentences（引用了 [N] 的那句回答正文，去掉 [N]）在 doc_content 做
      最长子串 / 去重 8 字共子串匹配，找到首次命中偏移 -> 取前后 halo_chars 做中心窗口；
    - 其次：focus_keywords（doc._keywords 或用户 query 的关键词）同法定位；
    - 再次：若 doc 本身 ≤max_chars，直接原文；否则取首句/长句 + 末尾补省略号；
    - 最严格要求：最终 len() <= max_chars（不是 ≈，是硬截）。
    """
    raw = (doc_content or "").strip()
    if not raw:
        return ""
    if len(raw) <= max_chars:
        return raw
    # 1. 候选焦点词：focus_sentences -> 取每个≥8字的子串；focus_keywords 补齐
    needles: List[str] = []
    for fs in (focus_sentences or []):
        s = str(fs or "")
        # 去掉 [N] / [citation:N] / 空白 / 标点
        s = re.sub(r"\[\d+\]|\[citation:\d+\]|\s+", "", s)
        for size in (16, 12, 10, 8):
            for i in range(0, max(0, len(s) - size + 1)):
                cand = s[i:i + size]
                if len(cand) == size and cand not in needles:
                    needles.append(cand)
    for kw in (focus_keywords or []):
        k = str(kw or "").strip()
        if 4 <= len(k) <= 24 and k not in needles:
            needles.append(k)
    # 标题也算一个弱锚点（比如回答中提到了标题词）
    if doc_title and 4 <= len(doc_title) <= 24:
        needles.append(doc_title)
    # 2. 找首次命中（仅当调用方提供了 focus_sentences / focus_keywords / doc_title 任一"外部锚点"时才做）
    #    注意：不能在无锚点时用 doc._keywords(doc_title, raw)——那是文档自身滑窗 2-gram，100% 命中 raw 开头，
    #    会让中心窗口误偏到句首，退化成"头部硬截"，违背"focus=命中句中心窗口"的语义。
    any_anchor = bool((focus_sentences and any(True for _ in focus_sentences))
                      or (focus_keywords and any(True for _ in focus_keywords))
                      or doc_title)
    hit_start, hit_end = -1, -1
    if any_anchor:
        for needle in needles:
            p = raw.find(needle)
            if p >= 0:
                hit_start, hit_end = p, p + len(needle)
                break
        if hit_start < 0:
            # 兜底：用 doc._keywords 在 raw 里找第一个命中
            for kw in _extract_keywords(doc_title, raw)[:12]:
                p = raw.find(kw)
                if p >= 0:
                    hit_start, hit_end = p, p + len(kw)
                    break
    # 3. 中心窗口
    if hit_start >= 0:
        center = (hit_start + hit_end) // 2
        left = max(0, center - halo_chars)
        right = min(len(raw), center + halo_chars)
        window = raw[left:right]
        # 若仍超 max_chars：再以 hit 为中心向内收缩
        if len(window) > max_chars:
            pad = (len(window) - max_chars) // 2
            shift_l = left + pad
            shift_r = right - pad
            if shift_r - shift_l < max_chars:
                shift_r = min(len(raw), shift_l + max_chars)
            window = raw[shift_l:shift_r]
    else:
        # 完全没命中：优先第一句（句号前），没句号就硬截头部
        first_dot = max(raw.find("。"), raw.find("\n"))
        if 0 < first_dot <= max_chars:
            window = raw[:first_dot + 1]
        else:
            window = raw[:max_chars]
    window = window.strip()
    # 4. 硬兜底：max_chars
    if len(window) > max_chars:
        window = window[:max_chars - 1] + "…"
    # 5. 头尾非完整字符（非句首/非空白时补「…」前缀提示是窗口）
    if hit_start >= 0 and left > 0 and not window.startswith(("。", "！", "？", "\n", "；", "，", "、")):
        window = "…" + window[1:] if window.startswith("…") else "…" + window
        if len(window) > max_chars:
            window = window[:max_chars - 1] + "…"
    return window


__all__ = ["build_focused_snippet"]  # 方便 bus / main_agent / tool 引用


def assign_citations_by_overlap(
    sentence: str,
    docs: List[CitationDocument],
    *,
    top_k: int = 2,
    min_score: float = 0.03,
) -> List[int]:
    """给定一个回答句子，返回最可能引用的文档编号（按重叠分数降序）。

    算法：
      score(doc) = (命中 doc._keywords 的 tokens / doc._keywords 总量 的 Jaccard 近似)
                 + (sentence 中命中 doc.content 长度 ≥ 8 的长共子串数 × 0.1)
    返回 [] 表示不分配。
    """
    if not sentence or not docs:
        return []
    # sentence tokens：同样滑窗
    s_set = set()
    for size in (2, 3, 4):
        for m in re.finditer(r"[A-Za-z0-9\u4e00-\u9fff]+", sentence):
            seg = m.group(0)
            if len(seg) < size:
                continue
            for i in range(0, len(seg) - size + 1):
                s_set.add(seg[i:i + size])
    if not s_set:
        return []

    scored: List[Tuple[float, int]] = []
    for doc in docs:
        dk = set(doc._keywords)
        if not dk:
            continue
        inter = len(dk & s_set)
        union = len(dk | s_set)
        jaccard = inter / union if union else 0.0
        # 长共串加分：找 sentence 中直接出现的 ≥ 8 字的 doc.content 子片段
        bonus = 0.0
        if doc.content:
            # 逐句匹配：doc.content 中所有 ≥ 8 字且句末带标点的片段
            for sub in re.findall(r"[\u4e00-\u9fffA-Za-z0-9]{8,}", doc.content):
                if sub and sub in sentence:
                    bonus += 0.12
        total = jaccard + bonus
        if total >= min_score:
            scored.append((total, doc.index))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [idx for _, idx in scored[:top_k]]


# ======================================================================
# §3.6 Prompt Helper：给 DeepSeek / Qwen 注入"先思考后引用"规范
# ======================================================================

def patch_system_prompt_require_reasoning_and_citations(original: str) -> str:
    """在 system prompt 末尾追加两条强制要求：
    (1) 先输出 <think> 思考步骤（意图、检索、综合、风险四段），再输出最终正文。
    (2) 回答正文严格使用 [N] 角标引用上文 [citation:N] 所注入的参考文档，
        禁止使用「(N)」「[citation:N]」「[[N]]」等其他格式。
    """
    if not original:
        original = ""
    suffix_lines = [
        "",
        "",
        "【输出格式强制规范，违反将导致结果作废】",
        "1) 推理过程必须先输出，必须包裹在 <think>...</think> 标签内部。"
        "思考要包含四段：(a) 意图分类 (b) 检索计划 (c) 综合策略 (d) 风险核查。"
        "严禁省略任何一段；严禁把思考内容写到 <think> 标签之外。",
        "2) 最终正文必须在 <think> 结束之后开始，所有提到参考文档的句子，"
        "都必须在句末或事实词之后使用 [N] 角标引用，其中 N 是参考文档前面的编号。"
        "其他格式（例如 (N)、[[N]]、[citation:N]、或「根据参考文档 1」）全部禁止使用。"
        "一句话可以引用多个来源，例如：「茅台毛利率超过 90% [1][3]，同比小幅上升 [4]。」",
        "3) 最终正文末尾必须追加风险声明：「⚠️ 以上信息来自互联网公开资料，仅供参考，"
        "不构成投资建议。投资有风险，入市需谨慎，盈亏自负。」",
        "4) 【检索结果摘要强制】当给出「检索汇总/新闻速览/参考文档摘要」类输出时：",
        "   - 若某条检索结果内容是空、占位文本（「暂无/未找到/N/A/无」），或无法提取有效正文——**不要输出该条**，也不要写「未找到相关数据」字样占位。",
        "   - 摘要要**极度精简**：每条最多 1 句要点，全文（不含标题/链接/角标）**不得超过 400 个中文字**；超出将按末尾条目整条省略。",
        "5) 【小节并列禁用】最终正文最多只能有 1 个主分析/信息汇总小节；",
        "   严格禁止同时输出『一、检索结果汇总 …』紧接『二、XX市场最新行情与信息汇总报告 …』或任何『汇总报告』、『XX汇总』类两节并列。",
        "   若存在不同来源（网络检索、知识星球本地分析、工具研报）的信息，必须横向合并为**同一个单节**：",
        "   标题统一使用「一、检索结果与最新行情信息汇总」，要点按逻辑组内列点（不再分节）；相同要点去重，不要重复两次。",
        "6) 【时效性窗口强制（用户规则）】所有参考文档的时效性：默认只使用最近 1 个月（30 天）内发布/更新的新闻/讨论/研报；",
        "   若近 1 个月无任何命中，服务端已自动扩大到最近 3 个月（90 天）；超期条目已从上下文和来源池中剔除。",
        "   严禁基于已被剔除的超期内容输出任何观点；输出中不要写「未找到相关数据/N/A」等占位，若无结果则写「近 3 个月无相关检索结果」。",
    ]
    return original.rstrip() + "\n".join(suffix_lines)


# ======================================================================
# §3.7 便捷门面：一次性 ingest 原始流式文本（含 <think> 剥离 + 引用归一）
# ======================================================================

def create_stream_pipeline(
    on_reasoning_delta: Optional[Callable[[str], None]] = None,
    on_delta: Optional[Callable[[str], None]] = None,
    on_reasoning_segment_done: Optional[Callable[[str], None]] = None,
) -> Tuple[ThinkTagSplitter, Callable[[str, bool], List[Tuple[Optional[str], str, List[int]]]]]:
    """
    高层包装：返回 (splitter, ingest_fn)，ingest_fn(raw_text, is_final) 返回
      list of (type, normalized_text, citations)；type ∈ {reasoning,delta,reasoning_end,reasoning_start}。
    用法见 agent/ 层真实调用。
    """
    splitter = ThinkTagSplitter()
    accumulated_reasoning: List[str] = []

    def ingest(raw: str, is_final: bool) -> List[Tuple[Optional[str], str, List[int]]]:
        chunks = splitter.ingest(raw or "", is_final=is_final)
        out: List[Tuple[Optional[str], str, List[int]]] = []
        for c in chunks:
            if c.type == "reasoning":
                accumulated_reasoning.append(c.text)
                if on_reasoning_delta:
                    on_reasoning_delta(c.text)
                out.append(("reasoning", c.text, []))
            elif c.type == "reasoning_start":
                out.append(("reasoning_start", "", []))
            elif c.type == "reasoning_end":
                seg_text = "".join(accumulated_reasoning)
                accumulated_reasoning.clear()
                if on_reasoning_segment_done:
                    on_reasoning_segment_done(seg_text)
                out.append(("reasoning_end", seg_text, []))
            elif c.type == "delta":
                norm_text, cits = extract_citations_from_delta(c.text)
                if on_delta:
                    on_delta(norm_text)
                out.append(("delta", norm_text, cits))
        return out

    return splitter, ingest
