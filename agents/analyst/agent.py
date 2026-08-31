from agent.subagents.knowledge_base_agent import knowledge_base_agent
from agent.subagents.database_query_agent import database_query_agent
from agent.subagents.network_search_agent import network_search_agent
# 使用 SQLite 持久化 checkpointer：重启后对话历史不丢失，按 thread_id (= session_id) 隔离
# 注意：agent 用 astream (异步)，必须用 AsyncSqliteSaver；同步版 SqliteSaver 不支持 async 方法
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

# main_agent tool导入
from tools.markdown_tools import generate_markdown
from tools.pdf_tools import convert_md_to_pdf
from tools.upload_file_read_tool import read_file_content

from deepagents import create_deep_agent

from agent.llm import model
from agent.prompts import main_agent_content, format_prompt

# Loop Engineering — SKILL 自动加载（按用户提问关键词注入专业规范）
from agent.skill_manager import get_skill_manager

# Context Engineering 记忆管理（滑窗 + 摘要压缩 + 优先级排序）
from agent.memory_manager import get_memory_manager
# Progressive Tool Disclosure 渐进式工具披露（两阶段路由：零Schema→选子集披露）
from agent.tool_router import reset_route_state, set_ptd_query, reset_ptd_query
# Layer2: Context Engineer — 时效性去重、来源可靠性甄别、2000字精简裁剪
from agent.context_engineer import get_context_engineer
# 知识星球股票搜索工具（按股票名搜索研报/小作文/新闻 + Qwen8B分析汇总）
from tools.zsxq_tool import search_zsxq_by_stock
# Layer3: Trace 可观测性 — 记录每轮 input/output/tool_calls/token/latency
from agent.trace import get_trace_logger
# Layer3: Feedback Handler — 用户质疑反驳检测 + 错误学习记忆
from agent.feedback_handler import get_feedback_handler
# Layer3: Maker-Checker — 输出质量校验（数据一致性/风险声明/幻觉检测）
from agent.maker_checker import get_maker_checker
# Layer3: 可靠性组件 — 熔断器、错误分类、降级链、幻觉防护、SLO 监控
from agent.circuit_breaker import get_circuit_registry
from agent.error_classifier import get_error_classifier, ErrorQuadrant
from agent.degradation_chain import get_degradation_chain, DegradationTier
from agent.hallucination_guard import get_hallucination_guard
from agent.slo_monitor import get_slo_monitor, SLOEvent

from config.constants import (
    SLO_MAX_TASK_SEC,
    MAIN_AGENT_RECURSION_LIMIT,
    MAIN_AGENT_VERBOSE_MAX_LEN,
    MAIN_AGENT_VERBOSE_TOOL_RESULT_MAX_LEN,
    MAIN_AGENT_SESSION_HISTORY_LIMIT_DEFAULT,
    MAIN_AGENT_MEMORY_CONTEXT_WARN_LEN,
)

from api.monitor import monitor
import re
import asyncio
import uuid
import shutil
import time
import os
from contextvars import ContextVar
from pathlib import Path

# §3 / §4 协议适配与编排路由层新增引用（adapter）
try:
    from adapter import (
        build_citation_context,
        patch_system_prompt_require_reasoning_and_citations,
        assign_citations_by_overlap,
        normalize_citation_markers,
        ThinkTagSplitter,
    )
    _HAS_ADAPTER = True
except Exception as _e_adapter:
    # 导入失败不应阻塞启动（比如 adapter/__init__.py 被误删），给打印告警
    print(f"[main_agent] adapter 导入失败（断点续传/引用注入降级运行）: {_e_adapter}")
    _HAS_ADAPTER = False

from api.context import set_session_context, reset_session_context, set_thread_context

from langchain_core.messages import AIMessage

# 【工作环境指令】及后续"规则/工作目录"段落的过滤正则
# 匹配：换行 + 缩进空格 + 【工作环境指令】开始直到结尾的整段内容（包含、规则1-4等）
_HIDE_PROMPT_RE = re.compile(
    r"(?:\r?\n)\s*【工作环境指令】[\s\S]*$"
)
# 记忆上下文前缀过滤正则：匹配 "===== 以下是你与用户的历史对话记忆" 到 "===== 历史对话记忆结束" 整段
_HIDE_MEMORY_PREFIX_RE = re.compile(
    r"===== 以下是你与用户的历史对话记忆[\s\S]*?===== 历史对话记忆结束，请基于以上记忆理解上下文后回答用户当前问题 =====\s*【用户当前问题】：\s*"
)
# 错误规避前缀过滤正则：匹配 Layer3 FeedbackHandler 注入的 "[错误规避]" 段落
_HIDE_FEEDBACK_PREFIX_RE = re.compile(
    r"\[错误规避\][\s\S]*?(?======|\n【|用户当前问题)"
)

def _strip_hidden_instructions(content: str) -> str:
    """把拼接在用户消息中的内部规则段剥离，返回纯净的用户内容。

    剥离以下非用户输入的内部前缀：
    1. [错误规避] FeedbackHandler 注入的错误规避提示
    2. ===== 历史对话记忆 ===== Context Engineering 注入的记忆上下文
    3. 【工作环境指令】 路径/文件规则
    """
    if not content:
        return ""
    s = content
    # 1. 先剥离记忆上下文前缀（从开头到"【用户当前问题】："标记）
    s = _HIDE_MEMORY_PREFIX_RE.sub("", s)
    # 2. 剥离错误规避前缀
    s = _HIDE_FEEDBACK_PREFIX_RE.sub("", s)
    # 3. 剥离工作环境指令
    s = _HIDE_PROMPT_RE.sub("", s)
    return s.strip()


# ======================================================================
# §4 编排路由层助手（真实 reasoning 事件 + 引用归一 + fallback 动态引用）
# ======================================================================

_STRUCTURED_ITEMS_RE = re.compile(
    r"<structured>\s*(\{.*?\})\s*</structured>", re.I | re.S
)
_JSON_STRUCT_ITEMS_RE = re.compile(
    r'"_structured_items"\s*:\s*(\[[^\]]*\])', re.S
)


def _extract_structured_items_from_tool_texts(tool_texts: list) -> list:
    """从 _tool_result_texts 累积文本中提取所有工具注入的 `_structured_items`。

    兼容 3 种来源：
      1) Tavily 返回 dict 序列化的 result 字符串（包含 "_structured_items" JSON 片段）
      2) IMA 搜索结果末尾 "<structured>...</structured>" JSON 封装
      3) ZSXQ 搜索工具（search_zsxq_by_stock）把 items 塞进结果文本里的 _structured_items 字段
    """
    items: list = []
    import json as _json
    for txt in (tool_texts or []):
        if not isinstance(txt, str):
            continue
        # 先找 <structured>...</structured> 整体封装（IMA 模式）
        found_wrapped = _STRUCTURED_ITEMS_RE.findall(txt)
        raw_jsons_lists: list = []
        for w in found_wrapped:
            try:
                obj = _json.loads(w)
                items_list = obj.get("_structured_items") or []
                if isinstance(items_list, list):
                    raw_jsons_lists.append(items_list)
            except Exception:
                pass
        # 再找 "_structured_items": [...] JSON 片段（Tavily/ZSXQ 模式）
        for m in _JSON_STRUCT_ITEMS_RE.finditer(txt):
            try:
                arr = _json.loads(m.group(1))
                if isinstance(arr, list):
                    raw_jsons_lists.append(arr)
            except Exception:
                pass
        for arr in raw_jsons_lists:
            for raw in arr:
                if not isinstance(raw, dict):
                    continue
                # 规整化：把"未显式标 reliability"的条目按 source_type 补默认值
                if not raw.get("reliability"):
                    st = str(raw.get("source_type") or "")
                    if st in ("knowledge_base", "db"):
                        raw["reliability"] = "可靠"
                    elif st == "forum":
                        raw["reliability"] = "待验证"
                    else:
                        raw["reliability"] = raw.get("reliability") or "待验证"
                items.append(raw)
    return items


def _emit_model_cot_and_normalize_citations(
    *,
    thread_id: str,
    final_content: str,
    tool_result_texts: list,
) -> None:
    """§4.4 编排：把模型最终文本的 <think> 拆成独立 reasoning 事件；把正文引用角标归一；
    未标记引用时按"句子 x 文档" Jaccard overlap 动态分配引用。
    所有修改 done in-place on `final_content`（引用角标替换）。
    """
    # ----- 1. 把检索文档统一编号（1..N）并在 ThreadState 里注册 citation_meta -----
    from api.stream_bus import get_stream_bus_sync
    bus = get_stream_bus_sync()

    raw_docs = _extract_structured_items_from_tool_texts(tool_result_texts)
    citation_docs: list = []
    if raw_docs:
        doc_list, ctx_block = build_citation_context(raw_docs, start_index=1)
        citation_docs = doc_list
    else:
        doc_list = []

    # ----- 2. <think> 跨 chunk 状态机（非流式场景直接整段 ingest）-----
    splitter = ThinkTagSplitter()
    chunks = splitter.ingest(final_content or "", is_final=True)
    reasoning_text_parts = []
    body_text_parts = []
    for c in chunks:
        if c.type == "reasoning":
            reasoning_text_parts.append(c.text)
        elif c.type == "delta":
            body_text_parts.append(c.text)
    reasoning_full = "".join(reasoning_text_parts)
    body_full = "".join(body_text_parts)

    # ----- 3. 先 publish reasoning（独立 reasoning type=model_coT 事件）-----
    if reasoning_full.strip():
        # 思考可能很长：分段不超过 2000 字，按"自然段落"切
        rparts = _split_into_paragraphs(reasoning_full, max_chars=2000)
        for i, part in enumerate(rparts, 1):
            title = f"综合推理（模型 Chain-of-Thought）{i}/{len(rparts)}"
            bus.ev_reasoning(thread_id, title=title, content=part, stage="model_coT")
        bus.ev_progress(thread_id, stage="输出最终答案", percent=92,
                        detail=f"思考链解析完成，共 {len(rparts)} 段")

    # ----- 4. 正文引用角标归一化 + 同时在 state 中"已推送的 citation_meta"缓存 -----
    if body_full.strip():
        normalized_body, cits_ordered = normalize_citation_markers(body_full)
    else:
        normalized_body, cits_ordered = body_full, []

    # ----- 5. 如果模型完全没打角标（cits_ordered 空）→ 按 overlap fallback -----
    used_indices: list = []
    if citation_docs and (not cits_ordered):
        # 按句子切分（以句号/问号/叹号结尾，最多 150 字的语义块）
        for sentence in _split_into_sentences(normalized_body, max_chars=220):
            sent_indices = assign_citations_by_overlap(sentence, citation_docs, top_k=1, min_score=0.05)
            if not sent_indices:
                continue
            for idx in sent_indices:
                if idx not in used_indices:
                    used_indices.append(idx)
            # 把 [N] 角标挂到该句末尾（角标数量最多 3 个，避免刷屏）
            tag = "".join(f"[{idx}]" for idx in sent_indices[:3])
            if tag and tag not in sentence:
                # 找到句尾标点位置，把角标插到标点前
                inserted = False
                for pm in ("。", "！", "？", ".", "!", "?", "；", ";", "\n"):
                    if sentence.endswith(pm):
                        normalized_body = normalized_body.replace(sentence, sentence[:-len(pm)] + tag + pm, 1)
                        inserted = True
                        break
                if not inserted:
                    normalized_body = normalized_body.replace(sentence, sentence + tag, 1)

    # ----- 6. 把 normalized_body + 原 <think>（若存在）重新写回 final_content -----
    # 注意：思考不再保留在正文里，因为已经以 reasoning 事件单独推给前端了。
    # 但如果用户原本就有纯思考显示需求，这里保留一个"空 <think/>"的占位不破坏原调用方预期（可删）。
    final_content_out = normalized_body

    # --- 用户新增硬约束："检索结果汇总要精简，最多不超过400字；空/无效条目不输出" ---
    # (prompt 已在 adapter 两处强制 + build_citation_context 前置空过滤+摘要≤400)
    # 这里是代码层最后一道兜底：若检测到"检索汇总/新闻速览/参考摘要"类段落仍超 400 字 →
    # 按句号整条整句裁剪，不破坏角标与句子完整性。
    _SEARCH_SUMMARY_MAX = 400  # 与 adapter build_citation_context(summary_block_max) 保持一致
    _SECTION_HINTS = ("检索结果汇总", "检索汇总", "新闻速览", "参考摘要", "核心观点汇总",
                      "参考文档摘要", "新闻摘要", "要点汇总")
    try:
        def _trim_summary_paragraph(text: str, max_len: int) -> str:
            """按"句子"整句裁剪到 max_len（不含 risk_warn），保证末尾句完整。"""
            if len(text) <= max_len:
                return text
            import re as _re
            # 按句号/问号/叹号/分号/换行切句，保留句末标点
            sents = _re.findall(r"[^。！？!?；;\n]*[。！？!?；;\n]?", text)
            sents = [s for s in sents if s]
            out_parts: list = []
            acc = 0
            for s in sents:
                if acc + len(s) <= max_len:
                    out_parts.append(s)
                    acc += len(s)
                else:
                    break
            if not out_parts:
                # 连一句都装不下 → 按字硬切并补省略号
                return text[:max(1, max_len - 1)] + "…"
            joined = "".join(out_parts)
            if len(joined) > max_len:
                joined = joined[:max(1, max_len - 1)] + "…"
            return joined

        def _is_summary_related(t: str) -> bool:
            low = t.strip()[:12]
            return any((h in t) for h in _SECTION_HINTS) or (
                # 形如 "一、核心观点" "1. 新闻摘要" 的小节名
                sum(1 for h in ("汇总", "摘要", "要点", "速览") if h in low) >= 1
            )

        # 先按双换行分段；段首若匹配 summary 关键词 → 对"该段正文"做 400 字硬截断
        raw_paras = re.split(r"(\n{2,})", final_content_out or "")
        fixed_segments: list = []
        i = 0
        while i < len(raw_paras):
            seg = raw_paras[i]
            if not seg or "\n" in seg and set(seg) == {"\n"}:
                fixed_segments.append(seg)
                i += 1
                continue
            if _is_summary_related(seg):
                # 对段落正文（去掉首行标题/数字序号后）做长度约束
                lines = seg.split("\n", 1)
                head = lines[0]
                body = lines[1] if len(lines) > 1 else ""
                body_len_target = max(120, _SEARCH_SUMMARY_MAX - len(head) - 8)
                fixed_body = _trim_summary_paragraph(body, body_len_target)
                if fixed_body and fixed_body != body:
                    fixed_body = fixed_body.rstrip() + "（摘要已精简至≤{}字）".format(_SEARCH_SUMMARY_MAX)
                fixed_segments.append(head + ("\n" + fixed_body if fixed_body else ""))
            else:
                fixed_segments.append(seg)
            i += 1
        final_content_out = "".join(fixed_segments)

        # 终极兜底：若整段正文（不含风险声明）仍 > 2 * 400（即模型连续输出多段），按句整体压缩
        _RISK_WARN_INLINE = (
            "⚠️ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。"
            "投资有风险，入市需谨慎，盈亏自负。"
        )
        if _RISK_WARN_INLINE in final_content_out:
            main_body, _, warn_part = final_content_out.partition(_RISK_WARN_INLINE)
        else:
            main_body, warn_part = final_content_out, ""
        if len(main_body) > 2 * _SEARCH_SUMMARY_MAX:
            main_body = _trim_summary_paragraph(main_body, 2 * _SEARCH_SUMMARY_MAX)
            main_body = main_body.rstrip() + "（正文已按精简规则压缩）"
        final_content_out = (main_body + ("\n\n" + warn_part if warn_part else "")).strip()

        # --------- 新增：相邻双汇总标题去重合并（解决黄金问题冗余输出） ---------
        # 现象：用户问『黄金期货下跌，黄金ETF是否值得持有1-6月等待5000美金卖出』时，
        # 主模型有时会并列输出「一、检索结果汇总 …」紧接着「二、XX市场最新行情
        # 与信息汇总报告 …」两段（它们是不同源：第一段=网络检索摘要；
        # 第二段=zsxq本地分析或工具报告二次复述）。按用户要求必须合并为一条，
        # 不能让两节在同一 assistant 消息并列出现。
        try:
            import re as _re2
            _SUMMARY_HEAD_KW = ("检索结果汇总", "检索汇总", "新闻速览",
                                "核心观点汇总", "参考摘要", "新闻摘要", "要点汇总")
            _REPORT_HEAD_KW_SUFFIX = ("最新行情与信息汇总报告", "市场信息汇总报告",
                                      "行情信息汇总报告", "汇总报告")

            def _strip_heading_prefix(line: str) -> str:
                """去掉 一/二/三/1.2.3 等编号前缀，返回纯标题文本。"""
                return _re2.sub(
                    r"^\s*(?:[一二三四五六七八九十百]+|[\d]+)[\.、\s)]+\s*",
                    "", line.strip(),
                ).strip()

            def _is_summary_head(line: str) -> bool:
                s = _strip_heading_prefix(line)
                return any((kw in s) for kw in _SUMMARY_HEAD_KW)

            def _is_report_head_after_summary(line: str) -> bool:
                s = _strip_heading_prefix(line)
                return any(s.endswith(suf) or suf in s for suf in _REPORT_HEAD_KW_SUFFIX)

            def _norm_bullet(line: str) -> str:
                l = line.strip()
                l = _re2.sub(r"^\s*[-*•·]\s+", "", l)
                l = _re2.sub(r"^\s*(?:[一二三四五六七八九十百]+|[\d]+)[\.、\s)]+\s*", "", l)
                return l.strip()

            _lines = final_content_out.splitlines(keepends=True)
            # 扫描第一段"汇总类"标题所在的行号
            i = 0
            n = len(_lines)
            head1_idx = -1
            while i < n:
                if _is_summary_head(_lines[i]):
                    head1_idx = i
                    break
                i += 1
            if head1_idx >= 0:
                # 从 head1_idx 下一行开始扫：允许若干正文行（第1节内容），
                # 遇到下一个"小节标题"（Markdown # 标题 / 编号开头且 ≤24 字或含标题关键词）
                # 就判断；若是"汇总报告"类 → 执行合并。
                # 注：正文要点『1. xxxx』（一般 25~50 字）需要排除：长度阈值调到 ≤24
                # 且/或具备报告/汇总/建议等关键词才判为节标题。
                j = head1_idx + 1
                head2_idx = -1
                _END_OF_HEAD_SUFFIXES = ("结论", "建议", "分析", "策略", "评估",
                                         "操作策略", "策略建议", "风险提示",
                                         "交易建议", "买卖建议", "操作建议")
                while j < n:
                    lj = _lines[j].rstrip("\r\n")
                    if lj.strip() == "":
                        j += 1
                        continue
                    stripped = lj.strip()
                    numbered = bool(
                        _re2.match(
                            r"^(?:[一二三四五六七八九十百]+|[\d]+)[\.、\s)]",
                            stripped,
                        )
                    )
                    md_head = stripped.startswith("#")
                    # 节标题一般很短；正文要点『1. xxxx句子。』一般 25~50 字
                    no_body_len_ok = len(stripped) <= 24
                    # 「要点句通常带句号/问号/角标 [N]」——节标题（如『二、交易建议』）
                    # 很少以句号/问号/引用角标收尾，用这个信号帮短编号行去假阳性。
                    _punct_tail = stripped.endswith(("。", "！", "？", "!", "?", "、",
                                                     "）", ")", "]", "…", ";", "；"))
                    looks_titlish_no_punct_tail = no_body_len_ok and not _punct_tail
                    has_keyword = (
                        _is_report_head_after_summary(lj)
                        or _is_summary_head(lj)
                        or any(
                            _strip_heading_prefix(stripped).endswith(s)
                            or s in _strip_heading_prefix(stripped)
                            for s in _END_OF_HEAD_SUFFIXES
                        )
                    )
                    looks_like_head = (
                        md_head
                        or (numbered and has_keyword)                # 编号 + 标题关键词 → 节标题
                        or (numbered and looks_titlish_no_punct_tail)  # 编号 + 短 + 无句尾标点 → 标题
                        or (no_body_len_ok and has_keyword)          # 短 + 关键词 → 标题
                    )
                    if looks_like_head:
                        if _is_report_head_after_summary(lj):
                            head2_idx = j
                        break  # 下一节已经出现，结束扫描
                    j += 1
                if head2_idx >= 0:
                    # head1 正文范围 = (head1_idx+1) 到 (head2_idx-1)；head2 正文范围 = head2_idx+1 到段尾或下一head
                    seg1_lines = _lines[head1_idx + 1: head2_idx]
                    # 找到 head2 正文结束（下一个标题 或 EOF）
                    k = head2_idx + 1
                    while k < n:
                        lk = _lines[k].rstrip("\r\n")
                        if lk.strip() == "":
                            k += 1
                            continue
                        stripped_k = lk.strip()
                        numbered_k = bool(
                            _re2.match(
                                r"^(?:[一二三四五六七八九十百]+|[\d]+)[\.、\s)]",
                                stripped_k,
                            )
                        )
                        md_head_k = stripped_k.startswith("#")
                        no_body_len_ok_k = len(stripped_k) <= 24
                        _punct_tail_k = stripped_k.endswith(("。", "！", "？", "!", "?", "、",
                                                              "）", ")", "]", "…", ";", "；"))
                        looks_titlish_no_punct_tail_k = no_body_len_ok_k and not _punct_tail_k
                        has_keyword_k = (
                            _is_report_head_after_summary(lk)
                            or _is_summary_head(lk)
                            or any(
                                _strip_heading_prefix(stripped_k).endswith(s)
                                or s in _strip_heading_prefix(stripped_k)
                                for s in _END_OF_HEAD_SUFFIXES
                            )
                        )
                        looks_like_head_k = (
                            md_head_k
                            or (numbered_k and has_keyword_k)
                            or (numbered_k and looks_titlish_no_punct_tail_k)
                            or (no_body_len_ok_k and has_keyword_k)
                        )
                        if looks_like_head_k:
                            break
                        k += 1
                    seg2_end = k
                    seg2_lines = _lines[head2_idx + 1: seg2_end]
                    tail_lines = _lines[seg2_end:]

                    # 合并去重 bullet
                    seen = set()
                    merged: list = []
                    for ln in seg1_lines:
                        txt = ln.rstrip("\r\n")
                        if txt.strip() == "":
                            continue
                        key = _norm_bullet(txt)
                        if key and key not in seen:
                            seen.add(key)
                            merged.append(txt)
                    for ln in seg2_lines:
                        txt = ln.rstrip("\r\n")
                        if txt.strip() == "":
                            continue
                        key = _norm_bullet(txt)
                        if key and key not in seen:
                            seen.add(key)
                            merged.append(txt)

                    new_block_lines = ["一、检索结果与最新行情信息汇总"] + merged
                    # 若 merged 过多，按 400 字整句裁剪（遵循前一轮 ≤400 字规则）
                    _joined = "\n".join(new_block_lines)
                    if len(_joined) > 520:
                        _joined = _trim_summary_paragraph(_joined, 480)
                        _joined = _joined.rstrip() + "（两报告已合并并精简）"
                    # 重建 final_content_out
                    pre_lines = _lines[:head1_idx]
                    rebuilt = "".join(pre_lines).rstrip()
                    if rebuilt:
                        rebuilt += "\n\n"
                    rebuilt += _joined
                    tail_joined = "".join(tail_lines).lstrip("\r\n")
                    if tail_joined:
                        rebuilt += "\n\n" + tail_joined
                    final_content_out = rebuilt
        except Exception:
            # 兜底：合并失败时回退，不要中断主链路
            pass
    except Exception:
        # 兜底处理出错不要影响主链路
        pass

    # 风险声明兜底：确保末尾有 AGENTS.md 规定的声明（模型即使遗漏也要补）
    _RISK_WARN = (
        "⚠️ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。"
        "投资有风险，入市需谨慎，盈亏自负。"
    )
    if _RISK_WARN not in final_content_out:
        if final_content_out.strip() and not final_content_out.rstrip().endswith(("。", "！", "？", "\n", ".")):
            final_content_out += "。"
        if final_content_out.strip():
            final_content_out = final_content_out.rstrip() + "\n\n" + _RISK_WARN
        else:
            final_content_out = _RISK_WARN

    # ----- 7. 注册 citation_meta（用于"悬停卡片"）并下发一次 citation_meta + delta（最终文本整段） -----
    # 7.1 给 bus.set_citation_meta 的"聚焦 snippet 中心窗口"提供答案命中句 hint
    try:
        from api.stream_bus import get_stream_bus_sync as _gbus_sync
        _state = bus.get_thread_state(thread_id)
        _sents = _split_into_sentences(normalized_body, max_chars=220)
        _state._final_sentences_hint = list(_sents)[:12]  # 最多 12 句就够匹配
    except Exception:
        pass
    all_indices_used = list(dict.fromkeys(cits_ordered + used_indices))  # 有序去重
    for doc in citation_docs:
        idx = doc.index
        # 传 doc.content 原文，set_citation_meta 内部会用 CITATION_SNIPPET_MAX_CHARS（默认100）
        # + 上面的 _final_sentences_hint 做"命中句中心窗口"聚焦，不是头部硬截。
        bus.set_citation_meta(
            idx, title=doc.title, url=doc.url,
            source_type=doc.source_type, reliability=doc.reliability,
            channel=doc.channel, published_at=doc.published_at,
            snippet=doc.content,
        )
    # 未显式打标的 fallback 引用（used_indices）若不在 citation_docs 里，也尝试从 source_pool 取
    state = bus.get_thread_state(thread_id)
    items_to_push: list = []
    for idx in sorted(set(all_indices_used)):
        if idx <= 0:
            continue
        meta = state.get_citation_meta(idx)
        if meta is None:
            continue
        items_to_push.append(meta)
    if items_to_push:
        bus.ev_citation_meta(thread_id, items=[
            type(items_to_push[0])(**{k: getattr(it, k) for k in it.__dataclass_fields__})
            for it in items_to_push
        ])

    # 8. 作为 ev_delta 最终整段（用于 final_text_buffer 聚合与 SSE 断点续传）
    if final_content_out:
        bus.ev_delta(thread_id, text=final_content_out, is_reasoning=False)


def _split_into_paragraphs(text: str, max_chars: int = 2000) -> list:
    """按"\n\n"分段；每段再次按 "\n" + 纯长句按句号切；截断到 max_chars。"""
    if not text:
        return []
    paras = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    result = []
    for para in paras:
        if len(para) <= max_chars:
            result.append(para)
            continue
        # 按 "\n" 子段
        sub = [s.strip() for s in para.split("\n") if s.strip()]
        acc = ""
        for s in sub:
            if len(acc) + len(s) + 1 <= max_chars:
                acc = (acc + "\n" + s).strip()
            else:
                if acc:
                    result.append(acc)
                # 单句仍超过 → 按句号硬切
                if len(s) > max_chars:
                    for chunk in _chunk_by_punctuation(s, max_chars):
                        result.append(chunk)
                    acc = ""
                else:
                    acc = s
        if acc:
            result.append(acc)
    return result or [text[:max_chars]]


def _split_into_sentences(text: str, max_chars: int = 220) -> list:
    """按句号/问号/叹号切句（保留标点在句尾）。用于引用 fallback：每句做 overlap 打分。"""
    if not text:
        return []
    sentences = []
    start = 0
    for i, ch in enumerate(text):
        if ch in "。！？.!?；;\n":
            if i - start + 1 >= 4 or ch == "\n":
                sent = text[start:i + 1].strip()
                if sent:
                    # 超长句按 max_chars 再切，避免 overlap 得分发散
                    if len(sent) > max_chars:
                        for c in _chunk_by_punctuation(sent, max_chars):
                            sentences.append(c)
                    else:
                        sentences.append(sent)
                start = i + 1
    if start < len(text):
        tail = text[start:].strip()
        if tail:
            if len(tail) > max_chars:
                sentences.extend(_chunk_by_punctuation(tail, max_chars))
            else:
                sentences.append(tail)
    return [s for s in sentences if len(s.strip()) >= 6]


def _chunk_by_punctuation(text: str, max_chars: int) -> list:
    """仍然过长的句子：按逗号/顿号等再切；最后兜底按 max_chars 硬切。"""
    if not text:
        return []
    out = []
    buf = ""
    for i, ch in enumerate(text):
        buf += ch
        if len(buf) >= max_chars and ch in (",", "，", "、", " "):
            out.append(buf.strip())
            buf = ""
    if buf.strip():
        out.append(buf.strip())
    if not out:
        return [text[:max_chars]]
    merged = []
    acc = ""
    for seg in out:
        if len(acc) + len(seg) + 1 <= max_chars:
            acc = (acc + seg).strip() if not acc else acc + seg
        else:
            if acc:
                merged.append(acc[:max_chars])
            acc = seg
    if acc:
        merged.append(acc[:max_chars])
    return merged


# ======================================================================
# Actor Model 适配：由 server.py lifespan 注入句柄
# 保留原调用签名不变，内部改为向 Actor 邮箱发消息
# ======================================================================
from typing import Any as _Any

_cb_actor: _Any = None
_slo_actor: _Any = None
_SLO_MSG_RECORD = "record_event"


def _set_cb_actor(actor: _Any) -> None:
    """注入 CircuitBreakerActor（circuit_breaker.py 内同步也有一份桥接，这里主要用于未来扩展）。"""
    global _cb_actor
    _cb_actor = actor


def _set_slo_actor(actor: _Any) -> None:
    """注入 SLOMonitorActor。get_slo_monitor() 的 record_event 会优先通过 Actor 消息。"""
    global _slo_actor
    _slo_actor = actor
    # 给 slo_monitor 模块也打个补丁：record_event 有 Actor 就先 Actor 后本地（幂等双写）
    import agent.slo_monitor as _slo_mod
    _orig_record = _slo_mod.SLOMonitor.record_event

    def _bridged_record_event(self: _slo_mod.SLOMonitor, event: _slo_mod.SLOEvent) -> None:
        _orig_record(self, event)  # 本地 threading.Lock 先写
        if _slo_actor is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if loop.is_running():
            try:
                payload = {
                    "session_id": event.session_id,
                    "timestamp": event.timestamp,
                    "success": event.success,
                    "latency_sec": event.latency_sec,
                    "token_count": getattr(event, "token_count", 0),
                    "final_tier": event.final_tier,
                    "hit_hard_limit": event.hit_hard_limit,
                    "hallucination_passed": event.hallucination_passed,
                    "hallucination_confidence": event.hallucination_confidence,
                    "error_quadrant": event.error_quadrant,
                    "circuit_open": event.circuit_open,
                }
                asyncio.create_task(_slo_actor.send(_SLO_MSG_RECORD, payload))
            except Exception:
                pass

    _slo_mod.SLOMonitor.record_event = _bridged_record_event  # type: ignore[assignment]

# 持久化 checkpointer：消息存到 data/checkpointer.db，跨进程/重启保留
# AsyncSqliteSaver 必须在事件循环内初始化（需要 aiosqlite 连接），故 agent 改为惰性创建
import aiosqlite
# ===== CancellationToken: 跨层级取消联动检查点 =====
from agent.request_context import (
    check_cancelled,
    current_context,
    current_token,
)
_project_root = Path(__file__).resolve().parents[1]
_data_dir = _project_root / "data"
_data_dir.mkdir(parents=True, exist_ok=True)
_checkpointer_db = _data_dir / "checkpointer.db"

# ==================== 静默模式（快捷按钮用） ====================
# 场景：用户点击前端"盘前新闻 / 盘前小作文热度 / 复盘预测"快捷按钮时，
# 后端控制台不应刷 verbose 级 print（如 5000 字最终结果）。
# 机制：ContextVar<bool> + 全局 QUIET 环境变量双重控制；
#       set_quiet_mode(True) 仅影响当前请求链路，不串台；
#       MOSS_QUIET=1 为全局硬静默（生产部署可开启）。
_GLOBAL_QUIET = os.getenv("MOSS_QUIET", "").strip() in ("1", "true", "TRUE", "on")
_QUIET_MODE: ContextVar[bool] = ContextVar("moss_main_agent_quiet", default=False)


def set_quiet_mode(quiet: bool):
    """在当前请求链路中开启/关闭静默模式。返回旧值，用于恢复。"""
    old = _QUIET_MODE.get()
    _QUIET_MODE.set(bool(quiet))
    return old


def _is_quiet() -> bool:
    return _GLOBAL_QUIET or _QUIET_MODE.get()


def _log_info(msg: str):
    """info 级打印：静默模式下不输出。"""
    if not _is_quiet():
        print(msg)


def _log_verbose_result(prefix: str, content: str, max_len: int = MAIN_AGENT_VERBOSE_MAX_LEN):
    """结果类打印（主 agent 最终输出/工具结果等）：静默模式下跳过；
    非静默时也截断到 max_len，避免把整段 5000 字 markdown 刷到控制台。"""
    if _is_quiet():
        return
    text = (content or "").strip().replace("\n", " ")
    if len(text) > max_len:
        text = text[:max_len] + f"...(共{len(content)}字符)"
    print(f"{prefix}: {text}")


from dataclasses import dataclass, field as _field
try:
    from typing import Set as _Set
except Exception:  # pragma: no cover
    _Set = set  # type: ignore[assignment,misc]


_SEARCH_TOOL_NAMES: _Set[str] = {"internet_search", "search_zsxq_by_stock"}
_SEARCH_SUBAGENT_TYPES: _Set[str] = {"网络搜索助手"}

_PREMARKET_HINTS = ("盘前新闻")


@dataclass
class _SearchCallGuard:
    """单次 Agent 执行链路内的搜索调用计数器。"""

    enabled: bool = False
    limit: int = 3
    count: int = 0
    blocked: int = 0
    # 已执行工具名清单（去重保留顺序），便于超时诊断时一并报告
    executed_tool_names: list = _field(default_factory=list)


_SEARCH_GUARD_CTX: ContextVar = ContextVar(
    "moss_main_agent_search_guard", default=None
)

def _format_executed_tools_for_error(guard) -> str:
    """把已执行过的工具名拼成字符串，附在超时/拦截报告里便于排障。"""
    if guard is None or not getattr(guard, "executed_tool_names", None):
        return "（无工具调用记录）"
    names = [str(n) for n in guard.executed_tool_names if n]
    if not names:
        return "（无工具调用记录）"
    return " -> ".join(names)
# ================================================================

# 全局变量，在 get_main_agent() 中惰性初始化
_main_agent = None
_checkpointer = None
_async_sqlite_conn = None


async def get_main_agent():
    """惰性初始化并返回 main_agent。首次调用在事件循环内建立 aiosqlite 连接 + 建表。"""
    global _main_agent, _checkpointer, _async_sqlite_conn
    if _main_agent is None:
        # aiosqlite.connect 返回 Connection 对象，需在事件循环中 __aenter__ 才真正连接
        _async_sqlite_conn = aiosqlite.connect(str(_checkpointer_db))
        await _async_sqlite_conn.__aenter__()
        _checkpointer = AsyncSqliteSaver(_async_sqlite_conn)
        # 建表（幂等）
        await _checkpointer.setup()
        _main_agent = create_deep_agent(
            model=model,  # type: ignore[arg-type]
            system_prompt=(
                # §4.2 在 system prompt 末尾硬性拼接"先思考后引用"强制规范
                # （DeepSeek / Qwen 都会按此约束：<think> 四段 CoT + 正文 [N] 角标）
                patch_system_prompt_require_reasoning_and_citations(main_agent_content['system_prompt'])
                if _HAS_ADAPTER else main_agent_content['system_prompt']
            ),
            tools=[generate_markdown, convert_md_to_pdf, read_file_content, search_zsxq_by_stock],
            checkpointer=_checkpointer,
            subagents=[  # type: ignore[arg-type]
                database_query_agent,
                network_search_agent,
                knowledge_base_agent
            ]
        )
        print("[main_agent] AsyncSqliteSaver 初始化完成，agent 已就绪")
    return _main_agent

# 执行
"""
  1. 执行主智能体 一定选异步，原因：对应多个客户端
  2. 什么时候触发我们智能体的调用或者执行？？？
  3. 客户端 -》 api/task -> fastapi 接口 -》 异步执行 -》 main_agent的运行 （异步方法）
  4. main_agent执行stream流式处理 -》 调用工具 -》 已经埋好了点  
                                   调用子智能体 -》 结果解析 -》 name = task -> monitor -> 发送子智能体
                                   调用最终结果 -》 结果 -》 monitor -> 发送结果的方法
                                   开启调用以后 -》 当前会话 -》 文件夹地址 -》 推送到前端
"""



project_root_path = Path(__file__).parents[1].resolve() # 绝对 解析路径标识以及软连接

async def run_deep_agent(task_query, session_id, user_id=None, quiet: bool = False):
    """
    定义流式+异步执行主智能体！！
    执行过程中，返回：会话文件化通知 / 调用子智能体 / 工具执行进度 / 最终结果（通过 monitor 推前端）。
    参数:
      task_query: 前端提问的问题
      session_id: 每个前端会话对应的标识（1. 存储 session_id 到 ContextVars；2. session_id
          对应 output 输出目录），同时作为 LangGraph thread_id，由 AsyncSqliteSaver
          持久化对话历史，实现连续对话。
      user_id: 可选，所属用户 ID；传入时会更新会话标题与时间戳。
      quiet: 快捷按钮（盘前新闻/盘前小作文热度/复盘预测）专用，开启后：
    """
    if quiet:
        set_quiet_mode(True)
    # 剥离隐藏指令后的纯净用户问题 → 供 SKILL / PTD / 记忆 / 搜索守卫使用
    pure_user_query_inner = _strip_hidden_instructions(task_query)
    # ===== Layer3 补丁：盘前新闻搜索调用次数守卫（仅在盘前快捷按钮链路生效） =====
    # 若 query 含"一次综合网络搜索"+"韭研社区...东方财富股吧"等关键词组合，就
    # 初始化搜索调用次数上限 1，第 2 次搜索在工具调用检查点直接拦截 + 取消令牌，
    # 同时也用纯净版 query 再判一次（防止 hidden 指令里有 / 缺失关键词的情况），两次是或关系
    _tok_g = _SEARCH_GUARD_CTX.get()
    # ===== Progressive Tool Disclosure：每次新请求重置路由状态（确保两阶段从0开始） =====
    reset_route_state()
    # 剥离【工作环境指令】的用户原始问题 → 存到 PTD query ctx，供启发式兜底关键词提取
    _pure_q_for_ptd = _strip_hidden_instructions(task_query)
    ptd_token = set_ptd_query(_pure_q_for_ptd)

    _log_info(f"当前会话的main_agent开始执行了！ 会话id:{session_id} user:{user_id}")
    # 重置 IMA 知识库搜索计数器，防止上一个请求的计数影响当前请求
    from tools.ragflow_tools import reset_call_count
    reset_call_count()
    # 更新会话元数据：首条消息时自动生成标题（user_id+关键词+日期），刷新 updated_at
    if user_id:
        try:
            from api import storage
            existing = storage.get_session(session_id)
            if existing and (existing.get("title") in (None, "", "新会话")):
                auto_title = storage.generate_default_title(user_id, task_query)
                storage.update_session_title(session_id, auto_title)
            storage.touch_session(session_id)
        except Exception as e:
            print(f"[main_agent] 更新会话元数据失败（不影响主流程）: {e}")
    # 准备工作 【1. session_dir（前端） 2. relative_session_dir (大模型) 3. 上传的文件拼接专属提示词】
    # project_root_path / output / session_session_id(uuid)
    # 当前会话存储生成文件的专属文件夹
    session_dir = project_root_path / "output" / f"session_{session_id}"
    # 文件夹可能没有，第一次请求要创建
    session_dir.mkdir(parents=True, exist_ok=True)
    # \  \n \t -> /
    session_dir_str = str(session_dir).replace("\\","/")
    # 获取相对文件夹
    # session_dir : project_root_path / output / session_session_id(uuid)
    # project_root_path : project_root_path
    # relative_session_dir_str: / output / session_session_id(uuid)
    relative_session_dir_str = str(session_dir.relative_to(project_root_path)).replace("\\","/")

    # 继续准备 1. 当前会话的对应的session_id session_dir 存储到contextVars [后续工具获取，socket -> 推送消息] 2.调用monitor给前端推送session_dir信息
    session_dir_token = set_session_context(session_dir_str)  # 存储的当前会话对应的文件夹地址
    session_id_token = set_thread_context(session_id)  #获取当前会话的session_id对应socket
    monitor.report_session_dir(session_dir_str)  # 当前会话对应的文件夹地址推送给起前端！

    # 执行main_agent
    config = {
        "configurable":{
            "thread_id":session_id
        }
    }

    # 构建提示词：从 prompts.yml runtime_prompts 加载工作环境指令模板
    path_instruction = format_prompt(
        "main_agent.path_instruction",
        relative_session_dir_str=relative_session_dir_str,
    )
    # 反馈结果
    # 剥离【工作环境指令】的用户原始问题 → 用于记忆存储（避免把工作目录规则塞进历史摘要）
    pure_user_query = _strip_hidden_instructions(task_query)

    # ===== Layer3: Feedback Handler — 检测用户是否在质疑/反驳之前的结果 =====
    feedback_prefix = ""
    is_user_challenge = False
    try:
        fh = get_feedback_handler()
        is_user_challenge = fh.detect_challenge(pure_user_query)
        if is_user_challenge:
            # 用户在质疑 → 构建错误规避提示 + 重搜上下文
            error_avoidance = await fh.build_error_avoidance_prompt(session_id, pure_user_query)
            if error_avoidance:
                feedback_prefix = error_avoidance + "\n"
            _log_info(f"[FeedbackHandler] 检测到用户质疑，已加载错误规避上下文")
    except Exception as fb_err:
        print(f"[FeedbackHandler] 初始化失败（不致命）: {fb_err}")

    # ===== Layer3: Trace 可观测性 — 记录开始时间 =====
    _trace_start = time.time()
    _trace_tool_calls = []
    # 收集本轮工具结果文本，供幻觉防护（RAG 引用追踪）使用
    _tool_result_texts: list[str] = []
    # SLO 事件追踪变量
    _slo_success = False
    _slo_final_tier = 1
    _slo_hit_hard_limit = False
    _slo_hallucination_passed: bool | None = None
    _slo_hallucination_confidence: float | None = None
    _slo_error_quadrant: str | None = None
    _slo_circuit_open = False

    # ===== [Cancellation Check] 入口：在任何重计算之前先判断用户是否已取消 =====
    check_cancelled("main_agent.entry")

    # ===== Layer3: 熔断器准入检查 — main_agent 整体熔断保护 =====
    try:
        cb_registry = get_circuit_registry()
        main_cb = cb_registry.get_or_create("main_agent")
        if not main_cb.allow_request():
            _slo_circuit_open = True
            # 熔断属于安全事件，静默模式下也要打印（避免用户看不到失败原因）
            print(f"[CircuitBreaker] main_agent 熔断中，会话 {session_id} 被拒绝")
            monitor.report_error(
                "⚠️ 系统熔断保护已触发（近期错误过多），请稍后 30 秒后重试。"
            )
            # 返回静态兜底，不执行后续 LLM 调用
            return (
                "⚠️ 当前系统熔断保护已触发，请稍后重试。\n\n"
                "⚠️ 以上信息来自系统熔断保护，不构成投资建议。"
                "投资有风险，入市需谨慎，盈亏自负。"
            )
    except Exception as cb_err:
        print(f"[CircuitBreaker] 准入检查异常（不致命）: {cb_err}")

    try:
        # ===== [Cancellation Check] 大模型初始化前 =====
        check_cancelled("main_agent.before_agent_init")
        # 惰性获取 agent（首次调用会初始化 AsyncSqliteSaver）
        agent = await get_main_agent()

        # ===== 记忆管理：构造 Context Engineering 三段式上下文，拼到用户问题前面 =====
        # ===== [Cancellation Check] 构造上下文前 =====
        check_cancelled("main_agent.before_memory_context")
        mm = get_memory_manager()
        memory_context_str = await mm.build_prompt_context(session_id, pure_user_query)

        # ===== Layer2: Context Engineer — 对记忆上下文做精简裁剪（2000字阈值）=====
        try:
            ce = get_context_engineer()
            if memory_context_str and len(memory_context_str) > MAIN_AGENT_MEMORY_CONTEXT_WARN_LEN:
                # 按 query 关联度裁剪记忆上下文，保留高相关度的关键信息
                memory_entries = [{"content": memory_context_str, "source": "memory", "timestamp": ""}]
                memory_context_str = ce.build_context(memory_entries, pure_user_query)
                _log_info(f"[ContextEngineer] 记忆上下文已精简裁剪至 {len(memory_context_str)} 字")
        except Exception as ce_err:
            print(f"[ContextEngineer] 精简裁剪失败（不致命）: {ce_err}")

        memory_prefix = ""
        if memory_context_str:
            # 从 prompts.yml runtime_prompts 加载记忆上下文前缀模板
            memory_prefix = format_prompt(
                "main_agent.memory_prefix",
                memory_context_str=memory_context_str,
            )
            # 打印记忆统计信息到日志，方便排查
            try:
                stats = await mm.get_stats(session_id)
                _log_info(f"[MemoryManager] 会话 {session_id} 状态: turns={stats['turn_count']}, "
                      f"keys={stats['key_decision_count']}, summaries={stats['summary_segment_count']}")
            except Exception:
                pass

        # ===== [Cancellation Check] SKILL 注入前 =====
        check_cancelled("main_agent.before_skill_inject")

        # ===== Loop Engineering — SKILL 自动加载：按关键词匹配注入专业规范 =====
        skill_injected_prefix = ""
        try:
            sm = get_skill_manager()
            matched_block = sm.build_skill_prefix(pure_user_query, max_skills=2)
            if matched_block:
                skill_injected_prefix = format_prompt(
                    "main_agent.skill_injected_prefix",
                    skill_block=matched_block,
                )
                matched_names = [sd.name for sd in sm.match_skills(pure_user_query)[:2]]
                _log_info(f"[SkillManager] 已注入 SKILL: {matched_names}")
        except Exception as sk_err:
            # skill 加载失败不影响主流程，仅打印日志
            print(f"[SkillManager] 自动注入失败（不致命）: {sk_err}")

        # 最终发送给 LLM 的消息：
        # [错误规避前缀] + [SKILL 注入前缀] + [记忆前缀] + 用户原始提问 + 工作环境指令
        final_user_content = feedback_prefix + skill_injected_prefix + memory_prefix + task_query + path_instruction

        # 记录本轮的最终助手回答，循环结束后写入记忆
        current_assistant_reply = ""

        # ===== [Cancellation Check] LLM astream 启动前：最后一次机会取消再投入大模型算力 =====
        check_cancelled("main_agent.before_astream")

        # 执行
        async for chunk in agent.astream({
            "messages":[
                {
                    "role":"user","content":final_user_content
                }
            ]
        },config=config,recursion_limit=MAIN_AGENT_RECURSION_LIMIT):  # type: ignore[call-arg]
            # ===== [Cancellation Check] 每轮 chunk 到达后再检查（同步循环体内的取消也能被感知）=====
            check_cancelled("main_agent.inside_astream")
            # {"model [大模型决定调用工具 子智能体  最终结果] / tools" : {messages:[xxx...]}}
            for node_name,state in chunk.items():
                if not state or "messages" not in state: continue
                messages = state["messages"]
                if messages and isinstance(messages,list):
                    last_msg = messages[-1]
                    if node_name == 'model':
                        if last_msg.tool_calls:
                            # 工具和子智能体
                            for tool_call in last_msg.tool_calls:
                                _tname = tool_call.get('name', '') or ''
                                _targs = tool_call.get('args', {}) if isinstance(
                                    tool_call.get('args'), dict) else {}
                                if tool_call['name'] == 'task':
                                    # 调用某个子智能体
                                    sub_name = tool_call['args']['subagent_type']
                                    monitor.report_assistant(sub_name,{'description':tool_call['args']['description']})
                                    # 子智能体即将进入 LLM 思考阶段（决定调用哪些工具），提前告知前端，消除 tool_start 前的日志空白期
                                    monitor.report_thinking(sub_name)
                                else:
                                    # 直接工具调用（如 search_zsxq_by_stock / generate_markdown 等）
                                    # 必须报告给前端，否则用户在工具执行期间看不到任何进度
                                    direct_tool_name = tool_call.get('name', '未知工具')
                                    direct_tool_args = tool_call.get('args', {})
                                    # 构造用户可读的描述
                                    if direct_tool_name == 'search_zsxq_by_stock':
                                        stock = direct_tool_args.get('stock_name', '')
                                        monitor.report_tool(direct_tool_name, {'stock_name': stock})
                                        monitor.report_thinking(f"知识星球搜索「{stock}」")
                                    elif direct_tool_name == 'generate_markdown':
                                        monitor.report_tool(direct_tool_name, direct_tool_args)
                                        monitor.report_thinking("生成文档")
                                    elif direct_tool_name == 'convert_md_to_pdf':
                                        monitor.report_tool(direct_tool_name, direct_tool_args)
                                        monitor.report_thinking("转换PDF")
                                    elif direct_tool_name == 'read_file_content':
                                        fname = direct_tool_args.get('filename', '')
                                        monitor.report_tool(direct_tool_name, {'filename': fname})
                                        monitor.report_thinking(f"读取文件「{fname}」")
                                    else:
                                        monitor.report_tool(direct_tool_name, direct_tool_args)
                                        monitor.report_thinking(direct_tool_name)
                        elif last_msg.content:
                            # 最终结果
                            final_content = last_msg.content
                            # ===== 过滤 LLM 在 tool_calls 边界场景返回的"实质空"内容 =====
                            # DeepSeek API 在 tool_calls 模式下，模型第一轮决定调用工具时，
                            # content 字段有时会返回字面字符串 "[]"（DeepSeek 序列化空
                            # content 的方式），同时 tool_calls 列表偶尔会变成空 []（falsy），
                            # 导致走到本分支把 "[]" 当最终结果推送前端。
                            # 这里把所有"实质为空"的字符串形态都过滤掉：
                            #   "[]" / "null" / "None" / "{}" / '""' / "''" / 纯空白
                            if isinstance(final_content, str):
                                _stripped = final_content.strip()
                                if (not _stripped) or _stripped in (
                                    "[]", "null", "None", "null", "{}", '""', "''", "()"
                                ):
                                    # 跳过本轮空内容，不当成最终结果
                                    continue
                            # ===== §4.4 真实 <think> 推理 emit + 引用角标归一 + 引用映射下发 =====
                            # 1) 把思考内容与正文拆分开，思考单独作为 reasoning 事件给前端折叠面板
                            #    （依据 Experience 2176257 & 401403）。
                            # 2) 正文所有 [citation:N] / [[N]] / (N) 统一为 [N]，同时补元数据
                            if _HAS_ADAPTER and isinstance(final_content, str):
                                try:
                                    _emit_model_cot_and_normalize_citations(
                                        thread_id=session_id,
                                        final_content=final_content,
                                        tool_result_texts=_tool_result_texts,
                                    )
                                except Exception as _emit_err:
                                    print(f"[main_agent] cot/citation 预处理异常（不致命，走原路径）：{_emit_err}")
                            _log_verbose_result("主智能体执行结果", final_content, max_len=MAIN_AGENT_VERBOSE_MAX_LEN)
                            monitor.report_task_result(final_content)
                            current_assistant_reply = final_content
                            # ===== Layer4 新增：未命中本地缓存时回填 STOCK_CACHE_DIR =====
                            # 仅在用户问句里能 extract 出有效股票实体时写；同日同小时重复提问会被同文件覆盖，
                            # 避免重复写入；风险声明如缺失由 write_stock_cache 自动追加。
                            try:
                                from config.constants import STOCK_CACHE_ENABLED as _sce_on
                                if _sce_on:
                                    from cache.stock_cache import extract_stock_name, write_stock_cache
                                    _stk = extract_stock_name(pure_user_query_inner)
                                    if _stk:
                                        _wpath = write_stock_cache(
                                            _stk, final_content, source="query_writeback"
                                        )
                                        if _wpath:
                                            _log_info(f"[StockCache] 回填缓存（query_writeback）：{_wpath}")
                            except Exception as _cache_wb_err:
                                # 回填失败不影响主回答结果，仅打印日志（避免刷屏）
                                print(f"[StockCache] 回填失败（不影响主流程）：{_cache_wb_err!r}")
                    elif node_name == 'tools':
                        # tools 节点：工具执行完成，返回 ToolMessage
                        # 必须把工具结果推送给前端，否则用户看不到中间分析内容
                        if hasattr(last_msg, 'content') and last_msg.content:
                            tool_result_text = last_msg.content
                            # ===== 过滤工具结果的"实质空"内容（同上）=====
                            # 某些工具（如 execute_sql_query / search_knowledge_base）
                            # 在无数据时返回字面字符串 "[]" 或 "null"，不应推送前端
                            if isinstance(tool_result_text, str):
                                _stripped_tr = tool_result_text.strip()
                                if (not _stripped_tr) or _stripped_tr in (
                                    "[]", "null", "None", "{}", '""', "''", "()"
                                ):
                                    # 跳过空工具结果，不推送前端
                                    continue
                            # 收集工具结果文本，供幻觉防护（RAG 引用追踪）使用
                            _tool_result_texts.append(tool_result_text)
                            # 从 ToolMessage 中提取工具名
                            tool_name = getattr(last_msg, 'name', '') or getattr(last_msg, 'tool_name', '') or 'tool'
                            # 推送工具结果摘要给前端
                            monitor.report_tool_end(tool_name, tool_result_text)
                            _log_verbose_result(f"[Tool Result] {tool_name}", tool_result_text, max_len=MAIN_AGENT_VERBOSE_TOOL_RESULT_MAX_LEN)

        # ===== Layer3: 熔断器 — 记录 main_agent 成功 =====
        try:
            main_cb = get_circuit_registry().get_or_create("main_agent")
            if current_assistant_reply and current_assistant_reply.strip():
                main_cb.record_success()
                _slo_success = True
            else:
                main_cb.record_failure()
        except Exception:
            pass

        # ===== Layer3: Maker-Checker — 输出质量校验（数据一致性/风险声明/幻觉检测）=====
        try:
            mc = get_maker_checker()
            is_valid, issues = await mc.check_output(
                pure_user_query, current_assistant_reply
            )
            if not is_valid and issues:
                # 追加校验问题到输出，提醒用户注意
                current_assistant_reply += f"\n\n---\n⚠️ 自动校验发现以下问题：\n{issues}"
                print(f"[MakerChecker] 输出校验未通过：{issues[:200]}")
            else:
                print(f"[MakerChecker] 输出校验通过")
        except Exception as mc_err:
            print(f"[MakerChecker] 校验异常（不致命）: {mc_err}")

        # ===== Layer3: 幻觉防护 — RAG 引用追踪 + JSON Schema + LLM-as-Judge =====
        try:
            hg = get_hallucination_guard()
            hall_report = await hg.verify(
                user_query=pure_user_query,
                agent_output=current_assistant_reply,
                tool_results=_tool_result_texts,
            )
            _slo_hallucination_passed = hall_report.passed
            _slo_hallucination_confidence = hall_report.confidence
            if not hall_report.passed:
                warning = hall_report.render_warning()
                if warning:
                    current_assistant_reply += warning
                print(f"[HallucinationGuard] 幻觉防护未通过："
                      f"unverified_numbers={len(hall_report.unverified_numbers)}, "
                      f"unverified_codes={len(hall_report.unverified_stock_codes)}, "
                      f"citation_gaps={len(hall_report.citation_gaps)}, "
                      f"confidence={hall_report.confidence:.2f}")
            else:
                print(f"[HallucinationGuard] 幻觉防护通过 (confidence={hall_report.confidence:.2f})")
        except Exception as hg_err:
            print(f"[HallucinationGuard] 幻觉防护异常（不致命）: {hg_err}")

    except (asyncio.CancelledError, KeyboardInterrupt) as _cancel_err:
        # ====== 按取消原因分级：避免"任务被用户取消"误报（用户实际没点停止）======
        # 可能的触发来源（至少 5 种）：
        #   1) reason="user_stop_clicked"              → 用户真的点了 STOP 按钮
        #   2) reason="websocket_disconnected"         → 浏览器关闭/网络抖动
        #   3) reason="timeout: deadline Ns reached"   → CancellationToken 180s 超时
        #   4) reason=None / "" / "cancelled"           → SessionRegistryActor 用
        #        old_task.cancel() 替换同线程旧任务（REGISTER_AGENT_TASK /
        #        REGISTER_BG_TASK 内部），只是正常的"旧任务回收"语义，不算异常
        #   5) "parent_cancelled" / 其他                → 父令牌级联取消
        from agent.request_context import current_token, RequestCancelledError
        _tok = current_token()
        _reason: str = ""
        if isinstance(_cancel_err, RequestCancelledError):
            _reason = getattr(_cancel_err, "reason", "") or ""
        if not _reason and _tok is not None:
            _reason = getattr(_tok, "reason", "") or ""

        _is_user_stop = ("user_stop_clicked" in _reason) or ("stop_clicked" in _reason)
        _is_ws = "websocket_disconnected" in _reason
        _is_timeout = _reason.startswith("timeout:") or "deadline" in _reason
        _is_search_limit = "search_call_limit" in _reason
        _is_task_replace = ((not _reason) or
                            "session_actor_replaced" in _reason or
                            _reason in ("cancelled", "task_replaced"))
        _is_parent = "parent_cancelled" in _reason
        # 从 ContextVar 读出已执行工具名，便于超时/搜索拦截的诊断报告
        _cur_guard = _SEARCH_GUARD_CTX.get()
        _exec_tools = _format_executed_tools_for_error(_cur_guard)
        # 已执行的搜索次数（如果有守卫）
        _search_cnt = (
            getattr(_cur_guard, "count", 0) or 0 if _cur_guard is not None else 0
        )
        _search_blocked = (
            getattr(_cur_guard, "blocked", 0) or 0 if _cur_guard is not None else 0
        )

        if _is_user_stop:
            # 明确用户行为：控制台 + 前端都告知停止
            print(f"[Agent] 会话 {session_id} 任务被用户主动取消 (stop 按钮)")
            monitor.report_error("⏹ 任务已停止")
        elif _is_ws:
            # 正常连接管理：用户关浏览器属于正常路径，前端已断线，不用再 report_error
            print(f"[Agent] 会话 {session_id} 连接已断开，自动终止任务 "
                  f"(reason={_reason!r})")
            # 不调用 monitor.report_error：前端离线，发了也白发
        elif _is_search_limit:
            # 搜索调用次数超限 → 已经在拦截点报告过错误，这里只打印一条日志
            # 用于辅助排查（不重复 report_error，避免前端弹两次停止/错误提示）。
            print(
                f"[SearchGuard] 会话 {session_id} 因重复搜索达上限而主动终止 "
                f"(count={_search_cnt}, blocked={_search_blocked}, tools={_exec_tools}, "
                f"reason={_reason!r})"
            )
        elif _is_timeout:
            # 超时：系统内部失败，提示用户重试；同时附带已执行工具链路清单，
            # 便于直接判断"是不是调了 6 次搜索导致 210s 超时"，不用再去翻日志。
            elapsed = round(getattr(_tok, "age_sec", 0.0), 1) if _tok else 0.0
            print(
                f"[Agent][超时] 会话 {session_id} 任务在运行 {elapsed}s 后因超时自动终止 "
                f"(timeout_reason={_reason!r}, search_calls={_search_cnt}, tools={_exec_tools})"
            )
            monitor.report_error(
                f"⏱ 任务超时（{elapsed}s），可能是网络搜索或 LLM 响应较慢，"
                f"建议拆分问题后重试。已执行工具链路：{_exec_tools}，"
                f"其中搜索类调用 {_search_cnt} 次，被拦截 {_search_blocked} 次。"
            )
        elif _is_task_replace:
            # 最常见误报：同线程新任务来了，SessionRegistryActor 把旧的 cancel 掉；
            # 这是正常的"任务替换"，既不是错误也不是用户操作
            _log_info(f"[Agent] 会话 {session_id} 旧任务被系统回收（新任务已启动）"
                      f" — 不向用户显示停止提示")
            # 绝对不能 report_error！否则用户点新按钮时，旧任务取消会弹"任务已停止"
        elif _is_parent:
            # 父令牌取消（比如请求级 CancellationToken.cancel 触发级联）
            print(f"[Agent] 会话 {session_id} 任务因父链路取消而终止 (reason={_reason!r})")
            monitor.report_error("⏹ 任务已停止")
        else:
            # 其他未知原因：保留原提示，但加上 reason 便于排障
            print(f"[Agent] 会话 {session_id} 任务已取消 (unknown reason={_reason!r})")
            monitor.report_error("⏹ 任务已停止")
        # 不 re-raise，让 finally 正常执行清理；任务标记为 done
    except Exception as e :
        # 报错推送错误信息给前端
        print(f"[Agent] 会话 {session_id} 执行异常: {e}")
        monitor.report_error(f"执行主智能体发生异常：{str(e)}")
        # ===== Layer3: 熔断器 — 记录 main_agent 失败 + 错误分类 =====
        try:
            main_cb = get_circuit_registry().get_or_create("main_agent")
            main_cb.record_failure()
            classifier = get_error_classifier()
            cls_err = classifier.classify(e)
            _slo_error_quadrant = cls_err.quadrant.value
            print(f"[ErrorClassifier] 异常归类: {cls_err.quadrant.value} "
                  f"({cls_err.error_type}) — {cls_err.action}")
        except Exception:
            pass
    finally:
        # ===== Layer3: SLO 监控 — 记录本轮可靠性事件 =====
        try:
            slo = get_slo_monitor()
            latency_sec = time.time() - _trace_start
            # 检查是否触达硬上限（时间 > 150s）
            if latency_sec > SLO_MAX_TASK_SEC:
                _slo_hit_hard_limit = True
            slo_event = SLOEvent(
                session_id=session_id,
                timestamp=time.time(),
                success=_slo_success,
                latency_sec=latency_sec,
                final_tier=_slo_final_tier,
                hit_hard_limit=_slo_hit_hard_limit,
                hallucination_passed=_slo_hallucination_passed,
                hallucination_confidence=_slo_hallucination_confidence,
                error_quadrant=_slo_error_quadrant,
                circuit_open=_slo_circuit_open,
            )
            slo.record_event(slo_event)
            print(f"[SLO] 会话 {session_id} 事件已记录: "
                  f"success={_slo_success}, latency={latency_sec:.1f}s, "
                  f"hallucination_pass={_slo_hallucination_passed}")
        except Exception as slo_err:
            print(f"[SLO] 事件记录失败（不致命）: {slo_err}")

        # ===== Layer3: Trace 可观测性 — 记录完整 trace =====
        try:
            tl = get_trace_logger()
            latency_ms = int((time.time() - _trace_start) * 1000)
            # 获取 PTD 披露的工具列表
            from agent.tool_router import _get_or_init_state
            ptd_state = _get_or_init_state()
            ptd_tools = sorted(ptd_state.selected_tool_ids) if ptd_state else []
            # 获取记忆统计
            memory_stats = {}
            try:
                memory_stats = await mm.get_stats(session_id)
            except Exception:
                pass
            await tl.log_turn(
                session_id=session_id,
                user_input=pure_user_query[:2000],
                assistant_output=(current_assistant_reply or "")[:5000],
                tool_calls=_trace_tool_calls,
                latency_ms=latency_ms,
                ptd_tools=ptd_tools,
                memory_stats=memory_stats,
            )
            print(f"[Trace] 会话 {session_id} 本轮耗时 {latency_ms}ms，工具调用 {len(_trace_tool_calls)} 次")
        except Exception as trace_err:
            print(f"[Trace] 记录失败（不致命）: {trace_err}")

        # ===== Layer3: Feedback Handler — 如果用户在质疑，学习这个错误 =====
        try:
            if is_user_challenge and current_assistant_reply:
                fh = get_feedback_handler()
                await fh.learn_error(session_id, pure_user_query, current_assistant_reply, "user_challenge")
                print(f"[FeedbackHandler] 已记录用户质疑错误模式")
        except Exception:
            pass

        # ===== 记忆管理：将本轮问答写入记忆库（成功完成的非空回答才写）=====
        try:
            # 取消令牌下的策略：已取消 → 不做新的 IO（避免半截回复污染长期记忆）
            ctx = current_context()
            cancelled = ctx.is_cancelled if ctx is not None else False
            if (not cancelled) and pure_user_query and current_assistant_reply and current_assistant_reply.strip():
                check_cancelled("main_agent.before_memory_write")
                await mm.add_turn(session_id, pure_user_query, current_assistant_reply.strip())
        except Exception as mm_err:
            print(f"[MemoryManager] 写入本轮记忆失败（不致命）: {mm_err}")
        # ===== Progressive Tool Disclosure：清理 PTD query ctx =====
        reset_ptd_query(ptd_token)
        # 释放存储的地址和session_id
        reset_session_context(session_dir_token, session_id_token)

    # 返回本轮最终回复，供"复盘预测"等场景复用结果
    return current_assistant_reply


async def get_session_history(session_id: str, limit: int = MAIN_AGENT_SESSION_HISTORY_LIMIT_DEFAULT):
    """从 LangGraph checkpointer 中读取指定会话的对话历史。

    用于前端切换会话时恢复聊天记录。返回 [{role, content, type}, ...]。
    只返回 user / assistant 的可见消息，过滤掉 tool 内部消息。
    """
    try:
        agent = await get_main_agent()
        config = {"configurable": {"thread_id": session_id}}
        # aget_state_history 从新到旧迭代，取最新的一个快照即可（它包含完整消息列表）
        latest_state = None
        async for chunk in agent.aget_state_history(config):  # type: ignore[arg-type]
            latest_state = chunk
            break  # 只取第一个（最新）
        if not latest_state or not latest_state.values:
            return []
        msgs = []
        for m in latest_state.values.get("messages", []):
            role = None
            content = ""
            # 兼容 dict / BaseMessage 两种形态
            if isinstance(m, dict):
                role = m.get("role") or m.get("type")
                content = m.get("content", "")
            else:
                role = getattr(m, "type", None)
                content = getattr(m, "content", "")
            # 只保留用户与助手可见消息，跳过 tool / tool_call 内部消息
            if role in ("user", "human"):
                # 剥掉拼接在用户问题末尾的【工作环境指令】段，避免前端显示规则
                cleaned = _strip_hidden_instructions(content)
                if cleaned:  # 被完全剥离的情况（如测试数据）不显示
                    msgs.append({"role": "user", "content": cleaned, "type": "user"})
            elif role in ("assistant", "ai") and content and not getattr(m, "tool_calls", None):
                # 盘前小作文热度总结以特定标题开头，标记 type 供前端靠右显示
                msg_type = "zsxq" if content.startswith("知识星球财经资讯分析总结") else "assistant"
                msgs.append({"role": "assistant", "content": content, "type": msg_type})
        # 只返回最新的 limit 条消息，避免历史过长
        return msgs[-limit:]
    except Exception as e:
        print(f"[main_agent] 读取会话历史失败: {e}")
        return []

