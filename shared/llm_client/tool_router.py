"""
Progressive Tool Disclosure —— 渐进式工具披露路由器。

业界两阶段范式（解决 Tool Schema 吃上下文 Token 的问题）：
  - Stage 0 路由阶段：不暴露任何 tools JSON Schema，只在 prompt 末尾注入一份 200 字
    的"极简工具选择菜单"（ID + 类别 + 一句话功能 + 触发关键词），
    让模型在回答开头输出 `__TOOL_ROUTE__:["id1","id2"]`。此时零 Schema 开销。
  - Stage 1 执行阶段：仅将阶段一选中的工具子集的**完整 Schema**注入下一次 LLM 调用，
    让模型做精确参数填充。平均 Schema 大小从 ~6KB → ~1.5KB（节省 75%）。
  - Stage 2 兜底披露：模型若尝试调用未披露的工具（输出 tool_calls 中 name 不在
    已披露集合），自动把该工具追加到披露集合并原地重试，最多兜底 2 次防循环。

上层业务零侵入：通过在 model 外包一层 Runnable 拦截 astream/ainvoke 的 tools 参数
与 prompt 来实现。所有状态用 ContextVar 做 per-request 隔离。
"""
from __future__ import annotations

import re
import json
import copy
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, AsyncIterator

from langchain_core.runnables import Runnable
from langchain_core.messages import (
    AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
)
from langchain_core.outputs import ChatGenerationChunk
from langchain_core.tools import BaseTool


# ======================================================================
# 配置（可用 .env 覆盖）
# ======================================================================
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# 运行时提示词模板访问器（路由菜单的 header/footer 抽取到 prompts.yml）
from agent.prompts import format_prompt

# 取消联动检查点（路由/执行前主动检查）
from agent.request_context import check_cancelled

from config.constants import PTD_MAX_TOOLS_PER_ROUND, PTD_TOKEN_FALLBACK_PER_TOOL_ESTIMATE

# 阶段零工具选择菜单最多允许选几个工具（防止模型又把全部选回来）
MAX_TOOLS_PER_ROUND = int(os.getenv("PTD_MAX_TOOLS_PER_ROUND", str(PTD_MAX_TOOLS_PER_ROUND)))
# 兜底披露最多允许几次追加（防止无限循环）
MAX_FALLBACK_DISCLOSURES = int(os.getenv("PTD_MAX_FALLBACK_DISCLOSURES", "2"))
# 是否启用：关闭则退化为全量工具暴露（便于 A/B 对比）
ENABLED = os.getenv("PTD_ENABLED", "1").strip() not in ("0", "false", "False", "off")

# 工具选择指令注入标记（避免重复注入）
_ROUTE_MENU_MARKER = "【PTD工具路由菜单】"

# ======================================================================
# 工具分类 & 索引注册表
# ======================================================================
@dataclass
class ToolIndexEntry:
    """路由阶段的极简索引条目（一行 50~80 字，避免 Token 浪费）。"""
    tool_id: str               # 对应 LangChain tool.name
    category: str              # 类别：[文件生成] / [数据库] / [知识库] / [网络搜索] / [其他]
    summary: str               # 一句话功能（<30字）
    trigger_keywords: List[str] = field(default_factory=list)  # 启发式关键词

    def menu_line(self) -> str:
        return (
            f"  - [{self.tool_id}] ({self.category}) {self.summary}；"
            f"触发词: {'/'.join(self.trigger_keywords[:3]) if self.trigger_keywords else '无'}"
        )


# 预设索引（项目实际工具）。子 agent 在 deepagents 中打包为统一 task 工具，
# 按 subagent_type 区分子类型并单列——任一命中即整体披露 task 工具。
PRESET_INDEX: Dict[str, ToolIndexEntry] = {
    # ----- 主 Agent 直接工具 -----
    "generate_markdown": ToolIndexEntry(
        tool_id="generate_markdown",
        category="文件生成",
        summary="把文本写入 .md Markdown 文件",
        trigger_keywords=["生成文档", "保存md", "写markdown", "输出markdown", "生成报告", "研报", "整理文档",
                          "生成md", "导出markdown", "编写文档", "写报告"],
    ),
    "convert_md_to_pdf": ToolIndexEntry(
        tool_id="convert_md_to_pdf",
        category="文件生成",
        summary="把已生成的 .md 文档转换为 PDF 格式",
        # 注意：纯"pdf"过于宽泛（读取"xxx.pdf"也会命中），只保留"生成/转换/导出PDF"这类主动动作
        trigger_keywords=["生成pdf", "转pdf", "导出pdf", "打印成pdf", "下载成pdf", "输出pdf", "保存为pdf"],
    ),
    "read_file_content": ToolIndexEntry(
        tool_id="read_file_content",
        category="文件读取",
        summary="解析用户上传/服务器已有文件（PDF/Excel/Word/Markdown/TXT 等）并按指令抽取内容",
        trigger_keywords=["读取文件", "读取上传", "上传的文件", "解析excel", "解析pdf", "解析word",
                          "分析文件", "读取附件", "打开我上传的", "读取文档", "读xlsx",
                          "读pdf", "读docx", "读csv", "提取附件内容"],
    ),
    "search_zsxq_by_stock": ToolIndexEntry(
        tool_id="search_zsxq_by_stock",
        category="知识星球搜索",
        # summary/keywords 均收紧：只保留明确指向知识星球的词，避免"盘前新闻"等
        # 批量任务误触发（"研报"太宽泛；"热度"由 /api/zsxq-analysis 独立入口处理）
        summary="仅在用户明确询问'知识星球/小作文/圈子'中某只股票讨论时调用；批量分析任务禁用",
        trigger_keywords=["知识星球", "zsxq", "小作文", "圈子", "社群", "星球"],
    ),
    # ----- 子 Agent 的 task/xxx 虚拟项（共享底层 task 工具）-----
    "task/网络搜索助手": ToolIndexEntry(
        tool_id="task/网络搜索助手",
        category="网络搜索",
        summary="调用子助手联网搜索新闻、公告、宏观信息、股价走势等公开内容",
        trigger_keywords=["新闻", "搜索", "联网", "最近", "最新消息", "今天", "股价", "行情", "研报新闻",
                          "宏观", "政策新闻", "公告", "股吧", "互联网", "网络", "实时"],
    ),
    "task/数据库查询助手": ToolIndexEntry(
        tool_id="task/数据库查询助手",
        category="数据库",
        summary="调用子助手查询内部 MySQL 业务数据表（药品/库存/销售明细等）",
        trigger_keywords=["数据库", "sql", "查询表", "销售数据", "库存", "商品", "明细", "统计数据",
                          "内部数据", "业务数据", "表名", "mysql", "数据表", "销售记录"],
    ),
    "task/RAGFlow助手": ToolIndexEntry(
        tool_id="task/RAGFlow助手",
        category="专有知识库",
        summary="调用子助手搜索 IMA 内部知识库（研报/手册/企业内文档）",
        trigger_keywords=["知识库", "研报", "内部文档", "ima", "专有知识", "手册", "企业内部",
                          "ragflow", "知识图谱", "内部资料", "国产替代"],
    ),
}

# 虚拟项 → 实际 LangChain tool.name 的映射（task/* 都映射到底层 task 工具）
VIRTUAL_TO_REAL: Dict[str, str] = {
    "task/网络搜索助手": "task",
    "task/数据库查询助手": "task",
    "task/RAGFlow助手": "task",
}


# ======================================================================
# Per-request 路由状态（基于 ContextVar，协程间独立）
# ======================================================================
@dataclass
class _RouteState:
    stage: int = 0                         # 0=路由阶段, 1=执行阶段, 2=兜底披露
    selected_tool_ids: Set[str] = field(default_factory=set)  # 已选中的真实 tool.name 集合
    fallback_count: int = 0                 # 兜底次数

# 每个请求链路（asyncio Task）独立的状态
_route_state_ctx: ContextVar[Optional[_RouteState]] = ContextVar("ptd_route_state", default=None)


def reset_route_state() -> None:
    """在每个新用户请求开始时调用，清理上一个请求的路由状态。"""
    _route_state_ctx.set(None)


def _get_or_init_state() -> _RouteState:
    st = _route_state_ctx.get()
    if st is None:
        st = _RouteState()
        _route_state_ctx.set(st)
    return st


# ======================================================================
# 工具选择菜单生成 & 路由结果解析
# ======================================================================
def build_route_menu() -> str:
    """生成阶段零的极简工具选择菜单（200~300字，远小于完整 Schema ~6KB）。"""
    # 菜单 header/footer 从 prompts.yml runtime_prompts 段加载，动态部分（菜单项）就地拼接
    header = format_prompt(
        "tool_router.route_menu_header",
        line_sep="\n" + "=" * 10,
        marker=_ROUTE_MENU_MARKER,
        max_tools_per_round=MAX_TOOLS_PER_ROUND,
    )
    footer = format_prompt(
        "tool_router.route_menu_footer",
        line_sep_end="=" * (20 + len(_ROUTE_MENU_MARKER)),
    )
    lines = [header, "", "【可选工具列表】（工具ID在[]内，按需挑选）："]
    for e in PRESET_INDEX.values():
        lines.append(e.menu_line())
    lines += ["", footer, ""]
    return "\n".join(lines)


_ROUTE_OUTPUT_RE = re.compile(
    r"__TOOL_ROUTE__\s*:\s*(\[[^\]]*\])",
    re.IGNORECASE | re.DOTALL,
)


def parse_route_output(content: str) -> Tuple[List[str], str]:
    """
    解析模型的阶段零输出：(选中 tool_id 列表, 去掉路由行后的剩余正文)。
    若解析失败，返回空列表和原内容。
    """
    if not content:
        return [], content
    m = _ROUTE_OUTPUT_RE.search(content)
    if not m:
        return [], content
    try:
        ids = json.loads(m.group(1))
        if not isinstance(ids, list):
            ids = []
    except Exception:
        ids = []
    # 过滤掉非法值 & 去重，限制最多 MAX_TOOLS_PER_ROUND
    normalized: List[str] = []
    seen = set()
    for i in ids:
        if not isinstance(i, str):
            continue
        if i not in PRESET_INDEX:
            continue
        if i in seen:
            continue
        seen.add(i)
        normalized.append(i)
        if len(normalized) >= MAX_TOOLS_PER_ROUND:
            break
    # 去掉路由行
    cleaned = _ROUTE_OUTPUT_RE.sub("", content, count=1).lstrip()
    return normalized, cleaned


# ======================================================================
# 启发式兜底（当模型不按协议输出路由时，按 query 关键词选）
# ======================================================================
def heuristic_select_from_query(query_text: str) -> Set[str]:
    """基于关键词的启发式选工具（保底方案），返回真实 tool.name 集合。
    匹配不区分大小写，以兼容"PDF"/"pdf"/"研报.PDF"等多种写法。"""
    real_ids: Set[str] = set()
    q = (query_text or "").lower()
    # 先匹配虚拟项 → 再转真实
    for virtual_id, entry in PRESET_INDEX.items():
        if any(kw.lower() in q for kw in entry.trigger_keywords):
            real_ids.add(VIRTUAL_TO_REAL.get(virtual_id, virtual_id))
    # PDF 依赖 markdown，自动连带
    if "convert_md_to_pdf" in real_ids:
        real_ids.add("generate_markdown")
    # 没命中任何关键词也至少给出空集合
    return real_ids


# ======================================================================
# 主 Agent 工具集合的动态裁剪（执行阶段只暴露选中子集）
# ======================================================================
def _tool_name(tool: Any) -> str:
    """兼容 BaseTool / StructuredTool / 普通 @tool 装饰函数的取 name 方式。"""
    if isinstance(tool, BaseTool):
        return tool.name
    if hasattr(tool, "name"):
        return str(tool.name)
    return str(getattr(tool, "__name__", ""))


def filter_tools_by_selected(all_tools: List[Any], selected_real_ids: Set[str]) -> List[Any]:
    """根据选中的真实 tool.name 集合，过滤全量 tools 列表。"""
    if not selected_real_ids:
        return []
    return [t for t in all_tools if _tool_name(t) in selected_real_ids]


# ======================================================================
# 渐进式披露 Runnable 包装器
# ======================================================================
class ProgressiveToolDisclosureModel(Runnable):
    """
    对 BaseChatModel 的 astream / ainvoke 做透明包装，实现渐进式工具披露。

    使用方式：
        base_model = init_chat_model(...)
        model = ProgressiveToolDisclosureModel(base_model)
        # 之后正常把 model 传给 create_deep_agent(model=model, ...) —— 无侵入
    """

    def __init__(self, base_model: Runnable, *, verbose: bool = True):
        super().__init__()
        self._base = base_model
        self._verbose = verbose
        # 缓存：原始完整 tools 列表（从第一次调用的 kwargs["tools"] 捕获）
        self._original_tools: Optional[List[Any]] = None
        # 自适应门控：工具池很小时（全量 Schema 开销 <= 路由菜单开销）PTD 是负优化，
        # 由 benchmarks/bench_ptd_tokens.py 实测驱动（4 工具池 Schema≈607 tok < 菜单≈678 tok）
        self._adaptive_off: bool = False

    # --------------------------------------------------------------
    # LangChain Runnable 接口
    # --------------------------------------------------------------
    @property
    def InputType(self):
        return self._base.InputType

    @property
    def OutputType(self):
        return self._base.OutputType

    def __getattr__(self, item):
        """把未显式定义的属性/方法代理到底层 model，兼容上层反射调用。"""
        return getattr(self._base, item)

    def invoke(self, *args, **kwargs):
        raise NotImplementedError("PTD 包装器仅用于异步场景（ainvoke/astream）。")

    async def ainvoke(self, messages, config=None, **kwargs):
        # ===== [Cancellation Check] PTD/工具路由启动前（阶段零 LLM 调用前）=====
        check_cancelled("ptd.ainvoke.entry")
        # ---- 关键：阶段零在 wrapper 内部消化，不返回给上层 ----
        state = _get_or_init_state()
        if ENABLED and state.stage == 0:
            # 内部执行阶段零：路由选择（不抛出异常给上层，失败则走启发式兜底）
            try:
                await self._ainvoke_stage0_internal(messages, config, kwargs)
            except Exception as e:
                if self._verbose:
                    print(f"[PTD] 阶段零内部调用异常（走启发式兜底）: {e}")
                self._fallback_select_via_heuristic(messages)
        # 直接进阶段一
        return await self._ainvoke_stageN(messages, config, kwargs)

    async def astream(self, messages, config=None, **kwargs):
        # ===== [Cancellation Check] PTD/流式工具路由启动前 =====
        check_cancelled("ptd.astream.entry")
        state = _get_or_init_state()
        if ENABLED and state.stage == 0:
            # 内部先执行阶段零（全量收完不走流式，避免吐路由选择给前端）
            try:
                await self._ainvoke_stage0_internal(messages, config, kwargs)
            except Exception as e:
                if self._verbose:
                    print(f"[PTD] 阶段零内部调用异常（走启发式兜底）: {e}")
                self._fallback_select_via_heuristic(messages)
        # 阶段一流式透传到上层
        async for chunk in self._astream_stageN(messages, config, kwargs):
            yield chunk

    # --------------------------------------------------------------
    # 阶段零（内部）：拿到路由选择，更新 state；不返回任何内容给上层
    # --------------------------------------------------------------
    async def _ainvoke_stage0_internal(self, messages, config, orig_kwargs) -> None:
        # 1. 捕获原始全量 tools（只在首次捕获）
        if "tools" in orig_kwargs and orig_kwargs["tools"] and self._original_tools is None:
            self._original_tools = list(orig_kwargs["tools"])
        if self._verbose:
            total_schema_chars = self._estimate_total_tool_schema_chars(self._original_tools or [])
            print(f"[PTD] 阶段0：路由阶段（零Tool Schema），全量Schema约 {total_schema_chars} 字符 ≈ "
                  f"{max(1, total_schema_chars // 4)} tokens")

        # 1.5 自适应门控：全量 Schema 开销 <= 路由菜单开销时，PTD 净收益为负 → 整请求旁路
        #     （省掉阶段零那次额外 LLM 调用 + 菜单注入开销，直接全量披露）
        if self._original_tools:
            full_chars = self._estimate_total_tool_schema_chars(self._original_tools)
            if full_chars <= len(build_route_menu()):
                self._adaptive_off = True
                _get_or_init_state().stage = 1
                if self._verbose:
                    print(f"[PTD] 自适应门控触发：全量Schema({full_chars}字符) <= 路由菜单"
                          f"({len(build_route_menu())}字符)，PTD 负优化 → 本次请求全量披露旁路")
                return

        # 2. 构造阶段零请求：注入菜单 + 去掉 tools
        injected_msgs = self._append_menu_to_last_user(messages)
        stage0_kwargs = dict(orig_kwargs)
        stage0_kwargs.pop("tools", None)
        stage0_kwargs.pop("tool_choice", None)

        # 3. 用 ainvoke（非流式）跑路由阶段，避免中途吐 chunk 给前端
        route_ai: AIMessage = await self._base.ainvoke(
            injected_msgs, config=config, **stage0_kwargs
        )
        content = getattr(route_ai, "content", "") or ""

        # 4. 解析路由选择
        selected_virtual, _ = parse_route_output(content)
        state = _get_or_init_state()

        if selected_virtual:
            selected_real: Set[str] = set()
            for v in selected_virtual:
                selected_real.add(VIRTUAL_TO_REAL.get(v, v))
        else:
            # 解析失败 → 启发式兜底
            if self._verbose:
                print("[PTD] 路由协议解析失败 → 关键词启发式兜底")
            selected_real = self._fallback_select_via_heuristic(messages, _commit=False)

        # PDF → 自动联动 markdown
        if "convert_md_to_pdf" in selected_real and self._original_tools:
            # 检查原 tools 是否有 generate_markdown
            names = {_tool_name(t) for t in self._original_tools}
            if "generate_markdown" in names:
                selected_real.add("generate_markdown")

        state.selected_tool_ids = selected_real
        state.stage = 1

        if self._verbose:
            schema_chars = self._estimate_total_tool_schema_chars(
                filter_tools_by_selected(self._original_tools or [], selected_real)
            )
            total = self._estimate_total_tool_schema_chars(self._original_tools or [])
            saving = (1 - (schema_chars / max(1, total))) * 100 if total else 0
            print(f"[PTD] 路由结果：{sorted(selected_real)}，阶段一 Schema ≈ {schema_chars} 字符 "
                  f"({saving:.0f}% 节省)")

    def _fallback_select_via_heuristic(self, messages, *, _commit: bool = True) -> Set[str]:
        """从最后一条 user 消息中提取关键词启发式选中工具。"""
        query_text = ""
        if isinstance(messages, list):
            for m in reversed(messages):
                if isinstance(m, HumanMessage):
                    query_text = m.content or ""
                    break
                elif isinstance(m, dict) and m.get("role") == "user":
                    query_text = m.get("content", "") or ""
                    break
        # 也合并 ContextVar 存的 query（若有）
        extra = _ptd_query_ctx.get() or ""
        if extra and extra not in query_text:
            query_text = query_text + " " + extra
        real_ids = heuristic_select_from_query(query_text)
        if _commit:
            state = _get_or_init_state()
            state.selected_tool_ids = real_ids
            state.stage = 1
        return real_ids

    # --------------------------------------------------------------
    # 阶段N（对外）：实际执行阶段，按已选工具子集裁剪 tools schema
    # --------------------------------------------------------------
    async def _ainvoke_stageN(self, messages, config, kwargs):
        # ===== [Cancellation Check] 阶段N LLM 调用前（用户已经取消了就别再投模型算力）=====
        check_cancelled("ptd.stageN.entry")
        new_messages, new_kwargs, mode = self._pre_call_stageN(messages, kwargs)
        result = await self._base.ainvoke(new_messages, config=config, **new_kwargs)
        # 兜底披露：若调用了未披露的工具 → 追加并**内部重新调用阶段N**
        need_fb, missing = self._check_missing_tool_disclosure(result)
        state = _get_or_init_state()
        if need_fb and state.fallback_count < MAX_FALLBACK_DISCLOSURES:
            state.fallback_count += 1
            state.selected_tool_ids.update(missing)
            if self._verbose:
                print(f"[PTD] 工具调用引用未披露工具 {missing}，兜底追加后内部重调阶段一")
            # 重新走 pre_call 应用新的披露集合，再调用一次
            new_messages2, new_kwargs2, _ = self._pre_call_stageN(messages, kwargs)
            result = await self._base.ainvoke(new_messages2, config=config, **new_kwargs2)
        return result

    async def _astream_stageN(self, messages, config, kwargs):
        new_messages, new_kwargs, mode = self._pre_call_stageN(messages, kwargs)

        chunks: List[Any] = []
        async for chunk in self._base.astream(new_messages, config=config, **new_kwargs):
            chunks.append(chunk)
            yield chunk

        # 检查是否需要兜底（流式已经 yield 出去了，但如果工具缺失，下一轮会带完整披露）
        full_msg = self._merge_chunks_to_ai(chunks)
        need_fb, missing = self._check_missing_tool_disclosure(full_msg)
        state = _get_or_init_state()
        if need_fb and state.fallback_count < MAX_FALLBACK_DISCLOSURES:
            state.fallback_count += 1
            state.selected_tool_ids.update(missing)
            if self._verbose:
                print(f"[PTD] （流式）检测到未披露工具调用 {missing}，已追加披露集合，"
                      "下一轮 LLM 调用时生效")

    # --------------------------------------------------------------
    # 调用前（阶段N）：仅裁剪 tools，不做路由菜单注入
    # --------------------------------------------------------------
    def _pre_call_stageN(
        self, messages: Any, kwargs: Dict[str, Any]
    ) -> Tuple[Any, Dict[str, Any], str]:
        if not ENABLED or self._adaptive_off:
            return messages, kwargs, "bypass"
        # 捕获原始全量 tools（只捕获首次，防止前面漏掉）
        if "tools" in kwargs and kwargs["tools"] and self._original_tools is None:
            self._original_tools = list(kwargs["tools"])

        state = _get_or_init_state()
        selected_real = set(state.selected_tool_ids)

        # 兜底披露阶段（stage>=2）也复用同一套裁剪逻辑
        if self._verbose and state.stage >= 1:
            schema_chars = self._estimate_total_tool_schema_chars(
                filter_tools_by_selected(self._original_tools or [], selected_real)
            )
            total = self._estimate_total_tool_schema_chars(self._original_tools or [])
            saving = (1 - (schema_chars / max(1, total))) * 100 if total else 0
            print(f"[PTD] 阶段{state.stage}：仅披露 {len(selected_real)} 个 tools "
                  f"({sorted(selected_real)})，Schema ≈ {schema_chars}/{total} 字符（节省{saving:.0f}%）")

        new_kwargs = dict(kwargs)
        if self._original_tools is not None:
            subset = filter_tools_by_selected(self._original_tools, selected_real)
            if subset:
                new_kwargs["tools"] = subset
            else:
                new_kwargs.pop("tools", None)
                new_kwargs.pop("tool_choice", None)
        return messages, new_kwargs, "exec_stage"

    @staticmethod
    def _estimate_total_tool_schema_chars(tools: List[Any]) -> int:
        """粗略估算工具集合的 OpenAI function Schema 字符数（用于日志展示节省比例）。"""
        total = 0
        for t in tools:
            try:
                if hasattr(t, "get_input_schema"):
                    schema = t.get_input_schema()
                    import json as _json
                    total += len(_json.dumps(schema, ensure_ascii=False))
                else:
                    # 降级：拿 docstring + 名字估
                    total += len(_tool_name(t)) * 3
                    ds = getattr(t, "description", "") or getattr(t, "__doc__", "") or ""
                    total += len(ds)
            except Exception:
                total += PTD_TOKEN_FALLBACK_PER_TOOL_ESTIMATE  # 给一个保守估计值
        return total

    # --------------------------------------------------------------
    # 辅助：工具缺失检查 / message 合并
    # --------------------------------------------------------------
    def _check_missing_tool_disclosure(self, message: Any) -> Tuple[bool, Set[str]]:
        """检查 AIMessage.tool_calls 中是否调用了不在当前已披露集合中的工具。"""
        state = _get_or_init_state()
        if state.stage < 1:
            return False, set()
        tool_calls = getattr(message, "tool_calls", None) or []
        if not tool_calls:
            return False, set()
        missing: Set[str] = set()
        for tc in tool_calls:
            # tool_call 可能是 dict（langchain 新旧版本兼容）
            name = tc.get("name") if isinstance(tc, dict) else getattr(tc, "name", None)
            if not name:
                continue
            if name not in state.selected_tool_ids:
                missing.add(name)
        return (len(missing) > 0), missing

    @staticmethod
    def _append_menu_to_last_user(messages: Any) -> Any:
        """在最后一条 Human/User message 的末尾追加路由菜单。"""
        if not isinstance(messages, list):
            return messages
        # 深拷贝一份，避免污染原列表
        msgs = list(messages)
        menu = build_route_menu()
        for idx in range(len(msgs) - 1, -1, -1):
            m = msgs[idx]
            if isinstance(m, HumanMessage):
                new_content = (m.content or "") + menu
                # 替换为新的 HumanMessage（保留其它字段）
                new_kwargs: Dict[str, Any] = {"content": new_content}
                for k in ("name", "id", "additional_kwargs", "response_metadata"):
                    if hasattr(m, k):
                        v = getattr(m, k)
                        if v is not None:
                            new_kwargs[k] = v
                msgs[idx] = HumanMessage(**new_kwargs)
                break
            elif isinstance(m, dict) and m.get("role") == "user":
                m2 = dict(m)
                m2["content"] = (m2.get("content") or "") + menu
                msgs[idx] = m2
                break
        return msgs

    @staticmethod
    def _merge_chunks_to_ai(chunks: List[Any]) -> AIMessage:
        """把流式 chunks 合并为一个 AIMessage（content 拼接 + tool_calls 汇总）。"""
        content_parts: List[str] = []
        tool_calls: List[Dict] = []

        for c in chunks:
            # ChatGenerationChunk → 里面有 message
            if isinstance(c, ChatGenerationChunk):
                msg = c.message
            else:
                msg = c
            # 提取 content
            c_cont = getattr(msg, "content", None)
            if isinstance(c_cont, str) and c_cont:
                content_parts.append(c_cont)
            elif isinstance(c_cont, list):
                for seg in c_cont:
                    if isinstance(seg, str):
                        content_parts.append(seg)
                    elif isinstance(seg, dict) and "text" in seg:
                        content_parts.append(seg["text"])
            # 提取 tool_calls 增量
            inc_tc = getattr(msg, "tool_call_chunks", None) or getattr(msg, "tool_calls", None) or []
            for tc in inc_tc:
                if isinstance(tc, dict):
                    d = tc
                else:
                    d = {
                        "name": getattr(tc, "name", None),
                        "args": getattr(tc, "args", None),
                        "id": getattr(tc, "id", None),
                        "index": getattr(tc, "index", None),
                    }
                if not d.get("name"):
                    continue
                # 如果有 index，则归入对应位置（适用于 Chunk 增量）
                idx = d.get("index")
                if idx is not None and 0 <= idx < len(tool_calls):
                    # 合并
                    existing = tool_calls[idx]
                    existing["name"] = d.get("name") or existing.get("name")
                    existing["id"] = d.get("id") or existing.get("id")
                    if d.get("args"):
                        if isinstance(existing.get("args"), str) and isinstance(d.get("args"), str):
                            existing["args"] += d["args"]
                        elif isinstance(existing.get("args"), dict) and isinstance(d.get("args"), dict):
                            existing["args"].update(d["args"])
                        else:
                            existing["args"] = d.get("args")
                else:
                    tool_calls.append({
                        "name": d.get("name"),
                        "args": d.get("args") or {},
                        "id": d.get("id") or "",
                    })

        return AIMessage(
            content="".join(content_parts),
            tool_calls=tool_calls or None,
        )


# 用户原始提问的 ContextVar（供启发式兜底提取关键词）
_ptd_query_ctx: ContextVar[Optional[str]] = ContextVar("ptd_query", default=None)


def set_ptd_query(query: str):
    """在新请求开始时设置用户原始提问。"""
    return _ptd_query_ctx.set(query)


def reset_ptd_query(token=None):
    if token is not None:
        _ptd_query_ctx.reset(token)
