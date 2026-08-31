# -*- coding: utf-8 -*-
"""
流式输出统一事件协议（SSE / WebSocket 共用）。

设计目标：
  1. 前后端单一真源，避免 SSE 和 WS 两套事件字段不一致；
  2. 每个事件自包含（足够前端渲染，不需要拼接多事件上下文）；
  3. 兼容 LangChain astream_events 的 v1/v2 事件字段；
  4. 所有时间戳使用毫秒 epoch，前端本地时区格式化。

协议格式（SSE）：
    event: <type>           ← 事件类型，用作 EventSource 的 addEventListener 分支
    id:    <event_id>       ← 单调递增序号，断线续传用 Last-Event-ID
    data:  <json payload>   ← 统一 JSON，字段随 type 变化
    <空行>

事件类型总览（12 类 + 心跳 + 3 类断点续传控制）：
  open           : 流式会话打开（首包，<50ms 必须到达）
  delta          : LLM 增量文本 token（打字机效果的核心增量源，is_reasoning=true 表示思考 token）
  reasoning      : 推理过程 / Chain-of-Thought（一等公民，独立事件；前端默认折叠面板展示）
  tool_call      : 工具/子智能体 调用开始（含调用参数摘要）
  tool_result    : 工具/子智能体 执行完成（含结果摘要 + 来源索引条目）
  retrieve_result: 单一检索通道（Tavily / IMA知识库 / 知识星球 ZSXQ）返回的候选文档集（供引用元数据前置）
  citation_meta  : 引用映射元数据：[N] ←→ 来源 title/url/reliability。与最终回答 delta[N] 角标一一对应
  source_ref     : 信息来源索引列表快照（[1][2]... 引用数据，单独事件，避免污染 delta 流）
  progress       : 进度百分比 + 阶段提示（替代原有 monitor thinking）
  done           : 流式完成信号（含完整最终文本 + token 统计）
  error          : 异常 + 取消（error.cancelled=true 表示用户取消）
  heartbeat      : 空心跳，间隔 15s，防止代理/浏览器超时断连
  # ---------- 断点续传专用（3 类控制事件）----------
  replay_start   : 通知客户端 "下面一段是你断线期间的事件回放"（mode=continue|resync|full）
  replay_end     : 通知客户端 "回放结束，接下来是实时事件"（replay_count, gap_count: 0 表示完美续传无缺口）
  gap            : 通知客户端 "请求的 Last-Event-ID 早于服务端缓存起点，已经有缺口，需要执行全量重置"
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ======================================================================
# 事件类型枚举
# ======================================================================

class StreamEventType(str, Enum):
    OPEN = "open"
    DELTA = "delta"
    REASONING = "reasoning"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    RETRIEVE_RESULT = "retrieve_result"  # 检索通道返回候选文档（一等公民：Tavily/IMA/ZSXQ 各一条）
    CITATION_META = "citation_meta"      # 引用映射元数据 [N] → title/url/reliability
    SOURCE_REF = "source_ref"
    PROGRESS = "progress"
    DONE = "done"
    ERROR = "error"
    HEARTBEAT = "heartbeat"
    # ---------- 断点续传控制事件 ----------
    REPLAY_START = "replay_start"  # 回放开始
    REPLAY_END = "replay_end"      # 回放结束 → 转为实时流
    GAP = "gap"                    # 断点不可续：缓存已溢出，需全量重置


# ======================================================================
# 各事件的 payload dataclass（强类型 + 易文档）
# ======================================================================

@dataclass
class OpenPayload:
    """会话打开：首包，给前端 request_id / thread_id / 服务端已接收确认。"""
    request_id: str
    thread_id: str
    user_id: str = ""
    server_ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    # 前端用这个来显示"预计耗时"区间：[min_sec, max_sec]
    eta_seconds: List[int] = field(default_factory=lambda: [10, 60])


@dataclass
class DeltaPayload:
    """LLM 增量文本。index 是顺序号，前端可做去重/排序。"""
    index: int
    text: str
    # 是否属于"推理过程 token"（vLLM/Llama3 带 <think> 标签）——前端区分渲染
    is_reasoning: bool = False
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ReasoningPayload:
    """一段完整的推理 / CoT。前端默认折叠，点击展开。

    一等公民：每段 reasoning 都有 stage（阶段），与 progress 一一对应。
    常见 stage:
      - "intent_classify" : 意图分类（路由选择哪个检索通道 / 是否需要工具）
      - "retrieval_plan"  : 检索规划（决定查 Tavily 什么、IMA 哪个知识库、ZSXQ 什么群）
      - "synthesis_plan"  : 综合推理（如何把 3 路检索结果 + qwen-8b 整合）
      - "risk_check"      : 输出前风险核查（含 AGENTS.md 风险声明判断）
      - "model_coT"       : 真实模型输出的 <think> 思考（如 DeepSeek Reasoner / Qwen <think>）
    """
    reasoning_id: str
    title: str                      # 例如："为什么选择搜索 Tavily 而不是 RAG？"
    content: str                    # 完整推理内容（可含 Markdown）
    stage: str = "model_coT"        # 阶段标识，用于前端 tab/折叠面板
    elapsed_ms: int = 0             # 这段推理花费的毫秒数
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ToolCallPayload:
    """工具/子智能体开始调用。"""
    call_id: str
    tool_name: str
    tool_category: str              # "tool" / "subagent" / "llm"
    args_snippet: Dict[str, Any] = field(default_factory=dict)  # 只放摘要字段，不放大体积参数
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ToolResultPayload:
    """工具/子智能体结束。附带一条"来源索引候选"，如果 source_refs 非空，
    会被前端记录到"总来源索引池"中统一编号。"""
    call_id: str
    tool_name: str
    success: bool
    duration_ms: int = 0
    result_snippet: str = ""        # 摘要，≤200 字（长内容前端不展示）
    # 来源引用候选：每个是一条"引用卡片"，source_index 统一编号从 bus 给出
    source_refs: List[Dict[str, Any]] = field(default_factory=list)
    # 检索通道命中的原始文档集（供编排路由层后续 [citation:N] 注入 & 回答映射）
    retrieved_docs: List[Dict[str, Any]] = field(default_factory=list)
    channel: str = ""               # "tavily" / "ima" / "zsxq" / "db" / "other"
    error_msg: str = ""
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


# ======================================================================
# 引用 & 检索元数据事件（2 类新增：retrieve_result / citation_meta）
# ======================================================================

@dataclass
class RetrieveResultItem:
    """单个检索候选文档（Tavily 结果条目 / IMA 文档 / ZSXQ 单条帖子）。"""
    doc_id: str                     # 通道内唯一，如 "ima:<kb_name>:<doc_id>" 或 "tavily:<hash>"
    title: str
    url: str = ""
    content: str = ""               # 截断到 2000 字以内的正文
    source_type: str = "web"        # "web" / "pdf" / "knowledge_base" / "forum" / "db"
    reliability: str = "待验证"     # Context Engineer 三档：可靠 / 权威 / 待验证
    channel: str = ""               # "tavily" / "ima" / "zsxq" / "db"
    score: float = 0.0              # 检索分数（归一化 0-1，越高越匹配）
    published_at: str = ""          # 原始发布日期（如有）
    meta: Dict[str, Any] = field(default_factory=dict)  # 通道专属，如 IMA 的 knowledge_base 名


@dataclass
class RetrieveResultPayload:
    """一路检索通道返回的候选文档集（独立事件）。
    前端可把它作为"实时检索预览"展示在右侧面板中，不必等最终回答。"""
    channel: str                    # "tavily" / "ima" / "zsxq" / "db"
    query: str                      # 该检索通道实际执行的 query
    items: List[RetrieveResultItem]
    duration_ms: int = 0
    success: bool = True
    error_msg: str = ""
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class CitationMetaItem:
    """一条引用的元数据：[N] → title/url/reliability。

    与 source_ref 的区别：
      - source_ref 是"当前所有来源"快照，用于文末参考文献列表。
      - citation_meta 是"引用角标 [N]" 到具体来源的 1:1 映射，增量逐条推送，
        便于后端在流式回答 delta 中解析到 [N] 时，给前端补 metadata，让用户
        可以在正文悬停角标时弹出"来源卡片"。
    """
    index: int                      # [N] 中的编号，统一分配从 1 开始
    title: str
    url: str = ""
    source_type: str = "web"
    reliability: str = "待验证"
    snippet: str = ""               # 模型回答中引用这一条的那 1~2 句片段（如有）
    published_at: str = ""
    channel: str = ""               # "tavily" / "ima" / "zsxq" / "db"


@dataclass
class CitationMetaPayload:
    """增量下发若干引用角标映射。"""
    items: List[CitationMetaItem]
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class SourceRefItem:
    """一条引用来源。前端在正文 [n] 处点击时弹出该卡片。"""
    index: int                      # 统一编号（1, 2, 3... 全局按出现顺序）
    title: str
    url: str = ""
    source_type: str = "web"        # "web" / "pdf" / "db" / "knowledge_base" / "forum"
    reliability: str = "待验证"     # "可靠" / "待验证" —— 匹配 Context Engineer 甄别结果
    snippet: str = ""               # 引用该来源的片段
    published_at: str = ""          # 原始发布日期（如有）


@dataclass
class SourceRefPayload:
    """一次性下发"当前所有已收集来源"的列表，用于在文末渲染参考文献区。"""
    items: List[SourceRefItem]
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ProgressPayload:
    """进度 + 阶段提示。替代原 WS monitor_event 的 thinking/tool_start。"""
    stage: str                      # e.g. "分析问题", "联网搜索", "读取财报", "生成答案"
    percent: int = 0                # 0 - 100，粗略估算
    detail: str = ""                # 详细提示，例如："正在搜索 Tavily(3/5)"
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class DonePayload:
    """流式完成信号。含完整文本 + 统计，前端可做校验。"""
    final_text: str
    # Token 统计（如 LLM SDK 提供）
    usage: Dict[str, int] = field(default_factory=dict)  # prompt_tokens / completion_tokens / total_tokens
    total_duration_ms: int = 0
    source_ref_count: int = 0       # 来源索引总数
    tool_call_count: int = 0        # 工具调用总数
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ErrorPayload:
    """错误 / 用户取消。"""
    message: str
    code: str = "INTERNAL_ERROR"    # CANCELLED / TIMEOUT / AUTH_FAILED / RATE_LIMIT / INTERNAL_ERROR
    cancelled: bool = False         # True 表示是用户主动取消或 SSE 断开
    recoverable: bool = True
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


# ======================================================================
# 断点续传控制事件 Payload（3 类）
# ======================================================================

@dataclass
class ReplayStartPayload:
    """断线后重连：服务端告诉客户端「开始为你回放缺失事件」。

    mode 含义：
      - "continue" ：完美续传，Last-Event-ID 正好在缓冲内，零缺口
      - "resync"   ：有缺口（Last-Event-ID 已溢出环形缓冲），但服务端
                     用 final_text + 完整来源列表给客户端"一次性重同步"
      - "full"     ：服务端无法恢复（重启/进程冷启动），客户端应清屏重渲染
    """
    mode: str                       # "continue" | "resync" | "full"
    request_id: str                 # 本次重连请求的 id（日志追踪）
    last_event_id: str = ""         # 客户端传上来的 ID
    expected_replay_count: int = 0  # 预计回放多少条事件（0 表示走 resync 重同步而非逐条回放）
    has_gap: bool = False           # 是否有缺口
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ReplayEndPayload:
    """回放结束：后续帧是"实时事件"。"""
    replay_count: int = 0           # 实际回放了多少条事件
    gap_count: int = 0              # 期间缺口数（>0 说明不是完美续传）
    from_cache_ts_ms: int = 0       # 环形缓冲内最早事件的 ts
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class GapPayload:
    """Last-Event-ID 对应的事件不在缓冲内（环形缓冲已满溢出或服务端重启）。

    客户端收到 gap 后：
      1) 从当前流式消息中清掉"未完成打字机文本"（如有 replay_start.resync 模式，
         后续会给一份 final_text 完整重同步）；
      2) 在 UI 底部 toast 提示「已恢复，但部分过程可能遗漏」或类似。
    """
    reason: str                     # "buffer_overflow" | "server_restart" | "not_found" | "session_done"
    last_event_id: str = ""
    first_cached_id: str = ""       # 环形缓冲内最早事件 id，辅助日志
    suggestion: str = "resync"      # "resync" 表示会立即发完整 final_text；"restart" 表示请重新提问
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


# ======================================================================
# SSE 分帧序列化器（核心：保证事件边界稳定）
# ======================================================================

# 根据 ExperienceRecall 教训：
#   - 事件必须做稳定分帧：data: <json> + 以 \n\n 结尾
#   - 禁止直接 `yield f"data: {chunk.content}\n\n"` —— 多事件粘包/拆包会崩前端解析
#   - JSON 内部换行 \n 不会影响 SSE 协议（SSE data: 允许多行，以空行结束；但我们用单行 JSON）

class SSEFrame:
    """SSE 帧序列化工具。每个方法返回一个完整的 SSE 帧字符串（结尾带 \n\n）。"""

    @staticmethod
    def _frame(type: str, data: Dict[str, Any], event_id: Optional[str] = None) -> str:
        frame_lines: List[str] = []
        frame_lines.append(f"event: {type}")
        if event_id is not None:
            frame_lines.append(f"id: {event_id}")
        # ensure_ascii=False：中文直接输出，省流量且 readable；无 \n 保证单行
        payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        # 防御：把 payload 中的 \r / \n 全部替换（理论上 json.dumps separators 紧凑模式不会出换行，但要防万一）
        payload = payload.replace("\r", " ").replace("\n", " ")
        frame_lines.append(f"data: {payload}")
        # 结尾空行：SSE 事件分隔符
        frame_lines.append("")
        frame_lines.append("")
        return "\n".join(frame_lines)

    # ---------- 便捷构造器（每个事件一种，避免调用方猜字段）----------

    @staticmethod
    def open(p: OpenPayload, event_id: str) -> str:
        return SSEFrame._frame(StreamEventType.OPEN.value, asdict(p), event_id)

    @staticmethod
    def delta(p: DeltaPayload, event_id: str) -> str:
        return SSEFrame._frame(StreamEventType.DELTA.value, asdict(p), event_id)

    @staticmethod
    def reasoning(p: ReasoningPayload, event_id: str) -> str:
        return SSEFrame._frame(StreamEventType.REASONING.value, asdict(p), event_id)

    @staticmethod
    def tool_call(p: ToolCallPayload, event_id: str) -> str:
        return SSEFrame._frame(StreamEventType.TOOL_CALL.value, asdict(p), event_id)

    @staticmethod
    def tool_result(p: ToolResultPayload, event_id: str) -> str:
        data = asdict(p)
        return SSEFrame._frame(StreamEventType.TOOL_RESULT.value, data, event_id)

    @staticmethod
    def retrieve_result(p: RetrieveResultPayload, event_id: str) -> str:
        data = {
            "channel": p.channel,
            "query": p.query,
            "items": [asdict(x) for x in p.items],
            "duration_ms": p.duration_ms,
            "success": p.success,
            "error_msg": p.error_msg,
            "ts_ms": p.ts_ms,
        }
        return SSEFrame._frame(StreamEventType.RETRIEVE_RESULT.value, data, event_id)

    @staticmethod
    def citation_meta(p: CitationMetaPayload, event_id: str) -> str:
        data = {
            "items": [asdict(x) for x in p.items],
            "ts_ms": p.ts_ms,
        }
        return SSEFrame._frame(StreamEventType.CITATION_META.value, data, event_id)

    @staticmethod
    def source_ref(p: SourceRefPayload, event_id: str) -> str:
        data = {"items": [asdict(x) for x in p.items], "ts_ms": p.ts_ms}
        return SSEFrame._frame(StreamEventType.SOURCE_REF.value, data, event_id)

    @staticmethod
    def progress(p: ProgressPayload, event_id: str) -> str:
        return SSEFrame._frame(StreamEventType.PROGRESS.value, asdict(p), event_id)

    @staticmethod
    def done(p: DonePayload, event_id: str) -> str:
        return SSEFrame._frame(StreamEventType.DONE.value, asdict(p), event_id)

    @staticmethod
    def error(p: ErrorPayload, event_id: str) -> str:
        return SSEFrame._frame(StreamEventType.ERROR.value, asdict(p), event_id)

    # ---------- 断点续传控制事件构造器 ----------
    @staticmethod
    def replay_start(p: ReplayStartPayload, event_id: str) -> str:
        return SSEFrame._frame(StreamEventType.REPLAY_START.value, asdict(p), event_id)

    @staticmethod
    def replay_end(p: ReplayEndPayload, event_id: str) -> str:
        return SSEFrame._frame(StreamEventType.REPLAY_END.value, asdict(p), event_id)

    @staticmethod
    def gap(p: GapPayload, event_id: str) -> str:
        return SSEFrame._frame(StreamEventType.GAP.value, asdict(p), event_id)

    @staticmethod
    def heartbeat(event_id: str) -> str:
        # 心跳是唯一无意义 payload 的事件：用 comment 也可以，但走统一格式方便前端计数
        return SSEFrame._frame(StreamEventType.HEARTBEAT.value,
                               {"ts_ms": int(time.time() * 1000)}, event_id)

    @staticmethod
    def comment(msg: str = "keepalive") -> str:
        """SSE comment（以冒号开头），不触发任何前端事件，纯保活用途。"""
        return f": {msg}\n\n"


def new_event_id() -> str:
    """生成单调 + 可追踪的 event_id：时间戳毫秒 + 4位随机片段。"""
    return f"{int(time.time()*1000):x}-{uuid.uuid4().hex[:4]}"
