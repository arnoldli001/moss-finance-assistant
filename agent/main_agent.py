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

from api.context import set_session_context, reset_session_context, set_thread_context

from langchain_core.messages import AIMessage

# 【工作环境指令】及后续"规则/工作目录/上传文件"段落的过滤正则
# 匹配：换行 + 缩进空格 + 【工作环境指令】开始直到结尾的整段内容（包含上传文件、规则1-4等）
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
            system_prompt=main_agent_content['system_prompt'],
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
# project_root_path = Path(__file__).parents[1].absolute() # 绝对
# main_agent.invoke()
# main_agent.stream()
# main_agent.astream() [选他]
async def run_deep_agent(task_query,session_id,user_id=None):
    """
    定义流式+异步执行主智能体！！
    执行过程中，返回  会话文件化返回  调用子智能体  调用最终结果 （monitor）
    task_query: 前端提问的问题
    session_id: 每个前端会话对应的标识 （1.存储session_id ContextVars 2.session_id 给他创建对应的output输出地址）
                 同时作为 LangGraph thread_id，由 SqliteSaver 持久化对话历史，实现连续对话
    user_id: 可选，所属用户 ID；传入时会更新会话标题与时间戳
    """
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
    # 准备工作 【1. session_dir（前端） 2. relative_session_dir (大模型) 3. 上传的文件拼接上传文件专属提示词】
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

    #处理上传文件 （updated / session_session_id）
    updated_dir_path = project_root_path / "updated" / f"session_{session_id}"
    updated_info_prompt = "" # 有上传文件，拼接上传文件专属解析位置的提示词
    if updated_dir_path.exists():
        files = [ f.name  for f in updated_dir_path.iterdir()  if f.is_file()]
        # 将上传文件统一赋值到 output_dir 方便前端统一读取 session_dir
        if files:
            for filename in files:
                # 将原文件 -》 复制 -》 目标文件中  （copy2 保留原文件修改时间和权限等元数据）
                shutil.copy2(updated_dir_path / filename, session_dir / filename)
            # 构建提示词：从 prompts.yml 的 runtime_prompts 段加载模板，动态填入文件列表
            files_block = "\n".join([f"    - {f}" for f in files])
            updated_info_prompt = format_prompt("main_agent.updated_info_prompt", files_block=files_block)

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
        updated_info_prompt=updated_info_prompt,
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
                                # 记录工具调用到 trace
                                _trace_tool_calls.append({
                                    "name": tool_call.get('name', ''),
                                    "args": str(tool_call.get('args', ''))[:200],
                                })
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
                            _log_verbose_result("主智能体执行结果", final_content, max_len=MAIN_AGENT_VERBOSE_MAX_LEN)
                            monitor.report_task_result(final_content)
                            current_assistant_reply = final_content
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
        _is_task_replace = ((not _reason) or
                            "session_actor_replaced" in _reason or
                            _reason in ("cancelled", "task_replaced"))
        _is_parent = "parent_cancelled" in _reason

        if _is_user_stop:
            # 明确用户行为：控制台 + 前端都告知停止
            print(f"[Agent] 会话 {session_id} 任务被用户主动取消 (stop 按钮)")
            monitor.report_error("⏹ 任务已停止")
        elif _is_ws:
            # 正常连接管理：用户关浏览器属于正常路径，前端已断线，不用再 report_error
            print(f"[Agent] 会话 {session_id} 连接已断开，自动终止任务 "
                  f"(reason={_reason!r})")
            # 不调用 monitor.report_error：前端离线，发了也白发
        elif _is_timeout:
            # 超时：系统内部失败，需要提示用户重试
            elapsed = round(getattr(_tok, "age_sec", 0.0), 1) if _tok else 0.0
            print(f"[Agent][超时] 会话 {session_id} 任务在运行 {elapsed}s 后因超时自动终止 "
                  f"(timeout_reason={_reason!r})")
            monitor.report_error(
                f"⏱ 任务超时（{elapsed}s），可能是网络搜索或 LLM 响应较慢，建议拆分问题后重试"
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

