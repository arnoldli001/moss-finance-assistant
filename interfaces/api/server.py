#coding = utf-8

import shared.compat_bootstrap

import sys
import os
# 强制 stdout/stderr 行缓冲，确保 uvicorn --reload 模式下 print 日志实时输出
# reconfigure 是 Python 3.7+ TextIOWrapper 的方法，但类型存根 TextIO 未声明，故用 type: ignore
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except AttributeError:
    pass
import uuid
import asyncio

# 加载 .env 环境变量（find_dotenv 确保从项目根目录查找）
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import uvicorn
import logging
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

# 过滤 uvicorn 错误日志中的 WebSocketDisconnect traceback（客户端正常断开不应打印堆栈）
class _WSDisconnectFilter(logging.Filter):
    _KEYWORDS = ("WebSocketDisconnect", "1005", "1006", "NO_STATUS_RCVD", "ABNORMAL_CLOSURE")

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        for kw in self._KEYWORDS:
            if kw in msg:
                return False
        # 检查异常信息（traceback 中的异常类型和消息）
        if record.exc_info:
            try:
                exc_type_str = str(record.exc_info[0])
                exc_val_str = str(record.exc_info[1])
                for kw in self._KEYWORDS:
                    if kw in exc_type_str or kw in exc_val_str:
                        return False
            except Exception:
                pass
        return True
from starlette.responses import JSONResponse, Response
from pydantic import BaseModel, Field#负责定义请求体（Request Body）的结构和响应（Response）的格式
from typing import Any, List, Optional, Dict, Tuple
from collections import defaultdict, deque
from datetime import datetime, timedelta

# ====== 企业级鉴权 & 限流（P0 修复：API 全裸 → JWT+RBAC+4 级 QPM 限流）======
from fastapi import Depends as _Depends  # 统一给所有业务端点 Depends() 使用
from shared.utils.auth import (
    CurrentUser,
    TokenResponse,
    create_token_pair,
    refresh_access_token,
    create_guest_token,
    get_current_user,
    get_current_user_optional,
    get_current_user_websocket,
    current_user_id_must_match,
)
from shared.utils.rate_limiter import (
    RateLimiter,
    get_rate_limiter as _get_global_rl,
)
# 旧治理层 RBACPolicy（config/rbac_policy.json 中 4 档 QPM）已实现但未接线：
# 本次中间件 AUTH_AND_RATE_MIDDLEWARE 会统一按「JWT role」执行 4 档 QPM 限流，
# 并把 CurrentUser → 旧 RBACPolicy.UserContext 同步写 ContextVar，避免破坏旧治理层调用。
import governance.guardrails.rbac as _rbac_mod
# 上面一行等效：from governance.guardrails import rbac as _rbac_mod
# 后续用 _rbac_mod.RBACPolicy / _rbac_mod._current_user 访问。

def _find_project_root(start: Path) -> Path:
    """向上查找项目根（锚点：AGENTS.md/.git/main.py/requirements.txt），
    避免硬编码 parents[N] 因目录迁移（api/server.py → interfaces/api/server.py）动态失效。"""
    cur = start.resolve()
    anchors = ("AGENTS.md", ".git", "main.py", "requirements.txt", ".env.example")
    for p in [cur, *cur.parents]:
        if any((p / a).exists() for a in anchors):
            return p
    # 兜底：本文件位于 <project>/interfaces/api/ → parents[2] = 项目根
    return cur.parents[2]


# Add project root to sys.path
current_dir = Path(__file__).resolve().parent
project_root = _find_project_root(current_dir)
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# ===== Actor Model 集成：导入 Actor 基类与具体 Actor =====
from agent.actor_base import get_actor_system
from agent.actors import (
    SessionRegistryActor, SRMsg,
    CircuitBreakerActor, CBMsg,
    ConnectionManagerActor, ConnMsg,
    SLOMonitorActor, SLOMsg,
)
# 会话任务 Actor 句柄（ lifespan 启动后赋值，API 中使用）
_session_actor: Optional[SessionRegistryActor] = None
_circuit_breaker_actor: Optional[CircuitBreakerActor] = None
_conn_manager_actor: Optional[ConnectionManagerActor] = None
_slo_monitor_actor: Optional[SLOMonitorActor] = None

# ===== CancellationToken 集成：跨层级取消联动 =====
from agent.request_context import (
    create_request_context,
    bind_request_context,
    unbind_request_context,
    cancel_by_thread_id,
    RequestContext,
)

# ===== 全局常量集中引用（替代魔鬼数字，统一修改一处即全局生效）=====
from config.constants import (
    DEFAULT_AGENT_TIMEOUT_SEC,
    DEFAULT_BACKGROUND_TIMEOUT_SEC,
    SCHEDULER_STARTUP_WAIT_SEC,
    NGROK_CLEANUP_WAIT_SEC,
    NGROK_TUNNEL_MAX_POLL_ROUNDS,
    NGROK_TUNNEL_POLL_INTERVAL_SEC,
    SHORT_HTTP_TIMEOUT_SEC,
    DEFAULT_SERVER_PORT,
    NGROK_LOCAL_API_PORT,
    SCHEDULER_CANCEL_WAIT_SEC,
    SUBPROCESS_WAIT_TIMEOUT_SEC,
    RATE_LIMIT_PER_MINUTE,
    RATE_LIMIT_WINDOW_SEC,
    OLLAMA_DEFAULT_BASE_URL,
    OLLAMA_PROBE_TIMEOUT_SEC,
    OLLAMA_LAUNCH_POLL_INTERVAL_SEC,
    OLLAMA_LAUNCH_POLL_MAX_ROUNDS,
    OLLAMA_PULL_TIMEOUT_SEC,
    OLLAMA_MODELS_LIST_TIMEOUT_SEC,
    OLLAMA_PULL_LOG_TAIL_KEEP,
    OLLAMA_PULL_PROGRESS_INTERVAL_SEC,
    OLLAMA_PULL_PROGRESS_LINE_MAX_CHARS,
    OLLAMA_PULL_HARD_TIMEOUT_SEC,
    TEXT_SANITIZE_SHORT_TEXT_THRESHOLD_LEN,
    HTTP_CODE_TOO_MANY_REQUESTS,
    HTTP_CODE_NOT_FOUND,
    SERVER_OUTPUT_MAX_STDOUT_TAIL_LINES,
    SERVER_FINAL_RETURN_LINE_MIN_LEN,
    SERVER_JSON_DEBUG_LINE_MIN_LEN,
    SERVER_PROGRESS_SAFE_TRUNCATE_LEN,
    ZSXQ_BROWSER_LOCK_WAIT_TIMEOUT_SEC,
    STREAM_DISCONNECT_POLL_INTERVAL_SEC,
    STREAM_RESUME_BODY_LAST_EVENT_ID_ALLOW,
    STREAM_RESUME_COLD_RESTART_SUGGESTION,
)

# ===== P1-F：子路由拆分（盘前小作文热度 → interfaces/api/routes/zsxq.py）=====
# 路由 + zsxq/ollama 8 辅助函数 + 编排函数已迁移到独立模块。
# server.py 内其它代码（scheduler 定时回调 L641、复盘预测 L2265、L2337）
# 通过下面两条 re-import 复用公共编排：_run_zsxq_analysis / _save_zsxq_to_history / ZsxqAnalysisRequest。
# 注意：install_server_helpers 必须延迟到 _register_background_task 定义完成之后再调用，
# 避免出现「app 创建时尚未定义 _register_background_task」的 NameError。
from interfaces.api.routes.zsxq import (  # noqa: E402
    router as zsxq_router,
    _run_zsxq_analysis,
    _save_zsxq_to_history,
    ZsxqAnalysisRequest,
    install_server_helpers as _zsxq_install_server_helpers,
)
_ZSXQ_ROUTER_INSTALLED: bool = False


def _ensure_zsxq_router_installed_once() -> None:
    """延迟挂载 zsxq_router + 注入 server 级 helpers（只执行一次）。"""
    global _ZSXQ_ROUTER_INSTALLED
    if _ZSXQ_ROUTER_INSTALLED:
        return
    _zsxq_install_server_helpers(
        run_with_ctx=_run_with_ctx,
        register_background_task=_register_background_task,
        default_bg_timeout=_DEFAULT_BG_TIMEOUT,
    )
    app.include_router(zsxq_router)
    _ZSXQ_ROUTER_INSTALLED = True

# 默认超时：Agent 主流程 180s，后台分析 300s（知识星球抓取+分析较耗时）
_DEFAULT_AGENT_TIMEOUT: float = DEFAULT_AGENT_TIMEOUT_SEC
_DEFAULT_BG_TIMEOUT: float = DEFAULT_BACKGROUND_TIMEOUT_SEC


# ---- 流式输出：延迟载入（避免 server 启动导入链变重）----
def _load_stream_runtime():
    from api.stream_bus import (
        get_stream_bus_sync,
        install_monitor_bridge_to_bus,
    )
    return get_stream_bus_sync(), install_monitor_bridge_to_bus


async def _run_with_ctx(
    thread_id: str,
    user_id: Optional[str],
    session_dir: Optional[str],
    timeout_sec: Optional[float],
    corofn,
    *args,
    quiet: bool = False,
    **kwargs,
):
    """
    协程包装器：在真正执行 corofn 前，
    1) 创建三位一体的 RequestContext（取消令牌 + 元数据 + 超时）；
    2) 绑定到 ContextVar（这样在任何调用深度 check_cancelled() 都能拿到）；
    3) 注册当前 Task 为令牌的子任务 → 令牌 cancel 时 → task.cancel() 触发 CancelledError（双重保险）；
    4) 若 quiet=True，则开启 main_agent 的静默模式，避免控制台刷冗长结果；
    5) finally 中 unbind + dispose + 恢复 quiet。
    """
    ctx: Optional[RequestContext] = None
    bind_tok = None
    old_quiet = None
    try:
        # 快捷按钮：开启控制台静默模式，避免刷 Updated todo / 5000字结果
        if quiet:
            from agent.main_agent import set_quiet_mode as _set_qm
            old_quiet = _set_qm(True)
        ctx = create_request_context(
            thread_id=thread_id,
            user_id=user_id,
            session_dir=session_dir,
            timeout_sec=timeout_sec,
            extras={"entry": f"{corofn.__name__}" if hasattr(corofn, "__name__") else "unknown", "quiet": quiet},
        )
        bind_tok = bind_request_context(ctx)
        # 登记当前运行的 Task：一旦 CancellationToken.cancel()，Task.cancel() 也会被触发
        try:
            me = asyncio.current_task()
            if me is not None:
                ctx.token.register_child_task(me)
        except Exception:
            pass
        return await corofn(*args, **kwargs)
    finally:
        if old_quiet is not None:
            try:
                from agent.main_agent import set_quiet_mode as _set_qm_restore
                _set_qm_restore(old_quiet)
            except Exception:
                pass
        if bind_tok is not None:
            unbind_request_context(bind_tok)
        if ctx is not None:
            ctx.dispose()

from agent.main_agent import run_deep_agent, get_session_history
from agent.prompts import format_prompt
from api.monitor import manager
from api import storage

output_dir = project_root / "output"
output_dir.mkdir(exist_ok=True)

class TaskRequest(BaseModel):
    query: str
    thread_id: Optional[str] = None
    # ⚠️ v2 鉴权升级：业务端点 user_id **不再信任 Request Body 传入**，统一从 JWT 的 CurrentUser.user_id 取；
    # 此字段仅保留用于「未登录临时请求」（已被端点逻辑在 current_user.is_guest 时二次校验），
    # 普通登录用户传了也会被 CurrentUser.user_id 覆盖，避免水平越权。
    user_id: Optional[str] = None


class UserRequest(BaseModel):
    user_id: str
    display_name: Optional[str] = None


class SessionRequest(BaseModel):
    title: Optional[str] = None


# ======================================================================
# 【v2 P0 鉴权升级】认证相关 Request / 响应模型
# ======================================================================
class RegisterRequest(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=64,
                         description="登录用户名/手机号/邮箱。企业版建议唯一用户名")
    password: str = Field(..., min_length=6, max_length=128)
    display_name: Optional[str] = Field(default=None, max_length=64)


class LoginRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=128)


class RefreshRequest(BaseModel):
    refresh_token: str = Field(..., min_length=16)


class ChangePasswordRequest(BaseModel):
    old_password: str = Field(..., min_length=1, max_length=128)
    new_password: str = Field(..., min_length=6, max_length=128)

_PREMARKET_NEWS_SIGNATURE: tuple = (
    "韭研社区", "炒股吧", "同花顺股吧", "东方财富股吧",
    "利好还是利空的逻辑A股概念板块", "分别给出利好和利空个股清单",
)

def _is_premarket_news_query(query: str) -> bool:
    """判断 query 是否来自前端盘前新闻按钮。"""
    if not query:
        return False
    hits = sum(1 for kw in _PREMARKET_NEWS_SIGNATURE if kw in query)
    return hits >= 2

# —— 北京时间时区 ——
from datetime import timezone as _tz_mod, timedelta as _td_mod
_TZ_BEIJING = _tz_mod(_td_mod(hours=8))

def _beijing_now() -> "datetime":
    return datetime.now(_TZ_BEIJING)
def _weekday_cn_of(dt: "datetime") -> str:
    return ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][dt.weekday()]

def _format_time_range_hint(now_bj: "datetime") -> str:
    """按业务规则计算盘前新闻的搜索时间窗口并返回中文提示。
    规则（优先级从上到下命中即止）：
      1) 周一～周五 10:00–15:00 → 最近 6 个小时
      2) 周一～周五 15:00–24:00 → 当日 15:00 至 现在
      3) 其余时段（工作日 00:00–10:00 以及周末全天）→ 昨日 15:00 至 现在
         · 其中"昨日"跨周末时按自然日回退：周六→周五、周日→周六、周一→周日；
           这是用户指定规则的直接实现，不做"跳过周末回到周五"的特殊处理。
    """
    from datetime import timedelta as _td

    wd = now_bj.weekday()  # 0=周一 ... 4=周五, 5=周六, 6=周日
    hour = now_bj.hour
    minute = now_bj.minute
    mins_since_midnight = hour * 60 + minute  # 精确到分钟，避免 10:00 / 15:00 边界按整点漂移

    is_workday = 0 <= wd <= 4

    # 规则 1：工作日 10:00–15:00（含 10:00:00，不含 15:00:00） → 最近 6 小时
    if is_workday and (10 * 60) <= mins_since_midnight < (15 * 60):
        start = now_bj - _td(hours=6)
        return f"{start.strftime('%m月%d日 %H:%M')} 至 现在（最近 6 小时）"

    # 规则 2：工作日 15:00–24:00（含 15:00:00 及之后） → 当日 15:00 起
    if is_workday and mins_since_midnight >= (15 * 60):
        start_today_15 = now_bj.replace(hour=15, minute=0, second=0, microsecond=0)
        return f"{start_today_15.strftime('%m月%d日')} 15:00 至 现在"

    # 规则 3：其余时段（工作日 0–10 点，以及周末全天） → 昨日 15:00 起
    yesterday = now_bj - _td(days=1)
    start_yesterday_15 = yesterday.replace(hour=15, minute=0, second=0, microsecond=0)
    return f"{start_yesterday_15.strftime('%m月%d日')} 15:00 至 现在"


def _rewrite_premarket_query_if_shortcut(query: str) -> tuple[str, bool]:
    """若 query 来自盘前新闻快捷按钮，则：
    1) 获取当前的北京时间 
    2) 剥离 prompt 中"查询今天是周几"的日期请求段，替换为系统已注入前缀
    3) 返回 (rewritten_query, was_rewritten)
    """
    if not _is_premarket_news_query(query):
        return query, False

    _now_bj = _beijing_now()
    _weekday_cn = _weekday_cn_of(_now_bj)
    _current_time_str = _now_bj.strftime("%Y年%m月%d日 %H:%M")
    _search_range_hint = _format_time_range_hint(_now_bj)

    import re as _re
    cleaned = query
    # 剥掉："1）查询今天是周几..." 直到遇到 "2）根据搜索时间范围，搜..." 之前
    cleaned = _re.sub(
        r"[0-9]+[）.)]\s*查询[^\n]{0,10}今天是周几.*?(?=[0-9]+[）.)]\s*(?:根据搜索时间范围，搜|搜韭研|获取美股)|(?=根据搜索时间范围，搜))",
        "",
        cleaned,
        count=1,
        flags=_re.DOTALL,
    )
    # 顺带去掉用户之前打的补丁："关于北京时间不需要确认依据，不需要交叉验证"
    cleaned = _re.sub(
        r"关于北京时间[^\n。；，]*?(?:不|无需|不需要)[^\n。；，]*?(?:交叉验证|来源依据|信息来源)[^\n。；，]{0,120}",
        "",
        cleaned,
        count=1,
    )
    # 去掉标点开头
    cleaned = cleaned.strip(" ，；。\n\t\r、,.;:")

    injected_prefix = (
        f"· 当前北京时间：{_current_time_str}（{_weekday_cn}）\n"
        f"· 搜索时间范围：{_search_range_hint}\n"
        f"  · 必须把所有信息需求（5 大社区热度 + 美股夜盘科技股）合并到同一条搜索 query 中执行一次综合搜索；\n"
        f"当第一次综合搜索返回的结果里出现「利好个股名单 / 利空个股名单 / 社区热度前10 "
        f"【硬性约束】禁止输出与结果无关或相关性小的内容。\n"
    )
    final_query = injected_prefix + (cleaned or query)
    return final_query, True


# ======================================================================
# 【方案一：Router+Workflow 优先链路】——薄适配层（严格对齐旧 SSE 事件）
#   - 辅助函数 1：_try_run_workflow_push_events()
#       run_analysis_workflow() → 将 final_answer 按原股票缓存短路同款事件
#       序列（reasoning→progress→citation→source_ref→delta→task_result→done）
#       推入 StreamBus；任何异常抛给外层 → fallback 旧 run_deep_agent
#   - 辅助函数 2：_run_coro_with_workflow_priority()
#       给非流式 POST /api/task 的 _run_with_ctx 作为 corofn，内部：
#       先调 辅助函数1 → 异常则 fallback run_deep_agent（兼容兜底）
# ======================================================================

async def _try_run_workflow_push_events(
    query: str,
    thread_id: str,
    user_id: str,
    *,
    bus: "Any",
    quiet: bool = False,
    has_visual_input: bool = False,
    _start_monotonic: "Optional[float]" = None,
) -> "dict":
    """Router+analysis_workflow 结果桥接到 StreamBus 事件序列；失败直接抛异常。"""
    import time as _t
    _start_monotonic = _start_monotonic or _t.monotonic()
    from orchestration.workflows.analysis_workflow import (
        run_analysis_workflow, RISK_DISCLAIMER,
    )
    # 跑工作流 DAG（内部 Router 判定 + 缓存命中短路 + 源并发聚合 + 最终推理）
    result = await run_analysis_workflow(
        query, thread_id, user_id,
        has_visual_input=has_visual_input,
        enable_gemma4_router=True,
        bus=bus,
        quiet=quiet,
    )
    final_answer = result.final_answer or ""
    # 风险声明双保险（盘前缓存 md 自带；其他分支 workflow._final_analyst_answer 已兜底；
    # 这里再强制校验一次，极端情况下直接拼）
    if "不构成投资建议" not in final_answer:
        if not final_answer.endswith("\n"):
            final_answer += "\n"
        final_answer += "\n" + RISK_DISCLAIMER + "\n"

    branch = result.router_decision.branch.value
    _premarket_hit = bool(result.branch_trace.get("premarket_cache_hit"))
    _stock_cache_cnt = int(result.branch_trace.get("stock_cache_hit_count") or 0)
    cache_hit = _premarket_hit or (_stock_cache_cnt > 0)

    # --- 北京时间字符串（给 reasoning / citation 时间戳用） ---
    import datetime as _dt_iso
    try:
        from zoneinfo import ZoneInfo as _ZI
        _now_bj = _dt_iso.datetime.now(_ZI("Asia/Shanghai"))
    except Exception:
        _now_bj = _dt_iso.datetime.utcnow() + _dt_iso.timedelta(hours=8)
    _now_bj_str = _now_bj.strftime("%Y-%m-%d %H:%M:%S")

    # (1) 【去重保留变量】reasoning 面板已经在 workflow.run_analysis_workflow 内部
    #     通过 bus.ev_reasoning("🧭 智能路由") / ev_reasoning("📦 缓存命中 / 双源完成 / ...")
    #     分阶段推送；这里不再重复推同一块路由说明面板，防止前端出现两次"智能路由"折叠。
    #     reasoning_line 字符串保留，便于审计/日志/未来回溯（不改现有变量名减少兼容风险）。
    _parts = [
        f"【智能路由摘要：{branch}】decided_by="
        f"{result.router_decision.decided_by or 'unknown'}；"
        f"reason={result.router_decision.reason or '规则匹配'}"
    ]
    _stocks = (result.router_decision.extracted_stock_codes
               + result.router_decision.extracted_stock_names)
    if _stocks:
        _tail = "…" if len(_stocks) > 5 else ""
        _parts.append(f"；stocks={', '.join(_stocks[:5])}{_tail}")
    if cache_hit:
        _parts.append("；cache_hit=yes(TTL)")
    else:
        _parts.append("；workflow_completed=yes")
    reasoning_line = "".join(_parts)  # 仅用于审计/日志/兼容
    del _parts, _stocks  # 显式释放，防止后续误用

    # (2) progress：98% 结果就绪推送中（和 workflow 内部阶段不同语义：
    #     workflow 阶段是"路由→缓存→搜索→推理→保存"的过程；
    #     这里 98% 表示"所有过程结束，delta 打字机即将开始"，单独保留为前端状态机的阶段终点提示）
    _detail_map = {
        "pre_market_news":      f"盘前新闻分支就绪（缓存命中={_premarket_hit}）",
        "stock_query":          f"个股4源聚合就绪（本地缓存命中 {_stock_cache_cnt} 条）",
        "code_generation":      "代码生成分支就绪",
        "impact_analysis":      "影响分析双源聚合+deepseek-r1 推理就绪",
        "general_query":        "通用双源聚合+推理就绪",
        "vision":               "视觉多模态分析分支就绪",
        "preset_shortcut_other":"预设快捷按钮分支就绪",
        "fallback":             "兜底通用分支就绪",
    }
    try:
        bus.ev_progress(
            thread_id,
            stage=f"{branch} 结果就绪",
            percent=98,
            detail=(_detail_map.get(branch, "工作流执行完成") + "，正在推送最终结果 ..."),
        )
    except Exception:
        pass

    # (3) citation_meta + source_ref（至少 1 条，避免前端引用卡片空白）
    #     严格对齐 StreamEventBus.ev_retrieve_result 的内部模式：
    #       state.add_source → state.set_citation_meta → ev_citation_meta(indices=...) → ev_source_ref()
    _src_cnt_from_pool = 0
    try:
        from config.constants import (
            CITATION_TITLE_MAX_CHARS as _TMAX,
            CITATION_URL_MAX_CHARS as _UMAX,
        )
        _state = bus.get_thread_state(thread_id)
        _new_indices: List[int] = []
        _items_raw: List[Dict[str, Any]] = []
        if cache_hit:
            _items_raw.append({
                "doc_id": f"wf_local_cache_{branch}_1",
                "title": (f"本地缓存·{branch}（{_now_bj_str}）")[:_TMAX],
                "url": (f"about:local-cache#workflow_{branch}")[:_UMAX],
                "source_type": "local_cache",
                "reliability": "可靠（本地缓存命中，TTL 未过期）",
                "channel": ("premarket_cache_hit" if _premarket_hit else "stock_cache_hit"),
                "snippet": (final_answer[:120] if final_answer else "缓存命中结果"),
                "published_at": _now_bj_str,
            })
        else:
            _stats_parts = []
            for _k, _v in list((result.aggregator_stats or {}).items())[:6]:
                try:
                    _stats_parts.append(f"{_k}={_v}")
                except Exception:
                    continue
            _stats_str = "; ".join(_stats_parts) or "工作流双/四源综合聚合结果"
            _items_raw.append({
                "doc_id": f"wf_agg_{branch}_1",
                "title": (f"工作流聚合·{branch}（{_now_bj_str}）")[:_TMAX],
                "url": (f"about:workflow-aggregator#{branch}")[:_UMAX],
                "source_type": "aggregator",
                "reliability": "可靠",
                "channel": f"workflow_{branch}",
                "snippet": _stats_str or (final_answer[:100] if final_answer else ""),
                "published_at": _now_bj_str,
            })
        for raw in _items_raw:
            try:
                idx = _state.add_source(
                    title=str(raw.get("title", "")),
                    url=str(raw.get("url", "")),
                    source_type=str(raw.get("source_type", "web")),
                    reliability=str(raw.get("reliability", "待验证")),
                    snippet=str(raw.get("snippet", "")),
                    published_at=str(raw.get("published_at", "")),
                )
                if idx <= 0:
                    continue
                _channel = str(raw.get("channel", f"workflow_{branch}"))
                try:
                    _state.set_citation_meta(
                        idx,
                        title=str(raw.get("title", "")),
                        url=str(raw.get("url", "")),
                        source_type=str(raw.get("source_type", "web")),
                        reliability=str(raw.get("reliability", "待验证")),
                        channel=_channel,
                        published_at=str(raw.get("published_at", "")),
                        snippet=str(raw.get("snippet", "")),
                    )
                except Exception:
                    pass  # 旧版本 state 可能没有 set_citation_meta，不致命
                _new_indices.append(idx)
            except Exception:
                continue
        if _new_indices:
            try:
                # ev_citation_meta 的 indices= 形式（已在 state 中注册，自动映射 CitationMetaItem）
                bus.ev_citation_meta(thread_id, indices=_new_indices)
            except Exception:
                pass
            try:
                bus.ev_source_ref(thread_id)  # 无参数，自动从 thread_state.source_pool 快照
            except Exception:
                pass
        _src_cnt_from_pool = len(_new_indices)
    except Exception:
        pass

    # (4) 正文 delta：24 字/段推入（模拟打字机，与股票缓存命中短路同款）
    _text = final_answer or "（空结果）"
    _SEG_LEN = 24
    for _off in range(0, len(_text), _SEG_LEN):
        seg = _text[_off:_off + _SEG_LEN]
        try:
            # ev_delta 真实签名：ev_delta(thread_id, text, *, is_reasoning=False)
            bus.ev_delta(thread_id, text=seg, is_reasoning=False)
        except Exception:
            pass

    # (5) 【方案一移除 ev_task_result 调用】真实 StreamEventBus 没有 ev_task_result 方法。
    #     前端状态机只需：delta 增量 + done.final_text 全量 → 即可完整渲染结果；
    #     旧 monitor.report_task_result 仅影响 WS 监控面板，不影响前端 SSE 展示。
    #     这里通过 ev_done(force_final_text=final_answer) 同时完成终态标记+全量文本。

    # (6) 终态：ev_done(force_final_text=final_answer, usage=usage_dict)
    #     真实签名：ev_done(tid, *, usage: Dict[str,int]=None, force_final_text: str=None)
    total_ms = int((_t.monotonic() - _start_monotonic) * 1000)
    usage_dict: "Dict[str, Any]" = {
        "from_analysis_workflow": True,
        "branch": branch,
        "router_decision": result.router_decision.branch.value,
        "decided_by": result.router_decision.decided_by,
        "cache_hit": cache_hit,
        "total_duration_ms": total_ms,
    }
    if _src_cnt_from_pool:
        usage_dict["source_count_from_pool"] = _src_cnt_from_pool
    try:
        bus.ev_done(thread_id, usage=usage_dict, force_final_text=final_answer)
    except Exception as _done_err:
        # 防御：如果 usage= 的值不是 Dict[str, int]（兼容层可能强校验），退化成最简整数 usage
        try:
            bus.ev_done(thread_id, usage={"tokens": 0}, force_final_text=final_answer)
        except Exception:
            # 最后兜底：哪怕只发一条"文本 done delta"也要让前端收到结果
            try:
                bus.ev_delta(thread_id, text=final_answer, is_reasoning=False)
            except Exception:
                pass
        _ = _done_err  # 占位防止未使用告警

    return {"final_answer": final_answer, "workflow_result": result, "duration_ms": total_ms}


async def _run_coro_with_workflow_priority(
    query: str,
    thread_id: str,
    user_id: str,
    quiet: bool = False,
) -> None:
    """给非流式 POST /api/task 的 _run_with_ctx 作为 corofn。
    Router+Workflow 优先；**任何异常** → 静默 fallback 到旧 run_deep_agent（兼容兜底）。
    """
    from api.stream_bus import get_stream_bus_sync
    _bus = get_stream_bus_sync()
    try:
        await _try_run_workflow_push_events(
            query, thread_id, user_id, bus=_bus, quiet=quiet,
        )
        return
    except Exception as _wf_err:
        import traceback as _tb_wf
        import sys as _sys_wf_ns
        _tb_wf.print_exc(file=_sys_wf_ns.stderr)
        _sys_wf_ns.stderr.flush()
        print(
            f"[方案一 Router+Workflow 非流式] 异常 → fallback 旧 run_deep_agent: {_wf_err}",
            flush=True,
        )
    # --- 兼容兜底：旧 run_deep_agent 原逻辑 100% 保留 ---
    await run_deep_agent(query, thread_id, user_id, quiet=quiet)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 【Python 语法约束】全局变量声明必须出现在函数内对它们的**任何赋值之前**，
    # 且不能分散在两个分支里重复声明（否则 SyntaxError: assigned to before global declaration）。
    global _session_actor, _circuit_breaker_actor, _conn_manager_actor, _slo_monitor_actor

    # ---------- pytest/单测模式下跳过真实 Actor/Scheduler 生命周期 ----------
    # （storage 薄壳 / JWT auth / 限流 middleware 等纯 HTTP 基础设施不依赖 Actor）
    if os.getenv("MOSS_TEST_SKIP_LIFESPAN", "0") == "1":
        # 手动挂空句柄到 app.state 上避免 Depends handler 判断 None 时 AttributeError。
        # 所有公共方法都实现（send / ask / tell 等），async 方法返回 None 或空 dict，避免
        # 单测下 handler 抛 AttributeError 被 middleware catch 兜底转 500 干扰头验证。
        import asyncio as _ai

        class _FakeActorCls:
            """pytest 专用：对所有 Actor 公共接口 no-op 实现，签名覆盖 session_registry /
            circuit_breaker / connection_manager / slo_monitor 四类 Actor 的实际调用。"""
            def tell(self, *a, **kw): return None
            def send(self, *a, **kw):
                # 返回 awaitable（协程对象）兼容 `await sa.send(MSG, payload)` 调用
                async def _noop(): return None
                return _noop()
            def ask(self, *a, **kw):
                async def _empty(): return None
                return _empty()
            def stop(self, *a, **kw): return None
            def register(self, *a, **kw): return None
            # 防止其它属性访问抛 AttributeError（有些 handler 会访问 actor.props 之类）
            def __getattr__(self, _name):
                def _fallback(*a, **kw):
                    # 如果被 await，返回一个协程
                    async def _ac(): return None
                    return _ac()
                return _fallback

        fake = _FakeActorCls()
        app.state.session_actor = fake
        app.state.circuit_breaker_actor = fake
        app.state.conn_manager_actor = fake
        app.state.slo_monitor_actor = fake
        app.state.scheduler_task = None
        _session_actor = fake
        _circuit_breaker_actor = fake
        _conn_manager_actor = fake
        _slo_monitor_actor = fake
        print("[lifespan] MOSS_TEST_SKIP_LIFESPAN=1 已启用，跳过 Actor/Scheduler 启动")
        yield
        return

    # ---------- 启动逻辑 ----------
    loop = asyncio.get_running_loop()
    manager.set_loop(loop)
    print(f"[Server] WebSocket Manager bound to loop: {id(loop)}")

    # ===== Actor Model: 注册 + 启动所有 Actor =====
    system = get_actor_system()
    _session_actor = SessionRegistryActor("session_registry")
    _circuit_breaker_actor = CircuitBreakerActor("circuit_breaker")
    _conn_manager_actor = ConnectionManagerActor("connection_manager")
    from pathlib import Path as _Path
    _data_dir = project_root / "data"
    _slo_monitor_actor = SLOMonitorActor("slo_monitor", persist_db=_data_dir / "slo_events.db")
    system.register("session_registry", _session_actor)
    system.register("circuit_breaker", _circuit_breaker_actor)
    system.register("connection_manager", _conn_manager_actor)
    system.register("slo_monitor", _slo_monitor_actor)
    await system.start_all()
    # 把 ConnectionManager Actor 句柄注入到 monitor，供跨线程发送时使用
    from api import monitor as _monitor_mod
    _monitor_mod._set_conn_actor(_conn_manager_actor, loop)

    # ====== StreamBus 绑定：主循环 + monitor 事件桥接（SSE 流式输出基础）======
    try:
        bus_getter, installer = _load_stream_runtime()
        _bus = bus_getter() if callable(bus_getter) else bus_getter
        _bus.bind_loop(loop)
        installer()  # monitor._emit → bus 双写
        print("[StreamBus] 已绑定主循环 + monitor 桥接安装完成")
    except Exception as _stream_init_err:
        import traceback as _tb
        _tb.print_exc()
        print(f"[StreamBus] 初始化失败（不致命，SSE 将降级为原 POST+WS）: {_stream_init_err}")

    # 把熔断器 Actor 句柄注入到 main_agent 和 circuit_breaker 适配层
    from agent import main_agent as _ma_mod
    from agent import circuit_breaker as _cb_mod
    _cb_mod._set_cb_actor(_circuit_breaker_actor)
    _ma_mod._set_cb_actor(_circuit_breaker_actor)
    _ma_mod._set_slo_actor(_slo_monitor_actor)

    scheduler_task = None  # 调度器后台任务句柄（lifespan 关闭时cancel）

    # ---------- Layer4: 启动定时调度器（盘前小作文9:13 / 盘前新闻9:15）----------
    try:
        from agent.scheduler import get_scheduler, setup_preset_tasks
        scheduler = get_scheduler()

        # 盘前小作文热度回调：调用 zsxq 分析
        async def _zsxq_callback():
            # 使用默认会话ID（系统自动触发）
            default_thread = "scheduler_zsxq_auto"
            # 后台调度触发 → 静默模式，避免控制台刷日志
            await _run_with_ctx(
                default_thread, "system", None, _DEFAULT_BG_TIMEOUT,
                _run_zsxq_analysis, default_thread,
                quiet=True,
            )

        # 盘前新闻回调：调用主 Agent 搜索盘前新闻（后台静默）
        async def _news_callback():
            await _run_with_ctx(
                "scheduler_news_auto", "system", None, _DEFAULT_AGENT_TIMEOUT,
                _run_news_scheduler_callback,
                quiet=True,
            )

        async def _run_news_scheduler_callback():
            from agent.main_agent import run_deep_agent
            await run_deep_agent(
                "请搜索今日A股盘前新闻，包括重要公告、宏观政策、市场热点，按利好利空分类汇总",
                "scheduler_news_auto",
                user_id="system"
            )

        setup_preset_tasks(scheduler, _zsxq_callback, _news_callback)

        # ---------- Layer4 新增：热门股分析本地缓存预热（工作日 08:00 / 20:00） ----------
        # 由 cache.hot_stock_warmup.warmup 异步执行：DeepSeek 求 Top10 → 三通道综合 →
        # 写 STOCK_CACHE_DIR/YYYYMMDDHH_<股票>.txt；用户问到时优先读缓存秒回。
        try:
            from config.constants import (
                STOCK_CACHE_ENABLED, STOCK_CACHE_WARMUP_HOURS,
                STOCK_CACHE_WARMUP_MINUTE, STOCK_CACHE_WARMUP_WEEKDAY_ONLY,
            )
            from cache.hot_stock_warmup import warmup
            if STOCK_CACHE_ENABLED:
                _registered_warmup = 0
                for hour in STOCK_CACHE_WARMUP_HOURS:
                    scheduler.add_task(
                        name=f"热门股缓存预热 {hour:02d}:{STOCK_CACHE_WARMUP_MINUTE:02d}",
                        hour=int(hour),
                        minute=int(STOCK_CACHE_WARMUP_MINUTE),
                        callback=warmup,
                        weekday_only=bool(STOCK_CACHE_WARMUP_WEEKDAY_ONLY),
                    )
                    _registered_warmup += 1
                if _registered_warmup > 0:
                    print(f"[Scheduler] 已注册 {_registered_warmup} 个热门股缓存预热任务（时段"
                          f" {list(STOCK_CACHE_WARMUP_HOURS)}）")
        except Exception as _warmup_err:
            import traceback as _tb2
            _tb2.print_exc()
            print(f"[Scheduler] 热门股预热任务注册失败（不致命，功能跳过）: {_warmup_err}")

        # 注意：scheduler.start() 是一个无限轮询循环，不能直接 await（否则会阻塞 lifespan）
        # 必须用 asyncio.create_task 放到后台运行，任务句柄保存以便 shutdown 时 cancel
        scheduler_task = asyncio.create_task(scheduler.start(), name="ptd_scheduler_loop")
        # 等启动日志打完后再继续（保证 log 输出完整）
        await asyncio.sleep(SCHEDULER_STARTUP_WAIT_SEC)
        print(f"[Scheduler] 盘前自动化调度已启动(后台运行): {scheduler.get_next_run_times()}")
    except Exception as sched_err:
        import traceback as _tb
        _tb.print_exc()
        print(f"[Scheduler] 启动失败（不致命）: {sched_err}")

    # ---------- ngrok 公网隧道（可选）----------
    # 在 .env 中配置 NGROK_AUTHTOKEN 后，启动时自动创建公网隧道，手机可远程访问
    ngrok_tunnel = None
    ngrok_proc = None
    ngrok_token = os.getenv("NGROK_AUTHTOKEN", "").strip()
    ngrok_mod = None
    get_ngrok_config = None

    # 先尝试导入可选依赖 pyngrok；未安装时给友好提示，不影响局域网访问
    if ngrok_token:
        try:
            from pyngrok import ngrok as _ngrok_mod
            from pyngrok.conf import get_default as _get_ngrok_config
            ngrok_mod = _ngrok_mod
            get_ngrok_config = _get_ngrok_config
        except (ImportError, ModuleNotFoundError):
            print("\n" + "!" * 66)
            print("[ngrok] ⚠️  检测到已配置 NGROK_AUTHTOKEN，但未安装 pyngrok")
            print("[ngrok]    安装命令:  python -m pip install pyngrok==8.1.2")
            print("[ngrok]    或删除 .env 中的 NGROK_AUTHTOKEN 行，将跳过公网隧道")
            print("[ngrok]    本次启动：自动跳过 ngrok，仅开放局域网访问")
            print("!" * 66 + "\n")
            ngrok_token = ""  # 视为未配置，避免后续逻辑误判

    if ngrok_token and ngrok_mod is not None:
        try:
            import time
            import subprocess
            import json
            import urllib.request

            ngrok_mod.set_auth_token(ngrok_token)
            # 清理可能残留的旧 ngrok 进程
            try:
                ngrok_mod.kill()
                time.sleep(NGROK_CLEANUP_WAIT_SEC)
            except Exception:
                pass

            # 直接用 ngrok 命令行启动，加 --pooling-enabled 允许多实例共享同一域名
            # 彻底避免 ERR_NGROK_334: endpoint already online
            ngrok_bin = get_ngrok_config().ngrok_path
            ngrok_proc = subprocess.Popen(
                [ngrok_bin, "http", str(DEFAULT_SERVER_PORT), "--pooling-enabled", "--log=stdout"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            # 等待 ngrok 本地 API 就绪
            public_url = ""
            for _ in range(NGROK_TUNNEL_MAX_POLL_ROUNDS):
                time.sleep(NGROK_TUNNEL_POLL_INTERVAL_SEC)
                try:
                    resp = urllib.request.urlopen(
                        f"http://127.0.0.1:{NGROK_LOCAL_API_PORT}/api/tunnels",
                        timeout=SHORT_HTTP_TIMEOUT_SEC,
                    )
                    data = json.loads(resp.read())
                    tunnels = data.get("tunnels", [])
                    if tunnels:
                        public_url = tunnels[0].get("public_url", "")
                        if public_url:
                            break
                except Exception:
                    continue

            if public_url:
                ngrok_tunnel = public_url  # 存储公网 URL 用于关闭时清理
                print("\n" + "=" * 60)
                print(f"[ngrok] 🌐 公网访问地址: {public_url}")
                print("[ngrok] 手机任意网络均可访问此地址")
                print("=" * 60 + "\n")
            else:
                print("[ngrok] 隧道启动超时，未获取到公网地址")
        except Exception as e:
            print(f"[ngrok] 隧道启动失败（不影响局域网访问）: {e}")
            ngrok_proc = None  # 失败时避免关闭阶段误操作
    elif not ngrok_token:
        # 未配置 ngrok token，提示局域网访问地址
        print("\n[提示] 未配置 NGROK_AUTHTOKEN，仅支持局域网访问")
        print(f"[提示] 局域网访问: 手机连同一 WiFi 后访问 http://<本机IP>:{DEFAULT_SERVER_PORT}")
        print("[提示] 外网访问: 在 .env 中设置 NGROK_AUTHTOKEN 后重启\n")

    yield  # 应用在此运行，处理请求

    # ---------- 关闭逻辑 ----------
    # Layer4: 停止定时调度器（先 cancel 后台循环，再 stop 设置 _running=False）
    try:
        if scheduler_task is not None and not scheduler_task.done():
            scheduler_task.cancel()
            try:
                await asyncio.wait_for(scheduler_task, timeout=SCHEDULER_CANCEL_WAIT_SEC)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        from agent.scheduler import get_scheduler
        await get_scheduler().stop()
        print("[Scheduler] 定时调度器已停止")
    except Exception:
        pass
    # ===== Actor Model: 优雅停止所有 Actor =====
    try:
        await get_actor_system().stop_all()
    except Exception as _actor_stop_err:
        print(f"[ActorSystem] 停止异常（不致命）: {_actor_stop_err}")
    if ngrok_proc:
        try:
            ngrok_proc.terminate()
            ngrok_proc.wait(timeout=SUBPROCESS_WAIT_TIMEOUT_SEC)
            print("[ngrok] 隧道进程已关闭")
        except Exception:
            ngrok_proc.kill()


# 创建 FastAPI 应用，并传入 lifespan
app = FastAPI(
    title="DeepAgents API",
    lifespan=lifespan,
)

# 注意：zsxq_router 的挂载已改为延迟调用（见 _ensure_zsxq_router_installed_once()），
# 因为需要等 _register_background_task 定义完成后才注入 helpers，避免顶层 NameError。

# 配置 CORS（必须在 app 创建之后注册）
#跨域资源共享（CORS）配置，让前端网页（比如 http://localhost:3000）能够从浏览器向你的 FastAPI 后端
# （比如 http://localhost:8000）发起请求。
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===== 安全 & 鉴权 & 限流三合一中间件（P0 鉴权限流修复主入口） =====
# 执行顺序（FastAPI 中间件 = 栈，最后 add 的先 dispatch）：
#   CORSMiddleware → AuthAndRateLimitMiddleware → SecurityMiddleware → app.router
# 本层职责（与旧中间件兼容不破坏）：
#   1) 白名单：/health / /auth* / /docs /openapi.json /redoc /ws_*path?token=* （WS 端点在 handler 内自行校验 token，此处仅做「未登录也允许握手」以便 WS handler 中能 close 4401）
#   2) 解析 Authorization: Bearer <jwt>（失败抛 401）→ CurrentUser 对象
#   3) 旧治理层兼容：构建 RBACPolicy.UserContext 写 _rbac_current_user_ctx
#   4) Rate Limiting：按 (role, identity=user_id 或 client_ip) 走 4 档滑动窗口 QPM；命中返回 429 + Retry-After
#   5) 把 CurrentUser 注入 request.state.current_user（业务端点 Depends 可直接读，也可由 FastAPI Depends 另起）
_AUTH_PUBLIC_PREFIXES: Tuple[str, ...] = (
    "/health", "/docs", "/openapi.json", "/redoc",
    "/favicon.ico", "/static/",
    "/api/auth/", "/api/auth-",  # 登录/注册/刷新/游客/改密码 5 端点
    "/api/users",                 # 兼容旧 POST /api/users（明文 create_or_login）与 GET /api/users/{id}（router smoke 用例）
)
_AUTH_PUBLIC_EXACT: Tuple[str, ...] = ("/",)

# 旧 IP 限流兜底保留：未登录（无 JWT）时用 client_ip 当 key，不再只做 60/60s 统一，
# 而是按角色 QPM（guest=10 QPM / 登录 user 60 / admin 120 / owner 600）。


class AuthAndRateLimitMiddleware(BaseHTTPMiddleware):
    """P0 新中间件：JWT 认证 → 4 档角色限流 → 兼容旧 RBAC ContextVar。

    注意：
    - 非公开 HTTP 端点若缺 token：返回 401（/api/auth/* 登录注册除外，它们本身不依赖 token）；
    - /ws/* 和 /health 等：无 token 也放行；WS 层在 accept 之前再次调用 get_current_user_websocket，
      若为空则 close(code=4401)，不抛 HTTP。
    - 旧的 SecurityMiddleware（安全头 + 旧 IP 兜底限流）仍保留，但旧限流 deque 当 4 档 RL 已限时会被
      本层短路先返回 429，因此只做安全头功能。
    """

    def __init__(self, app, *, limiter: Optional[RateLimiter] = None,
                 rbac_policy: Optional["_rbac_mod.RBACPolicy"] = None):
        super().__init__(app)
        self._limiter = limiter or _get_global_rl()
        self._rbac_policy = rbac_policy or _rbac_mod.RBACPolicy()
        self._logger = logging.getLogger("moss.auth_rl")

    # ---------------- helpers ----------------
    def _is_public(self, path: str) -> bool:
        if path in _AUTH_PUBLIC_EXACT:
            return True
        for p in _AUTH_PUBLIC_PREFIXES:
            if path.startswith(p):
                return True
        return False

    def _is_websocket_path(self, path: str) -> bool:
        # WS 握手也走这里（FastAPI 路由注册 @app.websocket("/ws/{thread_id}")）
        return path == "/ws" or path.startswith("/ws/")

    def _get_client_ip(self, request: Request) -> str:
        # 优先 X-Forwarded-For（若放在 Nginx/ngrok 后），否则 client.host
        fwd = request.headers.get("X-Forwarded-For", "")
        if fwd:
            return fwd.split(",", 1)[0].strip() or (request.client.host if request.client else "unknown")
        return request.client.host if request.client else "unknown"

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method

        # ---------- STEP 1：白名单：不鉴权，仅按未登录 IP 限流（防 DDoS） ----------
        is_public = self._is_public(path)
        is_ws = self._is_websocket_path(path)

        if is_public and not is_ws:
            # 仅以 IP 为 key 限流（匿名访问保护），配额 guest=10 QPM
            ip = self._get_client_ip(request)
            ok, retry = self._limiter.hit(f"pub:{ip}", "guest")
            if not ok:
                return JSONResponse(
                    status_code=HTTP_CODE_TOO_MANY_REQUESTS,
                    content={"detail": {"code": "RATE_LIMIT_PUBLIC",
                                        "message": "公共资源访问过于频繁，请稍后再试"}},
                    headers={"Retry-After": str(retry)},
                )
            return await call_next(request)

        # ---------- STEP 2：JWT 解析（缺 token 视情况） ----------
        from fastapi import HTTPException as _HTTPEx, status as _status
        current: Optional[CurrentUser] = None
        auth_err: Optional[Exception] = None
        try:
            # 复用 Depends 同逻辑（不通过 Depends 因为我们是 middleware stack）
            current = await get_current_user(request)
        except _HTTPEx as e:
            auth_err = e

        if current is None and auth_err is not None:
            # WS：无 token 仍允许握手（在 WS handler 内 close 4401）
            if is_ws:
                # 仅按 IP 走 guest 限流，仍放行到 WS handler
                ip = self._get_client_ip(request)
                ok, retry = self._limiter.hit(f"ws:{ip}", "guest")
                if not ok:
                    return JSONResponse(
                        status_code=HTTP_CODE_TOO_MANY_REQUESTS,
                        content={"detail": {"code": "RATE_LIMIT_WS",
                                            "message": "WS 连接过于频繁"}},
                        headers={"Retry-After": str(retry)},
                    )
                request.state.current_user = None
                return await call_next(request)
            # 登录端点 POST /api/auth/*：公共但不走 is_public（我们通过前缀 /auth/ 判断）
            # 上面 _AUTH_PUBLIC_PREFIXES 已包含 /auth/ → 白名单分支已处理。
            # 非 WS + 非 auth + 缺 token = 直接返回鉴权错误。
            detail = getattr(auth_err, "detail", None) or {"code": "UNAUTHENTICATED", "message": "请先登录"}
            headers = {"WWW-Authenticate": "Bearer"}
            if isinstance(detail, dict) and isinstance(getattr(auth_err, "headers", None), dict):
                headers.update(getattr(auth_err, "headers", {}))
            return JSONResponse(
                status_code=getattr(auth_err, "status_code", _status.HTTP_401_UNAUTHORIZED),
                content={"detail": detail},
                headers=headers,
            )

        # ---------- STEP 3：CurrentUser 注入 request.state + 旧 RBAC ContextVar ----------
        assert current is not None
        request.state.current_user = current
        # 【JWT role 优先】直接按 JWT payload 的 current.role 构造 UserContext 注入旧治理层；
        # build_user_context(user_id) 会从 JSON 策略文件查 user->role 映射，可能与 JWT 不一致，
        # 所以这里绕过 build_user_context，按 get_role_config(current.role) 取 permissions/max_rows 配置。
        cfg = self._rbac_policy.get_role_config(current.role) or {}
        rbac_ctx = _rbac_mod.UserContext(
            user_id=current.user_id,
            role=current.role,
            permissions=set(cfg.get("permissions", []) or []),
            max_rows=cfg.get("max_rows_per_query", _rbac_mod.RBAC_ROW_LEVEL_MAX_ROWS)
                     if hasattr(_rbac_mod, "RBAC_ROW_LEVEL_MAX_ROWS") else 100,
            rate_limit_per_min=cfg.get("rate_limit_per_min", 60),
            allowed_endpoints=cfg.get("allowed_endpoints", []) or [],
        )
        rbac_token = _rbac_mod._current_user.set(rbac_ctx)

        try:
            # ---------- STEP 4：4 档角色限流（owner 600/admin 120/user 60/guest 10 QPM） ----------
            identity = current.user_id
            ok, retry = self._limiter.hit(identity, current.role)
            if not ok:
                return JSONResponse(
                    status_code=HTTP_CODE_TOO_MANY_REQUESTS,
                    content={"detail": {"code": "RATE_LIMIT_BY_ROLE",
                                        "message": f"角色 {current.role} 限流，请 {retry} 秒后再试",
                                        "role": current.role,
                                        "qpm": self._limiter.qpm_for_role(current.role)}},
                    headers={"Retry-After": str(retry)},
                )
            # ========== STEP 4-1：调用内层应用，并对 Starlette 行为兜底 ==========
            # Starlette BaseHTTPMiddleware.call_next 对「非 HTTPException 的普通 Python 异常」
            # 会直接重新 raise，而不是返回 500 Response（这是 0.37+ 官方行为差异）。
            # 因此我们必须手工 catch 转 JSONResponse 500，否则：
            #   ① X-User-Id / X-RateLimit-* 等安全头永不写入；② 客户端收到裸文本 Internal Server Error。
            try:
                response = await call_next(request)
            except Exception as _inner_exc:
                import traceback as _tb
                _tb.print_exc()  # 生产可替换为结构化日志
                response = JSONResponse(
                    status_code=500,
                    content={
                        "detail": {
                            "code": "INTERNAL_SERVER_ERROR",
                            "message": f"服务器内部错误: {type(_inner_exc).__name__}: {_inner_exc}",
                            "cls": type(_inner_exc).__name__,
                        }
                    },
                )

            # 调试响应头（正式版可关闭）：暴露 role + 剩余配额
            try:
                remaining = self._limiter.remaining(identity, current.role)
            except Exception:
                remaining = -1
            response.headers["X-User-Id"] = current.user_id
            response.headers["X-User-Role"] = current.role
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            response.headers["X-RateLimit-Limit"] = str(self._limiter.qpm_for_role(current.role))
            return response
        finally:
            _rbac_mod._current_user.reset(rbac_token)


# ===== 安全响应头中间件（剥离旧 IP 限流，仅做安全头） =====
class SecurityMiddleware(BaseHTTPMiddleware):
    """仅负责：添加安全响应头，防止 XSS / 点击劫持 / MIME 嗅探等攻击。

    限流 & 鉴权已上移至 AuthAndRateLimitMiddleware，此处不再重复。
    """

    async def dispatch(self, request: Request, call_next):
        # 1. 处理请求
        response = await call_next(request)

        # 2. 添加安全响应头
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["X-Powered-By"] = "MOSS-Finance-Assistant"  # 隐藏真实服务器信息
        return response


# ——— middleware 注册顺序（FastAPI 后注册 = 更外层，先执行）———
# CORSMiddleware（外层 #1） → AuthAndRateLimitMiddleware（外层 #2） → SecurityMiddleware（内层 #3）→ app.router
app.add_middleware(SecurityMiddleware)
# 注意：AuthAndRateLimitMiddleware 必须在 SecurityMiddleware 之后 add_middleware 才能「更外层先执行」
app.add_middleware(AuthAndRateLimitMiddleware)

# ======================================================================
# 按 thread_id 跟踪正在运行的 Agent 任务（改为 Actor 驱动，不再用全局可变字典）
# ======================================================================
# 迁移说明：原 _active_agent_tasks / _active_background_tasks / _background_tasks
# 全部收敛到 SessionRegistryActor 私有状态中。外部只能通过发消息与 Actor 交互，
# 状态修改只能发生在 Actor.handle_message 内（next_state = f(current_state, input)）。




# ======================================================================
# 健康检查端点（k8s/consul/monitor 探针友好）
# AGENTS.md 要求：每个响应必须含 X-Powered-By: MOSS-Finance-Assistant 响应头
# （该头已由 SecurityMiddleware 统一注入，这里仅负责返回 status）
# ======================================================================
@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "service": "MOSS-Finance-Assistant",
        "version": "2.0.0",   # 重构版本号
        "actors": ["session_registry", "circuit_breaker", "connection_manager", "slo_monitor"],
    }


# ======================================================================
# 【v2 P0 鉴权升级】认证 5 端点：register / login / refresh / guest / change-password
# ======================================================================
# 注意：这些端点本身走 _AUTH_PUBLIC_PREFIXES（/api/auth/*）白名单，
# middleware 不强制要 JWT；改密码端点内部显式 Depends(get_current_user) 要求登录态。
# ======================================================================

def _status_code_of(e: HTTPException) -> int:
    return getattr(e, "status_code", 401)


@app.post("/api/auth/register", tags=["auth"])
async def auth_register(req: RegisterRequest):
    """注册新用户并签发 JWT 对。同一 user_id 重复调用幂等返回「已存在」记录。"""
    # ⚠️ 禁止注册 guest_ 前缀账号（保留给游客自动生成）
    if req.user_id.startswith("guest_"):
        raise HTTPException(
            status_code=400,
            detail={"code": "RESERVED_USER_PREFIX",
                    "message": "用户名前缀 guest_ 为系统保留，请更换"},
        )
    try:
        user = storage.get_or_create_user(
            req.user_id,
            req.display_name or req.user_id,
            password=req.password,
            role="user",  # 自助注册一律 user，owner/admin 由 CLI/.env 授予
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail={"code": "BAD_REQUEST", "message": str(ve)})
    role = user.get("role") or "user"
    storage.touch_last_login(req.user_id)
    token = create_token_pair(req.user_id, role,
                              user.get("display_name") or req.user_id, False)
    return token


@app.post("/api/auth/login", tags=["auth"])
async def auth_login(req: LoginRequest):
    """账号密码登录并签发 JWT 对。密码强度若为旧算法会被原地升级为最新 hash。"""
    from fastapi import status as _st
    ok, need_upgrade, role = storage.verify_user_password(req.user_id, req.password)
    if not ok:
        # 注意：区分"用户不存在/密码错"统一 401，但 code 细分有利于前端提示
        user_row = storage.get_user(req.user_id)
        code = "USER_NOT_FOUND" if user_row is None else "PASSWORD_MISMATCH"
        if user_row and not user_row.get("has_password"):
            code = "NO_PASSWORD_SET"
        raise HTTPException(
            status_code=_st.HTTP_401_UNAUTHORIZED,
            detail={"code": code, "message": "用户名或密码错误"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    role = role or "user"
    if need_upgrade:
        # 登录成功后把旧算法哈希升级为当前最强算法（pbkdf2 → bcrypt 或低 iters → 高 iters）
        try:
            storage.update_password(req.user_id, req.password)
        except Exception:
            # 升级失败不影响登录
            pass
    storage.touch_last_login(req.user_id)
    display = (storage.get_user(req.user_id) or {}).get("display_name") or req.user_id
    token = create_token_pair(req.user_id, role, display,
                              is_guest=(role == "guest"))
    return token


@app.post("/api/auth/refresh", tags=["auth"])
async def auth_refresh(req: RefreshRequest):
    """用 refresh_token 换发新的 access_token + refresh_token。"""
    try:
        token = refresh_access_token(req.refresh_token)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=401,
                            detail={"code": "REFRESH_FAILED",
                                    "message": f"刷新失败：{type(e).__name__}"},
                            headers={"WWW-Authenticate": "Bearer"})
    # 刷新时顺带写 last_login（体现活跃）
    try:
        storage.touch_last_login(token.user["user_id"])
    except Exception:
        pass
    return token


@app.post("/api/auth/guest", tags=["auth"])
async def auth_guest():
    """一键生成游客账号 + JWT。role=guest，严格 10 QPM 限流。"""
    token_pair, user_id = create_guest_token()
    # 游客也落库（便于后续绑定正式账号、记录会话、统计 DAU）
    storage.get_or_create_user(user_id, token_pair.user["display_name"], role="guest")
    storage.touch_last_login(user_id)
    return token_pair


@app.post("/api/auth/change-password", tags=["auth"])
async def auth_change_password(req: ChangePasswordRequest,
                               current: CurrentUser = _Depends(get_current_user)):
    """登录用户自助改密码。owner/admin 也可用此端点改自己的。"""
    from fastapi import status as _st
    # 游客（空密码）不能改密码：必须先注册正式账号
    if current.is_guest:
        raise HTTPException(status_code=403,
                            detail={"code": "GUEST_CANNOT_CHANGE_PASSWORD",
                                    "message": "游客账号不支持改密码，请先注册正式账号"})
    ok, _need, _role = storage.verify_user_password(current.user_id, req.old_password)
    if not ok:
        raise HTTPException(
            status_code=_st.HTTP_401_UNAUTHORIZED,
            detail={"code": "OLD_PASSWORD_MISMATCH", "message": "旧密码错误"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        storage.update_password(current.user_id, req.new_password)
    except ValueError as ve:
        raise HTTPException(status_code=400,
                            detail={"code": "BAD_NEW_PASSWORD", "message": str(ve)})
    return {"status": "ok", "message": "密码已更新"}


# ======================================================================
# 业务端点：POST /api/task
# ======================================================================

@app.post("/api/task")
async def run_task(request: TaskRequest, current: CurrentUser = _Depends(get_current_user)):
    # 1. [ID 初始化] —— v2：绝对不信任 Body 中的 user_id，统一用 JWT CurrentUser
    thread_id = request.thread_id or str(uuid.uuid4())
    effective_user_id = current.user_id

    # 行级校验：若 thread_id 已存在，必须属于当前 user（owner/admin 豁免）
    existing_session = storage.get_session(thread_id)
    if existing_session:
        current_user_id_must_match(current, existing_session.get("user_id"))
        # 若 JWT user 与会话 owner 不一致（仅可能发生在 owner/admin 运营视角），保留原 user_id
        if existing_session.get("user_id"):
            effective_user_id = existing_session["user_id"]

    # 自动注册 + 确保会话记录存在
    storage.get_or_create_user(effective_user_id)
    # ===== P0 薄壳修复：不要直接调私有 storage._get_conn()（薄壳 * import 不导 _ 前缀） =====
    # 改用 storage.ensure_session(session_id=thread_id, ..., title="新会话") 幂等创建。
    storage.ensure_session(thread_id, effective_user_id, title="新会话")
    _raw_query = request.query or ""
    _effective_query = _raw_query
    _is_news_btn = _is_premarket_news_query(_raw_query)
    if _is_news_btn:
        _effective_query, _rewrote = _rewrite_premarket_query_if_shortcut(_raw_query)
        if _rewrote:
            print(f"[Premarket] prompt 已注入系统时间，剥除联网查询日期段 (thread={thread_id})")
    _cache_shorted = False
    try:
        from cache.stock_cache import extract_stock_name, query_cache_by_stock_name
        from config.constants import STOCK_CACHE_ENABLED
        if STOCK_CACHE_ENABLED:
            _stk = extract_stock_name(_effective_query)
            # 【PRE_MARKET 守卫】同 SSE 分支：盘前新闻按钮跳过股票缓存短路
            if _is_premarket_news_query(_effective_query) or (
                _effective_query and ("盘前新闻" in _effective_query)
            ):
                _stk = ""
            if _stk:
                _hit = query_cache_by_stock_name(_stk)
                if _hit is not None:
                    _cache_shorted = True
                    from api.monitor import monitor as _m1
                    _m1._bind_thread(thread_id)
                    _reasoning = (
                        "📦 【股票本地缓存命中】股票名：{n}；生成时间：{t}；"
                        "来源：{src}；新鲜度：{f}".format(
                            n=_hit.get("stock_name") or _stk,
                            t=(_hit.get("generated_at").strftime("%Y-%m-%d %H:%M:%S")
                               if hasattr(_hit.get("generated_at"), "strftime") else str(_hit.get("generated_at"))),
                            src=("系统预热(08/20点)" if str(_hit.get("source")) == "warmup"
                                 else "当日之前同类用户查询回填"),
                            f=("同小时新鲜" if bool(_hit.get("same_hour")) else
                               f"距今 {float(_hit.get('age_hours') or 0):.1f} 小时"),
                        )
                    )
                    try:
                        _m1.report_thinking(_reasoning)
                    except Exception:
                        pass
                    _txt = str(_hit.get("content") or "")
                    _m1.report_task_result(_txt)
    except Exception as _cache_shorted_err:
        import traceback as _tb_short
        _tb_short.print_exc()
        print(f"[StockCache] 非 SSE 命中短路失败（回退原链路）：{_cache_shorted_err}")
        _cache_shorted = False

    task: Optional[asyncio.Task] = None
    if not _cache_shorted:
        # 2. [后台执行] 异步运行 Agent（方案一：Router+Workflow 优先 → 异常 fallback 旧 run_deep_agent）
        #    仍用 _run_with_ctx 包裹：CancellationToken 三位一体绑定、Actor 注册注销、反向挂钩全部不变
        task = asyncio.create_task(
            _run_with_ctx(
                thread_id,
                effective_user_id,
                None,
                _DEFAULT_AGENT_TIMEOUT,
                _run_coro_with_workflow_priority,   # 【方案一】替换：原 run_deep_agent → 新包装优先 workflow
                _effective_query,
                thread_id,
                effective_user_id,
                quiet=_is_news_btn,
            )
        )
    # 登记：让 CancellationToken 在被取消时，也把对应 asyncio.Task 一起 cancel（双重保险）
    sa = _session_actor
    assert sa is not None, "SessionRegistryActor 未在 lifespan 中初始化"
    # 先注册（Actor 内部 cancel 旧的）
    await sa.send(SRMsg.REGISTER_AGENT_TASK, {
        "thread_id": thread_id,
        "task": task,
    })
    # 完成回调：发消息通知 Actor"若我仍是当前任务则清掉"
    def _on_done(t, tid=thread_id):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(
                    sa.send(SRMsg.UNREGISTER_IF_SELF, {
                        "thread_id": tid,
                        "task_id": id(t),
                        "task_type": "agent",
                    })
                )
            else:
                pass
        except Exception:
            pass
    if task is not None:
        task.add_done_callback(_on_done)

    # 3. [立即响应]
    return {"status": "started", "thread_id": thread_id, "user_id": effective_user_id}


# ======================================================================
# 流式输出 SSE 端点：POST /api/task/stream
# ======================================================================
# 设计要点：
#   * POST JSON body（query/thread_id/user_id） → fetch ReadableStream 消费，避开
#     EventSource 只能 GET 无法传 body 的限制；
#   * 首包 OPEN 帧先 yield（< 50ms）→ 再启动 agent → 实现"首 token 优化"；
#   * subscribe() bus → 监听 monitor 桥接来的所有事件 → 直接 yield SSE 帧；
#   * 三重资源守卫：(a) try/finally 中 unsubscribe + cancel_by_thread_id +
#     aclose() + asyncio.Task 取消；(b) request.is_disconnected() 轮询；
#     (c) 前端 beforeunload → POST /api/task/stop；
#   * 兼容原 POST /api/task + WebSocket：同时也注册到 Actor，旧路径监控仍能工作。
# ======================================================================

class _StreamTaskRequest(BaseModel):
    query: str
    thread_id: Optional[str] = None
    user_id: Optional[str] = None
    # 是否启用"增量 token 打字机"模式（默认 True；低端设备可关，用最终 done 全量重写）
    incremental: bool = True
    # 最大静默秒数：若 LLM 超过该秒不产出任何 token，前端会收到心跳，超过后返回错误
    max_silence_sec: float = 60.0
    # ---- 断点续传专用（header 已提供 Last-Event-ID，body 作为兜底） ----
    # 当浏览器某些代理/HTTP/2 网关吞掉 SSE 自定义 header 时，body.last_event_id 兜底生效
    last_event_id: Optional[str] = None


@app.post("/api/task/stream")
async def run_task_stream(req: _StreamTaskRequest, request: Request,
                          current: CurrentUser = _Depends(get_current_user)):
    """SSE 流式端点。返回 media_type=text/event-stream，客户端用 fetch + ReadableStream 消费。

    断点续传逻辑（核心分叉点）：
      1) 从 header `Last-Event-ID`（SSE 标准）+ 兜底 body.last_event_id 拿到 last_event_id；
      2) 若 (last_event_id 存在 并且 bus.has_thread_state(thread_id)) →
             → 走「RESUME」分支（不新建 agent）：
                i)   先发 replay_start(mode=continue|resync|full)
                ii)  有 gap（last 溢出/重启） → 发 gap 事件 + 一次性同步 final_text/source_ref（resync）
                iii) get_events_since(last_event_id) 逐条回放业务事件
                iv)  发 replay_end
                v)   如果 thread_state.finished → done/error 再发一次 → 结束；否则 subscribe 实时流继续
      3) 否则（全新会话 或 thread_id 没命中）→ 走「NEW」分支（原 SSE 端点逻辑：首包 OPEN → 启 agent → subscribe）
    """
    from fastapi.responses import StreamingResponse
    # 延迟加载，避免 import 链爆
    from api.stream_bus import get_stream_bus_sync, StreamSubscriber
    from api.stream_protocol import (
        SSEFrame, ErrorPayload, new_event_id, StreamEventType,
        ReplayStartPayload, ReplayEndPayload, GapPayload,
        DonePayload,
    )
    from agent.request_context import check_cancelled, RequestCancelledError

    # 1) ID 初始化 —— v2：不信任 Body，统一用 JWT CurrentUser
    thread_id = req.thread_id or str(uuid.uuid4())
    effective_user_id = current.user_id

    # 行级校验：thread 已存在则 owner 匹配 / admin 豁免
    existing_session = storage.get_session(thread_id)
    if existing_session:
        current_user_id_must_match(current, existing_session.get("user_id"))
        if existing_session.get("user_id"):
            effective_user_id = existing_session["user_id"]
    request_id = str(uuid.uuid4())

    # 确保用户 / 会话存在
    storage.get_or_create_user(effective_user_id)
    # ===== P0 薄壳修复：同上，改用 storage.ensure_session() 公开幂等 API =====
    storage.ensure_session(thread_id, effective_user_id, title="新会话")

    # 2) Prompt 预处理（盘前新闻快捷按钮）
    raw_query = req.query or ""
    effective_query = raw_query
    is_news_btn = _is_premarket_news_query(raw_query)
    if is_news_btn:
        effective_query, _ = _rewrite_premarket_query_if_shortcut(raw_query)

    # 3) Bus 初始化失败 → 503 降级
    try:
        bus = get_stream_bus_sync()
    except Exception as bus_init_err:
        raise HTTPException(status_code=503, detail=f"StreamBus 未初始化: {bus_init_err}")

    # ---- 断点续传：解析 last_event_id（header 优先 + body 兜底） ----
    header_last = request.headers.get("last-event-id") or request.headers.get("Last-Event-ID") or ""
    body_last = req.last_event_id if STREAM_RESUME_BODY_LAST_EVENT_ID_ALLOW else None
    last_event_id = (header_last or body_last or "").strip()
    # 判定是否命中"重连续传"分支：last_event_id 存在 AND 服务端 thread_state 命中 AND 已过期不成立
    is_resume = bool(last_event_id) and bus.has_thread_state(thread_id)

    agent_task: Optional[asyncio.Task] = None
    cancelled_reason: Optional[str] = None
    sa = _session_actor  # 可以为 None（测试模式下）

    async def _sse_generator_resume() -> "Any":
        """RESUME 分支：回放 ring 缓冲 →（必要时）resync → subscribe 实时流。

        关键约束：本分支"不新建 agent 任务"；
            - 如果 thread_state 没 finished → 后台一定有一个正在跑的 agent（之前的）
              此时只是重新订阅 bus 的实时事件（已有 agent 任务的 monitor 事件仍正常双写）
            - 如果 thread_state 已 finished → 回放完就发 done 并结束
        """
        nonlocal cancelled_reason
        _poll_interval = STREAM_DISCONNECT_POLL_INTERVAL_SEC
        _disconnected = False

        # (1) replay_start
        state = bus.get_thread_state(thread_id)
        has_gap, gap_count, replay_events = bus.get_events_since(thread_id, last_event_id)

        # 如果缓存为空（比如服务端重启，thread_state 已不在但 last_event_id 还在）
        if state is None or (not replay_events and not bus.earliest_event_id(thread_id)):
            # 冷启动：没缓冲，只能 full 清屏模式
            mode = "full"
            has_gap = True
            replay_events = []
        elif has_gap:
            mode = "resync" if STREAM_RESUME_COLD_RESTART_SUGGESTION == "resync" else "full"
        else:
            mode = "continue"

        yield SSEFrame.replay_start(ReplayStartPayload(
            mode=mode, request_id=request_id,
            last_event_id=last_event_id,
            expected_replay_count=len(replay_events),
            has_gap=has_gap,
        ), new_event_id())

        # (2) gap 事件（如果有缺口）
        if has_gap:
            reason = "buffer_overflow"
            first_cached = bus.earliest_event_id(thread_id)
            if not first_cached:
                reason = "server_restart" if mode == "full" else "not_found"
            suggestion = "resync" if mode in ("resync",) else "restart"
            yield SSEFrame.gap(GapPayload(
                reason=reason,
                last_event_id=last_event_id,
                first_cached_id=first_cached,
                suggestion=suggestion,
            ), new_event_id())

            # (2a) resync：一次性下发"当前最终文本 + 来源索引快照"，让前端哪怕丢了过程也能恢复
            if mode == "resync":
                # final_text
                final_text = state.final_text_buffer if state else ""
                if final_text:
                    # 用 delta "最后一条"的方式把 final_text 作为一条大的增量推下去
                    from api.stream_protocol import DeltaPayload
                    yield SSEFrame.delta(
                        DeltaPayload(index=-1, text=final_text, is_reasoning=False),
                        new_event_id(),
                    )
                # source_ref
                if state and state.source_count > 0:
                    yield SSEFrame.source_ref(state.snapshot_source_ref_payload(), new_event_id())
                # progress 提示
                from api.stream_protocol import ProgressPayload
                yield SSEFrame.progress(ProgressPayload(
                    stage="恢复断点" if not (state and state.finished) else "会话已结束",
                    percent=90 if state and not state.finished else 100,
                    detail=f"已从缓存恢复 {len(final_text)} 个字符，后续实时事件将无缝衔接"
                           if state and not state.finished else "该会话已完成，以下为最终内容。",
                ), new_event_id())

        # (3) 逐条回放业务事件
        replay_count = 0
        try:
            for (_eid, frame) in replay_events:
                if await request.is_disconnected():
                    _disconnected = True
                    break
                # 只回放 7 类业务事件（open/delta/.../source_ref/progress/done/error）
                # ring 里已不包含 replay/gap/heartbeat，这里直接 yield 即可
                yield frame
                replay_count += 1
        except Exception:
            pass

        # (4) replay_end
        yield SSEFrame.replay_end(ReplayEndPayload(
            replay_count=replay_count,
            gap_count=gap_count if has_gap else 0,
            from_cache_ts_ms=int(time.time() * 1000),
        ), new_event_id())

        # (5) 如果 session 已结束：补发一次 done/error（确保前端态机正确切 done）
        if state and state.finished:
            # final_text 已有，再发一次 DonePayload（done 事件让前端切换状态机）
            final_text = state.final_text_buffer
            # finish_reason → error payload / done payload
            if state.finish_reason in ("error", "cancelled", "timeout"):
                code = {"cancelled": "CANCELLED", "timeout": "TIMEOUT"}.get(state.finish_reason, "INTERNAL_ERROR")
                yield SSEFrame.error(ErrorPayload(
                    message=f"会话因 {state.finish_reason} 已结束（断点续传回放）",
                    code=code, cancelled=(state.finish_reason == "cancelled"), recoverable=False,
                ), new_event_id())
                return
            # done
            total_ms = int((time.monotonic() - state.start_monotonic) * 1000) if state else 0
            yield SSEFrame.done(DonePayload(
                final_text=final_text, usage={}, total_duration_ms=total_ms,
                source_ref_count=state.source_count, tool_call_count=state.tool_call_count,
            ), new_event_id())
            return

        # (6) 仍在进行中：subscribe 实时流 继续推（和 NEW 分支后半段相同逻辑，但不新建 agent_task）
        sub: StreamSubscriber = bus.subscribe(thread_id)
        try:
            sub_iter = sub.__aiter__()
            while True:
                if not _disconnected:
                    try:
                        if await request.is_disconnected():
                            _disconnected = True
                    except Exception:
                        _disconnected = True
                if _disconnected:
                    cancelled_reason = cancelled_reason or "sse_disconnected_resume"
                    try:
                        await cancel_by_thread_id(thread_id, cancelled_reason)
                    except Exception:
                        pass
                    err_frame = SSEFrame.error(
                        ErrorPayload(message=f"连接已断开: {cancelled_reason}",
                                     code="CANCELLED", cancelled=True, recoverable=False),
                        new_event_id(),
                    )
                    yield err_frame
                    return

                try:
                    anext_coro = sub_iter.__anext__()
                    frame = await asyncio.wait_for(anext_coro, timeout=_poll_interval)
                except asyncio.TimeoutError:
                    continue
                except StopAsyncIteration:
                    return

                yield frame
                if ("event: done" in frame) or ("event: error" in frame):
                    _drain_deadline = time.monotonic() + 1.0
                    while time.monotonic() < _drain_deadline:
                        try:
                            cc = sub_iter.__anext__()
                            f2 = await asyncio.wait_for(cc, timeout=0.1)
                            yield f2
                        except (asyncio.TimeoutError, StopAsyncIteration):
                            break
                    return
        finally:
            # 注意：RESUME 分支不 cancel agent_task（那是上一次连接创建的，
            #       本次连接只是"旁观订阅"，取消会让正在进行中的 agent 挂掉，影响其他订阅者）
            #       这里只需要退订订阅者队列即可。
            try:
                bus.unsubscribe(thread_id, sub)
            except Exception:
                pass

    async def _sse_generator_new() -> "Any":
        """NEW 分支（原 SSE 端点逻辑：首包 OPEN → 启 agent → subscribe）。
        【本地股票缓存新增】：在 reset_thread_state 之后、启 agent 之前，先判断是不是
        股票相关问句 → 命中当日当小时缓存 → 直接本地流式回灌，不启动 agent 链路。
        若未命中 → 走原 agent 链路；链路结束后由 main_agent.report_task_result 的缓存
        回填钩子自动把本轮最终结果（final_content）写到 STOCK_CACHE_DIR。
        """
        nonlocal agent_task, cancelled_reason
        _poll_interval = STREAM_DISCONNECT_POLL_INTERVAL_SEC
        _disconnected = False
        _open_yielded = False

        # ---------- Step A: 先 yield OPEN + PROGRESS（首包 < 50ms） ----------
        #   直接从 bus 里拿"开帧 + 进度"的首包，确保用户 50ms 内看到反馈。
        from api.stream_protocol import (
            OpenPayload, ProgressPayload, SSEFrame, new_event_id as _neid,
            DeltaPayload, DonePayload, SourceRefPayload, CitationMetaPayload,
        )
        _open_p = OpenPayload(request_id=request_id, thread_id=thread_id, user_id=effective_user_id)
        yield SSEFrame.open(_open_p, _neid())
        _prog_p = ProgressPayload(stage="接收请求", percent=3,
                                  detail="请求已到达后端，正在分配智能体资源 ...")
        yield SSEFrame.progress(_prog_p, _neid())
        _open_yielded = True

        # 重置当前 thread 的聚合态（final_text / sources / 计数），避免同会话多轮串台
        bus.reset_thread_state(thread_id)
        sub: StreamSubscriber = bus.subscribe(thread_id)
        # 再次把 OPEN 入 bus，保证其他并发订阅者（WS 多播 / 双开）也能收到
        bus.ev_open(thread_id, request_id=request_id, user_id=effective_user_id)
        bus.ev_progress(thread_id, stage="接收请求", percent=3,
                        detail="请求已到达后端，正在分配智能体资源 ...")

        # ================================================================
        # 【NEW】本地股票缓存短路命中：不启动 agent 链路，直接 SSE 打字机回灌
        # ================================================================
        cache_hit_payload: Optional[Dict[str, Any]] = None
        try:
            from cache.stock_cache import extract_stock_name, query_cache_by_stock_name, iter_cache_hit_chunks
            from config.constants import STOCK_CACHE_ENABLED
            if STOCK_CACHE_ENABLED:
                _stock = extract_stock_name(effective_query)
                # 【PRE_MARKET 守卫】防止盘前快捷按钮被 extract_stock_name 误抽出股票名，
                # 触发股票本地缓存短路（旧链路回填的污染文件）→ 跳过后文 Router+workflow 优先链路。
                if _is_premarket_news_query(effective_query) or (
                    effective_query and ("盘前新闻" in effective_query)
                ):
                    _stock = ""
                if _stock:
                    _hit = query_cache_by_stock_name(_stock)
                    if _hit is not None:
                        cache_hit_payload = _hit
        except Exception as _cache_err:
            import traceback as _tb_cache
            _tb_cache.print_exc()
            print(f"[StockCache] 查询命中时异常（不阻塞，走原链路）: {_cache_err}")
            cache_hit_payload = None

        if cache_hit_payload is not None:
            # Step B-缓存命中：直接本地回灌 delta，给前端"秒回"体验
            _hit_stock = str(cache_hit_payload.get("stock_name") or "")
            _hit_content = str(cache_hit_payload.get("content") or "")
            _hit_src = str(cache_hit_payload.get("source") or "")
            _hit_same_hour = bool(cache_hit_payload.get("same_hour"))
            _hit_age = float(cache_hit_payload.get("age_hours") or 0.0)
            _gen_at = cache_hit_payload.get("generated_at")
            _gen_at_str = _gen_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(_gen_at, "strftime") else str(_gen_at)
            # 1) reasoning 面板：放一行说明来源（"📦 本地缓存命中，生成时间 2026-08-29 08:00"）
            _reasoning_line = (
                f"📦 【股票本地缓存命中】股票名：{_hit_stock}；生成时间：{_gen_at_str}；"
                f"来源：{'系统预热(08/20点)' if _hit_src=='warmup' else '当日之前同类用户查询回填'}；"
                f"新鲜度：{'同小时新鲜' if _hit_same_hour else f'距今 {_hit_age:.1f} 小时'}"
            )
            _frame_reasoning = SSEFrame.delta(
                DeltaPayload(index=0, text=_reasoning_line + "\n\n", is_reasoning=True),
                _neid(),
            )
            yield _frame_reasoning
            bus.ev_delta(thread_id, text=_reasoning_line + "\n\n", is_reasoning=True)

            # 2) progress：更新进度条到 98%，说明走缓存
            _p = ProgressPayload(
                stage="本地股票缓存命中", percent=98,
                detail=(f"已命中当日「{_hit_stock}」缓存，直接回显（生成于 {_gen_at_str}）。"
                        f"若需最新即时行情，可在问句结尾加『请强制重新分析』触发重新联网。"),
            )
            yield SSEFrame.progress(_p, _neid())
            bus.ev_progress(thread_id, stage=_p.stage, percent=_p.percent, detail=_p.detail)

            # 3) 1 条空 citation_meta（缓存是离线正文，没有在线引用 [N]；保持前端引用卡片不报错）
            #   把 reasoning 中的"缓存文件路径"作为单条来源，让用户可以核实
            try:
                from config.constants import CITATION_TITLE_MAX_CHARS as _TMAX, CITATION_URL_MAX_CHARS as _UMAX
                from urllib.parse import quote
                _cache_path_raw = str(cache_hit_payload.get("path") or "")
                # 🔒 脱敏：不把服务器绝对路径暴露到前端 href；保留项目根下相对路径即可。
                try:
                    _cache_rel = str(Path(_cache_path_raw).resolve().relative_to(project_root)).replace("\\", "/")
                except Exception:
                    _cache_rel = "cache/stock_cache/<local>"
                _src_items = [{
                    "index": 1, "doc_id": "local_stock_cache_1",
                    "title": (f"本地缓存·{_hit_stock}（{_gen_at_str}）")[:_TMAX],
                    # 不输出 file:// 绝对盘符；输出 /static-redirect/<相对路径> 占位，前端即便点也能识别成本地资源
                    "url": (f"about:local-cache#" + quote(_cache_rel))[:_UMAX],
                    "source_type": "local_cache",
                    "reliability": "可靠（本地离线缓存，生成时间已标注）",
                    "channel": ("warmup_cache" if _hit_src == "warmup" else "queryback_cache"),
                    "snippet": _hit_content[:100],
                    "published_at": _gen_at_str,
                }]
                yield SSEFrame.citation_meta(CitationMetaPayload(thread_id=thread_id, items=_src_items), _neid())
                try:
                    bus.ev_citation_meta(thread_id, items=_src_items)
                except Exception:
                    pass
                yield SSEFrame.source_ref(SourceRefPayload(thread_id=thread_id, items=_src_items), _neid())
            except Exception:
                pass

            # 4) 正文按 24 字一段增量推送（模拟打字机效果）
            _segs = iter_cache_hit_chunks(_hit_content, segment_len=24)
            _final_text = _hit_content
            for _i, seg in enumerate(_segs):
                if await request.is_disconnected():
                    _disconnected = True
                    break
                _df = DeltaPayload(index=_i, text=seg, is_reasoning=False)
                yield SSEFrame.delta(_df, _neid())
                bus.ev_delta(thread_id, text=seg, is_reasoning=False)
                await asyncio.sleep(0.01)  # 10ms / 段，视觉平滑；总字数=800 ≈ 8s，接受

            # 5) done：final_text 作为全量字段再传一次（供前端重渲染用）
            total_ms = int(max(0.0, _hit_age) * 3600 * 1000) + 5  # 虚拟耗时，避免 0
            yield SSEFrame.done(DonePayload(
                final_text=_final_text,
                usage={"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0,
                       "from_stock_cache": True, "hit_stock": _hit_stock,
                       "hit_same_hour": _hit_same_hour, "hit_age_hours": _hit_age},
                total_duration_ms=total_ms,
                source_ref_count=1, tool_call_count=0,
            ), _neid())
            # 标记 bus 状态 finished，防止 WS 另一客户端连上后状态不一致
            try:
                bus.ev_delta(thread_id, text=_final_text, is_reasoning=False, _force_final=True)
            except Exception:
                pass
            try:
                bus.ev_done(thread_id, final_text=_final_text, usage={
                    "from_stock_cache": True, "hit_stock": _hit_stock})
            except Exception:
                pass
            # 订阅主循环（防止 done 之后残留事件）：跑 0.5s 排空后直接 return
            _drain_deadline = time.monotonic() + 0.5
            try:
                sub_iter = sub.__aiter__()
                while time.monotonic() < _drain_deadline:
                    if await request.is_disconnected():
                        break
                    try:
                        f2 = await asyncio.wait_for(sub_iter.__anext__(), timeout=0.05)
                        yield f2
                        if ("event: done" in f2) or ("event: error" in f2):
                            break
                    except (asyncio.TimeoutError, StopAsyncIteration):
                        continue
            finally:
                try:
                    bus.unsubscribe(thread_id, sub)
                except Exception:
                    pass
            return

        # ================================================================
        # 【方案一新增】Router+analysis_workflow 优先链路（SSE 分支）
        #   · 【关键流式】workflow 放后台 asyncio.create_task，yield 立刻进入 subscribe 循环。
        #     → 这样 workflow 内部的 bus.ev_progress / bus.ev_reasoning 中间阶段事件
        #       一产生就立刻被 subscribe 主循环 yield 给 HTTP 响应，前端能立即看到。
        #     → workflow 失败（异常）在 task 内捕获后，设置 _wf_fallback=true 并 bus.ev_error，
        #       由主循环的 _wf_fallback 判定触发 fallback 旧 run_deep_agent（不破坏兼容）。
        #   · **任何异常** → 打印 warning，并继续往下 Step B 启动旧 run_deep_agent
        #     （兼容兜底保留，符合用户"保守迁移+只改两接口"决策）
        # ================================================================
        import sys as _sys_wf
        _wf_fallback = False  # 是否需要 fallback（workflow 后台执行失败时置 True）
        bus.ev_progress(thread_id, stage="Router+Workflow 优先调度", percent=8,
                        detail="Router Agent 正在识别意图 + 工作流 DAG 执行中 ...")
        yield SSEFrame.progress(ProgressPayload(
            stage="Router+Workflow 优先调度", percent=8,
            detail="Router Agent 正在识别意图 + 工作流 DAG 执行中 ...",
        ), _neid())

        async def _wrapped_workflow():
            """后台 task：执行 Router+workflow 事件桥接；失败回日志并把 _wf_fallback 置 True。"""
            nonlocal _wf_fallback
            try:
                await _try_run_workflow_push_events(
                    effective_query, thread_id, effective_user_id,
                    bus=bus, quiet=is_news_btn,
                )
            except Exception as _wf_exc:
                _wf_fallback = True
                import traceback as _tb_wf
                import io as _io_wf
                _buf = _io_wf.StringIO()
                _tb_wf.print_exc(file=_buf)
                _tb_str = _buf.getvalue()
                _sys_wf.stderr.write("\n" + "=" * 64 + "\n")
                _sys_wf.stderr.write("【方案一 SSE WF 异常 → fallback】\n")
                _sys_wf.stderr.write("=" * 64 + "\n")
                _sys_wf.stderr.write(_tb_str)
                _sys_wf.stderr.write(
                    f"异常类型: {type(_wf_exc).__name__} | 消息: {_wf_exc!s}\n"
                )
                _sys_wf.stderr.write("=" * 64 + "\n\n")
                _sys_wf.stderr.flush()
                # stdout mirror（保证运维日志中一定可见）
                print("\n" + "=" * 64, flush=True)
                print("[方案一 Router+Workflow SSE] 异常 → fallback 旧 run_deep_agent", flush=True)
                print("=" * 64, flush=True)
                for _ln in _tb_str.splitlines():
                    print(f"  TB| {_ln}", flush=True)
                print(
                    f"异常类型: {type(_wf_exc).__name__} | 消息: {_wf_exc!s}",
                    flush=True,
                )
                print("=" * 64 + "\n", flush=True)
                del _buf, _tb_str
                # 给前端一条 reasoning/error：触发用户感知，然后主循环读到 _wf_fallback 启 fallback
                try:
                    bus.ev_progress(
                        thread_id, stage="工作流异常，切换至兼容兜底", percent=10,
                        detail=f"Router+workflow 异常（{type(_wf_exc).__name__}）→ 透明切换旧 Agent 链路 ...",
                    )
                except Exception:
                    pass

        agent_task = asyncio.create_task(
            _wrapped_workflow(), name=f"stream_wf_{thread_id[:8]}",
        )
        # 立刻进入 subscribe 实时流循环：中间阶段 events 立即 yield（不再等 workflow 结束）
        # （注意：如果 _wf_fallback=True 后需要让主循环 break 出去走 Step B fallback；
        #  而 workflow 成功后会发 bus.ev_done，主循环读到 done/error 自动结束并 return）
        try:
            sub_iter = sub.__aiter__()
            while True:
                if not _disconnected:
                    try:
                        if await request.is_disconnected():
                            _disconnected = True
                    except Exception:
                        _disconnected = True
                if _disconnected:
                    cancelled_reason = cancelled_reason or "sse_disconnected_wf"
                    try:
                        await cancel_by_thread_id(thread_id, cancelled_reason)
                    except Exception:
                        pass
                    if agent_task is not None and not agent_task.done():
                        agent_task.cancel()
                    err_frame = SSEFrame.error(
                        ErrorPayload(message=f"连接已断开: {cancelled_reason}",
                                     code="CANCELLED", cancelled=True,
                                     recoverable=False),
                        _neid(),
                    )
                    yield err_frame
                    return
                # 先检查是否需要 fallback（workflow 失败 → _wf_fallback=True 且 agent_task done）
                if _wf_fallback and (agent_task is None or agent_task.done()):
                    # 跳出到后面 Step B fallback 旧 run_deep_agent
                    break
                try:
                    anext_coro = sub_iter.__anext__()
                    frame = await asyncio.wait_for(anext_coro, timeout=_poll_interval)
                except asyncio.TimeoutError:
                    continue
                except StopAsyncIteration:
                    break

                yield frame
                if ("event: done" in frame) or ("event: error" in frame):
                    # workflow 正常结束 → 排空 1s 后直接 return
                    _drain_deadline = time.monotonic() + 1.0
                    while time.monotonic() < _drain_deadline:
                        try:
                            cc = sub_iter.__anext__()
                            f2 = await asyncio.wait_for(cc, timeout=0.1)
                            yield f2
                        except (asyncio.TimeoutError, StopAsyncIteration):
                            break
                    # 资源清理（finally 还会再做一次，双重保险）
                    try:
                        bus.unsubscribe(thread_id, sub)
                    except Exception:
                        pass
                    return
        finally:
            if agent_task is not None and not agent_task.done():
                agent_task.cancel()
            try:
                await cancel_by_thread_id(thread_id,
                                          cancelled_reason or "sse_generator_wf_finalize")
            except Exception:
                pass

        # --- workflow 失败 → 透明 fallback：继续往下 Step B 启动旧 run_deep_agent ---
        # （注：走到这里只可能是上面 while 通过 _wf_fallback break 跳出；正常 workflow 已 return）
        agent_task = None  # reset，后续 Step B 会新建 agent_task = _wrapped_run()

        # ---------- Step B: 启动 Agent（后台） ----------
        async def _wrapped_run():
            try:
                await _run_with_ctx(
                    thread_id,
                    effective_user_id,
                    None,
                    _DEFAULT_AGENT_TIMEOUT,
                    run_deep_agent,
                    effective_query,
                    thread_id,
                    effective_user_id,
                    quiet=is_news_btn,
                )
                bus.ev_done(thread_id)
            except asyncio.CancelledError as ce:
                cancelled_reason2 = cancelled_reason or "asyncio_cancelled"
                bus.ev_error(thread_id, message=f"任务已取消: {cancelled_reason2}",
                             code="CANCELLED", cancelled=True, recoverable=False)
                raise
            except RequestCancelledError as rce:
                cancelled_reason2 = cancelled_reason or "request_token_cancelled"
                bus.ev_error(thread_id, message=f"任务已取消: {cancelled_reason2}",
                             code="CANCELLED", cancelled=True, recoverable=False)
            except TimeoutError as te:
                bus.ev_error(thread_id, message=f"任务超时（>{_DEFAULT_AGENT_TIMEOUT}s）: {te}",
                             code="TIMEOUT", cancelled=False, recoverable=True)
            except Exception as e:
                import traceback as _tb
                _tb.print_exc()
                bus.ev_error(thread_id, message=f"内部错误: {e}",
                             code="INTERNAL_ERROR", cancelled=False, recoverable=False)

        agent_task = asyncio.create_task(_wrapped_run(), name=f"stream_agent_{thread_id[:8]}")

        if sa is not None:
            await sa.send(SRMsg.REGISTER_AGENT_TASK, {
                "thread_id": thread_id,
                "task": agent_task,
            })

            def _on_done(t, tid=thread_id):
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(
                            sa.send(SRMsg.UNREGISTER_IF_SELF, {
                                "thread_id": tid,
                                "task_id": id(t),
                                "task_type": "agent",
                            })
                        )
                except Exception:
                    pass
            agent_task.add_done_callback(_on_done)

        # ---------- Step C: 主循环：(sub reader) vs (disconnect poll) vs (agent_task done) ----------
        try:
            sub_iter = sub.__aiter__()
            while True:
                if not _disconnected:
                    try:
                        if await request.is_disconnected():
                            _disconnected = True
                    except Exception:
                        _disconnected = True
                if _disconnected:
                    cancelled_reason = cancelled_reason or "sse_disconnected"
                    try:
                        await cancel_by_thread_id(thread_id, cancelled_reason)
                    except Exception:
                        pass
                    if agent_task is not None and not agent_task.done():
                        agent_task.cancel()
                    err_frame = SSEFrame.error(
                        ErrorPayload(message=f"连接已断开: {cancelled_reason}",
                                     code="CANCELLED", cancelled=True,
                                     recoverable=False),
                        new_event_id(),
                    )
                    yield err_frame
                    return

                try:
                    anext_coro = sub_iter.__anext__()
                    frame = await asyncio.wait_for(anext_coro, timeout=_poll_interval)
                except asyncio.TimeoutError:
                    if agent_task is not None and agent_task.done():
                        _drain_deadline = time.monotonic() + 2.0
                        while time.monotonic() < _drain_deadline:
                            try:
                                cc = sub_iter.__anext__()
                                f2 = await asyncio.wait_for(cc, timeout=0.1)
                                yield f2
                            except (asyncio.TimeoutError, StopAsyncIteration):
                                break
                        return
                    continue
                except StopAsyncIteration:
                    return

                yield frame

                if ("event: done" in frame) or ("event: error" in frame):
                    _drain_deadline = time.monotonic() + 1.0
                    while time.monotonic() < _drain_deadline:
                        try:
                            cc = sub_iter.__anext__()
                            f2 = await asyncio.wait_for(cc, timeout=0.1)
                            yield f2
                        except (asyncio.TimeoutError, StopAsyncIteration):
                            break
                    return

        finally:
            if agent_task is not None and not agent_task.done():
                agent_task.cancel()
            try:
                await cancel_by_thread_id(thread_id,
                                          cancelled_reason or "sse_generator_finalize")
            except Exception:
                pass
            try:
                bus.unsubscribe(thread_id, sub)
            except Exception:
                pass
            if sa is not None and agent_task is not None:
                try:
                    await sa.send(SRMsg.UNREGISTER_IF_SELF, {
                        "thread_id": thread_id,
                        "task_id": id(agent_task),
                        "task_type": "agent",
                    })
                except Exception:
                    pass

    # ---------- 分派 ----------
    import time as time
    headers = {
        "Cache-Control": "no-cache, no-transform",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
        "X-Stream-Request-Id": request_id,
        "X-Powered-By": "MOSS-Finance-Assistant",
    }
    gen = _sse_generator_resume() if is_resume else _sse_generator_new()
    return StreamingResponse(
        gen,
        media_type="text/event-stream; charset=utf-8",
        headers=headers,
    )


@app.post("/api/task/stop")
async def stop_task(request: Request, current: CurrentUser = _Depends(get_current_user)):
    """停止指定会话当前正在执行的 Agent 任务（P0 鉴权：行级二次校验 owner）。

    执行顺序（毫秒级取消链路）：
      1) cancel_by_thread_id → CancellationToken 原子置位 →
         级联：子任务取消 + 回调执行 + 下一次 check_cancelled() 立即抛异常；
      2) STOP_AND_REMOVE_TASK → asyncio.Task.cancel() + 从 Actor 注册表移除。
    """
    import json as _json
    body = await request.body()
    try:
        data = _json.loads(body) if body else {}
    except Exception:
        data = {}
    thread_id = data.get("thread_id", "")
    if not thread_id:
        return {"status": "error", "message": "缺少 thread_id"}
    # ====== P0 行级二次校验：未登录/非 owner/admin 不得停止他人会话 ======
    sess = storage.get_session(thread_id)
    if sess and sess.get("user_id"):
        current_user_id_must_match(current, sess["user_id"])
    sa = _session_actor
    assert sa is not None
    # 第 1 步：先触发 CancellationToken 级联取消（同步立即返回）
    try:
        cancel_info = await cancel_by_thread_id(thread_id, "user_stop_clicked")
    except Exception as _e:
        cancel_info = {"found": False, "error": str(_e)}
    # 第 2 步：再触发 asyncio.Task.cancel()（STOP_AND_REMOVE_TASK 内部完成）
    result = await sa.ask(SRMsg.STOP_AND_REMOVE_TASK, {"thread_id": thread_id})
    stopped = bool(result and result.get("stopped"))
    print(
        f"[Stop] thread_id={thread_id} cancel_token={cancel_info} task_cancelled={stopped}"
    )
    if not stopped and (not cancel_info or not cancel_info.get("found")):
        return {"status": "not_found", "message": "当前没有正在执行的任务"}
    return {
        "status": "stopped",
        "thread_id": thread_id,
        "token_cancelled": cancel_info,
        "task_cancelled": stopped,
    }


async def _register_background_task(thread_id: str, task: asyncio.Task) -> None:
    """
    注册后台任务：改由 SessionRegistryActor 串行执行，
    原子地：(a) 取消同会话旧后台任务 + (b) 取消同会话聊天任务 + (c) 注册新任务。

    取消原因依旧保留：防止 checkpointer 并发写入竞态。
    实现方式改为 Actor 消息：取消+注册都在同一条消息处理中完成（无 await 交错），
    比原先"散落在 3 处 if 判断 + dict 写"的顺序更确定。
    """
    sa = _session_actor
    assert sa is not None
    await sa.send(SRMsg.REGISTER_BG_TASK, {
        "thread_id": thread_id,
        "task": task,
    })
    # done 回调：发消息给 Actor 自清理
    def _on_bg_done(t, tid=thread_id):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(
                    sa.send(SRMsg.UNREGISTER_IF_SELF, {
                        "thread_id": tid,
                        "task_id": id(t),
                        "task_type": "bg",
                    })
                )
        except Exception:
            pass
    task.add_done_callback(_on_bg_done)


# ===== P1-F：挂载 zsxq 子路由（此时 _run_with_ctx + _register_background_task 都已定义完成）=====
_ensure_zsxq_router_installed_once()


# ======================== 复盘预测接口 ========================


class ReviewPredictionRequest(BaseModel):
    thread_id: str
    user_id: Optional[str] = None
    # 用户可选输入：提及的个股或关注点，会带到 deepseek 分析
    user_query: Optional[str] = ""


async def _run_review_prediction(thread_id: str, user_id: Optional[str] = None, user_query: str = ""):
    """
    复盘预测：串行执行盘前小作文热度 + 盘前新闻搜索，汇总两路结果，
    调用 DeepSeek 结合"大盘指数预测" skill 给出指数走势预测与个股应对策略。

    流程：
        1. 执行盘前小作文热度分析 → zsxq_result（推送过程给前端）
        2. 执行盘前新闻搜索（main_agent）→ news_result（推送过程给前端）
        3. 加载 skills/index-prediction/SKILL.md 规则
        4. 构建 prompt：skill + 两路结果 + 用户提及的个股 → DeepSeek 分析
        5. 推送最终预测结果到前端对话区（含推理逻辑）
    """
    from api.context import set_thread_context, reset_session_context
    from api.monitor import monitor

    thread_token = set_thread_context(thread_id)
    try:
        # ===== 阶段 0：加载 skill 规则 =====
        skill_path = project_root / "skills" / "index-prediction" / "SKILL.md"
        try:
            skill_content = skill_path.read_text(encoding="utf-8")
        except Exception as e:
            print(f"[ReviewPrediction] 加载 skill 失败: {e}")
            skill_content = ""

        # ===== 阶段 1：盘前小作文热度分析 =====
        monitor._emit("tool_start", "【复盘预测 阶段1/3】执行盘前小作文热度分析...")
        zsxq_result = await _run_zsxq_analysis(thread_id, emit_to_frontend=False)
        if not zsxq_result.strip():
            zsxq_result = "（小作文热度分析无结果或执行失败）"
            monitor._emit("tool_start", "⚠️ 小作文热度分析未返回内容，继续执行后续步骤")
        else:
            monitor._emit("tool_start", f"✅ 小作文热度分析完成，结果长度 {len(zsxq_result)} 字符")

        # ===== 阶段 2：盘前新闻搜索（通过主 agent，内部会推送过程给前端）=====
        monitor._emit("tool_start", "【复盘预测 阶段2/3】执行盘前新闻搜索...")
        from agent.main_agent import run_deep_agent
        # 注入当前北京时间 + 搜索范围提示，避免 agent 联网搜索"今天是周几"浪费 token
        # 与盘前新闻快捷按钮 _rewrite_premarket_query_if_shortcut 复用同一套计算函数，
        # 保证"前端按钮"和"复盘预测阶段2"两个入口的时间窗口完全一致。
        _now_bj_pre = _beijing_now()
        _weekday_cn_pre = _weekday_cn_of(_now_bj_pre)
        _current_time_str_pre = _now_bj_pre.strftime("%Y年%m月%d日 %H:%M")
        _search_range_hint = _format_time_range_hint(_now_bj_pre)
        pre_market_prompt = format_prompt(
            "server.pre_market_prompt",
            current_time_str=_current_time_str_pre,
            weekday_cn=_weekday_cn_pre,
            search_range_hint=_search_range_hint,
        )
        news_result = await run_deep_agent(pre_market_prompt, thread_id, user_id)
        if not news_result or not news_result.strip():
            news_result = "（盘前新闻搜索无结果或执行失败）"
            monitor._emit("tool_start", "⚠️ 盘前新闻搜索未返回内容，继续执行后续步骤")
        else:
            monitor._emit("tool_start", f"✅ 盘前新闻搜索完成，结果长度 {len(news_result)} 字符")

        # ===== 阶段 3：调用 DeepSeek 综合分析 + 指数预测 =====
        monitor._emit("tool_start", "【复盘预测 阶段3/3】调用 DeepSeek 综合分析并生成指数预测...")
        monitor.report_thinking("大盘指数预测")

        user_stock_hint = ""
        if user_query and user_query.strip():
            user_stock_hint = f"\n\n【用户特别关注】用户在复盘预测中提及：{user_query.strip()}，请在个股应对策略部分重点分析。"

        # 注入当前实际日期时间（北京时间），避免 DeepSeek 从搜索结果推断错误日期
        from datetime import timezone, timedelta as _td
        _tz_bj = timezone(_td(hours=8))
        _now_bj = datetime.now(_tz_bj)
        _weekday_cn = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][_now_bj.weekday()]
        _current_time_str = _now_bj.strftime("%Y年%m月%d日 %H:%M") + f"（{_weekday_cn}）"
        _market_phase = "盘后" if _now_bj.hour >= 15 else ("盘中" if 9 <= _now_bj.hour < 15 else "盘前")

        # 分析提示词从 prompts.yml runtime_prompts 段加载模板，动态填入时间/skill/结果/个股提示
        analysis_prompt = format_prompt(
            "server.review_prediction.analysis_prompt",
            current_time_str=_current_time_str,
            market_phase=_market_phase,
            skill_content=skill_content,
            zsxq_result=zsxq_result[:4000],
            news_result=news_result[:4000],
            user_stock_hint=user_stock_hint,
        )

        # 调用 DeepSeek（使用原始 _base_model，不走 PTD 包装，避免工具路由干扰）
        from agent.llm import _base_model
        from langchain_core.messages import HumanMessage
        try:
            resp = await _base_model.ainvoke([HumanMessage(content=analysis_prompt)])
            analysis_result = resp.content if hasattr(resp, "content") else str(resp)
        except Exception as e:
            print(f"[ReviewPrediction] DeepSeek 分析异常: {e}")
            analysis_result = f"⚠️ DeepSeek 综合分析调用失败: {e}\n\n【盘前小作文热度】\n{zsxq_result[:1500]}\n\n【盘前新闻】\n{news_result[:1500]}"

        # ===== 推送最终结果到前端对话区 =====
        monitor.report_task_result(analysis_result)

        # ===== 保存到会话历史 =====
        try:
            await _save_zsxq_to_history(thread_id, analysis_result)
        except Exception as e:
            print(f"[ReviewPrediction] 保存历史失败（不致命）: {e}")

    except asyncio.CancelledError:
        # 与 main_agent 取消语义分级对齐，避免误报"用户取消"
        from agent.request_context import current_token, RequestCancelledError
        tok = current_token()
        _r: str = ""
        # 当前 CancelledError 可能由 RequestCancelledError 引发（check_cancelled 抛），
        # 或直接由 task.cancel(msg) 引发，从 __context__ 里也拿 reason
        _ctx_err = getattr(asyncio.CancelledError, "__context__", None)
        if isinstance(_ctx_err, RequestCancelledError):
            _r = getattr(_ctx_err, "reason", "") or ""
        if not _r and tok is not None:
            _r = getattr(tok, "reason", "") or ""
        if "user_stop_clicked" in _r or "stop_clicked" in _r:
            print(f"[ReviewPrediction] 会话 {thread_id} 任务被用户主动取消")
            monitor.report_error("⏹ 复盘预测已停止")
        elif "timeout:" in _r or "deadline" in _r:
            elapsed = round(getattr(tok, "age_sec", 0.0), 1) if tok else 0.0
            print(f"[ReviewPrediction][超时] 会话 {thread_id} 在 {elapsed}s 后超时终止 "
                  f"(reason={_r!r})")
            monitor.report_error(f"⏱ 复盘预测超时（{elapsed}s），请重试")
        elif "websocket_disconnected" in _r:
            print(f"[ReviewPrediction] 会话 {thread_id} 连接断开，任务终止 (reason={_r!r})")
        else:
            # 任务替换 / 级联取消：正常系统行为，不给用户显示停止
            print(f"[ReviewPrediction] 会话 {thread_id} 任务已取消 (reason={_r!r}，不向用户报错)")
    except Exception as e:
        import traceback
        traceback.print_exc()
        monitor.report_error(f"复盘预测异常: {e}")
    finally:
        reset_session_context(None, thread_token)


@app.post("/api/review-prediction")
async def run_review_prediction(req: ReviewPredictionRequest,
                                current: CurrentUser = _Depends(get_current_user)):
    """
    触发复盘预测：串行执行盘前小作文热度 + 盘前新闻搜索，
    调用 DeepSeek 结合 skill 规则给出指数走势预测与个股应对策略。
    结果通过 WebSocket 推送到前端对话区。

    P0 鉴权：不信任 req.user_id，统一用 JWT.current.user_id；若会话存在，行级校验 owner。
    """
    thread_id = req.thread_id
    effective_user_id = current.user_id
    sess = storage.get_session(thread_id)
    if sess:
        current_user_id_must_match(current, sess.get("user_id"))
        if sess.get("user_id"):
            effective_user_id = sess["user_id"]
    # 复盘预测 = 快捷按钮，控制台静默
    task = asyncio.create_task(
        _run_with_ctx(
            thread_id,
            effective_user_id,
            None,
            _DEFAULT_BG_TIMEOUT,
            _run_review_prediction,
            thread_id,
            effective_user_id,
            req.user_query or "",
            quiet=True,
        )
    )
    await _register_background_task(thread_id, task)  # 防止同会话重复触发
    return {"status": "started", "thread_id": thread_id, "user_id": effective_user_id}


# ======================== 用户与会话管理接口（P0 鉴权升级） ========================
# 兼容策略（A3d）：/api/users* 前缀仍保留在 middleware 白名单里（router smoke 测试兼容），
# 但下面每个端点显式 Depends(get_current_user_optional) + 登录态下强制 current_user_id_must_match
# 行级校验，保证「已登录用户不能操作他人资源」；未登录用户（旧端点调用）维持原行为但风险被限流保护。

def _require_role_admin_or_owner(current: CurrentUser) -> None:
    """运维端点 / 管理操作守卫：role != owner/admin → 403。"""
    if current.role not in ("owner", "admin"):
        raise HTTPException(
            status_code=403,
            detail={"code": "ADMIN_ONLY", "message": f"需要 owner/admin 角色（当前 {current.role}）"},
        )


@app.post("/api/users")
async def create_or_login_user(req: UserRequest,
                               current: Optional[CurrentUser] = _Depends(get_current_user_optional)):
    """登录/注册：用户不存在则创建，返回用户信息。

    兼容旧明文调用（无 JWT）：允许创建。
    已登录用户（有 JWT）：禁止把任意 user_id 创建为其他账号，仅能创建/读取自己的 user_id。
    """
    if current is not None and not current.is_guest:
        # 禁止「已登录的正式账号」创建他人 user_id（防止越权预埋会话）
        if req.user_id != current.user_id:
            raise HTTPException(
                status_code=403,
                detail={"code": "FORBIDDEN_CREATE_OTHERS",
                        "message": "已登录用户只能使用自己的 user_id"},
            )
    user = storage.get_or_create_user(req.user_id, req.display_name)
    return {"status": "ok", "user": user}


@app.get("/api/users/{user_id}")
async def get_user_info(user_id: str,
                        current: Optional[CurrentUser] = _Depends(get_current_user_optional)):
    """获取用户信息（P0：登录态下，普通用户只能读自己；owner/admin 可读任意）。"""
    user = storage.get_user(user_id)
    if not user:
        raise HTTPException(status_code=HTTP_CODE_NOT_FOUND, detail="用户不存在")
    if current is not None:
        current_user_id_must_match(current, user_id)
    return {"user": user}


@app.get("/api/users/{user_id}/sessions")
async def list_user_sessions(user_id: str,
                             current: Optional[CurrentUser] = _Depends(get_current_user_optional)):
    """列出某用户的所有会话。登录态强制行级校验。"""
    if not storage.get_user(user_id):
        raise HTTPException(status_code=HTTP_CODE_NOT_FOUND, detail="用户不存在")
    if current is not None:
        current_user_id_must_match(current, user_id)
    sessions = storage.list_sessions(user_id)
    return {"sessions": sessions}


@app.post("/api/users/{user_id}/sessions")
async def create_session(user_id: str, req: SessionRequest,
                         current: Optional[CurrentUser] = _Depends(get_current_user_optional)):
    """为用户新建一个会话，返回 session_id。登录态强制行级校验。"""
    if current is not None:
        current_user_id_must_match(current, user_id)
    storage.get_or_create_user(user_id)
    session = storage.create_session(user_id, req.title)
    return {"status": "ok", "session": session}


@app.get("/api/sessions/{session_id}/history")
async def get_history(session_id: str,
                      current: CurrentUser = _Depends(get_current_user)):
    """获取会话的对话历史（P0 强制登录 + 行级二次校验 owner）。"""
    session = storage.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    # ===== P0 双保险：JWT current_user 与会话 owner 匹配（owner/admin 豁免） =====
    current_user_id_must_match(current, session.get("user_id"))
    # get_session_history 在 async 上下文中返回 coroutine
    history = await get_session_history(session_id)
    return {"session_id": session_id, "messages": history}


@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str,
                         current: CurrentUser = _Depends(get_current_user)):
    """删除会话记录（幂等：不存在也返回成功，避免批量删除时 404 中断）。

    P0 强制登录 + verify_session_owner（双保险，不再信任 query 参数 user_id）。
    """
    existing = storage.get_session(session_id)
    if existing:
        # ===== 用户隔离检查：防止越权删除其他用户的会话 =====
        current_user_id_must_match(current, existing.get("user_id"))
        if not storage.verify_session_owner(session_id, current.user_id) and current.role not in ("owner", "admin"):
            raise HTTPException(status_code=403, detail=f"无权删除会话 {session_id}")
    storage.delete_session(session_id)
    # 清理记忆管理：滑窗/摘要/关键决策（失败不影响主流程）
    try:
        from agent.memory_manager import get_memory_manager
        mm = get_memory_manager()
        await mm.clear_session(session_id)
    except Exception as mm_err:
        print(f"[MemoryManager] 删除会话 {session_id} 记忆失败（不致命）: {mm_err}")
    for mod_name, cleanup_fn in [
        ("agent.trace", "get_trace_logger"),
        ("agent.feedback_handler", "get_feedback_handler"),
        ("agent.state_store", "get_state_store"),
    ]:
        try:
            mod = __import__(mod_name, fromlist=[cleanup_fn])
            obj = getattr(mod, cleanup_fn)()
            await obj.clear_session(session_id)
        except Exception:
            pass
    # 清理 LangGraph checkpointer 中该 thread_id 的历史
    try:
        from agent.main_agent import get_main_agent
        agent = await get_main_agent()
        config = {"configurable": {"thread_id": session_id}}
        await agent.aupdate_state(config, {"messages": []})  # type: ignore[attr-defined]
    except Exception:
        pass
    return {"status": "ok"}


class RenameRequest(BaseModel):
    title: str
    # P0 升级：user_id 字段保留兼容旧前端，但实际校验改为 JWT.current.user_id 强制匹配
    user_id: Optional[str] = None


@app.patch("/api/sessions/{session_id}/title")
async def rename_session(session_id: str, req: RenameRequest,
                         current: CurrentUser = _Depends(get_current_user)):
    """手动重命名会话标题（P0：JWT 登录态 + verify_session_owner）。"""
    title = req.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="标题不能为空")
    session = storage.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    # ===== 用户隔离检查：P0 双保险 =====
    current_user_id_must_match(current, session.get("user_id"))
    if not storage.verify_session_owner(session_id, current.user_id) and current.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail=f"无权修改会话 {session_id}")
    if not storage.update_session_title(session_id, title):
        raise HTTPException(status_code=404, detail="会话不存在")
    return {"status": "ok", "session_id": session_id, "title": title}


async def _langraph_collect_remove_ids(
    session_id: str, items: list,
) -> tuple[list[str], list[int]]:
    """从 LangGraph checkpointer 收集要删除的 message id 列表。

    Args:
        session_id: 会话ID
        items: [{"turn_index": int, "role": "user"|"assistant"|"all"}, ...]

    Returns:
        (to_remove_ids, kept_positions)
        - to_remove_ids: 待删除的 message.id 列表（去重）
        - kept_positions: 兜底 whole-update 时需要保留的 msg 位置集合
          （若 to_remove_ids 为空，用 kept_positions 重建 messages）
    """
    try:
        from langchain_core.messages import (
            AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage,
        )
        from agent.main_agent import get_main_agent

        agent = await get_main_agent()
        config = {"configurable": {"thread_id": session_id}}
        state = await agent.aget_state(config)  # type: ignore[attr-defined]
        msgs: list = (state.values or {}).get("messages", []) if state else []
        if not msgs:
            return [], []

        # 拆 turns_msgs：每个元素 = {turn_idx: 1-based, positions: [msg_idx], human_pos, ai_positions}
        turns_msgs: list[dict] = []
        cur_turn: dict | None = None
        for i, m in enumerate(msgs):
            if isinstance(m, HumanMessage):
                if cur_turn is not None:
                    turns_msgs.append(cur_turn)
                cur_turn = {"positions": [i], "human_pos": i, "ai_positions": []}
            elif isinstance(m, SystemMessage):
                continue
            else:
                if cur_turn is not None:
                    cur_turn["positions"].append(i)
                    if not isinstance(m, HumanMessage):
                        cur_turn["ai_positions"].append(i)
                # 否则还没出现 HumanMessage 的 AIMessage，跳过
        if cur_turn is not None:
            turns_msgs.append(cur_turn)

        # 收集要删除的位置集合
        remove_positions: set[int] = set()
        for item in items:
            ti = int(item.get("turn_index", 0))
            role = item.get("role", "all")
            if ti < 1 or ti > len(turns_msgs):
                continue
            tm = turns_msgs[ti - 1]
            if role == "all":
                remove_positions.update(tm["positions"])
            elif role == "user":
                if tm["human_pos"] is not None:
                    remove_positions.add(tm["human_pos"])
            elif role == "assistant":
                remove_positions.update(tm["ai_positions"])

        # 收集 msg.id
        to_remove_ids: list[str] = []
        for pos in remove_positions:
            if 0 <= pos < len(msgs):
                mid = getattr(msgs[pos], "id", None)
                if mid:
                    to_remove_ids.append(mid)

        # 兜底 whole-update：保留未在 remove_positions 中的所有消息位置
        kept_positions = [i for i in range(len(msgs)) if i not in remove_positions]
        return to_remove_ids, kept_positions
    except Exception as e:
        print(f"[TurnDelete] _langraph_collect_remove_ids 异常: {e}")
        return [], []


async def _langraph_apply_removal(
    session_id: str, to_remove_ids: list[str], kept_positions: list[int],
) -> None:
    """应用 LangGraph 清理：优先 RemoveMessage，失败兜底 whole-update。"""
    if not to_remove_ids and not kept_positions:
        return
    try:
        from langchain_core.messages import (
            SystemMessage, RemoveMessage as _RM,
        )
        from agent.main_agent import get_main_agent

        agent = await get_main_agent()
        config = {"configurable": {"thread_id": session_id}}

        if to_remove_ids:
            try:
                removes = [_RM(id=mid) for mid in to_remove_ids]
                await agent.aupdate_state(
                    config, {"messages": removes},
                )  # type: ignore[attr-defined]
                print(f"[TurnDelete] LangGraph RemoveMessage 删除 {len(removes)} 条（session {session_id}）")
                return
            except Exception as lg_err:
                print(f"[TurnDelete] LangGraph RemoveMessage 失败（降级 whole-update）: {lg_err}")

        # 兜底：whole-update
        if kept_positions:
            state = await agent.aget_state(config)  # type: ignore[attr-defined]
            msgs: list = (state.values or {}).get("messages", []) if state else []
            system_msgs = [m for m in msgs if isinstance(m, SystemMessage)]
            new_msgs = [m for i, m in enumerate(msgs) if i in set(kept_positions)]
            if system_msgs:
                # 系统消息已在 msgs 中，不需要再前置
                pass
            try:
                await agent.aupdate_state(config, {"messages": new_msgs})
                print(f"[TurnDelete] LangGraph whole-update：ms {len(msgs)}→{len(new_msgs)}")
            except Exception as lg2_err:
                print(f"[TurnDelete] LangGraph whole-update 也失败（不致命）: {lg2_err}")
    except Exception as e:
        print(f"[TurnDelete] _langraph_apply_removal 异常（不致命）: {e}")


@app.delete("/api/sessions/{session_id}/turns/{turn_index}")
async def delete_session_turn(
    session_id: str,
    turn_index: int,
    role: Optional[str] = None,
    current: CurrentUser = _Depends(get_current_user),
):
    """删除某会话的单条/整轮消息（右键删除）。

    P0 升级：强制 JWT 登录 + current_user_id_must_match 双保险，不信任 query user_id。

    Args:
        session_id: 会话ID
        turn_index: 轮次序号（1-based）
        role: 可选 "user" / "assistant" / "all"（默认 "all"）
    """
    session = storage.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    # ===== P0 双保险：JWT 登录态 match session owner（owner/admin 豁免）=====
    current_user_id_must_match(current, session.get("user_id"))
    if not storage.verify_session_owner(session_id, current.user_id) and current.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail=f"无权修改会话 {session_id}")
    if turn_index < 1:
        raise HTTPException(status_code=400, detail="turn_index 必须 >= 1")

    role_val = (role or "all").lower()
    if role_val not in ("user", "assistant", "all"):
        raise HTTPException(status_code=400, detail="role 必须是 user / assistant / all")

    from agent.memory_manager import get_memory_manager
    mm = get_memory_manager()

    # 1. memory 层删除
    if role_val == "all":
        deleted = await mm.delete_turn(session_id, turn_index)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"未找到第 {turn_index} 轮对话")
        mem_result = "row_removed"
    else:
        mem_result = await mm.delete_message(session_id, turn_index, role_val)
        if mem_result == "not_found":
            raise HTTPException(status_code=404, detail=f"未找到第 {turn_index} 轮对话")

    # 2. LangGraph checkpointer 同步清理
    try:
        to_remove_ids, kept_positions = await _langraph_collect_remove_ids(
            session_id, [{"turn_index": turn_index, "role": role_val}],
        )
        await _langraph_apply_removal(session_id, to_remove_ids, kept_positions)
    except Exception as e:
        print(f"[TurnDelete] LangGraph 清理异常（不致命）: {e}")

    return {
        "status": "ok",
        "session_id": session_id,
        "deleted_turn": turn_index,
        "role": role_val,
        "result": mem_result,
    }


class BatchDeleteRequest(BaseModel):
    items: list  # [{"turn_index": int, "role": "user"|"assistant"|"all"}, ...]
    # P0 升级：保留字段兼容，实际以 JWT.current.user_id 为准
    user_id: Optional[str] = None


@app.post("/api/sessions/{session_id}/messages/batch-delete")
async def batch_delete_messages(session_id: str, req: BatchDeleteRequest,
                                current: CurrentUser = _Depends(get_current_user)):
    """批量删除多条消息（多选模式批量删除）。

    P0 升级：强制 JWT 登录 + 行级 owner 双保险，不信任 req.user_id。
    """
    session = storage.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    # ===== P0 双保险 =====
    current_user_id_must_match(current, session.get("user_id"))
    if not storage.verify_session_owner(session_id, current.user_id) and current.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail=f"无权修改会话 {session_id}")
    if not req.items:
        raise HTTPException(status_code=400, detail="items 不能为空")

    # 参数校验
    cleaned_items = []
    for it in req.items:
        ti = int(it.get("turn_index", 0))
        role = (it.get("role") or "all").lower()
        if ti < 1:
            raise HTTPException(status_code=400, detail=f"turn_index 必须 >= 1，收到 {ti}")
        if role not in ("user", "assistant", "all"):
            raise HTTPException(status_code=400, detail=f"role 必须是 user/assistant/all，收到 {role}")
        cleaned_items.append({"turn_index": ti, "role": role})

    from agent.memory_manager import get_memory_manager
    mm = get_memory_manager()

    # 1. memory 批量删除（内部按 turn_index 降序处理）
    result = await mm.batch_delete_messages(session_id, cleaned_items)

    # 2. LangGraph 一次性收集所有 msg.id 统一清理
    try:
        to_remove_ids, kept_positions = await _langraph_collect_remove_ids(
            session_id, cleaned_items,
        )
        await _langraph_apply_removal(session_id, to_remove_ids, kept_positions)
    except Exception as e:
        print(f"[BatchDelete] LangGraph 清理异常（不致命）: {e}")

    return {
        "status": "ok",
        "session_id": session_id,
        **result,
    }


# ======================== Layer4: 调度器状态 & Layer3: Trace 查询接口（P0：admin/owner 守卫） ========================

@app.get("/api/scheduler/status")
async def scheduler_status(current: CurrentUser = _Depends(get_current_user)):
    """查看定时调度器状态和下次运行时间（P0：仅 owner/admin 可访问运维端点）。"""
    _require_role_admin_or_owner(current)
    try:
        from agent.scheduler import get_scheduler
        s = get_scheduler()
        return {
            "running": s._running,
            "next_runs": s.get_next_run_times(),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/traces/{session_id}")
async def get_traces(session_id: str, limit: int = 10,
                     current: CurrentUser = _Depends(get_current_user)):
    """查询指定会话的 Trace 记录（可观测性：P0 行级 owner 校验，防止读他人 trace）。

    owner/admin 可访问任意会话的 trace。
    """
    session = storage.get_session(session_id)
    if session:
        current_user_id_must_match(current, session.get("user_id"))
    else:
        # 会话不存在时，运维角色允许直接走（不泄露）；普通 user = 404 语义上一致
        _require_role_admin_or_owner(current)
    try:
        from agent.trace import get_trace_logger
        tl = get_trace_logger()
        traces = await tl.get_recent_traces(session_id, limit)
        return {"session_id": session_id, "traces": traces}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/traces/{session_id}/latency")
async def get_latency_stats(session_id: str,
                            current: CurrentUser = _Depends(get_current_user)):
    """查询指定会话的延迟统计（avg/p50/p95）。P0：行级 owner 校验。"""
    session = storage.get_session(session_id)
    if session:
        current_user_id_must_match(current, session.get("user_id"))
    else:
        _require_role_admin_or_owner(current)
    try:
        from agent.trace import get_trace_logger
        tl = get_trace_logger()
        stats = await tl.get_latency_stats(session_id)
        return {"session_id": session_id, "stats": stats}
    except Exception as e:
        return {"error": str(e)}


# ======================== Layer3: SLO 监控 & 可靠性状态接口（P0：admin/owner 守卫） ========================

@app.get("/api/slo/status")
async def slo_status(current: CurrentUser = _Depends(get_current_user)):
    """SLO 监控状态端点。P0：仅 owner/admin 可访问。"""
    _require_role_admin_or_owner(current)
    try:
        # 先拿熔断器快照（异步并行，省时间）
        cb_task = asyncio.create_task(
            _circuit_breaker_actor.ask(CBMsg.SNAPSHOT_ALL, {})
            if _circuit_breaker_actor else asyncio.sleep(0, result={})
        )
        slo_snap = await _slo_monitor_actor.ask(SLOMsg.SNAPSHOT, {}) if _slo_monitor_actor else None
        cb_snap = await cb_task
        # 若 slo_snap 已经通过 SLOActor 自己填了 circuit_breakers（兼容旧逻辑）则不覆盖
        if slo_snap and isinstance(slo_snap, dict):
            if "circuit_breakers" not in slo_snap or not slo_snap["circuit_breakers"]:
                slo_snap["circuit_breakers"] = cb_snap or {}
            # SNAPSHOT 消息中通过 payload 传 cb_snapshost 给 Actor 也可以，这里做个兜底合并
            if _slo_monitor_actor is not None:
                slo_snap2 = await _slo_monitor_actor.ask(
                    SLOMsg.SNAPSHOT, {"circuit_breakers_snapshot": cb_snap or {}}
                )
                if slo_snap2:
                    return slo_snap2
        return slo_snap or {"error": "SLOMonitorActor 未初始化"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/circuit-breakers")
async def circuit_breaker_status(current: CurrentUser = _Depends(get_current_user)):
    """查询所有熔断器当前状态。P0：仅 owner/admin 可访问。"""
    _require_role_admin_or_owner(current)
    try:
        if _circuit_breaker_actor is None:
            return {"error": "CircuitBreakerActor 未初始化"}
        snaps = await _circuit_breaker_actor.ask(CBMsg.SNAPSHOT_ALL, {})
        return {"breakers": snaps or {}}
    except Exception as e:
        return {"error": str(e)}


@app.websocket("/ws/{thread_id}")
async def websocket_endpoint(websocket: WebSocket, thread_id: str):
    print(f"会话向我们发起了请求：{thread_id} 对应：{websocket}")
    """
    WebSocket 实时通讯核心接口 (P0 升级：握手后 accept 前先鉴权 close(4401))。

    目标：
    1. 建立长连接，实现服务端与前端的双向通信。
    2. 绑定 `thread_id`，实现会话级消息隔离。
    3. 维持心跳 (Keep-Alive)，防止连接超时。

    P0 鉴权：
      - 先从 query `?token=` 或 Authorization: Bearer 解析 JWT（get_current_user_websocket）。
      - 若为空 → close(code=4401, reason="Unauthorized: need valid JWT via ?token=")。
      - 若 session 已存在 → current_user_id_must_match（owner/admin 豁免），否则 close 4403。
      - 有效登录后用 current.user_id 登记到 bus/connection manager。
    """
    # =====================================================================
    # A3c: P0 升级 —— WS 握手后先鉴权，失败不 accept，直接 close 4401 / 4403
    # =====================================================================
    cu = await get_current_user_websocket(websocket)
    if cu is None:
        try:
            await websocket.close(code=4401, reason="Unauthorized: 需要有效的 JWT（URL ?token= 或 Authorization header）")
        except Exception:
            pass
        return
    # 行级校验（session 已存在时）：owner/admin 可进任意会话；普通 user/guest 仅能进入自己的会话
    sess = storage.get_session(thread_id)
    if sess and sess.get("user_id"):
        try:
            current_user_id_must_match(cu, sess["user_id"])
        except HTTPException:
            try:
                await websocket.close(code=4403, reason=f"Forbidden: 无权进入会话 {thread_id}")
            except Exception:
                pass
            return
    effective_user_id = (sess.get("user_id") if sess and sess.get("user_id") else cu.user_id)
    # ===== 登记连接：把 JWT current_user 写进 state（方便未来 bus 做权限） =====
    try:
        websocket.state.current_user_id = effective_user_id  # type: ignore[attr-defined]
        websocket.state.current_user_role = cu.role          # type: ignore[attr-defined]
    except Exception:
        pass

    # 1. [握手] 先 accept 才能建立真正 TCP 连接（ConnectionManagerActor 不负责 accept，只负责登记）
    try:
        await websocket.accept()
        # ===== Actor Model: 通过 ConnectionManagerActor 登记连接（不再用 manager.connect 直接改字典）=====
        if _conn_manager_actor is not None:
            await _conn_manager_actor.send(ConnMsg.CONNECT, {
                "thread_id": thread_id,
                "websocket": websocket,
                "user_id": effective_user_id,
                "role": cu.role,
            })
        else:
            # 兜底（Actor 未初始化时，兼容旧 manager.connect）
            await manager.connect(websocket, thread_id)

        # 2. [循环] 保持连接活跃
        while True:
            # 3. [监听] 接收前端消息 (通常是 ping 心跳)
            try:
                data = await websocket.receive_text()
            except WebSocketDisconnect:
                break
            except Exception:
                break

            # 4. [响应] 回复 pong 消息
            try:
                await websocket.send_json({
                    "type": "pong",
                    "message": f"服务端已收到: {data}"
                })
            except Exception:
                break

    except WebSocketDisconnect:
        pass
    except Exception as e:
        if "disconnect" not in str(e).lower() and "1005" not in str(e) and "1006" not in str(e):
            print(f"[WebSocket] 连接异常: {e}")

    finally:
        # ===== 关键顺序（来自并发竞态教训）：先置位取消 → 再从管理器移除 =====
        # 若先移除再取消 → 时间窗内"发送端判断连接存在→随后被移除→继续发送"导致异常；
        # 正确顺序：
        #   (1) cancel_by_thread_id：CancellationToken 置位 → 任何正在执行的代码
        #       下一次 check_cancelled() 立即抛异常（毫秒级），同时子任务被级联 cancel；
        #   (2) STOP_AND_REMOVE_TASK：asyncio.Task.cancel()（异步 await 点生效）；
        #   (3) 最后才从 ConnectionManager 中移除 WebSocket 登记（不会再有新发送者）。
        try:
            # (1) 令牌级取消
            await cancel_by_thread_id(thread_id, "websocket_disconnected")
            # (2) asyncio.Task 级取消（SessionRegistryActor 串行保证原子性）
            if _session_actor is not None:
                await _session_actor.send(SRMsg.STOP_AND_REMOVE_TASK, {"thread_id": thread_id})
        except Exception:
            pass
        # (3) 最后才从连接管理器移除
        try:
            if _conn_manager_actor is not None:
                await _conn_manager_actor.send(ConnMsg.DISCONNECT, {
                    "thread_id": thread_id,
                    "websocket": websocket,
                })
            else:
                manager.disconnect(websocket, thread_id)
        except Exception:
            pass


# ======================== 前端静态资源 ========================
# 挂载 static 目录，提供 index.html 及其静态资源
static_dir = project_root / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    """根路由返回聊天前端首页。"""
    index_file = static_dir / "index.html"
    if not index_file.exists():
        return HTMLResponse("<h1>前端文件未生成</h1><p>请先创建 static/index.html</p>", status_code=404)
    resp = HTMLResponse(index_file.read_text(encoding="utf-8"))
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return resp


def kill_port(port: int):
    """启动前清理占用指定端口的残留进程，避免多进程抢端口导致卡死"""
    import subprocess
    import os
    current_pid = str(os.getpid())
    try:
        result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True, timeout=5
        )
        pids = set()
        for line in result.stdout.splitlines():
            # 匹配形如 "TCP 0.0.0.0:8000 ... LISTENING 12345" 的行
            if f":{port}" in line and "LISTENING" in line:
                parts = line.split()
                pid = parts[-1]
                if pid != current_pid and pid != "0":
                    pids.add(pid)
        for pid in pids:
            subprocess.run(["taskkill", "/PID", pid, "/F"], timeout=5)
            print(f"[启动] 已清理占用端口 {port} 的残留进程: PID {pid}")
    except FileNotFoundError:
        # 非 Windows 环境无 netstat/taskkill，跳过
        pass
    except Exception as e:
        print(f"[启动] 端口清理检查跳过: {e}")


if __name__ == "__main__":
    kill_port(8000)
    # 安全策略：配置了 NGROK_AUTHTOKEN 且 pyngrok 可用时。
    _env_ngrok = os.getenv("NGROK_AUTHTOKEN", "").strip()
    _pyngrok_available = False
    if _env_ngrok:
        try:
            import pyngrok
            _pyngrok_available = True
        except (ImportError, ModuleNotFoundError):
            _pyngrok_available = False
    has_ngrok = bool(_env_ngrok and _pyngrok_available)
    bind_host = "127.0.0.1" if has_ngrok else "0.0.0.0"
    if has_ngrok:
        print("[安全] 检测到 NGROK_AUTHTOKEN + pyngrok 可用，server 绑定 127.0.0.1，真实 IP 已隐藏")
    elif _env_ngrok and not _pyngrok_available:
        print("[安全] 检测到 NGROK_AUTHTOKEN 但 pyngrok 未安装，server 绑定 0.0.0.0（局域网可访问）")
        print("[安全] 安装 pyngrok 后将自动改为 127.0.0.1 + ngrok 公网隧道")
    else:
        print("[安全] 未配置 NGROK_AUTHTOKEN，server 绑定 0.0.0.0（局域网可访问）")
    # reload=False：--reload 模式下 worker 子进程 stdout 走管道到 reloader，会导致 print 日志被缓冲不可见
    # 注册日志过滤器：屏蔽 WebSocketDisconnect（客户端正常断开）的 traceback 噪音
    ws_filter = _WSDisconnectFilter()
    logging.getLogger("uvicorn.error").addFilter(ws_filter)
    logging.getLogger("uvicorn.access").addFilter(ws_filter)
    logging.getLogger("uvicorn.asgi").addFilter(ws_filter)
    # 同时拦截 asyncio 未处理异常中的 WebSocketDisconnect
    _orig_excepthook = asyncio.get_event_loop().get_exception_handler() if asyncio.get_event_loop() else None
    def _asyncio_excepthook(loop, context):
        exc = context.get("exception")
        if exc and "WebSocketDisconnect" in type(exc).__name__:
            return  # 静默处理
        if _orig_excepthook:
            _orig_excepthook(loop, context)
        else:
            loop.default_exception_handler(context)
    try:
        asyncio.get_event_loop().set_exception_handler(_asyncio_excepthook)
    except Exception:
        pass
    uvicorn.run(app, host=bind_host, port=8000)