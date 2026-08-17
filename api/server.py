#coding = utf-8
import sys
import os
# 强制 stdout/stderr 行缓冲，确保 uvicorn --reload 模式下 print 日志实时输出
# reconfigure 是 Python 3.7+ TextIOWrapper 的方法，但类型存根 TextIO 未声明，故用 type: ignore
try:
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]
    sys.stderr.reconfigure(line_buffering=True)  # type: ignore[attr-defined]
except AttributeError:
    pass
import uuid
import asyncio

# 加载 .env 环境变量（find_dotenv 确保从项目根目录查找）
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

#异步输入输出,编写异步并发代码，让程序在等待耗时任务时能转而执行其他任务，实现单线程高并发效果
#事件循环（Event Loop）：asyncio 的核心调度器，负责管理协程和任务、监听 IO 事件，按优先级调度可执行任务 。
#协程（Coroutine）：最小执行单元，通过 async def 定义，可暂停可恢复，执行到异步操作时暂停并交还控制权给事件循环 。
#任务（Task）：协程对象的封装，用于将协程加入事件循环调度，支持查看执行状态和获取结果 。
#Future：表示异步操作结果的底层抽象类，用于存储未完成任务的最终结果，Task 是其子类 。
# asyncio.run()：异步程序入口，运行主协程并自动管理事件循环的创建和关闭 。示例：asyncio.run(main_coro)
# asyncio.create_task()：将协程封装为任务，立即加入事件循环调度实现多协程并发 。示例：task = asyncio.create_task(coro)
# asyncio.gather()：批量调度多个任务，等待所有完成后按顺序返回结果列表 。示例：results = await asyncio.gather(task1, task2, task3)
# asyncio.sleep()：模拟异步耗时操作，不阻塞事件循环，期间可调度其他任务 。示例：await asyncio.sleep(2)
# asyncio.wait_for()：为异步任务设置超时时间，超时未完成则抛出TimeoutError异常 。示例：result = await asyncio.wait_for(task, timeout=3)
# 适合使用：IO密集型操作（网络请求、文件读写、数据库查询等），在等待IO完成时可切换执行其他任务，大幅提升效率 。
# 不适合使用：CPU密集型计算任务，协程切换反而会增加开销降低性能，应使用多进程或多线程 。
# Python 3.11+进阶：支持TaskGroup结构化并发，可安全管理任务生命周期并自动处理异常 。
# 多线程结合：处理CPU 密集型任务时可通过 asyncio.to_thread() 将任务交给线程池执行，避免阻塞事件循环 。
import uvicorn
import logging
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
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
from starlette.responses import Response
from pydantic import BaseModel#负责定义请求体（Request Body）的结构和响应（Response）的格式
from typing import List, Optional, Dict
from collections import defaultdict, deque
from datetime import datetime, timedelta
import shutil

# Add project root to sys.path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
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
)

# 默认超时：Agent 主流程 180s，后台分析 300s（知识星球抓取+分析较耗时）
_DEFAULT_AGENT_TIMEOUT: float = DEFAULT_AGENT_TIMEOUT_SEC
_DEFAULT_BG_TIMEOUT: float = DEFAULT_BACKGROUND_TIMEOUT_SEC


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

# Import agent runner and monitor
# 注意：agent.main_agent 导入时会初始化 main_agent，这可能需要几秒钟
from agent.main_agent import run_deep_agent, get_session_history
from agent.prompts import format_prompt
from api.monitor import manager
from api import storage

# 挂载输出目录，以便前端访问生成的静态文件
# 假设输出目录位于项目根目录下的 output
output_dir = project_root / "output"
output_dir.mkdir(exist_ok=True)

# 定义上传目录 updated
updated_dir = project_root / "updated"
updated_dir.mkdir(exist_ok=True)

#POST：发号施令，创建新东西（服务器说了算，URL 谁也不知道）。
# PUT：对号入座，整体替换（客户端指定位置，全量覆盖）。
# PATCH：修修补补，只改局部（只带变化的字段）。
# DELETE：斩草除根，彻底移除（指定路径，干净利落）。
class TaskRequest(BaseModel):
    query: str
    thread_id: Optional[str] = None
    user_id: Optional[str] = None


class UserRequest(BaseModel):
    user_id: str
    display_name: Optional[str] = None


class SessionRequest(BaseModel):
    title: Optional[str] = None
# BaseModel 提供了三大核心价值，让你少写大量 if/else 校验代码：
# 自动类型校验：如果前端传的 age 是字符串 "abc" 而模型定义是 int，FastAPI 会自动返回 422 错误（Unprocessable Entity）并提示错误原因。
# 数据转换（反序列化）：自动将前端传过来的 JSON 字符串，转换成 Python 对象（TaskRequest 实例）。
# 数据导出（序列化）：可以轻松将 Python 对象转回字典（model_dump()）或 JSON 字符串（model_dump_json()），用于返回给前端。


# ================================================================
# 盘前新闻 prompt 预处理，在 server 侧识别该按钮请求后，用 Python datetime(timezone+8) 直接算好当前北京时间、
# 星期、搜索时间范围，替换掉 prompt 中的日期查询段，拼为"【系统已注入，无需联网查日期】"
# 前缀。"复盘预测"路径中的盘前新闻 prompt 也复用此逻辑，消除双逻辑漂移。
_PREMARKET_SHORTCUT_SIGNATURE: tuple = (
    "韭研社区", "炒股吧", "同花顺股吧", "东方财富股吧",
    "利好还是利空的逻辑A股概念板块", "分别给出利好和利空个股清单",
)


def _is_premarket_shortcut_query(query: str) -> bool:
    """判断 query 是否来自前端盘前新闻快捷按钮。"""
    if not query:
        return False
    hits = sum(1 for kw in _PREMARKET_SHORTCUT_SIGNATURE if kw in query)
    return hits >= 2


def _rewrite_premarket_query_if_shortcut(query: str) -> tuple[str, bool]:
    """若 query 来自盘前新闻快捷按钮，则：
    1) 获取当前的北京时间 
    2) 剥离 prompt 中"查询今天是周几"的日期请求段，替换为系统已注入前缀
    3) 返回 (rewritten_query, was_rewritten)
    """
    if not _is_premarket_shortcut_query(query):
        return query, False

    from datetime import timezone, timedelta
    _tz_bj = timezone(timedelta(hours=8))
    _now_bj = datetime.now(_tz_bj)
    _weekday_cn = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][_now_bj.weekday()]
    _current_time_str = _now_bj.strftime("%Y年%m月%d日 %H:%M")
    _hour = _now_bj.hour
    _wd = _now_bj.weekday()  # 0=周一 5=周六 6=周日

    # 搜索时间范围：周一早上10点前 / 周末 → 从周五15:00 起；其他 → 昨天15:00 起
    if _wd == 0 and _hour < 10 or _wd in (5, 6):
        _delta_days = (_wd - 4) if _wd >= 5 else 7  # 周六:1, 周日:2, 周一:7
        _last_fri = _now_bj - timedelta(days=_delta_days)
        _search_range_hint = f"{_last_fri.strftime('%m月%d日')} 15:00 至 现在"
    else:
        _yesterday = _now_bj - timedelta(days=1)
        _search_range_hint = f"{_yesterday.strftime('%m月%d日')} 15:00 至 现在"

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
        f"【系统已注入当前北京时间 · 严禁联网或单独输出时间确认段落】\n"
        f"· 当前北京时间：{_current_time_str}（{_weekday_cn}）\n"
        f"· 搜索时间范围：{_search_range_hint}\n"
        f"【硬性约束】禁止输出以下内容（出现即失败）：\n"
        f"  1) '今天是周几/当前日期/北京时间'的任何形式验证段或多源交叉验证表格；\n"
        f"  2) 对 time.is / 时间校准网 / timeanddate / time.org.cn 等时间类网站的引用；\n"
        f"  3) '时刻存在小幅偏差 / ±N 分钟不确定性'等时间精度讨论；\n"
        f"  4) 单独的【当前北京时间】或【查询结果】章节。\n"
        f"请立即跳过时间查询，直接执行用户实际任务：\n"
    )
    final_query = injected_prefix + (cleaned or query)
    return final_query, True


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---------- 启动逻辑 ----------
    loop = asyncio.get_running_loop()
    manager.set_loop(loop)
    print(f"[Server] WebSocket Manager bound to loop: {id(loop)}")

    # ===== Actor Model: 注册 + 启动所有 Actor =====
    global _session_actor, _circuit_breaker_actor, _conn_manager_actor, _slo_monitor_actor
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


# ===== 安全中间件：安全响应头 + 请求速率限制 =====
class SecurityMiddleware(BaseHTTPMiddleware):
    """
    1. 添加安全响应头，防止 XSS / 点击劫持 / MIME 嗅探等攻击
    2. 基于 IP 的请求速率限制，防止暴力刷接口和 DDoS
    """
    # 速率限制：每个 IP 每分钟最多 60 次请求（WebSocket 和静态资源除外）
    RATE_LIMIT = RATE_LIMIT_PER_MINUTE
    RATE_WINDOW = RATE_LIMIT_WINDOW_SEC  # 秒
    _ip_records: Dict[str, deque] = defaultdict(deque)

    async def dispatch(self, request: Request, call_next):
        # 1. 请求速率限制（跳过 WebSocket 和静态资源）
        client_ip = request.client.host if request.client else "unknown"
        path = request.url.path
        if not path.startswith("/ws/") and not path.startswith("/static/"):
            now = datetime.now()
            records = self._ip_records[client_ip]
            # 清理过期记录
            while records and records[0] < now - timedelta(seconds=self.RATE_WINDOW):
                records.popleft()
            if len(records) >= self.RATE_LIMIT:
                return Response(
                    content='{"error":"请求过于频繁，请稍后再试"}',
                    status_code=HTTP_CODE_TOO_MANY_REQUESTS,
                    media_type="application/json",
                    headers={"Retry-After": str(self.RATE_WINDOW)}
                )
            records.append(now)

        # 2. 处理请求
        response = await call_next(request)

        # 3. 添加安全响应头
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["X-Powered-By"] = "MOSS-Finance-Assistant"  # 隐藏真实服务器信息
        return response


app.add_middleware(SecurityMiddleware)

# ======================================================================
# 按 thread_id 跟踪正在运行的 Agent 任务（改为 Actor 驱动，不再用全局可变字典）
# ======================================================================
# 迁移说明：原 _active_agent_tasks / _active_background_tasks / _background_tasks
# 全部收敛到 SessionRegistryActor 私有状态中。外部只能通过发消息与 Actor 交互，
# 状态修改只能发生在 Actor.handle_message 内（next_state = f(current_state, input)）。


@app.post("/api/task")
async def run_task(request: TaskRequest):
    # 1. [ID 初始化]
    thread_id = request.thread_id or str(uuid.uuid4())
    # 若提供了 user_id：自动注册用户 + 确保会话记录存在（用前端传入的 thread_id）
    if request.user_id:
        storage.get_or_create_user(request.user_id)
        if not storage.get_session(thread_id):
            import datetime as _dt
            now = _dt.datetime.now().isoformat(timespec="seconds")
            # 新建会话时用占位标题，首条消息后由 main_agent 自动生成 user_id+关键词+日期
            storage._get_conn().execute(
                "INSERT OR IGNORE INTO sessions (session_id, user_id, title, created_at, updated_at) VALUES (?,?,?,?,?)",
                (thread_id, request.user_id, "新会话", now, now),
            )
            storage._get_conn().commit()

    # 2. [后台执行] 异步运行 Agent，不阻塞主线程
    # ===== Actor Model: 通过 SessionRegistryActor 注册任务（串行保证原子性）=====
    #   - Actor 内部会先 cancel 同会话的旧任务再注册新的
    #   - 状态修改完全在 Actor 邮箱中：REGISTER_AGENT_TASK 消息处理是原子的
    # ===== CancellationToken: 用 _run_with_ctx 包裹，创建三位一体上下文 =====
    #   - 默认 180s 超时；check_cancelled() 在任何调用深度主动检查
    #   - WebSocket 断开 / STOP 接口 → cancel_by_thread_id 毫秒级取消
    # ===== Prompt 重写：盘前新闻快捷按钮专用预处理 =====
    #   - 检测到是盘前新闻按钮 prompt → 用 Python 注入北京时间/星期/搜索范围，
    #     彻底移除原 prompt 中"1）查询今天是周几"的联网日期查询段
    _raw_query = request.query or ""
    _effective_query = _raw_query
    _is_shortcut_btn = _is_premarket_shortcut_query(_raw_query)
    if _is_shortcut_btn:
        _effective_query, _rewrote = _rewrite_premarket_query_if_shortcut(_raw_query)
        if _rewrote:
            print(f"[Premarket] prompt 已注入系统时间，剥除联网查询日期段 (thread={thread_id})")
    task = asyncio.create_task(
        _run_with_ctx(
            thread_id,
            request.user_id,
            None,
            _DEFAULT_AGENT_TIMEOUT,
            run_deep_agent,
            _effective_query,
            thread_id,
            request.user_id,
            quiet=_is_shortcut_btn,
        )
    )
    # 登记：让 CancellationToken 在被取消时，也把对应 asyncio.Task 一起 cancel（双重保险）
    # 注意：token 在 _run_with_ctx 内部创建并绑定到 ContextVar，这里在外层无法拿到。
    # 解决：在 token.cancel 回调中 → cancel(task) 由 child_task 机制完成，
    # 而 task.cancel 不会反向触发 token.cancel（但 run_deep_agent 里有 except CancelledError）。
    # 故我们添加反向挂钩：利用 register_callback 的方式——在 _run_with_ctx 内部通过 current_context
    # 调 token.register_child_task(current_running_task) 即可。为避免重复逻辑，此处简单保持：
    # 外部 task.cancel() 与内部 token.cancel() 两者任一即生效（幂等）。
    sa = _session_actor
    assert sa is not None, "SessionRegistryActor 未在 lifespan 中初始化"
    # 先注册（Actor 内部 cancel 旧的）
    await sa.send(SRMsg.REGISTER_AGENT_TASK, {
        "thread_id": thread_id,
        "task": task,
    })
    # 完成回调：发消息通知 Actor"若我仍是当前任务则清掉"
    # 回调可能运行在任意时刻，但 send() 是线程安全地往邮箱投递 → Actor 内部串行处理
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
                # 事件循环已停，Actor 也 stop 了；无需动作
                pass
        except Exception:
            pass
    task.add_done_callback(_on_done)

    # 3. [立即响应]
    return {"status": "started", "thread_id": thread_id, "user_id": request.user_id}


@app.post("/api/task/stop")
async def stop_task(request: Request):
    """停止指定会话当前正在执行的 Agent 任务。
    执行顺序（毫秒级取消链路）：
      1) cancel_by_thread_id → CancellationToken 原子置位 →
         级联：子任务取消 + 回调执行 + 下一次 check_cancelled() 立即抛异常；
      2) STOP_AND_REMOVE_TASK → asyncio.Task.cancel() + 从 Actor 注册表移除。
    双管齐下：同步代码段的取消由 (1) 在 check_cancelled() 点拦截，
    异步 await 点的取消由 (2) 通过 CancelledError 拦截。
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


# ======================== 盘前小作文热度分析接口 ========================

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


class ZsxqAnalysisRequest(BaseModel):
    thread_id: str
    user_id: Optional[str] = None


def _find_latest_today_txt(news_dir: Path, today_prefix: str):
    """查找当天最新的 txt 总结文件（文件名以 YYYYMMDD 开头，精确到秒命名）"""
    candidates = sorted(
        [f for f in news_dir.glob(f"{today_prefix}*.txt") if f.is_file()],
        key=lambda f: f.name,
        reverse=True,
    )
    return candidates[0] if candidates else None


async def _save_zsxq_to_history(thread_id: str, txt_content: str):
    """把盘前小作文热度的用户消息和结果存入会话历史（checkpointer），刷新后可恢复。
    同时写入 Context Engineering 记忆管理，供后续摘要压缩和关键决策检索。"""
    try:
        from langchain_core.messages import HumanMessage, AIMessage
        from agent.main_agent import get_main_agent
        agent = await get_main_agent()
        config = {"configurable": {"thread_id": thread_id}}
        await agent.aupdate_state(config, {"messages": [  # type: ignore[attr-defined]
            HumanMessage(content="盘前小作文热度"),
            AIMessage(content=txt_content),
        ]})
        # 同步写入记忆管理（该条为高优关键决策）
        try:
            from agent.memory_manager import get_memory_manager
            mm = get_memory_manager()
            await mm.add_turn(thread_id, "盘前小作文热度分析", txt_content)
        except Exception as mm_err:
            print(f"[ZSXQ分析] 写入记忆管理失败（不致命）: {mm_err}")
    except Exception as e:
        print(f"[ZSXQ分析] 保存会话历史失败: {e}")


async def _probe_ollama(base_url: str = "http://localhost:11434", timeout: float = 3.0) -> bool:
    """快速探测 Ollama 服务是否在线（GET /api/tags）。"""
    import urllib.request
    import urllib.error
    req = urllib.request.Request(f"{base_url}/api/tags", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()
        return True
    except Exception:
        return False


def _find_ollama_exe() -> str | None:
    """定位 ollama 可执行文件的绝对路径。

    优先顺序：
      1. 从 PATH 中查找（shutil.which / Get-Command 思路的纯 Python 实现）
      2. Windows 常见用户级安装目录兜底：
         - %LOCALAPPDATA%\\Programs\\Ollama\\ollama.exe
         - %PROGRAMFILES%\\Ollama\\ollama.exe
      3. macOS/Linux: /usr/local/bin/ollama / usr/bin/ollama
    失败返回 None。
    """
    import shutil
    exe = shutil.which("ollama")
    if exe:
        return exe
    candidates = []
    if sys.platform.startswith("win"):
        localapp = os.environ.get("LOCALAPPDATA")
        progfiles = os.environ.get("PROGRAMFILES")
        if localapp:
            candidates.append(os.path.join(localapp, "Programs", "Ollama", "ollama.exe"))
        if progfiles:
            candidates.append(os.path.join(progfiles, "Ollama", "ollama.exe"))
    else:
        candidates.extend(["/usr/local/bin/ollama", "/usr/bin/ollama"])
    for c in candidates:
        if os.path.isfile(c):
            return c
    return None


async def _ollama_models_list(ollama_exe: str, timeout: float = OLLAMA_MODELS_LIST_TIMEOUT_SEC) -> list[str]:
    """通过 `ollama list` CLI 获取已安装模型名列表。失败返回空列表。"""
    try:
        proc = await asyncio.create_subprocess_exec(
            ollama_exe, "list",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except Exception as e:
        print(f"[Ollama] ollama list 调用失败: {e}")
        return []
    try:
        stdout_bytes, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        return []
    if proc.returncode != 0:
        return []
    text = stdout_bytes.decode("utf-8", errors="replace")
    names: list[str] = []
    for line in text.splitlines():
        if not line or line.lower().startswith("name"):
            continue  # 跳过表头/空行
        parts = line.split()
        if parts:
            names.append(parts[0])
    return names


async def _ensure_ollama_ready(
    model: str = "qwen3:8b",
    *,
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
    emit_progress,  # callable(msg) -> None，推到前端 tool_start
) -> tuple[bool, str]:
    """确保 Ollama 服务在线且指定模型已拉取。

    顺序：
      1. 定位 ollama CLI → 找不到直接返回 False + 明确提示
      2. 探测服务端口 → 未启动则后台拉起 `ollama serve`，最多等 30s
      3. `ollama list` 检查模型是否已安装 → 未安装则 `ollama pull <model>`

    返回 (ok, message)：ok=False 时 message 是可直接推到前端的错误说明。
    """
    # ---------- 阶段 1：定位 CLI ----------
    ollama_exe = _find_ollama_exe()
    if not ollama_exe:
        hint = (
            "未找到 ollama 可执行文件。请先安装 Ollama：https://ollama.com/download 。"
            "安装完成后，若仍提示此错误，请关闭并重新打开本程序（刷新 PATH），"
            "或将安装目录加入系统环境变量 PATH。"
        )
        emit_progress(f"❌ {hint}")
        return False, hint

    # ---------- 阶段 2：服务启动 ----------
    if not await _probe_ollama(base_url, timeout=OLLAMA_PROBE_TIMEOUT_SEC):
        emit_progress(f"🔧 Ollama 服务未运行，正在后台启动 `{ollama_exe} serve` …")
        print(f"[Ollama] 自动后台启动服务: {ollama_exe} serve")
        try:
            # Windows 下用 DETACHED_PROCESS / CREATE_NO_WINDOW 让 ollama serve 完全脱离父进程
            import subprocess as _sp
            kwargs: dict = {
                "stdout": _sp.DEVNULL,
                "stderr": _sp.DEVNULL,
                "stdin": _sp.DEVNULL,
                "close_fds": True,
            }
            if sys.platform.startswith("win"):
                # CREATE_NO_WINDOW = 0x08000000，避免弹黑框
                kwargs["creationflags"] = 0x08000000
            _sp.Popen([ollama_exe, "serve"], **kwargs)
        except Exception as e:
            msg = f"❌ 无法启动 Ollama 服务: {e}"
            emit_progress(msg)
            return False, msg

        # 轮询等待服务就绪
        ready = False
        import asyncio as _aio
        for i in range(OLLAMA_LAUNCH_POLL_MAX_ROUNDS):
            await _aio.sleep(OLLAMA_LAUNCH_POLL_INTERVAL_SEC)
            if await _probe_ollama(base_url, timeout=OLLAMA_PROBE_TIMEOUT_SEC):
                ready = True
                break
            if i % 5 == 4:
                emit_progress(f"⏳ Ollama 服务启动中…已等待 {i + 1} 秒")
        if not ready:
            msg = (
                f"❌ Ollama 服务在 {OLLAMA_LAUNCH_POLL_MAX_ROUNDS} 秒内未就绪，可能是首次启动较慢或被防火墙拦截。"
                "请手动在终端执行 `ollama serve` 后重试。"
            )
            emit_progress(msg)
            return False, msg
        emit_progress("✅ Ollama 服务已就绪")
    else:
        emit_progress("✅ Ollama 服务已运行")

    # ---------- 阶段 3：模型检查 ----------
    installed = await _ollama_models_list(ollama_exe)
    # 支持部分匹配（用户 tag 写 qwen3:8b 时，registry 返回 qwen3:8b 或带数字 hash 前缀都算命中）
    has_model = any(name == model or name.split(":")[0] == model.split(":")[0] for name in installed)
    if not has_model:
        emit_progress(f"📥 尚未拉取模型 `{model}`，正在后台下载并解压（~5GB，首次可能 10-30 分钟，请耐心等待）…")
        print(f"[Ollama] 开始拉取模型: {ollama_exe} pull {model}")
        try:
            proc = await asyncio.create_subprocess_exec(
                ollama_exe, "pull", model,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except Exception as e:
            msg = f"❌ 无法调用 ollama pull: {e}"
            emit_progress(msg)
            return False, msg
        # pull 非常慢，给宽限；每 20s 推一次心跳避免前端误以为卡住
        import asyncio as _aio2
        pull_start = _aio2.get_event_loop().time()
        done: bool = False
        rc: int | None = None
        last_lines: list[str] = []

        async def _reader():
            nonlocal last_lines
            if proc.stdout is None:
                return
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                t = line.decode("utf-8", errors="replace").strip()
                if not t:
                    continue
                last_lines.append(t)
                if len(last_lines) > OLLAMA_PULL_LOG_TAIL_KEEP:
                    last_lines = last_lines[-OLLAMA_PULL_LOG_TAIL_KEEP:]

        reader_task = _aio2.create_task(_reader())
        try:
            while True:
                await _aio2.sleep(OLLAMA_PULL_PROGRESS_INTERVAL_SEC)
                if proc.returncode is not None:
                    break
                elapsed = int(_aio2.get_event_loop().time() - pull_start)
                last = last_lines[-1] if last_lines else "下载中…"
                # 把 pull 输出里的最后一行截断后推前端（通常是百分比进度）
                if len(last) > OLLAMA_PULL_PROGRESS_LINE_MAX_CHARS:
                    last = last[:OLLAMA_PULL_PROGRESS_LINE_MAX_CHARS] + "…"
                emit_progress(f"📥 模型下载中（已等 {elapsed}s）：{last}")
                if elapsed > OLLAMA_PULL_HARD_TIMEOUT_SEC:  # 60 分钟硬上限
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    break
            try:
                rc = proc.returncode
            except Exception:
                rc = None
            done = True
        finally:
            reader_task.cancel()
        if rc != 0:
            tail = "\n".join(last_lines[-5:])
            msg = (
                f"❌ 模型 `{model}` 拉取失败（退出码 {rc}）。\n"
                f"最近输出：{tail}\n"
                f"请手动在终端执行 `ollama pull {model}` 后重试；"
                f"若网络较慢，可考虑设置镜像源后再拉取。"
            )
            emit_progress(msg)
            return False, msg
        emit_progress(f"✅ 模型 `{model}` 已就绪")
    else:
        emit_progress(f"✅ 模型 `{model}` 已安装")

    return True, "ok"


async def _run_zsxq_analysis(thread_id: str, emit_to_frontend: bool = True) -> str:
    """
    执行盘前小作文热度分析：
    - 若当天已有 txt 总结，直接复用，跳过抓取
    - 否则运行 test_zsxq.py 完整流程（抓取 + LLM 分析）
    完成后通过 WebSocket 将 txt 总结推送到前端对话区。

    参数：
        emit_to_frontend: 是否将结果推送到前端。复盘预测场景下设为 False，
                         只拿结果不推送，避免中间结果污染对话区。
    返回：txt 总结内容（失败时返回空字符串）
    """
    from api.context import set_thread_context, reset_session_context
    from api.monitor import monitor

    thread_token = set_thread_context(thread_id)
    news_dir = project_root / "zsxq_news"
    today_prefix = datetime.now().strftime("%Y%m%d")

    try:
        # 1. 优先复用当天已有总结，跳过抓取
        latest_txt = _find_latest_today_txt(news_dir, today_prefix)
        if latest_txt:
            monitor._emit("tool_start", f"检测到当日已有总结：{latest_txt.name}，跳过抓取直接返回")
            txt_content = latest_txt.read_text(encoding="utf-8")
            if txt_content.strip():
                if emit_to_frontend:
                    monitor.report_task_result(txt_content)
                await _save_zsxq_to_history(thread_id, txt_content)
                return txt_content
            # 内容为空则继续走抓取流程

        # 2. 没有则运行 test_zsxq.py（Playwright sync API 不能在 async 上下文运行）
        monitor.report_thinking("盘前小作文热度分析")
        script_path = project_root / "test_zsxq.py"

        # 2a. Ollama 自动准备：定位 CLI → 启动服务 → 检查/拉取模型
        # 每一步都向前端推进度，失败时给出可操作提示而不是笼统 "分析失败"
        from functools import partial
        emit_step = partial(monitor._emit, "tool_start")
        ok, err_msg = await _ensure_ollama_ready("qwen3:8b", emit_progress=emit_step)
        if not ok:
            print(f"[ZSXQ分析] Ollama 准备失败: {err_msg}")
            monitor.report_error(err_msg)
            return ""

        monitor._emit("tool_start", "开始抓取知识星球并调用 Ollama 分析", {"script": str(script_path)})

        # 2b. 传 --quiet 抑制原始模型输出/巨大 JSON 转储，避免前端视觉空白
        process = await asyncio.create_subprocess_exec(
            sys.executable, str(script_path), "--quiet",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(project_root),
        )

        # 3. 实时读取输出并推送进度（避免长时间无响应）
        # 【重要】推送策略分层（避免大段空白）：
        #   - 只把真正的"阶段里程碑"通过 tool_start 推前端（会落成独立气泡）
        #   - 中间细碎进度（抓取第 N 条、滚动、API 请求等）全部合并到顶部 thinking 文本
        #     （report_thinking → 只更新 data-base，不落新气泡）
        #   - [ZSXQ]/[ZSXQ-Search] 内部调试行：即便子进程没 --quiet 或 wrapper 失效，
        #     这里也二次过滤，只作为 thinking 更新文本
        stdout_lines = []
        try:
            if process.stdout is None:
                raise RuntimeError("子进程 stdout 未就绪")
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                stdout_lines.append(text)
                stripped = text.strip()
                if not stripped:
                    continue

                # 分类判断：里程碑 / 中间进度 / 跳过
                # ---- 里程碑（作为 tool_start 推，落成独立 event 更新顶部气泡）----
                milestone: str | None = None
                if "⚠ Ollama 调用失败" in stripped or "⚠ Ollama 预检异常" in stripped \
                        or "⚠ 分析过程出错" in stripped:
                    milestone = stripped
                elif stripped.startswith("[分析结果]"):
                    milestone = stripped
                elif stripped.startswith("═══ 抓取完成") or "════════════" in stripped and "抓取完成" in stripped:
                    milestone = stripped

                # ---- 跳过：巨大 JSON 转储 / 纯分隔线（非里程碑的 60+ ============== 线）----
                if "最终返回" in stripped and len(stripped) > SERVER_FINAL_RETURN_LINE_MIN_LEN:
                    continue
                if len(stripped) > SERVER_JSON_DEBUG_LINE_MIN_LEN and (stripped.lstrip()[0] in '{[' if stripped.lstrip() else False):
                    continue

                # ---- 截断过长行 ----
                safe_text = stripped if len(stripped) <= SERVER_PROGRESS_SAFE_TRUNCATE_LEN else stripped[:SERVER_PROGRESS_SAFE_TRUNCATE_LEN] + "…"

                if milestone:
                    monitor._emit("tool_start", safe_text)
                else:
                    # 中间进度：[分析] / [抓取] / [ZSXQ] 的调试/进度，
                    # 合并到顶部 thinking 文本，不落成独立气泡 → 无空白叠加
                    if any(kw in stripped for kw in ("[抓取]", "[分析]", "[ZSXQ]", "分析结果", "总结已保存")):
                        # report_thinking 如果已有 thinkingEl，则只改 data-base
                        # （前端 tool_start 也会走 thinkingEl 更新路径）
                        # 这里用 monitor._emit 包一层 thinking 消息，沿用 tool_start 对 thinkingEl 的复用
                        monitor.report_thinking("盘前小作文热度：" + safe_text)
        except Exception as e:
            print(f"[ZSXQ分析] 读取脚本输出异常: {e}")

        await process.wait()

        if process.returncode != 0:
            tail = "\n".join(stdout_lines[-SERVER_OUTPUT_MAX_STDOUT_TAIL_LINES:])
            print(f"[ZSXQ分析] test_zsxq.py 执行失败（退出码 {process.returncode}）\n{tail}")
            # 错误分类：识别 Ollama 未启动等具体错误，给出可操作提示
            combined = tail
            if "WinError 10061" in combined or "Ollama 连接失败" in combined or "Ollama 服务未启动" in combined:
                monitor.report_error(
                    "Ollama 服务未启动，请在终端运行 `ollama serve` 并拉取模型 `ollama pull qwen3:8b` 后重试。"
                )
            elif "未找到" in combined or "FileNotFoundError" in combined:
                monitor.report_error("分析脚本或依赖未找到，请联系管理员")
            else:
                monitor.report_error("分析失败，请稍后重试")
            return ""

        # 4. 读取刚生成的最新 txt 总结文件（精确到秒命名）
        latest_txt = _find_latest_today_txt(news_dir, today_prefix)
        if not latest_txt:
            monitor.report_error("未找到总结文件")
            return ""

        txt_content = latest_txt.read_text(encoding="utf-8")
        if not txt_content.strip():
            monitor.report_error("总结文件内容为空")
            return ""

        # 5. 推送最终结果到前端对话区（作为 assistant 消息显示）
        if emit_to_frontend:
            monitor.report_task_result(txt_content)
        await _save_zsxq_to_history(thread_id, txt_content)
        return txt_content
    except FileNotFoundError as e:
        print(f"[ZSXQ分析] 脚本或 Python 解释器不存在: {e}")
        monitor.report_error("分析脚本未找到，请联系管理员")
    except Exception as e:
        import traceback
        traceback.print_exc()
        monitor.report_error("分析过程出现异常，请稍后重试")
    finally:
        reset_session_context(None, thread_token)
    return ""


@app.post("/api/zsxq-analysis")
async def run_zsxq_analysis(req: ZsxqAnalysisRequest):
    """
    触发 test_zsxq.py 完整流程（知识星球抓取 + Ollama LLM 分析），
    将生成的 txt 总结内容通过 WebSocket 推送到前端对话区。
    """
    thread_id = req.thread_id
    # 后台异步执行，不阻塞响应；附带 CancellationToken（300s 超时 + STOP/DISCONNECT 级联取消）
    # 小作文热度是快捷按钮 → quiet=True 避免控制台刷 verbose print
    task = asyncio.create_task(
        _run_with_ctx(
            thread_id,
            None,
            None,
            _DEFAULT_BG_TIMEOUT,
            _run_zsxq_analysis,
            thread_id,
            quiet=True,
        )
    )
    await _register_background_task(thread_id, task)  # 防止同会话重复触发
    return {"status": "started", "thread_id": thread_id}


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
        from datetime import timezone, timedelta as _td
        _tz_bj = timezone(_td(hours=8))
        _now_bj_pre = datetime.now(_tz_bj)
        _weekday_cn_pre = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][_now_bj_pre.weekday()]
        _current_time_str_pre = _now_bj_pre.strftime("%Y年%m月%d日 %H:%M")
        # 根据星期几和小时确定搜索时间范围（与原 prompt 逻辑一致）
        _hour = _now_bj_pre.hour
        _wd = _now_bj_pre.weekday()  # 0=周一, 5=周六, 6=周日
        if _wd == 0 and _hour < 10 or _wd in (5, 6):
            # 周一10点前 或 周六/周日：搜索范围从周五下午三点起
            _last_fri = _now_bj_pre - timedelta(days=_wd - 4 if _wd >= 5 else 7)
            _search_range_hint = f"{_last_fri.strftime('%m月%d日')} 15:00 至 现在"
        else:
            # 其他情况：昨天下午三点起
            _yesterday = _now_bj_pre - timedelta(days=1)
            _search_range_hint = f"{_yesterday.strftime('%m月%d日')} 15:00 至 现在"
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
async def run_review_prediction(req: ReviewPredictionRequest):
    """
    触发复盘预测：串行执行盘前小作文热度 + 盘前新闻搜索，
    调用 DeepSeek 结合 skill 规则给出指数走势预测与个股应对策略。
    结果通过 WebSocket 推送到前端对话区。
    """
    thread_id = req.thread_id
    # 复盘预测 = 快捷按钮，控制台静默
    task = asyncio.create_task(
        _run_with_ctx(
            thread_id,
            req.user_id,
            None,
            _DEFAULT_BG_TIMEOUT,
            _run_review_prediction,
            thread_id,
            req.user_id,
            req.user_query or "",
            quiet=True,
        )
    )
    await _register_background_task(thread_id, task)  # 防止同会话重复触发
    return {"status": "started", "thread_id": thread_id}


# ======================== 用户与会话管理接口 ========================

@app.post("/api/users")
async def create_or_login_user(req: UserRequest):
    """登录/注册：用户不存在则创建，返回用户信息。"""
    user = storage.get_or_create_user(req.user_id, req.display_name)
    return {"status": "ok", "user": user}


@app.get("/api/users/{user_id}")
async def get_user_info(user_id: str):
    """获取用户信息。"""
    user = storage.get_user(user_id)
    if not user:
        raise HTTPException(status_code=HTTP_CODE_NOT_FOUND, detail="用户不存在")
    return {"user": user}


@app.get("/api/users/{user_id}/sessions")
async def list_user_sessions(user_id: str):
    """列出某用户的所有会话。"""
    if not storage.get_user(user_id):
        raise HTTPException(status_code=HTTP_CODE_NOT_FOUND, detail="用户不存在")
    sessions = storage.list_sessions(user_id)
    return {"sessions": sessions}


@app.post("/api/users/{user_id}/sessions")
async def create_session(user_id: str, req: SessionRequest):
    """为用户新建一个会话，返回 session_id。"""
    storage.get_or_create_user(user_id)
    session = storage.create_session(user_id, req.title)
    return {"status": "ok", "session": session}


@app.get("/api/sessions/{session_id}/history")
async def get_history(session_id: str, user_id: Optional[str] = None):
    """获取会话的对话历史（用于前端切换会话时恢复聊天记录）。

    **并发隔离说明**：必须传入 user_id 参数，系统会校验该 session 是否属于该 user_id。
    若 user_id 为空或与 session 的 owner 不匹配，返回 403 防止越权读取他人会话。
    """
    session = storage.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    # ===== 用户隔离检查：防止越权读取其他用户的对话历史 =====
    if not user_id:
        raise HTTPException(status_code=400, detail="缺少必要参数: user_id")
    if not storage.verify_session_owner(session_id, user_id):
        raise HTTPException(
            status_code=403,
            detail=f"无权访问会话 {session_id}（不属于用户 {user_id}）",
        )
    # get_session_history 在 async 上下文中返回 coroutine
    history = await get_session_history(session_id)
    return {"session_id": session_id, "messages": history}


@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str, user_id: Optional[str] = None):
    """删除会话记录（幂等：不存在也返回成功，避免批量删除时 404 中断）。
    同时清理 LangGraph checkpointer 历史 与 Context Engineering 记忆数据。

    **并发隔离说明**：必须传入 user_id 参数，校验 session owner 后才能删除。
    否则恶意用户可通过猜测 session_id 删除任意他人的对话、研报、分析记录。
    """
    existing = storage.get_session(session_id)
    if existing:
        # ===== 用户隔离检查：防止越权删除其他用户的会话 =====
        if not user_id:
            raise HTTPException(status_code=400, detail="缺少必要参数: user_id")
        if not storage.verify_session_owner(session_id, user_id):
            raise HTTPException(
                status_code=403,
                detail=f"无权删除会话 {session_id}（不属于用户 {user_id}）",
            )
    storage.delete_session(session_id)
    # 清理记忆管理：滑窗/摘要/关键决策（失败不影响主流程）
    try:
        from agent.memory_manager import get_memory_manager
        mm = get_memory_manager()
        await mm.clear_session(session_id)
    except Exception as mm_err:
        print(f"[MemoryManager] 删除会话 {session_id} 记忆失败（不致命）: {mm_err}")
    # Layer3: 清理 Trace / Feedback / Layer4: 清理 State
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
        # langgraph 1.x 用 aupdate_state 置空 messages 来清理
        await agent.aupdate_state(config, {"messages": []})  # type: ignore[attr-defined]
    except Exception:
        pass
    return {"status": "ok"}


class RenameRequest(BaseModel):
    title: str
    user_id: Optional[str] = None  # 用户隔离校验用：防止修改其他用户会话标题


@app.patch("/api/sessions/{session_id}/title")
async def rename_session(session_id: str, req: RenameRequest):
    """手动重命名会话标题。

    **并发隔离说明**：校验 req.user_id（新增字段）与 session 的 owner 匹配。
    """
    title = req.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="标题不能为空")
    session = storage.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    # ===== 用户隔离检查：防止越权修改其他用户的会话标题 =====
    user_id = getattr(req, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=400, detail="缺少必要字段: user_id")
    if not storage.verify_session_owner(session_id, user_id):
        raise HTTPException(
            status_code=403,
            detail=f"无权修改会话 {session_id}（不属于用户 {user_id}）",
        )
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
    user_id: Optional[str] = None,
    role: Optional[str] = None,
):
    """删除某会话的单条/整轮消息（右键删除）。

    Args:
        session_id: 会话ID
        turn_index: 轮次序号（1-based）
        user_id: 越权校验用
        role: 可选 "user" / "assistant" / "all"（默认 "all"）
              - "all": 整轮删除（user + assistant 一起删，turn_index 后段前移）
              - "user": 只删用户提问，保留助手回答；若助手回答也为空则整行删
              - "assistant": 只删助手回答，保留用户提问；若用户提问也为空则整行删
    """
    session = storage.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    if not user_id:
        raise HTTPException(status_code=400, detail="缺少必要参数: user_id")
    if not storage.verify_session_owner(session_id, user_id):
        raise HTTPException(
            status_code=403,
            detail=f"无权修改会话 {session_id}（不属于用户 {user_id}）",
        )
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
    user_id: Optional[str] = None


@app.post("/api/sessions/{session_id}/messages/batch-delete")
async def batch_delete_messages(session_id: str, req: BatchDeleteRequest):
    """批量删除多条消息（多选模式批量删除）。

    请求体：
        {
            "items": [
                {"turn_index": 1, "role": "user"},
                {"turn_index": 2, "role": "assistant"},
                {"turn_index": 3, "role": "all"}
            ],
            "user_id": "xxx"
        }

    role 取值：user / assistant / all（all = 整轮删）
    内部按 turn_index 降序处理，避免前移导致索引错乱。
    LangGraph checkpointer 一次性收集所有 msg.id 统一 RemoveMessage。
    """
    session = storage.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    if not req.user_id:
        raise HTTPException(status_code=400, detail="缺少必要字段: user_id")
    if not storage.verify_session_owner(session_id, req.user_id):
        raise HTTPException(
            status_code=403,
            detail=f"无权修改会话 {session_id}（不属于用户 {req.user_id}）",
        )
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


# ======================== Layer4: 调度器状态 & Layer3: Trace 查询接口 ========================

@app.get("/api/scheduler/status")
async def scheduler_status():
    """查看定时调度器状态和下次运行时间"""
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
async def get_traces(session_id: str, limit: int = 10):
    """查询指定会话的 Trace 记录（可观测性：input/output/tool_calls/latency/token）"""
    try:
        from agent.trace import get_trace_logger
        tl = get_trace_logger()
        traces = await tl.get_recent_traces(session_id, limit)
        return {"session_id": session_id, "traces": traces}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/traces/{session_id}/latency")
async def get_latency_stats(session_id: str):
    """查询指定会话的延迟统计（avg/p50/p95）"""
    try:
        from agent.trace import get_trace_logger
        tl = get_trace_logger()
        stats = await tl.get_latency_stats(session_id)
        return {"session_id": session_id, "stats": stats}
    except Exception as e:
        return {"error": str(e)}


# ======================== Layer3: SLO 监控 & 可靠性状态接口 ========================

@app.get("/api/slo/status")
async def slo_status():
    """
    SLO 监控状态端点（Actor 化）：向 SLOMonitorActor 发送 SNAPSHOT 消息取快照。
    熔断器子快照从 CircuitBreakerActor 取完后一并传入。
    """
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
async def circuit_breaker_status():
    """查询所有熔断器当前状态（Actor 化：向 CircuitBreakerActor 请求 SNAPSHOT_ALL）"""
    try:
        if _circuit_breaker_actor is None:
            return {"error": "CircuitBreakerActor 未初始化"}
        snaps = await _circuit_breaker_actor.ask(CBMsg.SNAPSHOT_ALL, {})
        return {"breakers": snaps or {}}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/upload")
async def upload_files(files: List[UploadFile] = File(...), thread_id: str = Form(...)):
    """
    文件上传接口 (File Upload)。

    目标：
    1. 接收用户上传的一个或多个文件。
    2. 保存到 `updated/session_{thread_id}` 目录。
    3. 供 Agent 在后续任务中读取和分析。

    Args:
        files (List[UploadFile]): 文件对象列表。
        thread_id (str): 关联的任务会话 ID。
    """
    # 1. [目录准备] 确保上传目录存在
    target_dir = updated_dir / f"session_{thread_id}"
    target_dir.mkdir(parents=True, exist_ok=True)
    # 同时同步到 output 目录，使文件列表接口立即可见
    output_session_dir = output_dir / f"session_{thread_id}"
    output_session_dir.mkdir(parents=True, exist_ok=True)

    saved_files = []
    # 2. [保存] 遍历并写入文件
    for file in files:
        # UploadFile.filename 类型为 Optional[str]，需显式判空，避免类型错误
        if not file.filename:
            continue
        filename = file.filename
        file_path = target_dir / filename
        # 使用二进制模式写入，支持各种文件格式 (图片、PDF、文本等)
        # shutil.copyfileobj 高效复制文件流，避免一次性加载大文件到内存
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        # 同步复制到 output 目录，供 /api/files 立即查询和 /api/download 下载
        shutil.copy2(file_path, output_session_dir / filename)
        saved_files.append(filename)

    # 3. [响应] 返回成功保存的文件列表
    return {"status": "uploaded", "files": saved_files}


@app.get("/api/download")
async def download_file(path: str):
    """
    文件下载接口 (File Download)。

    目标：
    1. 根据绝对路径下载文件。
    2. 严格的安全检查，防止越权访问。

    Args:
        path (str): 文件的绝对路径 (通常从 list_files 接口获取)。
    """
    # 1. [安全检查] 路径解析与越权校验
    try:
        abs_path = Path(path).resolve()
        output_abs = output_dir.resolve()
    except Exception:
        return {"error": "无效的路径参数"}

    # 必须确保请求的文件在 output 目录下
    if not abs_path.is_relative_to(output_abs):
        return {"error": "拒绝访问: 只能下载输出目录下的文件"}

    # 2. [存在性检查]
    if not abs_path.exists():
        return {"error": "文件不存在"}

    # 3. [响应] 返回文件流 (浏览器自动触发下载)
    return FileResponse(abs_path, filename=abs_path.name)


@app.get("/api/files")
async def list_files(path: str):
    """
    文件列表查询接口 (File Explorer)。

    目标：
    1. 列出指定目录下的所有生成文件。
    2. 提供文件元数据（大小、时间、下载链接）。
    3. 严格的安全检查，防止路径遍历攻击。

    Args:
        path (str): 目标目录的绝对路径 (必须在 output 目录下)。
    """
    # 1. [调试] 打印请求路径
    print(f"[DEBUG] 请求文件列表: {path}")

    # 2. [解析] 获取绝对路径对象
    try:
        abs_path = Path(path).resolve()
        output_abs = output_dir.resolve()
    except Exception as e:
        print(f"[ERROR] 路径解析失败: {e}")
        return {"error": f"路径无效: {e}"}

    # 3. [安全] 检查路径是否越界 (Path Traversal Check)
    if not abs_path.is_relative_to(output_abs):
        print(f"[ERROR] 拒绝访问: {abs_path} 不在 {output_abs} 目录下")
        return {"error": "拒绝访问: 只能访问输出目录下的文件"}

    # 4. [检查] 目录是否存在
    if not abs_path.exists():
        return {"error": "目录不存在"}

    files = []
    try:
        # 5. [遍历] 递归查找所有文件
        for file_path in abs_path.rglob("*"):
            if file_path.is_file():
                # 计算相对路径，生成下载 URL
                stat = file_path.stat()
                files.append({
                    "name": file_path.name,
                    "type": "file",
                    "path": str(file_path),
                    # "url": f"/outputs/{url_path}",
                    "size": stat.st_size,
                    "mtime": stat.st_mtime
                })

    except Exception as e:
        print(f"[ERROR] 遍历文件失败: {e}")
        return {"error": str(e)}

    # 6. [排序] 按修改时间倒序排列 (最新的在前)
    files.sort(key=lambda x: x.get("mtime", 0), reverse=True)
    print(f"[DEBUG] 找到 {len(files)} 个文件")
    return {"files": files}


# 当浏览器请求 ws://localhost:8000/ws/thread_123 时：
# 1. 路由匹配 ：FastAPI 发现这个 URL 匹配了你写的 @app.websocket("/ws/{thread_id}") 。
# 2. 创建对象 ：FastAPI (基于 Starlette) 会立刻在 主事件循环 中实例化一个 WebSocket 对象。
#    - 这个对象封装了底层的 TCP 连接、HTTP 握手信息、以及后续的消息收发方法 ( send_text , receive_text 等)。
# 3. 注入参数 ：FastAPI 自动把这个刚创建好的 WebSocket 对象，作为参数传给你的 websocket_endpoint(websocket, ...) 函数。
@app.websocket("/ws/{thread_id}")
async def websocket_endpoint(websocket: WebSocket, thread_id: str):
    print(f"会话向我们发起了请求：{thread_id} 对应：{websocket}")
    """
    WebSocket 实时通讯核心接口 (Real-time Communication)。

    目标：
    1. 建立长连接，实现服务端与前端的双向通信。
    2. 绑定 `thread_id`，实现会话级消息隔离。
    3. 维持心跳 (Keep-Alive)，防止连接超时。

    执行步骤：
    1. 握手：接受 WebSocket 连接请求。
    2. 注册：将连接实例绑定到 `monitor.manager`，关联 `thread_id`。
    3. 循环：进入消息监听循环，处理前端发送的心跳或指令。
    4. 异常：捕获断开连接异常，清理资源。

    Args:
        websocket (WebSocket): WebSocket 连接实例。
        thread_id (str): 当前会话的唯一标识。
    """
    # 1. [握手] 先 accept 才能建立真正 TCP 连接（ConnectionManagerActor 不负责 accept，只负责登记）
    try:
        await websocket.accept()
        # ===== Actor Model: 通过 ConnectionManagerActor 登记连接（不再用 manager.connect 直接改字典）=====
        if _conn_manager_actor is not None:
            await _conn_manager_actor.send(ConnMsg.CONNECT, {
                "thread_id": thread_id,
                "websocket": websocket,
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
    # 安全策略：配置了 NGROK_AUTHTOKEN 且 pyngrok 可用时，绑定 127.0.0.1（仅本地，通过 ngrok 隐藏真实 IP）
    #           否则绑定 0.0.0.0（局域网可访问）。
    # 注意：若只配置了 NGROK_AUTHTOKEN 但未安装 pyngrok，也必须走 0.0.0.0 —— 否则 bind 到回环地址导致
    #       局域网设备无法访问，且 lifespan 内已打印安装提示告知用户。
    _env_ngrok = os.getenv("NGROK_AUTHTOKEN", "").strip()
    _pyngrok_available = False
    if _env_ngrok:
        try:
            import pyngrok  # noqa: F401
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