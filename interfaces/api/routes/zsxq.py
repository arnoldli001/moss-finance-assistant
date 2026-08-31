#coding = utf-8
"""盘前小作文热度分析子路由（从 interfaces/api/server.py P1-F 拆分出来）。

拆分范围（原 server.py L1782-L2226 块）：
  - 6 个 zsxq/ollama 辅助函数：
      _find_latest_today_txt / _save_zsxq_to_history /
      _probe_ollama / _find_ollama_exe / _ollama_models_list / _ensure_ollama_ready /
      _fetch_zsxq_txt_summary / _push_zsxq_summary_via_ws
  - 1 个编排函数：_run_zsxq_analysis（仍作为 server.py 内调度器/复盘预测的共享编排层，保持原签名零破坏）
  - 1 个 Pydantic 请求体：ZsxqAnalysisRequest
  - 1 个 HTTP 路由：POST /api/zsxq-analysis（最终被 FastAPI include_router 挂载时补上 /api 前缀后的完整路径与原接口一致）

【关键兼容策略】
  1. 公共编排函数 `_run_zsxq_analysis` 作为模块级 re-export 对象存在；server.py 内其它调用方
     （scheduler 定时回调 L641、复盘预测 L2265、L2337 _save_zsxq_to_history）会通过
     `from interfaces.api.routes.zsxq import _run_zsxq_analysis, _save_zsxq_to_history`
     显式拿到，避免把 150+ 行代码继续滞留 server.py。
  2. 私有 `_run_with_ctx` / `_register_background_task` 属于 server.py 的 HTTP 通用 Cancellation
     编排，zsxq 路由运行时通过闭包回调从 server.py 里「注入」这些 helpers，避免拆分后循环依赖。
     （实际实现中我们改为：在 zsxq.py 里以「模块级 set 变量」注册这些 server-level helpers，
      由 server.py lifespan 启动早期调用 install_server_helpers(...) 一次性注入。）
  3. APIRouter 不添加 /api 前缀（由主 server.app 已经做统一 /api 前缀的不做，防止出现 /zsxq-analysis 和
     /api/zsxq-analysis 双路径并存），因此这里 prefix=""，路由里显式写 /api/zsxq-analysis 完整路径。
     （与当前 server.py 其它 POST /api/* 直接挂载方式一致。）
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

# 全局常量（Ollama 相关）
from config.constants import (
    OLLAMA_DEFAULT_BASE_URL,
    OLLAMA_PROBE_TIMEOUT_SEC,
    OLLAMA_LAUNCH_POLL_INTERVAL_SEC,
    OLLAMA_LAUNCH_POLL_MAX_ROUNDS,
    OLLAMA_MODELS_LIST_TIMEOUT_SEC,
    OLLAMA_PULL_LOG_TAIL_KEEP,
    OLLAMA_PULL_PROGRESS_INTERVAL_SEC,
    OLLAMA_PULL_PROGRESS_LINE_MAX_CHARS,
    OLLAMA_PULL_HARD_TIMEOUT_SEC,
)

# ---------------- 模块级公共导出（server.py 其它路径复用）----------------
__all__ = [
    "router",
    "ZsxqAnalysisRequest",
    "_run_zsxq_analysis",
    "_save_zsxq_to_history",
    "install_server_helpers",
    "_find_latest_today_txt",
    "_probe_ollama",
    "_find_ollama_exe",
    "_ensure_ollama_ready",
]


class ZsxqAnalysisRequest(BaseModel):
    thread_id: str
    user_id: Optional[str] = None


# ===========================================================
# server-level helpers 注入槽位（拆分后避免循环 import server.py）
# ===========================================================
# 由 server.py lifespan 或模块初始化完成后调用 install_server_helpers(...) 写入。
# zsxq 路由里的 `_register_background_task` / `_run_with_ctx` / `_DEFAULT_BG_TIMEOUT`
# 均从此处取，否则 POST /api/zsxq-analysis 的后台任务注册找不到这两个函数。
_SERVER_RUN_WITH_CTX = None
_SERVER_REGISTER_BG_TASK = None
_SERVER_DEFAULT_BG_TIMEOUT: float = 300.0  # 兜底与 DEFAULT_BACKGROUND_TIMEOUT_SEC 一致


def install_server_helpers(*, run_with_ctx, register_background_task, default_bg_timeout: float):
    """安装 server.py 提供的 HTTP 编排 helpers（Cancellation + 后台任务注册）。

    本函数可重复调用（幂等）。拆分后：
      - server.py 启动早期（lifespan 之前 / 导入 routes.zsxq 之后）调用一次即可；
      - 调用完之后，POST /api/zsxq-analysis 即可拥有和原 server.py 完全一致的
        CancellationToken + SessionRegistryActor 任务注册行为。
    """
    global _SERVER_RUN_WITH_CTX, _SERVER_REGISTER_BG_TASK, _SERVER_DEFAULT_BG_TIMEOUT
    _SERVER_RUN_WITH_CTX = run_with_ctx
    _SERVER_REGISTER_BG_TASK = register_background_task
    _SERVER_DEFAULT_BG_TIMEOUT = float(default_bg_timeout)


# ===========================================================
# 辅助函数 1/8：查找当天最新的 txt 总结文件
# ===========================================================
def _find_latest_today_txt(news_dir: Path, today_prefix: str):
    """查找当天最新的 txt 总结文件（文件名以 YYYYMMDD 开头，精确到秒命名）"""
    candidates = sorted(
        [f for f in news_dir.glob(f"{today_prefix}*.txt") if f.is_file()],
        key=lambda f: f.name,
        reverse=True,
    )
    return candidates[0] if candidates else None


# ===========================================================
# 辅助函数 2/8：把 zsxq txt 结果写入会话历史 + 记忆管理
# ===========================================================
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


# ===========================================================
# 辅助函数 3/8：Ollama 服务在线探测
# ===========================================================
async def _probe_ollama(base_url: str = "http://localhost:11434", timeout: float = 3.0) -> bool:
    """快速探测 Ollama 服务是否在线（GET /api/tags）。"""
    import urllib.request
    req = urllib.request.Request(f"{base_url}/api/tags", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()
        return True
    except Exception:
        return False


# ===========================================================
# 辅助函数 4/8：定位 ollama 可执行文件绝对路径
# ===========================================================
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


# ===========================================================
# 辅助函数 5/8：ollama list CLI 获取已安装模型名
# ===========================================================
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


# ===========================================================
# 辅助函数 6/8：确保 Ollama 服务在线 + 指定模型已拉取
# ===========================================================
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


# ===========================================================
# 辅助函数 7/8：抓取 + 返回 zsxq txt 总结（server 端薄适配层）
# ===========================================================
async def _fetch_zsxq_txt_summary(thread_id: str) -> str:
    """
    【server 端薄适配层】盘前小作文热度 **抓取 + 返回 txt 总结**。
      - 若当天已有 txt 总结，直接复用，跳过抓取；
      - 否则运行 tools/zsxq_analysis_runner.py 完整流程（抓取 + LLM 分析）。

    实现已全部下沉到独立工具文件 tools/zsxq_crawler_tool.py 的
    `fetch_zsxq_latest_summary_async(...)`，供主 Agent / 调度协调 Agent / Server
    三方共享同一份实现，不再双份维护。本函数的额外价值：
      1) 包一层 set_thread_context / reset_session_context，
         保证 monitor / ConnectionManagerActor 在任意调用深度都能拿到 thread_id；
      2) 把工具内部的 progress callback 对接到当前 HTTP 会话的 ToolMonitor：
         里程碑 → monitor._emit("tool_start", msg)，中间进度 → report_thinking。
    返回值：
        成功 → 非空 txt 总结字符串；失败 → ""（monitor.error 已由适配层显式转报）。
    """
    from api.context import set_thread_context, reset_session_context
    from api.monitor import monitor

    thread_token = set_thread_context(thread_id)
    try:
        # ---- 工具内部 progress:tool_start 已经被适配层翻译为 thinking/tool_start；
        #      这里先补一条 thinking 起点，避免抓 Ollama 前 1~3 秒视觉空白。
        monitor.report_thinking("盘前小作文热度分析")

        # 适配：把工具的 progress(msg) 翻译为 HTTP 会话可见的 monitor 事件
        def _progress_cb(msg: str) -> None:
            if not msg:
                return
            # 明确终点 / 失败的里程碑消息 → 作为独立气泡 tool_start
            if any(tag in msg for tag in ("✅", "❌", "🏁", "📡", "📥", "🔧")):
                # 去掉前缀 emoji 空格：让 thinking 更新更整齐
                base = msg
                for _pre in ("🏁 ", "⏳ "):
                    if base.startswith(_pre):
                        base = base[len(_pre):]
                        break
                monitor._emit("tool_start", msg)
                if base.startswith("盘前小作文热度：") or msg.startswith("⏳ "):
                    monitor.report_thinking(base)
                return
            # 中间等待（⏳ / 没 emoji 的过程文本）→ 合并到顶部 thinking 不独立气泡
            base = msg[len("⏳ 盘前小作文热度："):] if msg.startswith("⏳ 盘前小作文热度：") else msg
            monitor.report_thinking("盘前小作文热度：" + base)

        # 直接调用独立工具（抓取 + 返回 txt）：progress、today_prefix、Ollama helper
        # 都由新工具内聚，不再 server.py 里双份实现，避免漂移。
        from tools.zsxq_crawler_tool import fetch_zsxq_latest_summary_async
        txt_content = await fetch_zsxq_latest_summary_async(
            emit_progress=_progress_cb, quiet=False,
        )

        # ---- 适配层补全：抓失败时兜底 report_error（工具内部只负责 stderr + cb 推）
        if not txt_content:
            # 进度回调已把具体失败类型 cb 过；这里只补一条空结果用户可见 error
            monitor.report_error("盘前小作文热度分析未返回结果，请检查日志或稍后重试")
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


# ===========================================================
# 辅助函数 8/8：WS 推送 txt 总结 + 写会话历史
# ===========================================================
async def _push_zsxq_summary_via_ws(thread_id: str, txt_content: str) -> None:
    """
    把抓取到的 txt 总结通过 WebSocket 推送到前端对话区。
    推送通道（沿用已被前端 handleWSMessage(task_result) 消费的链路）：
        monitor.report_task_result(txt_content)
          → ToolMonitor._emit(event="task_result", payload={"result": <安全净化后的 txt>})
            → ConnectionManagerActor SEND_TO_THREAD → WS 发送
              → app.js handleWSMessage(ev=='task_result')
                → appendMessage(currentTaskType=='zsxq' ? 'user' : 'assistant', result)

    同时会把"盘前小作文热度 + txt 总结"写入会话历史 _save_zsxq_to_history，
    刷新后可被历史恢复接口回显。
    """
    from api.context import set_thread_context, reset_session_context
    from api.monitor import monitor

    thread_token = set_thread_context(thread_id)
    try:
        if not txt_content or not txt_content.strip():
            return  # 空内容不推送、不落库
        monitor.report_task_result(txt_content)
        await _save_zsxq_to_history(thread_id, txt_content)
    finally:
        reset_session_context(None, thread_token)


# ===========================================================
# 公共编排函数：_run_zsxq_analysis（保持原签名，供 server 内多方复用）
# ===========================================================
async def _run_zsxq_analysis(thread_id: str, emit_to_frontend: bool = True) -> str:
    """
    编排函数：串起「抓取」和「推送」两个阶段。保持原调用签名不变，
    所有调用链（POST /api/zsxq-analysis、复盘预测 _run_review_prediction、scheduler 回调）
    无需改动。

    步骤：
        1) _fetch_zsxq_txt_summary(thread_id)：返回纯 txt 总结字符串，失败返回 ""；
        2) 非空 & emit_to_frontend → _push_zsxq_summary_via_ws(thread_id, txt)：
           走 WebSocket 的 task_result 事件将总结渲染进对话区 + 写历史；
        3) 非空 & (not emit_to_frontend) → 只写历史（复盘预测场景：阶段1结果要复用，
           但避免阶段1的独立结果提前把气泡写乱对话流）；
        4) 返回 txt 给编排上层。
    """
    txt_content = await _fetch_zsxq_txt_summary(thread_id)
    if not txt_content.strip():
        return ""
    if emit_to_frontend:
        await _push_zsxq_summary_via_ws(thread_id, txt_content)
    else:
        # 复盘预测阶段1：不推气泡，但仍把"盘前小作文热度 / AI 总结"写入会话历史，
        # 避免后续阶段引用小作文时 LangGraph 记忆里没上下文。
        try:
            await _save_zsxq_to_history(thread_id, txt_content)
        except Exception as _e:
            print(f"[_run_zsxq_analysis] 历史保存失败（不致命，emit=False）: {_e}")
    return txt_content


# ===========================================================
# APIRouter 实例 + 挂载 HTTP 路由
# ===========================================================
router = APIRouter(tags=["zsxq"])


@router.post("/api/zsxq-analysis")
async def run_zsxq_analysis(req: ZsxqAnalysisRequest):
    """
    触发 tools/zsxq_analysis_runner.py 完整流程（知识星球抓取 + Ollama LLM 分析），
    将生成的 txt 总结内容通过 WebSocket 推送到前端对话区。
    """
    if _SERVER_RUN_WITH_CTX is None or _SERVER_REGISTER_BG_TASK is None:
        raise RuntimeError(
            "zsxq router 未完成 install_server_helpers(...) 注入，"
            "请在 server.py startup 阶段调用 interfaces.api.routes.zsxq.install_server_helpers(...)。"
        )
    thread_id = req.thread_id
    # 后台异步执行，不阻塞响应；附带 CancellationToken（300s 超时 + STOP/DISCONNECT 级联取消）
    # 小作文热度是快捷按钮 → quiet=True 避免控制台刷 verbose print
    task = asyncio.create_task(
        _SERVER_RUN_WITH_CTX(
            thread_id,
            None,
            None,
            _SERVER_DEFAULT_BG_TIMEOUT,
            _run_zsxq_analysis,
            thread_id,
            quiet=True,
        )
    )
    await _SERVER_REGISTER_BG_TASK(thread_id, task)  # 防止同会话重复触发
    return {"status": "started", "thread_id": thread_id}
