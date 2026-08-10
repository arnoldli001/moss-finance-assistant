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

# Import agent runner and monitor
# 注意：agent.main_agent 导入时会初始化 main_agent，这可能需要几秒钟
from agent.main_agent import run_deep_agent, get_session_history
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---------- 启动逻辑 ----------
    loop = asyncio.get_running_loop()
    manager.set_loop(loop)
    print(f"[Server] WebSocket Manager bound to loop: {id(loop)}")

    # ---------- ngrok 公网隧道（可选）----------
    # 在 .env 中配置 NGROK_AUTHTOKEN 后，启动时自动创建公网隧道，手机可远程访问
    ngrok_tunnel = None
    ngrok_proc = None
    ngrok_token = os.getenv("NGROK_AUTHTOKEN", "").strip()
    if ngrok_token:
        try:
            from pyngrok import ngrok as ngrok_mod
            from pyngrok.conf import get_default as get_ngrok_config
            import time
            import subprocess
            import json
            import urllib.request

            ngrok_mod.set_auth_token(ngrok_token)
            # 清理可能残留的旧 ngrok 进程
            try:
                ngrok_mod.kill()
                time.sleep(2)
            except Exception:
                pass

            # 直接用 ngrok 命令行启动，加 --pooling-enabled 允许多实例共享同一域名
            # 彻底避免 ERR_NGROK_334: endpoint already online
            ngrok_bin = get_ngrok_config().ngrok_path
            ngrok_proc = subprocess.Popen(
                [ngrok_bin, "http", "8000", "--pooling-enabled", "--log=stdout"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            # 等待 ngrok 本地 API 就绪
            public_url = ""
            for _ in range(10):
                time.sleep(1)
                try:
                    resp = urllib.request.urlopen("http://127.0.0.1:4040/api/tunnels", timeout=2)
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
    else:
        # 未配置 ngrok token，提示局域网访问地址
        print("\n[提示] 未配置 NGROK_AUTHTOKEN，仅支持局域网访问")
        print("[提示] 局域网访问: 手机连同一 WiFi 后访问 http://<本机IP>:8000")
        print("[提示] 外网访问: 在 .env 中设置 NGROK_AUTHTOKEN 后重启\n")

    yield  # 应用在此运行，处理请求

    # ---------- 关闭逻辑 ----------
    if ngrok_proc:
        try:
            ngrok_proc.terminate()
            ngrok_proc.wait(timeout=5)
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
    RATE_LIMIT = 60
    RATE_WINDOW = 60  # 秒
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
                    status_code=429,
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
        response.headers["X-Powered-By"] = "DeepSearch-Pro"  # 隐藏真实服务器信息
        return response


app.add_middleware(SecurityMiddleware)


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
    # 注意：这里简单的使用 asyncio.create_task 触发，由 main_agent 内部负责实时推送
    asyncio.create_task(run_deep_agent(request.query, thread_id, request.user_id))

    # 3. [立即响应]
    return {"status": "started", "thread_id": thread_id, "user_id": request.user_id}


# ======================== 盘前小作文热度分析接口 ========================

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
    """把盘前小作文热度的用户消息和结果存入会话历史（checkpointer），刷新后可恢复。"""
    try:
        from langchain_core.messages import HumanMessage, AIMessage
        from agent.main_agent import get_main_agent
        agent = await get_main_agent()
        config = {"configurable": {"thread_id": thread_id}}
        await agent.aupdate_state(config, {"messages": [
            HumanMessage(content="盘前小作文热度"),
            AIMessage(content=txt_content),
        ]})
    except Exception as e:
        print(f"[ZSXQ分析] 保存会话历史失败: {e}")


async def _run_zsxq_analysis(thread_id: str):
    """
    执行盘前小作文热度分析：
    - 若当天已有 txt 总结，直接复用，跳过抓取
    - 否则运行 test_zsxq.py 完整流程（抓取 + LLM 分析）
    完成后通过 WebSocket 将 txt 总结推送到前端对话区。
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
                monitor.report_task_result(txt_content)
                await _save_zsxq_to_history(thread_id, txt_content)
                return
            # 内容为空则继续走抓取流程

        # 2. 没有则运行 test_zsxq.py（Playwright sync API 不能在 async 上下文运行）
        monitor.report_thinking("盘前小作文热度分析")
        script_path = project_root / "test_zsxq.py"
        monitor._emit("tool_start", "开始抓取知识星球并调用 Ollama 分析", {"script": str(script_path)})

        process = await asyncio.create_subprocess_exec(
            sys.executable, str(script_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(project_root),
        )

        # 3. 实时读取输出并推送进度（避免长时间无响应）
        stdout_lines = []
        try:
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                stdout_lines.append(text)
                # 关键进度行推送到前端
                if any(kw in text for kw in ("[抓取]", "[分析]", "[ZSXQ]", "分析结果", "总结已保存")):
                    monitor._emit("tool_start", text)
        except Exception as e:
            print(f"[ZSXQ分析] 读取脚本输出异常: {e}")

        await process.wait()

        if process.returncode != 0:
            tail = "\n".join(stdout_lines[-20:])
            print(f"[ZSXQ分析] test_zsxq.py 执行失败（退出码 {process.returncode}）\n{tail}")
            monitor._emit("error", "分析失败，请稍后重试")
            return

        # 4. 读取刚生成的最新 txt 总结文件（精确到秒命名）
        latest_txt = _find_latest_today_txt(news_dir, today_prefix)
        if not latest_txt:
            monitor._emit("error", "未找到总结文件")
            return

        txt_content = latest_txt.read_text(encoding="utf-8")
        if not txt_content.strip():
            monitor._emit("error", "总结文件内容为空")
            return

        # 5. 推送最终结果到前端对话区（作为 assistant 消息显示）
        monitor.report_task_result(txt_content)
        await _save_zsxq_to_history(thread_id, txt_content)
    except FileNotFoundError as e:
        print(f"[ZSXQ分析] 脚本或 Python 解释器不存在: {e}")
        monitor._emit("error", "分析脚本未找到，请联系管理员")
    except Exception as e:
        import traceback
        traceback.print_exc()
        monitor._emit("error", "分析过程出现异常，请稍后重试")
    finally:
        reset_session_context(None, thread_token)


@app.post("/api/zsxq-analysis")
async def run_zsxq_analysis(req: ZsxqAnalysisRequest):
    """
    触发 test_zsxq.py 完整流程（知识星球抓取 + Ollama LLM 分析），
    将生成的 txt 总结内容通过 WebSocket 推送到前端对话区。
    """
    thread_id = req.thread_id
    # 后台异步执行，不阻塞响应
    asyncio.create_task(_run_zsxq_analysis(thread_id))
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
        raise HTTPException(status_code=404, detail="用户不存在")
    return {"user": user}


@app.get("/api/users/{user_id}/sessions")
async def list_user_sessions(user_id: str):
    """列出某用户的所有会话。"""
    if not storage.get_user(user_id):
        raise HTTPException(status_code=404, detail="用户不存在")
    sessions = storage.list_sessions(user_id)
    return {"sessions": sessions}


@app.post("/api/users/{user_id}/sessions")
async def create_session(user_id: str, req: SessionRequest):
    """为用户新建一个会话，返回 session_id。"""
    storage.get_or_create_user(user_id)
    session = storage.create_session(user_id, req.title)
    return {"status": "ok", "session": session}


@app.get("/api/sessions/{session_id}/history")
async def get_history(session_id: str):
    """获取会话的对话历史（用于前端切换会话时恢复聊天记录）。"""
    session = storage.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    # get_session_history 在 async 上下文中返回 coroutine
    history = await get_session_history(session_id)
    return {"session_id": session_id, "messages": history}


@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    """删除会话记录（幂等：不存在也返回成功，避免批量删除时 404 中断）。"""
    storage.delete_session(session_id)
    return {"status": "ok"}


class RenameRequest(BaseModel):
    title: str


@app.patch("/api/sessions/{session_id}/title")
async def rename_session(session_id: str, req: RenameRequest):
    """手动重命名会话标题。"""
    title = req.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="标题不能为空")
    if not storage.update_session_title(session_id, title):
        raise HTTPException(status_code=404, detail="会话不存在")
    return {"status": "ok", "session_id": session_id, "title": title}


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
    # 1. [注册] 建立连接并绑定到管理器（放入 try 块，防止 accept 阶段异常逃逸）
    try:
        await manager.connect(websocket, thread_id)

        # 2. [循环] 保持连接活跃
        while True:
            # 3. [监听] 接收前端消息 (通常是 ping 心跳)
            try:
                data = await websocket.receive_text()
            except WebSocketDisconnect:
                # 客户端断开（切换会话/关闭页面），静默退出循环
                break
            except Exception:
                # 其他接收异常（连接已关闭等），静默退出
                break

            # 4. [响应] 回复 pong 消息（发送失败不致命，忽略即可）
            try:
                await websocket.send_json({
                    "type": "pong",
                    "message": f"服务端已收到: {data}"
                })
            except Exception:
                # 发送失败说明连接已断开，退出循环
                break

    except WebSocketDisconnect:
        # 5. [正常] connect 阶段或循环外的 WebSocketDisconnect（兜底）
        pass

    except Exception as e:
        # 6. [异常] 其他错误（如连接超时）
        if "disconnect" not in str(e).lower() and "1005" not in str(e):
            print(f"[WebSocket] 连接异常: {e}")

    finally:
        # 7. [清理] 无论正常/异常断开，都确保从管理器移除
        try:
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
    return HTMLResponse(index_file.read_text(encoding="utf-8"))


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
    # 安全策略：配置了 NGROK_AUTHTOKEN 时绑定 127.0.0.1（仅本地，通过 ngrok 隐藏真实 IP）
    #           未配置时绑定 0.0.0.0（局域网可访问）
    has_ngrok = bool(os.getenv("NGROK_AUTHTOKEN", "").strip())
    bind_host = "127.0.0.1" if has_ngrok else "0.0.0.0"
    if has_ngrok:
        print("[安全] 检测到 NGROK_AUTHTOKEN，server 绑定 127.0.0.1，真实 IP 已隐藏")
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