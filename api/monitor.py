import datetime
import asyncio
import re as _re
from typing import Any, Dict, Optional, TYPE_CHECKING
from fastapi import WebSocket
from api.context import get_thread_context

from config.constants import TEXT_SANITIZE_SHORT_TEXT_THRESHOLD_LEN

# 尝试导入全局运行时（用于脚本模式下的流式输出）
try:
    import builtins
except ImportError:
    builtins = None

# Actor Model：ConnectionManagerActor 句柄（由 server.py lifespan 注入）
# 用 Optional[Any] 规避循环 import
_conn_actor: Optional[Any] = None
_conn_actor_loop: Optional[asyncio.AbstractEventLoop] = None
# 消息类型常量（硬拷贝，避免跨模块 import 在导入时发生）
_CONN_MSG_SEND_TO_THREAD = "send_to_thread"


# ======================================================================
# 面向用户的输出内容净化器：
#   即使 LLM 违反 prompt 约束输出了不该出现的内部噪声，也在推送前统一剥掉，
#   保证用户聊天区永远看不到。所有对外出口（report_task_result /
#   report_tool_end / report_error）统一调用。
# ======================================================================
_NOISE_STRIP_RULES = [
    # Rule 1: 精确匹配 TodoWrite 风格的 "Updated todo list to [...]"
    #   典型：Updated todo list to [{"content":"...","status":"pending"},...]
    (
        _re.compile(
            r"Updated\s+todo\s+list\s+to\s*"
            r"\[\s*\{[^\]]*?\}\s*\]\s*",
            flags=_re.IGNORECASE | _re.DOTALL,
        ),
        "",
    ),
    # Rule 2: 中文"任务清单/待办列表"格式，含 content/status 键
    (
        _re.compile(
            r"(?:(?:Updated\s+)?(?:todo-list|todo list|待办清单|任务清单(?:规划)?|规划清单)[^\n]{0,10}(?:\n|：|:))"
            r"\s*\[.*?\}\s*\]\s*",
            flags=_re.IGNORECASE | _re.DOTALL,
        ),
        "",
    ),
    # Rule 3: 多源交叉验证"当前北京时间"的一整段（上个问题讨论过的噪声段）
    #   典型开头：'我已经通过多个网络时间源进行了交叉检索' 或 '## 查询结果' + '### 当前北京时间'
    #   到 '综合判断，当前（检索时刻）北京时间约为：' 结尾段
    (
        _re.compile(
            r"(?:我已经通过多个网络时间源进行了交叉检索|##\s*查询结果\s*\n\s*###\s*当前北京时间)"
            r".*?"
            r"(?:综合判断[^\n。]{0,80}北京时间约为[^\n。]{0,40}(?:。|\n)|##?\s*【?[查检]询结果【?】?\s*$)",
            flags=_re.DOTALL,
        ),
        "",
    ),
    # Rule 4: 多余章节：独立的 "### 检索依据（多源交叉验证）" 直到下一个 ###
    (
        _re.compile(
            r"^\s*###\s*检索依据（多源交叉验证）[^\n]*\n.*?(?=\n\s*###\s|\Z)",
            flags=_re.DOTALL | _re.MULTILINE,
        ),
        "",
    ),
    # Rule 5: time.is / 时间校准网 / timeanddate / time.org.cn 域名 + 行含 "缓存较旧，已过时"
    #   只有在周围上下文是验证日期的行里才做删除（避免真的在新闻里提到这些网站）
    (
        _re.compile(
            r"^\s*[|✓-]\s*(?:[^\n]*?(?:time\.is|时间校准网|timeanddate\.com|time\.org\.cn|time\.tianqi\.com)[^\n]*)"
            r"(?:缓存较旧|已过时|±10分钟|不确定性)[^\n]*\n",
            flags=_re.IGNORECASE | _re.MULTILINE,
        ),
        "",
    ),
]


def sanitize_user_facing_text(text: Optional[str]) -> str:
    """在发送给前端聊天区/工具结果气泡前，剥掉内部噪声。

    返回 str；None 视为空串。
    """
    if text is None:
        return ""
    if not isinstance(text, str):
        try:
            text = str(text)
        except Exception:
            return ""
    # 空文本直接返回
    if len(text) < TEXT_SANITIZE_SHORT_TEXT_THRESHOLD_LEN:
        # 即使短文本也要过滤"实质空"内容：
        # "[]" / "null" / "None" / "{}" / '""' 等常被 LLM/tool 当成空响应推送
        _stripped = text.strip()
        if _stripped in ("[]", "null", "None", "{}", '""', "''", "()", ""):
            return ""
        return text
    for _pat, _repl in _NOISE_STRIP_RULES:
        text = _pat.sub(_repl, text)
    # 清理连续 3 个以上空行
    text = _re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    # 最终再过滤一次"实质空"内容（净化后可能变成纯空字符）
    if text in ("[]", "null", "None", "{}", '""', "''", "()", ""):
        return ""
    return text


def _set_conn_actor(actor: Any, loop: asyncio.AbstractEventLoop) -> None:
    """由 server.py lifespan 注入 ConnectionManagerActor 句柄 + 主事件循环引用。"""
    global _conn_actor, _conn_actor_loop
    _conn_actor = actor
    _conn_actor_loop = loop


class ToolMonitor:
    """
    工具监控类。

    Actor Model 改造点：
    原 `_emit` → `run_coroutine_threadsafe(manager.send_to_thread(...))`
      → 跨线程直接修改 ConnectionManager.active_connections 共享字典
    新 `_emit` → `run_coroutine_threadsafe(actor.send(SEND_TO_THREAD, ...))`
      → 跨线程仅向 Actor 邮箱投递一条消息；真正的状态修改+WS发送只在 Actor 协程内串行发生。

    行为上等价，但状态修改被收敛到 ConnectionManagerActor.handle_message 单点，
    满足 next_state = f(current_state, input)。
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ToolMonitor, cls).__new__(cls)
            cls._instance.websocket_manager = None  # 预留给 FastAPI WebSocketManager
        return cls._instance

    def set_websocket_manager(self, manager):
        """设置 FastAPI 的 WebSocket 管理器（兼容旧路径）"""
        self.websocket_manager = manager

    def _emit(self, event_type: str, message: str, data: Optional[Dict[str, Any]] = None):
        """内部发送方法"""
        payload = {
            "type": "monitor_event",
            "event": event_type,
            "message": message,
            "data": data or {},
            "timestamp": datetime.datetime.now().isoformat()
        }

        # ===== 1. Actor Model 路径：优先通过 ConnectionManagerActor 发送 =====
        if _conn_actor is not None and _conn_actor_loop is not None:
            try:
                thread_id = get_thread_context()
                if thread_id:
                    # 向主循环投递"向 Actor 邮箱塞 SEND_TO_THREAD 消息"的协程。
                    # 工作线程里不能直接调用 asyncio.create_task（没有事件循环），
                    # 但 run_coroutine_threadsafe 可以把一个 coroutine 安全地丢给主循环。
                    # 注意：这里投递的是 `actor.send(...)` 本身；send() 内部是 put_nowait 入队，
                    # 不涉及 await WS 发送，因此很快返回，不会阻塞主循环太久。
                    async def _post_send():
                        try:
                            # actor.send() 内部已经会把消息序列化塞进邮箱队列
                            await _conn_actor.send(_CONN_MSG_SEND_TO_THREAD, {
                                "thread_id": thread_id,
                                "payload": payload,
                            })
                        except Exception as _ae:
                            print(f"[Monitor] Actor send 投递失败: {_ae}")
                    asyncio.run_coroutine_threadsafe(_post_send(), _conn_actor_loop)
            except Exception as e:
                print(f"[Monitor] Actor-based WS send failed: {e}")

        # ===== 2. 旧路径（兜底）：通过 ConnectionManager 发送（若 Actor 没初始化）=====
        elif self.websocket_manager:
            try:
                thread_id = get_thread_context()
                manager_loop = self.websocket_manager.loop
                if manager_loop and thread_id:
                    asyncio.run_coroutine_threadsafe(
                        self.websocket_manager.send_to_thread(payload, thread_id),
                        manager_loop
                    )
            except Exception as e:
                print(f"[Monitor] Legacy WS send failed: {e}")

        # 3. 尝试通过全局 runtime 输出 (DeepAgents 脚本模式)
        if builtins and hasattr(builtins, 'runtime') and hasattr(getattr(builtins, 'runtime', None), 'stream_writer'):
            try:
                runtime = getattr(builtins, 'runtime', None)
                if runtime is not None:
                    runtime.stream_writer(payload)  # type: ignore[attr-defined]
            except Exception:
                pass

        # 4. 控制台保底输出
        print(f"\n[Monitor:{event_type}] {message}")

    def report_tool(self, tool_name: str, args: Optional[Dict[str, Any]] = None):
        """报告工具开始执行"""
        self._emit("tool_start", f"开始执行工具: {tool_name}", {"tool_name": tool_name, "args": args})

    def report_tool_end(self, tool_name: str, result_preview: str = ""):
        """报告工具执行完成，推送结果给前端。

        在送入 WS 前会调用 sanitize_user_facing_text()，确保 LLM 输出的
        todo list / 日期交叉验证段等内部噪声不会出现在用户的工具结果气泡中。
        若净化后为空字符串（如 LLM/tool 返回的 "[]" / "null" 等实质空内容），
        则跳过本次推送，避免前端出现一条独立的 [] 空消息。
        """
        preview = sanitize_user_facing_text(result_preview[:5000] if result_preview else "")
        if not preview:
            # 净化后为空 → 跳过推送（避免前端出现独立的 [] / null 空消息气泡）
            return
        self._emit("tool_end", f"工具执行完成: {tool_name}",
                   {"tool_name": tool_name, "result": preview})

    def report_assistant(self, assistant_name: str, args: Optional[Dict[str, Any]] = None):
        """报告正在调用的子智能体进度"""
        self._emit("assistant_call", f"正在调用助手: {assistant_name}",
                   {"assistant_name": assistant_name, "args": args})

    def report_thinking(self, assistant_name: str = ""):
        """报告子智能体正在思考（LLM 决策中），消除 tool_start 之前的日志空白期"""
        label = f"助手「{assistant_name}」" if assistant_name else "智能体"
        self._emit("thinking", f"{label}正在思考中...",
                   {"assistant_name": assistant_name, "phase": "llm_thinking"})

    def report_task_result(self, result: str):
        """报告任务最终结果（最终 assistant 回复）。

        在送入 WS 前会调用 sanitize_user_facing_text()，这是防止 LLM 输出
        内部噪声的最后一道防线 —— 即使 prompt 约束 / server.py prompt 预处理
        因任何原因（server 未重启、缓存旧 prompt）失效，用户也看不到
        'Updated todo list to [...]' 或 '交叉验证当前北京时间' 的段落。
        若净化后为空字符串（如 LLM 返回的 "[]" / "null" 等实质空内容），
        则跳过推送，避免前端出现一条独立的 [] 空消息。
        """
        safe = sanitize_user_facing_text(result)
        if not safe:
            # 净化后为空 → 跳过推送（避免前端出现独立的 [] / null 空消息气泡）
            return
        self._emit("task_result", "任务执行完成", {"result": safe})

    def report_session_dir(self, path: str):
        """报告任务工作目录"""
        self._emit("session_created", f"工作目录已创建: {path}", {"path": path})

    def report_error(self, message: str):
        """报告错误信息（公共方法，供外部调用，避免直接调用私有 _emit）"""
        # 错误消息也做一次净化，避免级联取消等过程中的噪声字串被前端显示
        safe_msg = sanitize_user_facing_text(message)
        if safe_msg:
            self._emit("error", safe_msg)


# 全局单例实例
monitor = ToolMonitor()


class ConnectionManager:
    def __init__(self):
        # 同一 thread_id 允许多个连接并存（多标签页/多设备同时打开同一会话）
        self.active_connections: Dict[str, list] = {}
        # 延迟绑定 loop，防止初始化时 loop 不一致
        self.loop = None

    def set_loop(self, loop):
        """显式设置事件循环"""
        self.loop = loop
        monitor.set_websocket_manager(self)
        print(f"[Monitor] ConnectionManager manually bound to loop: {id(self.loop)}")

    async def connect(self, websocket: WebSocket, thread_id: str):
        # 允许同一会话多连接：两个标签页打开同一会话时互不踢掉
        await websocket.accept()
        conns = self.active_connections.setdefault(thread_id, [])
        conns.append(websocket)
        print(f"Client connected: {thread_id} (total: {len(conns)})")

    def disconnect(self, websocket: WebSocket, thread_id: str):
        conns = self.active_connections.get(thread_id)
        if not conns:
            return
        if websocket in conns:
            conns.remove(websocket)
            if not conns:
                del self.active_connections[thread_id]
        print(f"Client disconnected: {thread_id} (remaining: {len(self.active_connections.get(thread_id, []))})")

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def send_to_thread(self, message: dict, thread_id: str):
        conns = self.active_connections.get(thread_id)
        if not conns:
            return
        # 复制列表，避免发送过程中被 disconnect 修改
        for ws in list(conns):
            try:
                await ws.send_json(message)
            except Exception:
                # 发送失败的连接会被下次 disconnect 清理，这里不主动移除避免并发问题
                pass


manager = ConnectionManager()