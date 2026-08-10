import datetime
import asyncio
from typing import Any, Dict, Optional
from fastapi import WebSocket
from api.context import get_thread_context

# 尝试导入全局运行时（用于脚本模式下的流式输出）
try:
    import builtins
except ImportError:
    builtins = None


class ToolMonitor:
    """
    工具监控类，用于在工具执行过程中上报进度和状态。
    设计为单例模式，可在任何工具中直接导入使用。
    兼容 FastAPI WebSocket 和 脚本运行时的 stream_writer。

    使用示例:
    from api.monitor import monitor

    def my_tool(arg1):
        monitor.report_start("my_tool", {"arg1": arg1})
        ...
        monitor.report_running("my_tool", "正在处理数据...", progress=0.5)
        ...
        monitor.report_end("my_tool", result)
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ToolMonitor, cls).__new__(cls)
            cls._instance.websocket_manager = None  # 预留给 FastAPI WebSocketManager
        return cls._instance

    def set_websocket_manager(self, manager):
        """设置 FastAPI 的 WebSocket 管理器"""
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

        # 1. 优先尝试通过 FastAPI WebSocket 发送 (定向推送)
        if self.websocket_manager:
            try:
                # 获取当前线程 ID
                thread_id = get_thread_context()

                # 确保 loop 已加载 [fastapi的事件循环]
                manager_loop = self.websocket_manager.loop

                if manager_loop and thread_id:
                    # 统一使用 run_coroutine_threadsafe：
                    # - 在主线程 async 上下文中：call_soon_threadsafe 仍能正确调度
                    # - 在工作线程（LangGraph 同步工具执行线程）中：线程安全地投递到主循环
                    # 避免调用 asyncio.get_running_loop()，它在工作线程中会抛 RuntimeError
                    asyncio.run_coroutine_threadsafe(
                        self.websocket_manager.send_to_thread(payload, thread_id),
                        manager_loop
                    )
            except Exception as e:
                print(f"[Monitor] WebSocket send failed: {e}")

        # 2. 尝试通过全局 runtime 输出 (DeepAgents 脚本模式)
        # 这使得 simple_agents.py 中的 MockRuntime 能接收到数据
        if builtins and hasattr(builtins, 'runtime') and hasattr(getattr(builtins, 'runtime', None), 'stream_writer'):
            try:
                runtime = getattr(builtins, 'runtime', None)
                if runtime is not None:
                    runtime.stream_writer(payload)  # type: ignore[attr-defined]
            except Exception:
                pass

        # 3. 控制台保底输出 (方便调试)
        # 加上特殊前缀，方便肉眼识别
        print(f"\n[Monitor:{event_type}] {message}")

    def report_tool(self, tool_name: str, args: Optional[Dict[str, Any]] = None):
        """报告工具开始执行"""
        self._emit("tool_start", f"开始执行工具: {tool_name}", {"tool_name": tool_name, "args": args})

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
        """报告任务最终结果"""
        self._emit("task_result", "任务执行完成", {"result": result})

    def report_session_dir(self, path: str):
        """报告任务工作目录"""
        self._emit("session_created", f"工作目录已创建: {path}", {"path": path})

    def report_error(self, message: str):
        """报告错误信息（公共方法，供外部调用，避免直接调用私有 _emit）"""
        self._emit("error", message)


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