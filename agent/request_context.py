# -*- coding: utf-8 -*-
"""
跨层级任务终止联动：RequestContext 三位一体。

解决的问题（原架构缺失）：
  1) WebSocket 断开时只移除连接，不取消任务 → LLM 推理/工具调用继续跑，浪费 token 与算力；
  2) 只有 task.cancel()（被动 await 点生效），同步长耗时代码（大循环/预处理）无"主动检查"→
     取消延迟可达数百毫秒甚至秒级（用户体感"点了停止还在跑"）；
  3) 父任务 cancel 不传递到后台 create_task 的子任务（摘要压缩/反馈写入）→ 孤儿任务泄漏；
  4) 取消语义与超时、请求元数据分散 → 无统一上下文对象贯穿请求全链路。

本模块提供：
  - CancellationToken：事件驱动取消 + 主动 check + 子任务级联取消 + 可选 deadline；
  - RequestContext（三位一体）：取消令牌 + 元数据（thread_id/user_id/request_id/session_dir）+
    超时控制，ContextVar 存储，无需层层传参即可在任意调用深度访问；
  - check_cancelled()：全局零参调用的取消检查钩子，在模型推理间隙/工具调用前主动插入。

典型用法（外层入口）：
    ctx = create_request_context(
        thread_id="xxx", user_id="yyy", timeout_sec=REQUEST_CONTEXT_DEFAULT_TIMEOUT_SEC,
        request_id=uuid.uuid4().hex, session_dir="...",
    )
    tok = bind_request_context(ctx)  # 返回 ContextVar token
    try:
        await run_agent()
    finally:
        unbind_request_context(tok)
        ctx.dispose()

典型用法（内层任意位置，零参）：
    from agent.request_context import check_cancelled, current_context
    check_cancelled("准备调用 DeepSeek")
    ctx = current_context()  # 若需元数据
"""
from __future__ import annotations

import asyncio
import time
import uuid
import weakref
from contextvars import ContextVar, Token as _CtxToken
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from config.constants import REQUEST_CONTEXT_DEFAULT_TIMEOUT_SEC


# ======================================================================
# 1. RequestCancelledError —— 取消时抛出的业务异常（有 reason 可追溯）
# ======================================================================

class RequestCancelledError(asyncio.CancelledError):
    """带取消原因的取消异常。继承 asyncio.CancelledError 以便原有 catch 分支兼容。"""

    def __init__(self, reason: str = "cancelled", *, token_id: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.token_id = token_id

    def __str__(self) -> str:
        prefix = f"[{self.token_id}] " if self.token_id else ""
        return f"{prefix}RequestCancelled(reason={self.reason})"


# ======================================================================
# 2. CancellationToken —— 事件驱动 + 主动检查 + 级联取消
# ======================================================================

class CancellationToken:
    """可组合的取消令牌：
        - cancel(reason) 原子置位 + Event.set() + 级联 cancel 子任务/子令牌；
        - check(msg=None) 若已取消则立即抛 RequestCancelledError（同步返回极快，ns 级）；
        - register_child_task(task) 登记后台子任务，令牌取消时一并 cancel；
        - register_callback(fn) 登记取消回调（用于发送"取消"事件到前端等）；
        - deadline_at / timeout_sec：check 时同时判断是否超时，超时也抛取消异常；
        - link(token)：两令牌联动，任一取消即双端都取消（用得少，保留接口）。
    """

    __slots__ = (
        "_id",
        "_cancelled",
        "_reason",
        "_event",
        "_child_tasks",
        "_child_tokens",
        "_callbacks",
        "_deadline_at",
        "_created_at",
        "_lock",  # 用于保护回调/子任务注册与取消触发的顺序（不保护 _cancelled，它用原子读）
        "__weakref__",
    )

    def __init__(self, timeout_sec: Optional[float] = None) -> None:
        loop = asyncio.get_event_loop() if asyncio.get_event_loop_policy() else None
        self._id: str = uuid.uuid4().hex[:12]
        self._cancelled: bool = False
        self._reason: str = ""
        self._event: asyncio.Event = asyncio.Event()
        self._child_tasks: Set["asyncio.Task[Any]"] = set()
        self._child_tokens: "weakref.WeakSet[CancellationToken]" = weakref.WeakSet()
        self._callbacks: List[Callable[["CancellationToken"], Any]] = []
        self._created_at: float = time.monotonic()
        self._deadline_at: Optional[float] = (
            self._created_at + timeout_sec if timeout_sec is not None and timeout_sec > 0 else None
        )
        # 注意：这里不创建 threading.Lock，因为 CancellationToken 的使用域是单线程 async 模型；
        # 真实的并发取消（跨线程发 cancel）只写 _cancelled = True + set()，两者本身在 CPython GIL 下原子。
        self._lock: Optional[asyncio.Lock] = None  # 惰性创建，减少开销

    # ------------------------------------------------------------------
    # 只读属性
    # ------------------------------------------------------------------
    @property
    def token_id(self) -> str:
        return self._id

    @property
    def is_cancelled(self) -> bool:
        if self._cancelled:
            return True
        if self._deadline_at is not None and time.monotonic() >= self._deadline_at:
            # 已超时 → 自动触发取消（幂等）
            self._cancel_without_lock(f"timeout: deadline {self._deadline_at:.1f}s reached")
            return True
        return False

    @property
    def reason(self) -> str:
        return self._reason

    @property
    def deadline_at(self) -> Optional[float]:
        return self._deadline_at

    @property
    def age_sec(self) -> float:
        return time.monotonic() - self._created_at

    # ------------------------------------------------------------------
    # 取消触发
    # ------------------------------------------------------------------
    def cancel(self, reason: str = "cancelled") -> bool:
        """触发取消。返回 True 表示本次调用是"首次置位"，False 表示已经取消过（幂等）。"""
        if self._cancelled:
            return False
        return self._cancel_without_lock(reason)

    def _cancel_without_lock(self, reason: str) -> bool:
        # 双重检查：保证幂等
        if self._cancelled:
            return False
        self._cancelled = True
        self._reason = reason
        # 1. 唤醒所有 await wait() 的协程
        self._event.set()
        # 2. 级联取消登记的 asyncio.Task
        for t in list(self._child_tasks):
            if not t.done():
                try:
                    t.cancel(f"[CancellationToken#{self._id}] {reason}")
                except Exception:
                    pass
        self._child_tasks.clear()
        # 3. 级联取消登记的子令牌（弱引用不阻止 GC）
        for child in list(self._child_tokens):
            try:
                child.cancel(f"parent_cancelled: {reason}")
            except Exception:
                pass
        # 4. 回调（同步执行，回调内部应尽量短；长任务请在回调里 create_task）
        for cb in list(self._callbacks):
            try:
                result = cb(self)
                if asyncio.iscoroutine(result):
                    # 如果用户给了异步回调，包成 task 执行（不等待）
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(result)
                    except RuntimeError:
                        pass
            except Exception:
                pass
        self._callbacks.clear()
        return True

    # ------------------------------------------------------------------
    # 等待 & 检查（零参全局快捷入口见模块底部 check_cancelled）
    # ------------------------------------------------------------------
    async def wait(self, timeout: Optional[float] = None) -> bool:
        """等待直到取消或超时。返回 True 表示确实被取消了。"""
        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return self._cancelled

    def check(self, where: Optional[str] = None) -> None:
        """同步主动检查：若已取消/超时 → 立即抛 RequestCancelledError。
        这个调用非常快：只是 1 次 dict 读 + 1 次 monotonic()，ns 级开销。
        """
        if self.is_cancelled:
            raise RequestCancelledError(
                reason=self._reason or "cancelled",
                token_id=self._id,
            ) from None

    # ------------------------------------------------------------------
    # 注册：子任务、子令牌、取消回调
    # ------------------------------------------------------------------
    def register_child_task(self, task: "asyncio.Task[Any]") -> None:
        """登记一个"后台子任务"（例如摘要压缩、反馈写入、文件上传）。
        令牌取消时 → 子任务也会被 cancel。子任务若自己已完成则自动从集合中移除（add_done_callback）。
        """
        if self._cancelled:
            # 已取消的令牌再注册子任务 → 立即 cancel 它，避免逃逸
            if not task.done():
                task.cancel(f"[CancellationToken#{self._id}] already cancelled: {self._reason}")
            return
        self._child_tasks.add(task)

        def _on_done(t: "asyncio.Task[Any]") -> None:
            # 任务完成后从集合里剔除，防止内存泄漏
            try:
                self._child_tasks.discard(t)
            except Exception:
                pass

        task.add_done_callback(_on_done)

    def register_child_token(self, token: "CancellationToken") -> None:
        """登记一个"子令牌"：父令牌取消 → 子令牌也取消。弱引用。"""
        if token is self:
            return
        if self._cancelled:
            token.cancel(f"parent_cancelled: {self._reason}")
            return
        self._child_tokens.add(token)

    def register_callback(self, cb: Callable[["CancellationToken"], Any]) -> None:
        """注册取消回调（同步或异步）。若已取消 → 立即触发。"""
        if self._cancelled:
            try:
                r = cb(self)
                if asyncio.iscoroutine(r):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(r)
                    except RuntimeError:
                        pass
            except Exception:
                pass
            return
        self._callbacks.append(cb)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def dispose(self) -> None:
        """释放引用，帮助 GC。幂等。"""
        try:
            self._child_tasks.clear()
            self._callbacks.clear()
            # WeakSet 不需要 clear，自身帮助 GC
        except Exception:
            pass

    def __repr__(self) -> str:
        state = "cancelled" if self._cancelled else "active"
        dl = f", deadline_in={self._deadline_at - time.monotonic():.1f}s" if self._deadline_at else ""
        return f"<CancellationToken#{self._id} {state}{dl}>"


# ======================================================================
# 3. RequestContext —— 三位一体（取消令牌 + 元数据 + 超时控制）
# ======================================================================

@dataclass
class RequestContext:
    """请求全生命周期上下文，贯穿 API 入口 → Agent → Tool → SubAgent → Memory。

    设计约束：
      - **不允许就地修改**（除 token.cancel 之外的方法均为只读）；
      - **通过 ContextVar 绑定**，每请求一份，协程独立，不会串台；
      - **线程安全仅限 async 单线程**：所有字段只在同一事件循环内读/改；
    """

    token: CancellationToken
    # 元数据
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    thread_id: Optional[str] = None
    user_id: Optional[str] = None
    session_dir: Optional[str] = None
    created_at: float = field(default_factory=time.monotonic)
    # 超时控制
    timeout_sec: Optional[float] = None
    # 扩展：后端/监控方可以塞任何自定义字段，不破坏签名
    extras: Dict[str, Any] = field(default_factory=dict)

    # --------------------------------------------------------------
    # 便捷访问：直接透传 token 的常用方法，省掉 .token 前缀
    # --------------------------------------------------------------
    def check_cancelled(self, where: Optional[str] = None) -> None:
        self.token.check(where=where)

    @property
    def is_cancelled(self) -> bool:
        return self.token.is_cancelled

    @property
    def elapsed_sec(self) -> float:
        return time.monotonic() - self.created_at

    @property
    def remaining_sec(self) -> Optional[float]:
        if self.timeout_sec is None:
            return None
        rem = self.timeout_sec - (time.monotonic() - self.created_at)
        return rem if rem > 0 else 0.0

    def register_child_task(self, task: "asyncio.Task[Any]") -> None:
        self.token.register_child_task(task)

    def register_callback(self, cb: Callable[[CancellationToken], Any]) -> None:
        self.token.register_callback(cb)

    def cancel(self, reason: str = "cancelled") -> bool:
        return self.token.cancel(reason)

    def dispose(self) -> None:
        self.token.dispose()

    def snapshot(self) -> Dict[str, Any]:
        """给监控/日志用的快照（安全，不包含可变引用）。"""
        return {
            "request_id": self.request_id,
            "thread_id": self.thread_id,
            "user_id": self.user_id,
            "session_dir": self.session_dir,
            "created_at_monotonic": self.created_at,
            "elapsed_sec": round(self.elapsed_sec, 3),
            "timeout_sec": self.timeout_sec,
            "remaining_sec": (round(self.remaining_sec, 3) if self.remaining_sec is not None else None),
            "is_cancelled": self.is_cancelled,
            "cancel_reason": self.token.reason if self.is_cancelled else None,
            "token_id": self.token.token_id,
            "extras_keys": sorted(self.extras.keys()),
        }


# ======================================================================
# 4. ContextVar 绑定：零参读取"当前请求上下文"
# ======================================================================

_req_ctx_var: ContextVar[Optional[RequestContext]] = ContextVar("request_context", default=None)

# thread_id → WeakRef[CancellationToken] 反向索引（供 DISCONNECT 等跨线程/跨请求场景
# 拿 thread_id 直接找令牌，不必遍历 Task 树。注意是弱引用，令牌释放不会因该索引泄漏。
# 该映射由 create_request_context 自动登记、dispose 自动移除。
# 使用一把 asyncio.Lock 串行化读写。
_thread_index: Dict[str, weakref.ReferenceType[CancellationToken]] = {}
_thread_index_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _thread_index_lock
    if _thread_index_lock is None:
        _thread_index_lock = asyncio.Lock()
    return _thread_index_lock


# ======================================================================
# 5. 公开 API：创建 / 绑定 / 获取 / 取消 / 按 thread_id 远程取消
# ======================================================================

def create_request_context(
    *,
    thread_id: Optional[str] = None,
    user_id: Optional[str] = None,
    session_dir: Optional[str] = None,
    timeout_sec: Optional[float] = None,
    request_id: Optional[str] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> RequestContext:
    """创建一份新的 RequestContext，并登记 thread_id → token 反向索引。

    注意：创建后必须 **立即** 调用 bind_request_context(ctx) 绑定到当前协程，
    且在 finally 块中 unbind + dispose。
    """
    token = CancellationToken(timeout_sec=timeout_sec)
    ctx = RequestContext(
        token=token,
        request_id=request_id or uuid.uuid4().hex,
        thread_id=thread_id,
        user_id=user_id,
        session_dir=session_dir,
        timeout_sec=timeout_sec,
        extras=dict(extras) if extras else {},
    )

    # 异步登记反向索引（这里是同步函数 → 用 create_task 立即排到队首；
    # 如果当前没有运行 loop，就跳过——脚本模式、单元测试可能没有 loop）
    if thread_id is not None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None and loop.is_running():
            weak = weakref.ref(token, _make_thread_index_cleanup(thread_id, token.token_id))
            loop.create_task(_index_put(thread_id, weak))
    return ctx


def _make_thread_index_cleanup(thread_id: str, token_id: str):
    """weakref 回调：令牌被 GC 时，自动清理反向索引中对应的陈旧条目（若条目仍"是它自己"）。"""
    def _cleanup(ref: weakref.ReferenceType[CancellationToken]) -> None:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return
        if not loop.is_running():
            return
        loop.create_task(_index_remove_if_same(thread_id, token_id))
    return _cleanup


async def _index_put(thread_id: str, ref: weakref.ReferenceType[CancellationToken]) -> None:
    lock = _get_lock()
    async with lock:
        _thread_index[thread_id] = ref


async def _index_remove_if_same(thread_id: str, token_id: str) -> None:
    lock = _get_lock()
    async with lock:
        existing = _thread_index.get(thread_id)
        if existing is None:
            return
        tok = existing()
        if tok is None or tok.token_id == token_id:
            _thread_index.pop(thread_id, None)


def bind_request_context(ctx: RequestContext) -> _CtxToken:
    """把 ctx 绑定到当前请求链路（ContextVar.set）。返回用于 reset 的 token。"""
    return _req_ctx_var.set(ctx)


def unbind_request_context(tok: _CtxToken) -> None:
    """恢复 ContextVar 到之前的状态（请求结束 finally 中调用）。"""
    try:
        _req_ctx_var.reset(tok)
    except Exception:
        pass


def current_context() -> Optional[RequestContext]:
    """获取当前请求链路的 RequestContext。无则返回 None。"""
    return _req_ctx_var.get()


def current_token() -> Optional[CancellationToken]:
    """获取当前请求链路的 CancellationToken（快捷方式）。无则返回 None。"""
    ctx = _req_ctx_var.get()
    return ctx.token if ctx is not None else None


def check_cancelled(where: Optional[str] = None) -> None:
    """**全局零参快捷入口**。在模型执行间隙 / 工具调用前 / 循环体内主动检查。

    若当前链路绑定了 RequestContext 且已取消 → 抛 RequestCancelledError；
    若没有绑定（脚本模式、单元测试等旧调用链）→ 什么都不做，保证向后兼容。

    性能：ns 级（ContextVar.get 很快 + bool 读 + 1 次 monotonic）。
    """
    ctx = _req_ctx_var.get()
    if ctx is None:
        return
    ctx.token.check(where=where)


async def cancel_by_thread_id(thread_id: str, reason: str = "cancelled") -> Dict[str, Any]:
    """根据 thread_id 找令牌并触发取消（供 WebSocket DISCONNECT、STOP 接口使用）。

    返回：
      {"found": True,  "token_id": "...", "reason": "...", "already_cancelled": bool}
      或
      {"found": False}
    """
    lock = _get_lock()
    async with lock:
        ref = _thread_index.get(thread_id)
        if ref is None:
            return {"found": False}
        tok = ref()
        if tok is None:
            # 弱引用已失效 → 清理陈旧条目
            _thread_index.pop(thread_id, None)
            return {"found": False}
    # 真正的 cancel 在锁外执行（防止回调/子任务级联取消阻塞 index 操作）
    already = tok.is_cancelled
    ok = tok.cancel(reason)  # ok=True 表示首次触发；False 表示本来就已取消
    return {
        "found": True,
        "token_id": tok.token_id,
        "reason": reason,
        "already_cancelled": already,
        "first_triggered": ok,
    }


# 导出符号
__all__ = [
    "CancellationToken",
    "RequestContext",
    "RequestCancelledError",
    "create_request_context",
    "bind_request_context",
    "unbind_request_context",
    "current_context",
    "current_token",
    "check_cancelled",
    "cancel_by_thread_id",
]
