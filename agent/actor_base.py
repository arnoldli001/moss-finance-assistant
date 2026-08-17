"""
Actor Model 基础框架 —— 用"私有状态 + 消息邮箱 + 串行处理"替代共享可变状态。

核心思想：
  next_state = f(current_state, input)   —— 纯函数状态转换
  外部从不直接碰 Actor 内部状态，只能发消息；
  内部单协程串行从邮箱取消息 → 执行转换 → 产生新状态。

为什么不用 threading.Lock / asyncio.Lock 直接凑合？
  锁只能解决"不冲突"，解决不了"状态修改散布各处"：
  - 无法审计所有修改点（修改散落在业务 catch/finally/回调里）
  - 无法重放/回溯状态转换（没有统一 input→output 边界）
  - 异步回调（done_callback / run_coroutine_threadsafe）仍可偷偷改状态

Actor 模型把"状态修改权限"彻底收敛到 handle_message 一个函数内，
外部只能投递消息，天然保证确定性与可追溯性。
"""
from __future__ import annotations

import asyncio
import inspect
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Generic, Optional, TypeVar

from config.constants import ACTOR_MAILBOX_LIMIT_DEFAULT, ACTOR_ASK_DEFAULT_TIMEOUT_SEC


# ======================================================================
# 消息信封：每条消息自带唯一ID + reply_to（供 ask 模式回响应）
# ======================================================================

@dataclass
class Envelope:
    """邮箱中的消息信封。"""
    msg_id: str
    msg_type: str            # 消息类型标识，如 "record_success" / "allow_request"
    payload: Dict[str, Any]  # 业务参数
    # ask 模式下：发送方会创建一个 Future 等待响应，Actor 处理完 set_result
    reply_future: Optional["asyncio.Future[Any]"] = None
    # 投递时间戳（用于监控邮箱积压）
    sent_at: float = field(default_factory=lambda: __import__("time").time())


# ======================================================================
# 通用消息类型常量（集中定义，避免魔法字符串散落）
# ======================================================================

class Msg:
    """Actor 消息类型常量清单。每个 Actor 可按需扩展子类。"""
    # 生命周期
    STOP = "__actor_stop__"
    PING = "__actor_ping__"
    SNAPSHOT = "__actor_snapshot__"  # 请求当前私有状态只读快照


# ======================================================================
# Actor 基类
# ======================================================================

T = TypeVar("T")  # Actor 私有状态类型


class Actor(ABC, Generic[T]):
    """
    Actor 抽象基类。

    用法：
        class CounterActor(Actor[int]):
            def initial_state(self) -> int:
                return 0

            async def handle_message(self, state: int, env: Envelope) -> tuple[int, Any]:
                if env.msg_type == "inc":
                    n = env.payload.get("n", 1)
                    return state + n, None  # (新状态, 响应值)
                if env.msg_type == "get":
                    return state, state     # 状态不变，返回当前值
                return state, None

        actor = CounterActor("counter")
        await actor.start()
        await actor.send("inc", {"n": 5})          # send 模式：发完不管
        value = await actor.ask("get", {})         # ask 模式：等待响应
        await actor.stop()
    """

    def __init__(self, name: str, *, mailbox_limit: int = ACTOR_MAILBOX_LIMIT_DEFAULT):
        self.name = name
        # 消息邮箱：外部向里投递，内部循环单协程串行消费
        self._mailbox: "asyncio.Queue[Envelope]" = asyncio.Queue(maxsize=mailbox_limit)
        # 私有状态 —— 禁止外部直接访问！（Python 没有真正私有，但至少名字上表达意图）
        self.__state: Optional[T] = None
        self._started = False
        self._stopped = False
        # 后台消费循环 Task 句柄
        self._loop_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    @abstractmethod
    def initial_state(self) -> T:
        """返回 Actor 的初始私有状态。由子类实现。"""
        ...

    async def start(self) -> None:
        """启动 Actor：初始化状态 + 启动消费循环。幂等。"""
        if self._started:
            return
        self.__state = self.initial_state()
        self._loop_task = asyncio.create_task(
            self._run_loop(),
            name=f"actor_loop_{self.name}",
        )
        self._started = True
        # 打印一行标识，方便日志定位
        print(f"[Actor:{self.name}] 已启动 (pid_state={id(self.__state)})")

    async def stop(self) -> None:
        """停止 Actor：投递 STOP 消息，等待当前邮箱中已有消息处理完后优雅退出。"""
        if not self._started or self._stopped:
            return
        try:
            # STOP 是高优先级，但不插队：保证邮箱中 STOP 之前的消息都处理完
            await self.send(Msg.STOP, {})
            if self._loop_task is not None and not self._loop_task.done():
                await asyncio.wait_for(self._loop_task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            if self._loop_task is not None:
                self._loop_task.cancel()
        finally:
            self._stopped = True
            print(f"[Actor:{self.name}] 已停止")

    # ------------------------------------------------------------------
    # 对外接口：send / ask
    # ------------------------------------------------------------------

    async def send(self, msg_type: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """
        Send-and-Forget 模式：投递消息，**不等待响应，不阻塞**。

        典型用于：事件上报（熔断器记录成功/失败、SLO 记录事件、WS 推送等）。
        """
        if self._stopped:
            # 已停止的 Actor 不再接收消息（避免积压在邮箱里）
            print(f"[Actor:{self.name}] WARN: 已停止，丢弃消息 {msg_type}")
            return
        env = Envelope(
            msg_id=uuid.uuid4().hex[:12],
            msg_type=msg_type,
            payload=payload or {},
        )
        try:
            self._mailbox.put_nowait(env)
        except asyncio.QueueFull:
            # 邮箱爆了通常意味着：Actor 消费跟不上 or 泄漏未 stop
            # 降级：直接丢弃 + 告警，避免生产者被阻塞拖垮整个服务
            print(f"[Actor:{self.name}] ERROR: 邮箱已满，丢弃消息 {msg_type} "
                  f"(积压={self._mailbox.qsize()})")

    async def ask(self, msg_type: str, payload: Optional[Dict[str, Any]] = None,
                  timeout: float = ACTOR_ASK_DEFAULT_TIMEOUT_SEC) -> Any:
        """
        Request-Response 模式：投递消息并**等待响应 Future**。

        典型用于：需要返回值的查询（熔断器 allow_request?、记忆查询、SLO 快照等）。
        """
        if self._stopped:
            raise RuntimeError(f"Actor[{self.name}] 已停止，无法 ask({msg_type})")
        loop = asyncio.get_running_loop()
        reply: "asyncio.Future[Any]" = loop.create_future()
        env = Envelope(
            msg_id=uuid.uuid4().hex[:12],
            msg_type=msg_type,
            payload=payload or {},
            reply_future=reply,
        )
        try:
            self._mailbox.put_nowait(env)
        except asyncio.QueueFull:
            reply.cancel()
            raise RuntimeError(f"Actor[{self.name}] 邮箱已满，ask({msg_type}) 被拒绝")
        # 等待响应（带超时，防止 Actor 卡住导致调用方永久挂起）
        try:
            return await asyncio.wait_for(reply, timeout=timeout)
        except asyncio.TimeoutError:
            reply.cancel()
            raise TimeoutError(f"Actor[{self.name}] ask({msg_type}) 超时 ({timeout}s)")

    # ------------------------------------------------------------------
    # 内部：邮箱消费循环（单协程串行，无并发冲突）
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        """邮箱主循环：串行取信 → 调用 handle_message → 执行纯函数状态转换。"""
        try:
            while True:
                env = await self._mailbox.get()
                try:
                    # ------- next_state = f(current_state, input) 纯函数边界 -------
                    # 所有状态修改只能发生在 handle_message 内部，且必须通过返回值生效。
                    # 外部（回调/工具/其他协程）**无法**绕过这个边界直接改 __state。
                    new_state, response = await self._safe_handle(env)
                    # 用返回值覆盖旧状态：不做就地修改，保持引用透明
                    self.__state = new_state
                    # --------------------------------------------------------------

                    # ask 模式：把响应写回 Future（set_result 线程安全，因为我们在 Actor 协程里）
                    if env.reply_future is not None and not env.reply_future.done():
                        env.reply_future.set_result(response)
                except Exception as exc:
                    # 单条消息处理失败不影响整体循环（隔离故障）
                    print(f"[Actor:{self.name}] 处理消息 {env.msg_type}({env.msg_id}) 异常: {exc}")
                    # ask 模式下也要把异常传递出去，避免调用方死等
                    if env.reply_future is not None and not env.reply_future.done():
                        env.reply_future.set_exception(exc)
                finally:
                    self._mailbox.task_done()

                # STOP 消息（放在 finally 后处理，确保 STOP 之前的消息都已 set_result）
                if env.msg_type == Msg.STOP:
                    print(f"[Actor:{self.name}] 收到 STOP，优雅退出（邮箱剩余 {self._mailbox.qsize()} 条已丢弃）")
                    break
        except asyncio.CancelledError:
            print(f"[Actor:{self.name}] 消费循环被 cancel")
            raise

    async def _safe_handle(self, env: Envelope) -> tuple[T, Any]:
        """
        安全包装 handle_message：
        1. 统一处理 PING / SNAPSHOT 等内置消息
        2. 捕获子类 handle_message 的同步返回 vs await 兼容
        """
        # 内置：PING —— 健康检查
        if env.msg_type == Msg.PING:
            return self.__state, {"ok": True, "name": self.name, "queue": self._mailbox.qsize()}
        # 内置：SNAPSHOT —— 返回私有状态的只读快照（注意：若 state 是可变对象，子类应负责深拷贝）
        if env.msg_type == Msg.SNAPSHOT:
            snap = self._snapshot_state(self.__state)
            return self.__state, snap
        # 业务消息
        result = self.handle_message(self.__state, env)
        if inspect.isawaitable(result):
            result = await result
        # 兼容只返回 new_state 的写法（响应为 None）
        if isinstance(result, tuple) and len(result) == 2:
            return result
        return result, None

    # ------------------------------------------------------------------
    # 子类实现点
    # ------------------------------------------------------------------

    @abstractmethod
    async def handle_message(self, state: T, env: Envelope) -> tuple[T, Any] | T:
        """
        **唯一允许修改状态的函数** —— 纯函数状态转换。

        Args:
            state: 当前私有状态（**只读**，不要就地修改！）
            env:   消息信封

        Returns:
            (new_state, response) 或 new_state：
                - new_state：转换后的完整新状态（赋值给 self.__state）
                - response：ask 模式下返回给调用方的值（send 模式忽略）

        禁止事项（请在 code review 中严格卡）：
            ❌ state.field += 1            # 就地修改旧状态
            ❌ self.__state = new_state    # 直接写私有字段（由基类统一做）
            ❌ await other_actor.send(...) # 可以发消息给其他 Actor，但不要 await 响应做嵌套
        """
        ...

    # ------------------------------------------------------------------
    # 快照钩子：若状态包含可变 dict/list，子类负责深拷贝，防 SNAPSHOT 泄漏引用
    # ------------------------------------------------------------------

    def _snapshot_state(self, state: T) -> Any:
        """返回 state 的深拷贝快照。默认直接返回（不可变类型安全；可变类型子类需覆盖）。"""
        import copy as _copy
        try:
            return _copy.deepcopy(state)
        except Exception:
            return state

    # ------------------------------------------------------------------
    # 便捷属性：邮箱当前积压量（监控用）
    # ------------------------------------------------------------------

    @property
    def mailbox_size(self) -> int:
        return self._mailbox.qsize()

    @property
    def is_running(self) -> bool:
        return self._started and not self._stopped


# ======================================================================
# Actor 注册中心：统一管理所有 Actor 的启动/停止，避免散落各处
# ======================================================================

class ActorSystem:
    """
    全局 Actor 系统：集中注册 + 生命周期管理。

    使用方式（项目入口 lifespan 中）：
        system = ActorSystem()
        system.register("session_registry", SessionRegistryActor())
        system.register("circuit_breaker", CircuitBreakerActor())
        await system.start_all()
        # ... 运行 ...
        await system.stop_all()
    """

    def __init__(self):
        self._actors: Dict[str, Actor] = {}

    def register(self, name: str, actor: Actor) -> None:
        """注册一个 Actor（name 必须唯一）。"""
        if name in self._actors:
            raise ValueError(f"ActorSystem: 重复注册 Actor name={name}")
        self._actors[name] = actor

    def get(self, name: str) -> Actor:
        """按名字取 Actor。"""
        if name not in self._actors:
            raise KeyError(f"ActorSystem: 未注册 Actor name={name}")
        return self._actors[name]

    async def start_all(self) -> None:
        """并发启动所有注册的 Actor。"""
        tasks = [a.start() for a in self._actors.values()]
        if tasks:
            await asyncio.gather(*tasks)
        print(f"[ActorSystem] 已启动 {len(self._actors)} 个 Actor: {list(self._actors.keys())}")

    async def stop_all(self) -> None:
        """并发停止所有注册的 Actor（逆序，被依赖者后停）。"""
        tasks = [a.stop() for a in reversed(self._actors.values())]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        print(f"[ActorSystem] 已停止所有 Actor")

    def snapshot_all(self) -> Dict[str, Dict]:
        """返回所有 Actor 的基本信息快照（监控面板用）。"""
        return {
            name: {
                "running": a.is_running,
                "mailbox_size": a.mailbox_size,
                "state_type": type(a).__name__,
            }
            for name, a in self._actors.items()
        }


# 全局单例 ActorSystem（与项目现有 get_xxx() 风格对齐）
_system: Optional[ActorSystem] = None


def get_actor_system() -> ActorSystem:
    """获取全局 ActorSystem 单例。"""
    global _system
    if _system is None:
        _system = ActorSystem()
    return _system
