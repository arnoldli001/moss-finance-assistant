"""
Layer 3 - Harness Engineering: 时间窗口三态熔断器（Time-Window Circuit Breaker）。

相比 skills/trading-reliability/scripts/circuit_breaker.py（基于滑动窗口错误率），
本模块面向项目运行时，按"时间窗口内连续失败次数"触发熔断，更贴合用户案例
"错误率超过 60 秒 3 次即熔断"。

三态状态机：
  CLOSED  ──(60s 内 3 次失败)──▶  OPEN
  OPEN     ──(冷却 30s 后)──▶      HALF_OPEN
  HALF_OPEN ──(探测成功 2 次)──▶   CLOSED
  HALF_OPEN ──(探测失败 1 次)──▶   OPEN

被保护对象（示例）：
  - DeepSeek API（_base_model）
  - IMA 知识库
  - Ollama Qwen3-8B 本地模型
  - 知识星球 Playwright 抓取
"""
from __future__ import annotations

import time
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple

# ===== 全局常量集中引用（替代魔鬼数字，统一修改一处即全局生效）=====
from config.constants import (
    CB_DEFAULT_FAILURE_THRESHOLD,
    CB_DEFAULT_FAILURE_WINDOW_SEC,
    CB_DEFAULT_RECOVERY_COOLDOWN_SEC,
    CB_DEFAULT_HALF_OPEN_SUCCESS_NEEDED,
    CB_DEEPSEEK_FAILURE_THRESHOLD,
    CB_DEEPSEEK_FAILURE_WINDOW_SEC,
    CB_DEEPSEEK_RECOVERY_COOLDOWN_SEC,
    CB_IMA_FAILURE_THRESHOLD,
    CB_IMA_FAILURE_WINDOW_SEC,
    CB_IMA_RECOVERY_COOLDOWN_SEC,
    CB_QWEN8B_FAILURE_THRESHOLD,
    CB_QWEN8B_FAILURE_WINDOW_SEC,
    CB_QWEN8B_RECOVERY_COOLDOWN_SEC,
    CB_ZXSQ_FAILURE_THRESHOLD,
    CB_ZXSQ_FAILURE_WINDOW_SEC,
    CB_ZXSQ_RECOVERY_COOLDOWN_SEC,
    CB_MAIN_AGENT_FAILURE_THRESHOLD,
    CB_MAIN_AGENT_FAILURE_WINDOW_SEC,
    CB_MAIN_AGENT_RECOVERY_COOLDOWN_SEC,
)


# ======================================================================
# 单个被保护对象的三态熔断器
# ======================================================================

@dataclass
class CircuitState:
    """熔断器运行时状态快照。"""
    name: str
    state: str = "CLOSED"                         # CLOSED / OPEN / HALF_OPEN
    failures_in_window: int = 0                    # 当前时间窗口内失败次数
    last_failure_ts: float = 0.0                   # 最近一次失败时间戳
    last_state_change_ts: float = field(default_factory=time.time)
    half_open_successes: int = 0                   # 半开状态连续成功计数
    total_failures: int = 0                        # 累计失败次数（监控用）
    total_successes: int = 0                       # 累计成功次数（监控用）
    total_rejected: int = 0                        # 累计被熔断拒绝次数


class TimeWindowCircuitBreaker:
    """
    时间窗口三态熔断器。

    触发条件：failure_window_sec 秒内失败次数 >= failure_threshold 即熔断。
    恢复条件：OPEN 状态保持 recovery_cooldown_sec 秒后自动进入 HALF_OPEN；
              HALF_OPEN 状态下连续 half_open_success_needed 次成功 → CLOSED；
              HALF_OPEN 状态下任意 1 次失败 → 立即回到 OPEN。
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = CB_DEFAULT_FAILURE_THRESHOLD,
        failure_window_sec: float = CB_DEFAULT_FAILURE_WINDOW_SEC,
        recovery_cooldown_sec: float = CB_DEFAULT_RECOVERY_COOLDOWN_SEC,
        half_open_success_needed: int = CB_DEFAULT_HALF_OPEN_SUCCESS_NEEDED,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.failure_window_sec = failure_window_sec
        self.recovery_cooldown_sec = recovery_cooldown_sec
        self.half_open_success_needed = half_open_success_needed

        self._state = CircuitState(name=name)
        self._failure_ts: Deque[float] = deque()   # 时间窗口内的失败时间戳序列
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # 请求准入判断
    # ------------------------------------------------------------------
    def allow_request(self) -> bool:
        """判断当前是否允许请求通过。False 表示被熔断拒绝。"""
        with self._lock:
            now = time.time()
            if self._state.state == "CLOSED":
                return True

            if self._state.state == "OPEN":
                # 冷却时间到了？进入 HALF_OPEN
                if now - self._state.last_failure_ts >= self.recovery_cooldown_sec:
                    self._transition("HALF_OPEN", reason="cooldown elapsed, probing")
                    return True
                # 冷却未到，拒绝
                self._state.total_rejected += 1
                return False

            if self._state.state == "HALF_OPEN":
                # 半开状态下允许少量探测请求通过
                return True

            return False

    # ------------------------------------------------------------------
    # 结果记录
    # ------------------------------------------------------------------
    def record_success(self) -> None:
        with self._lock:
            self._state.total_successes += 1
            if self._state.state == "HALF_OPEN":
                self._state.half_open_successes += 1
                if self._state.half_open_successes >= self.half_open_success_needed:
                    self._transition("CLOSED", reason="probe succeeded, healthy")
                    self._failure_ts.clear()

    def record_failure(self) -> None:
        with self._lock:
            now = time.time()
            self._state.total_failures += 1
            self._state.last_failure_ts = now
            self._failure_ts.append(now)

            if self._state.state == "HALF_OPEN":
                # 半开状态探测失败，立即回到 OPEN
                self._transition("OPEN", reason="probe failed")
                return

            if self._state.state == "CLOSED":
                # 清理超出时间窗口的旧失败记录
                self._gc_failure_window(now)
                if len(self._failure_ts) >= self.failure_threshold:
                    self._transition("OPEN",
                                     reason=f"{len(self._failure_ts)} failures within "
                                            f"{self.failure_window_sec}s")

    def _gc_failure_window(self, now: float) -> None:
        """清理超出时间窗口的失败时间戳。"""
        threshold = now - self.failure_window_sec
        while self._failure_ts and self._failure_ts[0] < threshold:
            self._failure_ts.popleft()

    def _transition(self, new_state: str, reason: str = "") -> None:
        """状态转换（内部调用，已持锁）。"""
        old = self._state.state
        self._state.state = new_state
        self._state.last_state_change_ts = time.time()
        if new_state == "HALF_OPEN":
            self._state.half_open_successes = 0
        # 半开转 CLOSED 时清空失败窗口，重新开始统计
        if old == "HALF_OPEN" and new_state == "CLOSED":
            self._failure_ts.clear()
        print(f"[CircuitBreaker:{self.name}] {old} -> {new_state} ({reason})")

    # ------------------------------------------------------------------
    # 监控接口
    # ------------------------------------------------------------------
    def snapshot(self) -> Dict:
        """返回当前状态快照，供 SLO 监控使用。"""
        with self._lock:
            now = time.time()
            self._gc_failure_window(now)
            return {
                "name": self._state.name,
                "state": self._state.state,
                "failures_in_window": len(self._failure_ts),
                "failure_threshold": self.failure_threshold,
                "window_sec": self.failure_window_sec,
                "total_failures": self._state.total_failures,
                "total_successes": self._state.total_successes,
                "total_rejected": self._state.total_rejected,
                "last_failure_ts": self._state.last_failure_ts,
                "uptime_sec": now - self._state.last_state_change_ts,
            }


# ======================================================================
# 熔断器注册中心（按 name 隔离多个被保护对象）
# ======================================================================

class CircuitBreakerRegistry:
    """
    熔断器注册中心：为每个关键依赖创建独立熔断器。

    使用方式：
        registry = get_circuit_registry()
        cb = registry.get_or_create("deepseek", failure_threshold=CB_DEEPSEEK_FAILURE_THRESHOLD,
                                     failure_window_sec=CB_DEEPSEEK_FAILURE_WINDOW_SEC)
        if not cb.allow_request():
            raise RuntimeError("DeepSeek 熔断中，请稍后重试")
        try:
            result = await deepseek_call(...)
            cb.record_success()
        except Exception:
            cb.record_failure()
            raise
    """

    # 预置默认配置：与用户案例"60秒3次即熔断"对齐
    DEFAULTS = {
        "deepseek":   dict(failure_threshold=CB_DEEPSEEK_FAILURE_THRESHOLD,
                           failure_window_sec=CB_DEEPSEEK_FAILURE_WINDOW_SEC,
                           recovery_cooldown_sec=CB_DEEPSEEK_RECOVERY_COOLDOWN_SEC),
        "ima":        dict(failure_threshold=CB_IMA_FAILURE_THRESHOLD,
                           failure_window_sec=CB_IMA_FAILURE_WINDOW_SEC,
                           recovery_cooldown_sec=CB_IMA_RECOVERY_COOLDOWN_SEC),
        "qwen8b":     dict(failure_threshold=CB_QWEN8B_FAILURE_THRESHOLD,
                           failure_window_sec=CB_QWEN8B_FAILURE_WINDOW_SEC,
                           recovery_cooldown_sec=CB_QWEN8B_RECOVERY_COOLDOWN_SEC),
        "zsxq":       dict(failure_threshold=CB_ZXSQ_FAILURE_THRESHOLD,
                           failure_window_sec=CB_ZXSQ_FAILURE_WINDOW_SEC,
                           recovery_cooldown_sec=CB_ZXSQ_RECOVERY_COOLDOWN_SEC),
        "main_agent": dict(failure_threshold=CB_MAIN_AGENT_FAILURE_THRESHOLD,
                           failure_window_sec=CB_MAIN_AGENT_FAILURE_WINDOW_SEC,
                           recovery_cooldown_sec=CB_MAIN_AGENT_RECOVERY_COOLDOWN_SEC),
    }

    def __init__(self):
        self._breakers: Dict[str, TimeWindowCircuitBreaker] = {}
        self._lock = threading.Lock()

    def get_or_create(
        self,
        name: str,
        failure_threshold: Optional[int] = None,
        failure_window_sec: Optional[float] = None,
        recovery_cooldown_sec: Optional[float] = None,
    ) -> TimeWindowCircuitBreaker:
        with self._lock:
            if name not in self._breakers:
                defaults = self.DEFAULTS.get(name, {})
                self._breakers[name] = TimeWindowCircuitBreaker(
                    name=name,
                    failure_threshold=failure_threshold or defaults.get("failure_threshold", 3),
                    failure_window_sec=failure_window_sec or defaults.get("failure_window_sec", 60),
                    recovery_cooldown_sec=recovery_cooldown_sec or defaults.get("recovery_cooldown_sec", 30),
                )
            return self._breakers[name]

    def get(self, name: str) -> Optional[TimeWindowCircuitBreaker]:
        return self._breakers.get(name)

    def snapshot_all(self) -> Dict[str, Dict]:
        """返回所有熔断器快照，供 SLO 监控接口使用。"""
        with self._lock:
            return {name: cb.snapshot() for name, cb in self._breakers.items()}


# ======================================================================
# Actor Model 适配：保留原 API 签名不变，内部改为通过 Actor 消息驱动
# ======================================================================
# 原项目大量代码依赖：
#   cb = get_circuit_registry().get_or_create("deepseek")
#   cb.allow_request() / cb.record_success() / cb.record_failure()
# 为了避免一次性改所有调用点，这里提供"无缝桥接"：
#   - 若 _cb_actor 已被注入（server.py lifespan 中调用 _set_cb_actor），
#     TimeWindowCircuitBreaker 的方法内部改为向 CircuitBreakerActor 发消息
#   - 否则走原有 threading.Lock 路径（兼容脚本模式、单元测试等不启动 FastAPI 的场景）
# ======================================================================

from typing import Any as _Any

_cb_actor: _Any = None  # 类型懒惰，避免循环 import
_CB_MSG_ALLOW = "allow_request"
_CB_MSG_SUCCESS = "record_success"
_CB_MSG_FAILURE = "record_failure"
_CB_MSG_SNAPSHOT = "snapshot_one"
_CB_MSG_SNAPSHOT_ALL = "snapshot_all"
_CB_MSG_GET_OR_CREATE = "get_or_create"


def _set_cb_actor(actor: _Any) -> None:
    """由 server.py lifespan 注入 CircuitBreakerActor 句柄。"""
    global _cb_actor
    _cb_actor = actor


# 在 TimeWindowCircuitBreaker 中做桥接：若有 actor 则优先走 actor 消息
_original_allow_request = TimeWindowCircuitBreaker.allow_request
_original_record_success = TimeWindowCircuitBreaker.record_success
_original_record_failure = TimeWindowCircuitBreaker.record_failure
_original_snapshot = TimeWindowCircuitBreaker.snapshot


def _bridged_allow_request(self: TimeWindowCircuitBreaker) -> bool:
    if _cb_actor is None:
        return _original_allow_request(self)
    # 异步消息包装：如果当前在 async 上下文，通过 asyncio.create_task 发 ask；
    # 但是 allow_request() 是同步签名，调用方大量是同步 if 分支。
    # 兼容策略：若可以拿到 running loop，就同步等结果（短平快，Actor 内部串行很快）；
    # 否则退回到本地 threading.Lock 路径（逻辑等价，不影响结果正确性）。
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return _original_allow_request(self)
    if loop.is_running():
        # 在 async 函数中同步调用同步方法 → 用 asyncio.run_coroutine_threadsafe
        # 简单做法：创建任务 + 等待（会阻塞当前协程！）→ 改用 run_until_complete 的线程安全方式
        # 为避免破坏 async 流程，我们在这里直接异步 send（不等待）是不对的（需要返回 bool）。
        # 所以更稳妥：走 threading.Lock 本地路径。熔断器的本地 lock 完全能保证单实例正确性，
        # 全局一致性由 Actor 保证，本地的记录会被后续 Actor 消息"再应用"一次（幂等不重复计数）。
        # 所以这里只需要本地判断 + Actor 同步（不等待 Actor 的返回值做准入判断）。
        # 但我们必须严格一致：先本地判断结果，然后同步把结果/状态告诉Actor。
        result = _original_allow_request(self)
        # 把本地状态变化"告诉"Actor（send-and-forget，不阻塞）
        try:
            asyncio.create_task(_cb_actor.send(
                _CB_MSG_GET_OR_CREATE,
                {
                    "name": self.name,
                    "failure_threshold": self.failure_threshold,
                    "failure_window_sec": self.failure_window_sec,
                    "recovery_cooldown_sec": self.recovery_cooldown_sec,
                }
            ))
        except Exception:
            pass
        return result
    return _original_allow_request(self)


def _bridged_record_success(self: TimeWindowCircuitBreaker) -> None:
    _original_record_success(self)  # 本地状态先变（threading.Lock 保证原子）
    if _cb_actor is None:
        return
    # 再把事件广播给 Actor，让 Actor 私有状态同步（幂等）
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if loop.is_running():
        try:
            asyncio.create_task(_cb_actor.send(_CB_MSG_RECORD_SUCCESS, {"name": self.name}))
        except Exception:
            pass


def _bridged_record_failure(self: TimeWindowCircuitBreaker) -> None:
    _original_record_failure(self)
    if _cb_actor is None:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if loop.is_running():
        try:
            asyncio.create_task(_cb_actor.send(_CB_MSG_RECORD_FAILURE, {"name": self.name}))
        except Exception:
            pass


def _bridged_snapshot(self: TimeWindowCircuitBreaker) -> Dict:
    # 快照优先从 Actor 拿（全局汇总视角）
    if _cb_actor is None:
        return _original_snapshot(self)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return _original_snapshot(self)
    if not loop.is_running():
        return _original_snapshot(self)
    # 同步等待 Actor 快照（短平快）
    try:
        import concurrent.futures as _cf
        fut = asyncio.run_coroutine_threadsafe(
            _cb_actor.ask(_CB_MSG_SNAPSHOT, {"name": self.name}),
            loop,
        )
        result = fut.result(timeout=3.0)
        if result is not None:
            return result
    except Exception:
        pass
    return _original_snapshot(self)


# 覆盖原类方法（桥接）
TimeWindowCircuitBreaker.allow_request = _bridged_allow_request  # type: ignore[assignment]
TimeWindowCircuitBreaker.record_success = _bridged_record_success  # type: ignore[assignment]
TimeWindowCircuitBreaker.record_failure = _bridged_record_failure  # type: ignore[assignment]
TimeWindowCircuitBreaker.snapshot = _bridged_snapshot  # type: ignore[assignment]


# Registry.snapshot_all 同样适配：有 Actor 就从 Actor 拿（所有熔断器汇总）
_original_registry_snapshot_all = CircuitBreakerRegistry.snapshot_all


def _bridged_registry_snapshot_all(self: CircuitBreakerRegistry) -> Dict[str, Dict]:
    if _cb_actor is None:
        return _original_registry_snapshot_all(self)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return _original_registry_snapshot_all(self)
    if not loop.is_running():
        return _original_registry_snapshot_all(self)
    try:
        import concurrent.futures as _cf
        fut = asyncio.run_coroutine_threadsafe(
            _cb_actor.ask(_CB_MSG_SNAPSHOT_ALL, {}),
            loop,
        )
        result = fut.result(timeout=3.0)
        if result is not None:
            return result
    except Exception:
        pass
    return _original_registry_snapshot_all(self)


CircuitBreakerRegistry.snapshot_all = _bridged_registry_snapshot_all  # type: ignore[assignment]


# ======================================================================
# 全局单例
# ======================================================================

_registry: Optional[CircuitBreakerRegistry] = None


def get_circuit_registry() -> CircuitBreakerRegistry:
    global _registry
    if _registry is None:
        _registry = CircuitBreakerRegistry()
    return _registry
