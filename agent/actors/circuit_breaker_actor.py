"""
Actor 实现 #2 —— CircuitBreakerActor（熔断器）。

替换 agent/circuit_breaker.py 中的：
  - CircuitBreakerRegistry._breakers 全局字典
  - 每个 TimeWindowCircuitBreaker._state 被多协程就地修改的状态

原问题：
  1. 虽然用了 threading.Lock，但修改散布在 main_agent.py 各处：
     - try块正常结束 → cb.record_success()
     - except异常 → cb.record_failure()
     - 准入检查 → cb.allow_request() 可能触发 OPEN→HALF_OPEN 转换
  2. threading.Lock 解决不了"异步回调中调用 record_failure 跟主流程并发交错"的顺序问题
     （谁先拿到锁谁先改，结果非确定性）

Actor 化后：
  - 所有熔断器状态聚合在一个 Actor 私有状态字典中
  - allow_request / record_success / record_failure 全是消息，邮箱串行执行
  - 顺序 = 消息投递顺序，完全确定可重放
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple

from config.constants import (
    CB_DEEPSEEK_FAILURE_THRESHOLD, CB_DEEPSEEK_FAILURE_WINDOW_SEC, CB_DEEPSEEK_RECOVERY_COOLDOWN_SEC,
    CB_IMA_FAILURE_THRESHOLD, CB_IMA_FAILURE_WINDOW_SEC, CB_IMA_RECOVERY_COOLDOWN_SEC,
    CB_QWEN8B_FAILURE_THRESHOLD, CB_QWEN8B_FAILURE_WINDOW_SEC, CB_QWEN8B_RECOVERY_COOLDOWN_SEC,
    CB_ZXSQ_FAILURE_THRESHOLD, CB_ZXSQ_FAILURE_WINDOW_SEC, CB_ZXSQ_RECOVERY_COOLDOWN_SEC,
    CB_MAIN_AGENT_FAILURE_THRESHOLD, CB_MAIN_AGENT_FAILURE_WINDOW_SEC, CB_MAIN_AGENT_RECOVERY_COOLDOWN_SEC,
    CB_DEFAULT_FAILURE_THRESHOLD, CB_DEFAULT_FAILURE_WINDOW_SEC, CB_DEFAULT_RECOVERY_COOLDOWN_SEC,
    CB_DEFAULT_HALF_OPEN_SUCCESS_NEEDED,
)
from agent.actor_base import Actor, Envelope


# ======================================================================
# 消息类型常量
# ======================================================================

class CBMsg:
    """CircuitBreakerActor 消息类型。"""
    # ---- 读写 ----
    ALLOW_REQUEST = "allow_request"             # 准入检查（可能触发 OPEN→HALF_OPEN）
    RECORD_SUCCESS = "record_success"           # 记录成功（HALF_OPEN 探测成功→CLOSED）
    RECORD_FAILURE = "record_failure"           # 记录失败（可能触发 CLOSED→OPEN）
    # ---- 只读 ----
    SNAPSHOT_ONE = "snapshot_one"               # 单个熔断器快照
    SNAPSHOT_ALL = "snapshot_all"               # 全部熔断器快照（SLO 监控端点用）
    GET_OR_CREATE = "get_or_create"             # 确保某 name 的熔断器存在（惰性初始化）


# ======================================================================
# 单个熔断器的私有状态（纯数据结构，无方法、无 lock）
# ======================================================================

@dataclass
class _BreakerState:
    """单个熔断器的不可变状态结构（每次状态转换构造新对象）。"""
    name: str
    state: str = "CLOSED"                        # CLOSED / OPEN / HALF_OPEN
    failure_threshold: int = CB_DEFAULT_FAILURE_THRESHOLD   # 窗口内失败阈值
    failure_window_sec: float = CB_DEFAULT_FAILURE_WINDOW_SEC  # 失败时间窗口
    recovery_cooldown_sec: float = CB_DEFAULT_RECOVERY_COOLDOWN_SEC  # OPEN→HALF_OPEN 冷却秒数
    half_open_success_needed: int = CB_DEFAULT_HALF_OPEN_SUCCESS_NEEDED  # HALF_OPEN→CLOSED 需要连续成功数
    # 运行时
    half_open_successes: int = 0
    total_failures: int = 0
    total_successes: int = 0
    total_rejected: int = 0
    last_failure_ts: float = 0.0
    last_state_change_ts: float = field(default_factory=time.time)
    # 时间窗口内失败时间戳队列（按到达顺序）
    failure_ts: Deque[float] = field(default_factory=deque)


# ======================================================================
# Actor 私有状态总容器
# ======================================================================

@dataclass
class _CBRegistryState:
    breakers: Dict[str, _BreakerState] = field(default_factory=dict)


# ======================================================================
# 预置默认配置：与原 circuit_breaker.py DEFAULTS 对齐
# ======================================================================

_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "deepseek":   dict(failure_threshold=CB_DEEPSEEK_FAILURE_THRESHOLD, failure_window_sec=CB_DEEPSEEK_FAILURE_WINDOW_SEC, recovery_cooldown_sec=CB_DEEPSEEK_RECOVERY_COOLDOWN_SEC),
    "ima":        dict(failure_threshold=CB_IMA_FAILURE_THRESHOLD, failure_window_sec=CB_IMA_FAILURE_WINDOW_SEC, recovery_cooldown_sec=CB_IMA_RECOVERY_COOLDOWN_SEC),
    "qwen8b":     dict(failure_threshold=CB_QWEN8B_FAILURE_THRESHOLD, failure_window_sec=CB_QWEN8B_FAILURE_WINDOW_SEC, recovery_cooldown_sec=CB_QWEN8B_RECOVERY_COOLDOWN_SEC),
    "zsxq":       dict(failure_threshold=CB_ZXSQ_FAILURE_THRESHOLD, failure_window_sec=CB_ZXSQ_FAILURE_WINDOW_SEC, recovery_cooldown_sec=CB_ZXSQ_RECOVERY_COOLDOWN_SEC),
    "main_agent": dict(failure_threshold=CB_MAIN_AGENT_FAILURE_THRESHOLD, failure_window_sec=CB_MAIN_AGENT_FAILURE_WINDOW_SEC, recovery_cooldown_sec=CB_MAIN_AGENT_RECOVERY_COOLDOWN_SEC),
}


# ======================================================================
# Actor 实现
# ======================================================================

class CircuitBreakerActor(Actor[_CBRegistryState]):
    """
    熔断器 Actor。所有熔断状态修改均在此串行处理，顺序完全确定。

    外部调用示例：
        cb = actor_system.get("circuit_breaker")

        # 准入（ask）
        ok = await cb.ask(CBMsg.ALLOW_REQUEST, {"name": "deepseek"})
        if not ok:
            raise RuntimeError("熔断中")

        # 成功（send，不等待）
        await cb.send(CBMsg.RECORD_SUCCESS, {"name": "deepseek"})

        # 失败（send）
        await cb.send(CBMsg.RECORD_FAILURE, {"name": "deepseek"})
    """

    def initial_state(self) -> _CBRegistryState:
        return _CBRegistryState()

    # ------------------------------------------------------------------
    # 辅助：惰性 get_or_create（不暴露到外部，所有消息内部先确保存在）
    # ------------------------------------------------------------------

    @staticmethod
    def _ensure(state: _CBRegistryState, name: str,
                failure_threshold: Optional[int] = None,
                failure_window_sec: Optional[float] = None,
                recovery_cooldown_sec: Optional[float] = None,
                ) -> Tuple[_CBRegistryState, _BreakerState]:
        """确保 name 熔断器存在，返回 (新注册表, 熔断器状态)。"""
        if name in state.breakers:
            return state, state.breakers[name]
        defaults = _DEFAULTS.get(name, {})
        new_bs = _BreakerState(
            name=name,
            failure_threshold=failure_threshold or defaults.get("failure_threshold", CB_DEFAULT_FAILURE_THRESHOLD),
            failure_window_sec=failure_window_sec or defaults.get("failure_window_sec", CB_DEFAULT_FAILURE_WINDOW_SEC),
            recovery_cooldown_sec=recovery_cooldown_sec or defaults.get("recovery_cooldown_sec", CB_DEFAULT_RECOVERY_COOLDOWN_SEC),
        )
        new_breakers = dict(state.breakers)
        new_breakers[name] = new_bs
        new_state = _CBRegistryState(breakers=new_breakers)
        return new_state, new_bs

    # ------------------------------------------------------------------
    # 辅助：失败窗口 GC
    # ------------------------------------------------------------------

    @staticmethod
    def _gc_window(bs: _BreakerState, now: float) -> Deque[float]:
        threshold = now - bs.failure_window_sec
        dq = deque(bs.failure_ts)
        while dq and dq[0] < threshold:
            dq.popleft()
        return dq

    # ------------------------------------------------------------------
    # handle_message
    # ------------------------------------------------------------------

    async def handle_message(self, state: _CBRegistryState, env: Envelope):
        msg = env.msg_type
        p = env.payload

        # ---- GET_OR_CREATE：确保熔断器存在（幂等）----
        if msg == CBMsg.GET_OR_CREATE:
            name = p["name"]
            new_state, _ = self._ensure(
                state, name,
                failure_threshold=p.get("failure_threshold"),
                failure_window_sec=p.get("failure_window_sec"),
                recovery_cooldown_sec=p.get("recovery_cooldown_sec"),
            )
            return new_state, {"ok": True}

        # ---- ALLOW_REQUEST：准入检查（读+可能写状态转换）----
        if msg == CBMsg.ALLOW_REQUEST:
            name = p["name"]
            state, bs = self._ensure(state, name)
            now = time.time()
            new_bs = bs

            if bs.state == "OPEN":
                if now - bs.last_failure_ts >= bs.recovery_cooldown_sec:
                    # OPEN → HALF_OPEN
                    new_bs = _BreakerState(
                        **{**bs.__dict__,
                           "state": "HALF_OPEN",
                           "half_open_successes": 0,
                           "last_state_change_ts": now,
                           "failure_ts": self._gc_window(bs, now)}
                    )
                    print(f"[CircuitBreakerActor:{name}] OPEN -> HALF_OPEN (cooldown)")
                else:
                    # 拒绝：total_rejected +=1（仍在 OPEN）
                    rejected = bs.total_rejected + 1
                    new_bs = _BreakerState(
                        **{**bs.__dict__, "total_rejected": rejected}
                    )
                    # 返回 False
                    new_breakers = dict(state.breakers)
                    new_breakers[name] = new_bs
                    return _CBRegistryState(breakers=new_breakers), False

            # CLOSED 或 HALF_OPEN 或刚刚转完 HALF_OPEN → 允许
            new_breakers = dict(state.breakers)
            new_breakers[name] = new_bs
            return _CBRegistryState(breakers=new_breakers), True

        # ---- RECORD_SUCCESS：成功记录 ----
        if msg == CBMsg.RECORD_SUCCESS:
            name = p["name"]
            state, bs = self._ensure(state, name)
            now = time.time()
            new_dict = {**bs.__dict__, "total_successes": bs.total_successes + 1}
            if bs.state == "HALF_OPEN":
                new_hos = bs.half_open_successes + 1
                if new_hos >= bs.half_open_success_needed:
                    new_dict["state"] = "CLOSED"
                    new_dict["half_open_successes"] = 0
                    new_dict["failure_ts"] = deque()
                    new_dict["last_state_change_ts"] = now
                    print(f"[CircuitBreakerActor:{name}] HALF_OPEN -> CLOSED (healthy)")
                else:
                    new_dict["half_open_successes"] = new_hos
            new_bs = _BreakerState(**new_dict)
            new_breakers = dict(state.breakers)
            new_breakers[name] = new_bs
            return _CBRegistryState(breakers=new_breakers), None

        # ---- RECORD_FAILURE：失败记录 ----
        if msg == CBMsg.RECORD_FAILURE:
            name = p["name"]
            state, bs = self._ensure(state, name)
            now = time.time()
            new_dq = self._gc_window(bs, now)
            new_dq.append(now)
            new_dict = {
                **bs.__dict__,
                "total_failures": bs.total_failures + 1,
                "last_failure_ts": now,
                "failure_ts": new_dq,
            }
            if bs.state == "HALF_OPEN":
                new_dict["state"] = "OPEN"
                new_dict["last_state_change_ts"] = now
                print(f"[CircuitBreakerActor:{name}] HALF_OPEN -> OPEN (probe failed)")
            elif bs.state == "CLOSED":
                if len(new_dq) >= bs.failure_threshold:
                    new_dict["state"] = "OPEN"
                    new_dict["last_state_change_ts"] = now
                    print(f"[CircuitBreakerActor:{name}] CLOSED -> OPEN "
                          f"({len(new_dq)} failures in {bs.failure_window_sec}s)")
            new_bs = _BreakerState(**new_dict)
            new_breakers = dict(state.breakers)
            new_breakers[name] = new_bs
            return _CBRegistryState(breakers=new_breakers), None

        # ---- SNAPSHOT_ONE：单个熔断器快照 ----
        if msg == CBMsg.SNAPSHOT_ONE:
            name = p["name"]
            if name not in state.breakers:
                return state, None
            bs = state.breakers[name]
            now = time.time()
            cleaned = self._gc_window(bs, now)
            return state, {
                "name": bs.name,
                "state": bs.state,
                "failures_in_window": len(cleaned),
                "failure_threshold": bs.failure_threshold,
                "window_sec": bs.failure_window_sec,
                "total_failures": bs.total_failures,
                "total_successes": bs.total_successes,
                "total_rejected": bs.total_rejected,
                "last_failure_ts": bs.last_failure_ts,
                "uptime_sec": now - bs.last_state_change_ts,
            }

        # ---- SNAPSHOT_ALL：全部熔断器快照 ----
        if msg == CBMsg.SNAPSHOT_ALL:
            now = time.time()
            result = {}
            for name, bs in state.breakers.items():
                cleaned = self._gc_window(bs, now)
                result[name] = {
                    "name": bs.name,
                    "state": bs.state,
                    "failures_in_window": len(cleaned),
                    "failure_threshold": bs.failure_threshold,
                    "window_sec": bs.failure_window_sec,
                    "total_failures": bs.total_failures,
                    "total_successes": bs.total_successes,
                    "total_rejected": bs.total_rejected,
                    "last_failure_ts": bs.last_failure_ts,
                    "uptime_sec": now - bs.last_state_change_ts,
                }
            return state, result

        return state, None
