"""orchestration.loop —— Loop Engineering：多 Agent 协作 / 重试逻辑 / 循环终止 / 不同 Agent 不同超时。

重构.md §4 四层工程 · Loop Engineering：
  - 定义不同 Agent 不同任务的 超时时间、重试次数、终止条件（SLO 触发时立即 kill）
  - 通过 Maker-Checker 双校验进行 0~N 次迭代（默认最多两轮：Maker 1 轮 + Checker 1 轮）
  - 若单任务 SLO（SLO_MAX_TASK_SEC = 150s）已违规 → 直接返回降级答案，不再重试

对外：
    async def run_task_with_loop(
        task_fn,                  # 单次执行的 coroutine 工厂（每次重试重新实例化）
        *, branch: str,           # 路由分支名 → 查超时/重试策略表
        session_id: str = "",
        request_id: str = "",
        max_attempts: Optional[int] = None,
        per_call_timeout: Optional[float] = None,
        slo_monitor_actor = None, # 可选：SLO Monitor Actor 埋点
        circuit_breaker_actor = None,  # 可选：熔断器
        error_classifier = None,       # 可选：错误四象限分类
        logger = None,                 # 可选：审计日志
    ) -> Tuple[Any, List[Exception]]

Agent / Branch → 超时 & 重试策略表（从 config.constants.TIMEOUTS / DEGRADATION 对齐）：
    ┌───────────────────────┬──────────────────┬─────────────┬───────────────────────┐
    │ Branch                │ per_call_timeout │ max_attempts│ 终止条件              │
    ├───────────────────────┼──────────────────┼─────────────┼───────────────────────┤
    │ pre_market_news       │ 120s             │ 1           │ 盘前结果保存成功即可  │
    │ stock_query           │ 150s(180s DAG内) │ 1           │ SLO 违规立即返回      │
    │ general_query         │ 120s             │ 1           │ SLO 违规立即返回      │
    │ code_generation       │ 300s             │ 2           │ qwen2.5-coder 易幻觉  │
    │ impact_analysis       │ 200s             │ 1           │ deepseek-r1 推理稳定  │
    │ vision                │ 240s             │ 1           │ 多模态耗时较长        │
    │ preset_shortcut_other │ 150s             │ 1           │ 复用原逻辑            │
    └───────────────────────┴──────────────────┴─────────────┴───────────────────────┘
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, List, Optional, Tuple

# 默认策略表（可在 constants 中覆写；先硬编码默认值，constants 加载成功就覆盖）
DEFAULT_LOOP_POLICY: dict = {
    "pre_market_news":        {"timeout": 120.0, "max_attempts": 1},
    "preset_shortcut_other":  {"timeout": 150.0, "max_attempts": 1},
    "stock_query":            {"timeout": 180.0, "max_attempts": 1},
    "general_query":          {"timeout": 120.0, "max_attempts": 1},
    "code_generation":        {"timeout": 300.0, "max_attempts": 2},
    "impact_analysis":        {"timeout": 200.0, "max_attempts": 1},
    "vision":                 {"timeout": 240.0, "max_attempts": 1},
    "fallback":               {"timeout": 150.0, "max_attempts": 1},
}
# SLO 全局硬上限（对齐 constants.SLO_TARGETS["SLO_MAX_TASK_SEC"]=150.0）
GLOBAL_SLO_MAX_TASK_SEC = 150.0
GLOBAL_SLO_MAX_TOKENS = 1_000_000


def _load_constants_policy() -> None:
    """尝试从 shared.config.constants 拉更精确的超时（失败就用默认，不阻塞）。"""
    global GLOBAL_SLO_MAX_TASK_SEC, GLOBAL_SLO_MAX_TOKENS
    try:
        from shared.config.constants import TIMEOUTS, SLO_TARGETS  # type: ignore
        if isinstance(SLO_TARGETS, dict):
            if "SLO_MAX_TASK_SEC" in SLO_TARGETS:
                GLOBAL_SLO_MAX_TASK_SEC = float(SLO_TARGETS["SLO_MAX_TASK_SEC"])
            if "SLO_MAX_TOKENS" in SLO_TARGETS:
                GLOBAL_SLO_MAX_TOKENS = int(SLO_TARGETS["SLO_MAX_TOKENS"])
        if isinstance(TIMEOUTS, dict):
            # 覆盖 DEFAULT_LOOP_POLICY 中的 timeout 字段
            if "PREMARKET_ANALYSIS" in TIMEOUTS:
                DEFAULT_LOOP_POLICY["pre_market_news"]["timeout"] = float(TIMEOUTS["PREMARKET_ANALYSIS"])
            if "MAIN_AGENT" in TIMEOUTS:
                DEFAULT_LOOP_POLICY["stock_query"]["timeout"] = float(TIMEOUTS["MAIN_AGENT"])
                DEFAULT_LOOP_POLICY["general_query"]["timeout"] = float(TIMEOUTS["MAIN_AGENT"])
    except Exception:
        pass


_load_constants_policy()


def _classify_error(err: Exception) -> Tuple[str, bool]:
    """
    返回 (象限, should_retry)。
    四象限对齐 governance.guardrails.error_classifier：
      HARD_RETRY     = 网络超时 / 429 限流 / 5xx → 指数退避重试
      SOFT_NO_RETRY  = 模型幻觉 / 格式错 / 无权限 → 不重试（换模型或直接报错）
      FATAL_NO_RETRY = 配置类（API Key 过期/文件缺失） → 立即终止
      UNCLASSIFIED   = 其它 → 不重试（兜底，避免无限循环）
    """
    # 优先用 governance 错误分类器（若加载成功）
    try:
        from governance.guardrails.error_classifier import classify_error  # type: ignore
        from inspect import iscoroutinefunction as _icf
        fn = classify_error
        if _icf(fn):
            # 本层是同步包装，简化：调用失败就本地规则
            raise RuntimeError("async classifier not supported here")
        res = fn(err)
        if isinstance(res, tuple) and len(res) >= 2:
            return str(res[0]), bool(res[1])
        if isinstance(res, dict):
            return str(res.get("quadrant", "UNCLASSIFIED")), bool(res.get("should_retry", False))
    except Exception:
        pass
    # 本地兜底（四象限）
    msg = f"{type(err).__name__}: {err}".lower()
    # 致命（配置类）
    if any(k in msg for k in ("api key", "apikey", "auth", "unauthorized", "401", "403", "invalid token", "环境变量", "no such file", "not found")):
        return ("FATAL_NO_RETRY", False)
    # 可重试硬错误
    if any(k in msg for k in ("timeout", "timed out", "connection", "502", "503", "504", "429", "rate limit", "temporarily", "server error")):
        return ("HARD_RETRY", True)
    # 软错误（模型幻觉/输出格式）
    if any(k in msg for k in ("json", "parse", "invalid", "format", "halluc", "schema")):
        return ("SOFT_NO_RETRY", False)
    return ("UNCLASSIFIED", False)


async def run_task_with_loop(
    task_fn: Callable[[], Awaitable[Any]],
    *,
    branch: str = "fallback",
    session_id: str = "",
    request_id: str = "",
    max_attempts: Optional[int] = None,
    per_call_timeout: Optional[float] = None,
    slo_monitor_actor: Any = None,
    circuit_breaker_actor: Any = None,
    error_classifier_fn: Optional[Callable[[Exception], Tuple[str, bool]]] = None,
    audit_log_fn: Optional[Callable[..., Awaitable[None]]] = None,
) -> Tuple[Any, List[Exception]]:
    """
    Loop Engineering 统一入口。

    返回：(result, exception_list)
        - 成功时 result 为 task_fn 返回值，exception_list 可能有之前失败的异常
        - 全部失败时 result 为 None，exception_list 为每一次的异常
    """
    policy = DEFAULT_LOOP_POLICY.get(str(branch or "fallback").lower(), DEFAULT_LOOP_POLICY["fallback"])
    attempts = int(max_attempts if max_attempts is not None else policy["max_attempts"])
    per_timeout = float(per_call_timeout if per_call_timeout is not None else policy["timeout"])

    # 全局 SLO 起点：超过 GLOBAL_SLO_MAX_TASK_SEC 立即停止
    start_wall = time.monotonic()
    exceptions: List[Exception] = []

    for attempt in range(1, max(1, attempts) + 1):
        # 0) 熔断器检查：CLOSED 才允许执行（OPEN 直接返回失败）
        if circuit_breaker_actor is not None:
            try:
                if hasattr(circuit_breaker_actor, "ask"):
                    state = await circuit_breaker_actor.ask("GET_STATE")
                elif hasattr(circuit_breaker_actor, "get_state"):
                    state = circuit_breaker_actor.get_state()
                else:
                    state = "CLOSED"
                if str(state).upper() == "OPEN":
                    ex = RuntimeError(f"CircuitBreaker OPEN for branch={branch} (attempt {attempt}/{attempts})")
                    exceptions.append(ex)
                    return (None, exceptions)
            except Exception:
                pass

        # 1) SLO 已到硬上限 → 不重试直接返回
        elapsed_total = time.monotonic() - start_wall
        if elapsed_total >= GLOBAL_SLO_MAX_TASK_SEC:
            ex = TimeoutError(f"SLO 硬上限 {GLOBAL_SLO_MAX_TASK_SEC}s 已到（累计 {elapsed_total:.1f}s），不再重试")
            exceptions.append(ex)
            break

        # 2) 执行单次（带 per-call 超时）
        result: Any = None
        attempt_err: Optional[Exception] = None
        try:
            coro = task_fn()
            if not asyncio.iscoroutine(coro):
                # 同步函数 → 包一层 to_thread
                from asyncio import to_thread
                coro = to_thread(coro)  # type: ignore
            result = await asyncio.wait_for(coro, timeout=per_timeout)
            # 成功：记录 SLO + 熔断器成功 → 返回
            if slo_monitor_actor is not None:
                try:
                    if hasattr(slo_monitor_actor, "tell"):
                        slo_monitor_actor.tell({
                            "type": "RECORD_SUCCESS",
                            "branch": branch,
                            "session_id": session_id,
                            "request_id": request_id,
                            "elapsed_sec": time.monotonic() - start_wall,
                        })
                except Exception:
                    pass
            if circuit_breaker_actor is not None:
                try:
                    if hasattr(circuit_breaker_actor, "tell"):
                        circuit_breaker_actor.tell("ON_SUCCESS")
                except Exception:
                    pass
            return (result, exceptions)
        except asyncio.TimeoutError as e:
            attempt_err = TimeoutError(f"attempt {attempt}/{attempts} timed out after {per_timeout}s (branch={branch})")
        except Exception as e:
            attempt_err = e

        exceptions.append(attempt_err)
        # 3) 错误分类
        classify = error_classifier_fn or _classify_error
        try:
            quadrant, should_retry = classify(attempt_err)
        except Exception:
            quadrant, should_retry = "UNCLASSIFIED", False

        if audit_log_fn is not None:
            try:
                await audit_log_fn(
                    event="loop_attempt_failed",
                    branch=branch, session_id=session_id, request_id=request_id,
                    attempt=attempt, attempts=attempts,
                    quadrant=quadrant, should_retry=should_retry,
                    error_type=type(attempt_err).__name__,
                    error_msg=str(attempt_err)[:500],
                )
            except Exception:
                pass

        # FATAL 立即终止（API Key 过期 / 配置缺失 / 无权限，重试没用）
        if quadrant == "FATAL_NO_RETRY":
            break
        # SOFT_NO_RETRY 不重试（幻觉、格式错 → 换 Agent 或直接返回失败）
        if quadrant == "SOFT_NO_RETRY":
            break
        # HARD_RETRY：指数退避 + 还剩 attempts，才重试
        if should_retry and attempt < attempts:
            backoff = min(2 ** attempt, 30)  # 2s, 4s, 8s ... cap 30s
            # 也要算全局 SLO：如果 backoff 后会超 GLOBAL_SLO_MAX_TASK_SEC，直接放弃
            if (time.monotonic() - start_wall) + backoff >= GLOBAL_SLO_MAX_TASK_SEC:
                break
            await asyncio.sleep(backoff)
            continue
        # UNCLASSIFIED / HARD_RETRY 但 attempts 用完 → 退出
        break

    # 全部尝试失败：熔断器 failure 埋点
    if circuit_breaker_actor is not None and exceptions:
        try:
            if hasattr(circuit_breaker_actor, "tell"):
                circuit_breaker_actor.tell("ON_FAILURE")
        except Exception:
            pass
    return (None, exceptions)
