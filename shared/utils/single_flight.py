#coding = utf-8
"""single-flight 并发去重工具：同 key 的并发协程共享一次计算。

背景（2026-09-09）：多用户/多会话并发触发同类"当日市场信息"任务（盘前新闻 workflow、
zsxq 研报 runner、复盘预测）会双跑 8 路 Tavily 站点搜索（16 路并发触发限流）+ 双份
DeepSeek 综答 + 双份 Playwright/Ollama 子进程，互相拖垮导致前端长时间无输出（实测
两个会话各点一次，5-6 分钟零输出）。此类计算全局共享一次在业务上完全合理。

用法：
    from shared.utils.single_flight import run_single_flight, today_key
    result = await run_single_flight(today_key("premarket_news"), factory)

- 领导者（第一个到达者）：执行 factory()，结果放入 Future 并返回。
- 跟随者：等待共享结果（asyncio.shield 保护，不会被领导者之外的取消波及）。
- 领导者被取消（用户停止/超时）→ 跟随者收到 RuntimeError（明确可读）；
  跟随者自身被取消 → 正常传播 CancelledError。
- 领导者异常 → 跟随者收到同样异常。

【约束】factory 内部不得依赖"调用者各自的会话上下文"做推送/落库——
推送与落库应由调用方在拿到共享结果后，自行按自己的 thread 执行
（参考 zsxq.py 的 _run_zsxq_analysis / server.py 的 _run_review_prediction_dedup）。
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Any

_inflight: dict[str, asyncio.Future] = {}


def inflight_keys() -> list[str]:
    """当前在飞的 single-flight key 列表（测试/观测用）。"""
    return list(_inflight.keys())


def is_inflight(key: str) -> bool:
    """探测某 key 是否有在飞计算（用于给后来者推送 ♻️ 共享提示）。"""
    return key in _inflight


def today_key(prefix: str) -> str:
    """按天滚动的 flight key——当日市场信息类任务跨会话共享当日计算。"""
    return f"{prefix}:{datetime.now().strftime('%Y%m%d')}"


async def run_single_flight(key: str, factory: Callable[[], Any],
                            on_follow: Callable[[], None] | None = None) -> Any:
    """同 key 并发协程共享一次计算（leader 执行，follower 等待共享结果）。

    返回 factory() 的结果。异常语义见模块 docstring。

    on_follow：可选回调——当且仅当本调用被判定为跟随者时（注册表命中），
    在等待前同步调用。用于给前端推送 ♻️ 共享提示（避免"入口探测"与
    leader 注册之间的秒级竞态：探测时 leader 可能尚未注册，提示丢失）。
    """
    fut = _inflight.get(key)
    if fut is None:
        # ---- 领导者路径 ----
        fut = asyncio.get_running_loop().create_future()
        _inflight[key] = fut
        try:
            result = await factory()
            if not fut.done():
                fut.set_result(result)
            return result
        except asyncio.CancelledError:
            if not fut.done():
                fut.cancel()  # 通知跟随者：共享计算已被停止/超时
            raise
        except BaseException as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            _inflight.pop(key, None)

    # ---- 跟随者路径 ----
    if on_follow is not None:
        try:
            on_follow()
        except Exception:
            pass  # 提示失败不影响共享等待
    try:
        return await asyncio.shield(fut)
    except asyncio.CancelledError:
        if fut.cancelled():
            # 领导者被停止/超时/取消 → 转成明确错误；跟随者自身的取消照常传播
            raise RuntimeError(f"共享任务[{key}]已被停止或超时，请稍后重试") from None
        raise
