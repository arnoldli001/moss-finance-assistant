#coding = utf-8
"""全局并发闸：稀缺资源（Tavily / 本地 Ollama GPU）的并发上限 + 排队可观测。

多用户并发时 Tavily 超套餐并发会 429、单 GPU 多请求只会互相拖慢；
ConcurrencyGate 用 asyncio.Semaphore 实现"满载排队 + on_wait 提示 + 忙闲计数"。

有意不纳闸：Prompt 注入分类器 / Model Router 本地兜底（必须低延迟，不能被
长推理挤占）；zsxq runner（GPU 段在子进程内，整段纳闸会假排队，已有
single-flight + 每日调度错峰兜底）。
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Optional

from config.constants import OLLAMA_MAX_CONCURRENCY, TAVILY_MAX_CONCURRENCY

__all__ = ["ConcurrencyGate", "tavily_gate", "ollama_gate"]


class ConcurrencyGate:
    """async 信号量闸：限流 + 排队回调 + 忙闲可观测。"""

    def __init__(self, name: str, limit: int):
        self._name = name
        self._limit = max(1, int(limit))
        self._sem = asyncio.Semaphore(self._limit)
        self._active = 0
        self._waiting = 0

    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return self._waiting

    @property
    def limit(self) -> int:
        return self._limit

    async def run(self, factory: Callable[[], Any],
                  on_wait: Optional[Callable[[int], Any]] = None) -> Any:
        """在闸内执行 factory()。满载时先回调 on_wait(当前排队位数) 再排队等待。"""
        if self._active >= self._limit:
            self._waiting += 1
            try:
                if on_wait is not None:
                    try:
                        on_wait(self._waiting)
                    except Exception:
                        pass  # 提示失败不阻塞排队
                print(f"[并发闸:{self._name}] 已满({self._active}/{self._limit})，"
                      f"排队等待（第 {self._waiting} 位）...")
                await self._sem.acquire()
            finally:
                self._waiting -= 1
        else:
            await self._sem.acquire()
        self._active += 1
        try:
            return await factory()
        finally:
            self._active -= 1
            self._sem.release()


tavily_gate = ConcurrencyGate("tavily", TAVILY_MAX_CONCURRENCY)
ollama_gate = ConcurrencyGate("ollama", OLLAMA_MAX_CONCURRENCY)
