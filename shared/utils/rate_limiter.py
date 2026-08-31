# -*- coding: utf-8 -*-
"""基于内存滑动日志的角色分档限流（Sliding Window Log, O(1) peek/pop）。

4 档 QPM 与 config/rbac_policy.json 对齐：
  owner 600 / admin 120 / user 60 / guest 10。

设计：
- 单进程内线程/协程安全（threading.Lock，不依赖 asyncio.Lock，对同步/异步调用栈都可用）。
- 滑动日志：对 (key, role) 维护 deque(timestamp...)，窗口长度 60s。
- 命中限流返回 (False, retry_after_seconds)，通过返回 (True, 0)。
- 定期清理：每次 hit 先 pop 窗口外时间戳，避免无限增长；另加 5min 全量 GC 巡检。

使用：
  limiter = RateLimiter()
  ok, retry = limiter.hit("user_alice", "user", endpoint="/api/task")
  if not ok:
      raise HTTPException(429, headers={"Retry-After": str(retry)})
"""
from __future__ import annotations

import os
import threading
import time
from bisect import bisect_left
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple

# 4 档 QPM（与 config/rbac_policy.json role.rate_limit_per_min 保持一致）
DEFAULT_QPM: Dict[str, int] = {
    "owner": int(os.environ.get("RATE_LIMIT_QPM_OWNER", "600")),
    "admin": int(os.environ.get("RATE_LIMIT_QPM_ADMIN", "120")),
    "user":  int(os.environ.get("RATE_LIMIT_QPM_USER",  "60")),
    "guest": int(os.environ.get("RATE_LIMIT_QPM_GUEST", "10")),
}
DEFAULT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))
DEFAULT_FALLBACK_QPM = int(os.environ.get("RATE_LIMIT_QPM_FALLBACK", "30"))

GC_INTERVAL_SECONDS = 5 * 60  # 5 分钟清一次空桶
MAX_TS_BUCKET = 10_000  # 单 key 最多保留时间戳数量（安全上限，防 deque 膨胀）


@dataclass
class _Bucket:
    dq: Deque[float] = field(default_factory=deque)
    last_gc: float = field(default_factory=time.monotonic)


class RateLimiter:
    """内存态滑动日志限流。进程级单例即可。"""

    def __init__(self,
                 qpm: Optional[Dict[str, int]] = None,
                 window_seconds: int = DEFAULT_WINDOW_SECONDS,
                 fallback_qpm: int = DEFAULT_FALLBACK_QPM,
                 endpoint_separate: bool = False):
        self._qpm = dict(DEFAULT_QPM)
        if qpm:
            self._qpm.update(qpm)
        self._window = max(1, int(window_seconds))
        self._fallback = max(1, int(fallback_qpm))
        self._endpoint_separate = bool(endpoint_separate)
        self._buckets: Dict[Tuple[str, Optional[str]], _Bucket] = {}
        self._lock = threading.Lock()
        self._last_gc_run = time.monotonic()

    # ------------------------------ public api ------------------------------
    def qpm_for_role(self, role: Optional[str]) -> int:
        if not role:
            return self._fallback
        return self._qpm.get(role, self._fallback)

    def hit(self, identity: str, role: Optional[str],
            *, endpoint: Optional[str] = None) -> Tuple[bool, int]:
        """尝试放行一次请求。

        返回 (ok, retry_after_s)。ok=True 表示已通过（并计数）。
        retry_after_s 为建议重试秒数（仅 ok=False 时 > 0）。
        """
        key = (identity, endpoint) if self._endpoint_separate and endpoint else (identity, None)
        limit = self.qpm_for_role(role)
        now = time.monotonic()
        cutoff = now - self._window

        with self._lock:
            b = self._buckets.get(key)
            if b is None:
                b = _Bucket()
                self._buckets[key] = b
            # pop 窗口外
            dq = b.dq
            while dq and dq[0] < cutoff:
                dq.popleft()
            # 超量
            if len(dq) >= limit:
                oldest_in_window = dq[0]
                # 距离 oldest_in_window + window 的秒数就是需等待的最小时间
                retry = int(oldest_in_window + self._window - now) + 1
                return False, max(1, retry)
            # append 新时间戳
            if len(dq) < MAX_TS_BUCKET:
                dq.append(now)
            # 定期巡检 GC 空桶
            if now - self._last_gc_run > GC_INTERVAL_SECONDS:
                self._gc_unlocked(now)
                self._last_gc_run = now
            return True, 0

    def remaining(self, identity: str, role: Optional[str],
                  *, endpoint: Optional[str] = None) -> int:
        """当前窗口剩余配额（只读，不计数）。"""
        key = (identity, endpoint) if self._endpoint_separate and endpoint else (identity, None)
        limit = self.qpm_for_role(role)
        now = time.monotonic()
        cutoff = now - self._window
        with self._lock:
            b = self._buckets.get(key)
            if not b:
                return limit
            dq = b.dq
            # O(logN) 找到窗口左边界（不用实际 popleft，避免改状态）
            idx = bisect_left(dq, cutoff)
            used = len(dq) - idx
            return max(0, limit - used)

    def reset(self, identity: str, *, endpoint: Optional[str] = None) -> None:
        key = (identity, endpoint) if self._endpoint_separate and endpoint else (identity, None)
        with self._lock:
            self._buckets.pop(key, None)

    def reset_all(self) -> None:
        """清空所有 bucket 的所有时间戳（测试场景专用，避免测试间 QPM 计数污染）。"""
        with self._lock:
            self._buckets.clear()

    # ------------------------------ internal ------------------------------
    def _gc_unlocked(self, now: float) -> None:
        cutoff = now - self._window
        remove_keys = []
        for k, b in self._buckets.items():
            dq = b.dq
            while dq and dq[0] < cutoff:
                dq.popleft()
            if not dq:
                remove_keys.append(k)
        for k in remove_keys:
            del self._buckets[k]

    # stats（给 /api/slo/status 或运维端点使用）
    def stats(self) -> Dict[str, int]:
        with self._lock:
            n = len(self._buckets)
            total_ts = sum(len(b.dq) for b in self._buckets.values())
        return {"buckets": n, "total_timestamps": total_ts}


# 进程级默认单例（HTTP / WS 共用）
_global_limiter: Optional[RateLimiter] = None
_global_limiter_lock = threading.Lock()


def get_rate_limiter() -> RateLimiter:
    global _global_limiter
    if _global_limiter is None:
        with _global_limiter_lock:
            if _global_limiter is None:
                _global_limiter = RateLimiter()
    return _global_limiter
