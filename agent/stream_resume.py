# -*- coding: utf-8 -*-
"""
流式输出断点续传：WebSocket 断开后重连，正在生成的流式回复无法续传，用户只能重新问。

设计思路：
  1) StreamResumeStore: 存储每个 session 的 partial output + LLM context
     - memory 后端：OrderedDict LRU
     - redis 后端：跨进程共享，TTL 自动过期
  2) 服务端流式输出时同步写 store：每收到一个 token 就 append
  3) 客户端断线重连后，带 session_id + msg_id 调 /stream/resume 接口
  4) 服务端检查 store：
     - 命中且 LLM 还在生成 → 把后续 token 续推给新连接
     - 命中但 LLM 已结束 → 直接返回完整 partial output
     - 未命中 → 返回 404，让客户端重新提问

典型用法：
    # 服务端：流式生成时同步写 store
    from agent.stream_resume import get_stream_resume_store

    store = get_stream_resume_store()
    session = await store.begin(
        session_id="s1", msg_id="m1",
        llm_context={"messages": [...]},  # 用于断点续推给 LLM
    )

    async for token in llm.stream(prompt):
        await store.append(session_id="s1", msg_id="m1", chunk=token)
        await ws.send_text(token)

    await store.complete(session_id="s1", msg_id="m1")

    # 客户端重连后：服务端处理 /stream/resume
    async def handle_stream_resume(session_id, msg_id):
        session = await store.get(session_id, msg_id)
        if session is None:
            raise HTTPException(404, "无可续传内容")
        # 推送已生成的 partial output
        for chunk in session.partial_chunks:
            await ws.send_text(chunk)
        # 如果 LLM 还在生成，继续推送后续 token
        if session.is_active:
            async for token in session.continue_stream():
                await ws.send_text(token)
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, Dict, List, Optional

from config.constants import (
    STREAM_RESUME_BACKEND,
    STREAM_RESUME_MEMORY_MAX_SESSIONS,
    STREAM_RESUME_PARTIAL_MAX_CHARS,
    STREAM_RESUME_TOKEN_BUFFER_MAX,
    STREAM_RESUME_TOKEN_TTL_SEC,
    STREAM_RESUME_CONTINUE_MAX_TOKENS,
    STREAM_RESUME_REDIS_URL,
    STREAM_RESUME_REDIS_PREFIX,
)

logger = logging.getLogger(__name__)


# ======================================================================
# 会话状态
# ======================================================================

@dataclass
class StreamSession:
    """单次流式输出的会话状态。"""
    session_id: str
    msg_id: str
    user_id: str = ""
    created_at: float = field(default_factory=time.time)
    last_append_at: float = field(default_factory=time.time)

    # 已生成的 partial output（分块存储，避免单字符串过大）
    partial_chunks: List[str] = field(default_factory=list)
    partial_total_chars: int = 0

    # LLM 上下文（用于断点续推：把已有 partial 喂回去让 LLM 接着写）
    llm_context: Dict[str, Any] = field(default_factory=dict)

    # 状态：streaming / completed / failed
    status: str = "streaming"

    # 续推用的 queue：服务端流式生成时把 token 写进 queue，
    # 断线重连后客户端从这个 queue 读后续 token
    resume_queue: Optional[asyncio.Queue] = None

    # 续推回调：服务端用此 callback 让 LLM 接着生成
    continue_fn: Optional[Callable] = None

    @property
    def is_active(self) -> bool:
        """是否仍在生成中。"""
        return self.status == "streaming"

    @property
    def is_expired(self) -> bool:
        """是否已过期（超过 TTL）。"""
        return (time.time() - self.last_append_at) > STREAM_RESUME_TOKEN_TTL_SEC

    @property
    def partial_text(self) -> str:
        """拼接已生成的 partial output。"""
        return "".join(self.partial_chunks)

    def to_dict(self) -> Dict[str, Any]:
        """序列化（用于 redis 存储）。"""
        return {
            "session_id": self.session_id,
            "msg_id": self.msg_id,
            "user_id": self.user_id,
            "created_at": self.created_at,
            "last_append_at": self.last_append_at,
            "partial_chunks": self.partial_chunks,
            "partial_total_chars": self.partial_total_chars,
            "llm_context": self.llm_context,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "StreamSession":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ======================================================================
# 存储后端抽象
# ======================================================================

class StreamResumeStore:
    """流式续传存储。

    memory 后端用 OrderedDict（LRU 淘汰）；
    redis 后端跨进程共享，但 resume_queue 仅内存有效。
    """

    def __init__(
        self,
        backend: str = STREAM_RESUME_BACKEND,
        max_sessions: int = STREAM_RESUME_MEMORY_MAX_SESSIONS,
        max_partial_chars: int = STREAM_RESUME_PARTIAL_MAX_CHARS,
    ):
        self.backend_type = backend
        self.max_sessions = max_sessions
        self.max_partial_chars = max_partial_chars
        self._memory_store: "OrderedDict[str, StreamSession]" = OrderedDict()
        self._lock = asyncio.Lock()
        self._redis_client: Any = None

    def _key(self, session_id: str, msg_id: str) -> str:
        return f"{STREAM_RESUME_REDIS_PREFIX}{session_id}:{msg_id}"

    # ------------------------------------------------------------------
    # 生命周期：begin / append / complete / fail
    # ------------------------------------------------------------------

    async def begin(
        self,
        session_id: str,
        msg_id: str,
        user_id: str = "",
        llm_context: Optional[Dict[str, Any]] = None,
        continue_fn: Optional[Callable] = None,
    ) -> StreamSession:
        """开始一次流式输出。"""
        session = StreamSession(
            session_id=session_id,
            msg_id=msg_id,
            user_id=user_id,
            llm_context=llm_context or {},
            continue_fn=continue_fn,
        )
        session.resume_queue = asyncio.Queue()

        async with self._lock:
            # LRU 淘汰
            while len(self._memory_store) >= self.max_sessions:
                evicted_key, evicted = self._memory_store.popitem(last=False)
                logger.info(
                    "[stream_resume] LRU 淘汰 session=%s msg=%s",
                    evicted.session_id, evicted.msg_id,
                )

            key = self._key(session_id, msg_id)
            self._memory_store[key] = session

        if self.backend_type == "redis":
            await self._save_to_redis(session)

        logger.info(
            "[stream_resume] 开始 session=%s msg=%s user=%s",
            session_id, msg_id, user_id,
        )
        return session

    async def append(
        self,
        session_id: str,
        msg_id: str,
        chunk: str,
    ) -> None:
        """追加一个 token / chunk 到 partial output。"""
        key = self._key(session_id, msg_id)

        async with self._lock:
            session = self._memory_store.get(key)
            if session is None:
                logger.warning(
                    "[stream_resume] append 失败：session=%s msg=%s 不存在",
                    session_id, msg_id,
                )
                return

            if session.status != "streaming":
                return

            # 防超长
            if session.partial_total_chars + len(chunk) > self.max_partial_chars:
                logger.warning(
                    "[stream_resume] partial output 超 %d 字符，截断",
                    self.max_partial_chars,
                )
                chunk = chunk[: self.max_partial_chars - session.partial_total_chars]

            session.partial_chunks.append(chunk)
            session.partial_total_chars += len(chunk)
            session.last_append_at = time.time()

            # 推到 resume_queue，让重连的客户端能读到
            if session.resume_queue is not None:
                try:
                    session.resume_queue.put_nowait(chunk)
                except asyncio.QueueFull:
                    pass

            # LRU 更新
            self._memory_store.move_to_end(key)

        # redis 后端异步落盘（不阻塞主流）
        if self.backend_type == "redis":
            # 节流：每 10 个 chunk 落盘一次（避免高频写 redis）
            session = self._memory_store.get(key)
            if session and len(session.partial_chunks) % 10 == 0:
                asyncio.create_task(self._save_to_redis(session))

    async def complete(
        self,
        session_id: str,
        msg_id: str,
        final_output: Optional[str] = None,
    ) -> None:
        """标记流式输出完成。"""
        key = self._key(session_id, msg_id)
        async with self._lock:
            session = self._memory_store.get(key)
            if session is None:
                return
            session.status = "completed"
            session.last_append_at = time.time()
            # 把结束信号推到 queue
            if session.resume_queue is not None:
                await session.resume_queue.put(None)  # sentinel

        if self.backend_type == "redis":
            await self._save_to_redis(session)

        logger.info(
            "[stream_resume] 完成 session=%s msg=%s total_chars=%d",
            session_id, msg_id, session.partial_total_chars,
        )

    async def fail(
        self,
        session_id: str,
        msg_id: str,
        error: str = "",
    ) -> None:
        """标记流式输出失败。"""
        key = self._key(session_id, msg_id)
        async with self._lock:
            session = self._memory_store.get(key)
            if session is None:
                return
            session.status = "failed"
            session.llm_context["error"] = error
            if session.resume_queue is not None:
                await session.resume_queue.put(None)

    # ------------------------------------------------------------------
    # 重连续传
    # ------------------------------------------------------------------

    async def get(
        self,
        session_id: str,
        msg_id: str,
    ) -> Optional[StreamSession]:
        """获取会话状态。先查内存，再查 redis。"""
        key = self._key(session_id, msg_id)
        session = self._memory_store.get(key)
        if session is not None:
            if session.is_expired:
                logger.info("[stream_resume] session=%s 已过期", session_id)
                return None
            return session

        # redis 回查
        if self.backend_type == "redis":
            session = await self._load_from_redis(session_id, msg_id)
            if session is not None and not session.is_expired:
                # 重建 resume_queue
                session.resume_queue = asyncio.Queue()
                return session
        return None

    async def resume_stream(
        self,
        session_id: str,
        msg_id: str,
    ) -> AsyncIterator[str]:
        """重连续传：先推已生成的 partial output，再推后续 token。

        用法：
            async for chunk in store.resume_stream(session_id, msg_id):
                await ws.send_text(chunk)
        """
        session = await self.get(session_id, msg_id)
        if session is None:
            return  # 空 async iterator

        # 1) 先推已生成的 partial output（一次推完，让客户端立即看到内容）
        if session.partial_chunks:
            yield "".join(session.partial_chunks)

        # 2) 如果已完成，结束
        if session.status in ("completed", "failed"):
            return

        # 3) 推后续 token（从 resume_queue 读）
        if session.resume_queue is None:
            return

        while True:
            try:
                chunk = await asyncio.wait_for(
                    session.resume_queue.get(), timeout=STREAM_RESUME_TOKEN_TTL_SEC
                )
                if chunk is None:  # sentinel：流结束
                    break
                yield chunk
            except asyncio.TimeoutError:
                logger.warning(
                    "[stream_resume] 续传等待超时 session=%s msg=%s",
                    session_id, msg_id,
                )
                break

    # ------------------------------------------------------------------
    # 清理
    # ------------------------------------------------------------------

    async def cleanup_expired(self) -> int:
        """清理过期会话。返回清理数量。"""
        cleaned = 0
        async with self._lock:
            expired_keys = [
                k for k, s in self._memory_store.items() if s.is_expired
            ]
            for k in expired_keys:
                del self._memory_store[k]
                cleaned += 1
        if cleaned:
            logger.info("[stream_resume] 清理过期会话 %d 个", cleaned)
        return cleaned

    async def get_stats(self) -> Dict[str, Any]:
        """返回统计。"""
        async with self._lock:
            return {
                "backend": self.backend_type,
                "total_sessions": len(self._memory_store),
                "max_sessions": self.max_sessions,
                "by_status": {
                    s: sum(1 for x in self._memory_store.values() if x.status == s)
                    for s in ("streaming", "completed", "failed")
                },
            }

    # ------------------------------------------------------------------
    # redis 后端实现
    # ------------------------------------------------------------------

    async def _ensure_redis(self):
        if self._redis_client is None:
            import redis.asyncio as aioredis
            self._redis_client = aioredis.from_url(
                STREAM_RESUME_REDIS_URL, decode_responses=True
            )
        return self._redis_client

    async def _save_to_redis(self, session: StreamSession) -> None:
        try:
            client = await self._ensure_redis()
            key = self._key(session.session_id, session.msg_id)
            content = json.dumps(session.to_dict(), ensure_ascii=False)
            await client.set(key, content, ex=STREAM_RESUME_TOKEN_TTL_SEC)
        except Exception as e:
            logger.warning("[stream_resume] redis 保存失败: %s", e)

    async def _load_from_redis(
        self, session_id: str, msg_id: str
    ) -> Optional[StreamSession]:
        try:
            client = await self._ensure_redis()
            key = self._key(session_id, msg_id)
            content = await client.get(key)
            if not content:
                return None
            return StreamSession.from_dict(json.loads(content))
        except Exception as e:
            logger.warning("[stream_resume] redis 加载失败: %s", e)
            return None


# ======================================================================
# 全局单例
# ======================================================================

_global_store: Optional[StreamResumeStore] = None
_singleton_lock = asyncio.Lock()


async def get_stream_resume_store() -> StreamResumeStore:
    """获取全局 StreamResumeStore 单例。"""
    global _global_store
    if _global_store is None:
        async with _singleton_lock:
            if _global_store is None:
                _global_store = StreamResumeStore()
    return _global_store


def get_stream_resume_store_sync() -> StreamResumeStore:
    """同步获取单例（不连 redis，仅初始化内存结构）。"""
    global _global_store
    if _global_store is None:
        _global_store = StreamResumeStore()
    return _global_store
