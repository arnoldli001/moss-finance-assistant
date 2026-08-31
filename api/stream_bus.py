# -*- coding: utf-8 -*-
"""
流式事件总线 StreamEventBus。

职责：
  1. 单进程内 thread_id → set[Queue] 的多播：一个 thread_id 同时接 WS 和 SSE，
     两者都能收到完整事件流（标签页双开不丢消息）；
  2. 统一来源索引编号：每出现一条引用，给一个全局(按thread_id)自增 index，
     保证前端正文 [1][2] 编号与文末 SourceRefPayload.index 一致；
  3. 增量 token 聚合：每个 DELTA 都累积到 final_text_buffer，done 事件直接用，
     避免 SSE 断连后"拼不回完整文本"；
  4. 心跳后台任务：每个活跃订阅 15s 间隔自动发 heartbeat，防止 Nginx/CDN 超时；
  5. LRU 订阅上限：单进程最多 256 个活跃订阅，超了自动踢最旧的，防内存泄漏。

使用模式（发布端 —— monitor 桥接 / SSE 端点内部 agent 协程）：
    bus = get_stream_bus()
    bus.publish(thread_id, SSEFrame.delta(...))

使用模式（订阅端 —— SSE 生成器）：
    sub = bus.subscribe(thread_id)
    try:
        async for frame in sub:
            yield frame  # 直接作为 StreamingResponse 的一部分
    finally:
        bus.unsubscribe(thread_id, sub)

注意：
  * 所有 Queue / asyncio 对象都"绑定到主事件循环"。server.py lifespan 里
    调用 `get_stream_bus().bind_loop(loop)` 绑定。如果运行在脚本模式，
    subscribe() 时自动绑定当前 loop。
  * Queue 有上限 4096：订阅方消费慢，满了以后丢最旧的并记日志，不阻塞发布。
    （前端 EventSource 本地 TCP 缓冲 + SSE 缓冲一般够用，这是极端兜底。）
"""
from __future__ import annotations

import asyncio
import re as _re
import time
import weakref
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from api.stream_protocol import (
    CitationMetaItem,
    CitationMetaPayload,
    DeltaPayload,
    DonePayload,
    ErrorPayload,
    GapPayload,
    OpenPayload,
    ProgressPayload,
    ReasoningPayload,
    ReplayEndPayload,
    ReplayStartPayload,
    RetrieveResultItem,
    RetrieveResultPayload,
    SSEFrame,
    SourceRefItem,
    SourceRefPayload,
    StreamEventType,
    ToolCallPayload,
    ToolResultPayload,
    new_event_id,
)
from config.constants import (
    STREAM_BUS_MAX_SUBS,
    STREAM_BUS_QUEUE_MAXSIZE,
    STREAM_BUS_HEARTBEAT_INTERVAL_SEC,
    STREAM_RESUME_EVENT_RING_MAX,
    STREAM_RESUME_MAX_THREAD_STATES,
    STREAM_RESUME_DONE_SESSION_TTL_SEC,
    CITATION_SNIPPET_MAX_CHARS,
    CITATION_SNIPPET_HALO_CHARS,
    CITATION_TITLE_MAX_CHARS,
    CITATION_URL_MAX_CHARS,
)

import logging
logger = logging.getLogger(__name__)


def _build_snippet(
    content: str,
    title: str = "",
    focus_sentences: Optional[List[str]] = None,
    focus_keywords: Optional[List[str]] = None,
) -> str:
    """统一入口：抽取 ≤ CITATION_SNIPPET_MAX_CHARS 的"最相关片段"。
    有 adapter.build_focused_snippet 时优先用它做中心窗口抽取；失败兜底硬截头部。
    """
    raw = str(content or "")
    if not raw:
        return ""
    try:
        from adapter.stream_adapters import build_focused_snippet as _bf
        return _bf(
            raw,
            focus_sentences=focus_sentences,
            focus_keywords=focus_keywords,
            max_chars=CITATION_SNIPPET_MAX_CHARS,
            halo_chars=CITATION_SNIPPET_HALO_CHARS,
            doc_title=str(title or ""),
        )
    except Exception:
        return raw[:CITATION_SNIPPET_MAX_CHARS]


# ======================================================================
# 订阅者：包装一个 Queue + 元数据
# ======================================================================

class StreamSubscriber:
    """一个订阅者 = 一个 async for 循环读取的异步迭代器。"""

    __slots__ = (
        "thread_id", "sub_id", "created_at", "_queue", "_loop",
        "_closed", "_sent_sentinel", "__weakref__",
    )

    def __init__(self, thread_id: str, sub_id: str, loop: asyncio.AbstractEventLoop):
        self.thread_id = thread_id
        self.sub_id = sub_id
        self.created_at = time.monotonic()
        self._queue: "asyncio.Queue[Optional[str]]" = asyncio.Queue(
            maxsize=STREAM_BUS_QUEUE_MAXSIZE
        )
        self._loop = loop
        self._closed = False
        self._sent_sentinel = False

    # ----------------------------------------------------------
    # 发布端调用：投递一帧
    # ----------------------------------------------------------
    def enqueue(self, frame: str) -> None:
        """发布端投递。队列满 → 丢最旧的一条再放（保证发布端不阻塞）。"""
        if self._closed:
            return
        try:
            self._queue.put_nowait(frame)
        except asyncio.QueueFull:
            # 丢最旧 25%，一次性挪出空间（避免每次都丢一条抖动）
            dropped = 0
            try:
                drop_n = max(1, STREAM_BUS_QUEUE_MAXSIZE // 4)
                for _ in range(drop_n):
                    self._queue.get_nowait()
                    dropped += 1
                self._queue.put_nowait(frame)
            except Exception:
                pass
            if dropped:
                logger.warning(
                    "[StreamBus] sub=%s@%s 队列已满，丢弃最旧 %d 帧（消费端太慢）",
                    self.sub_id, self.thread_id, dropped,
                )

    def close(self) -> None:
        """关闭订阅：放入 sentinel None，迭代器退出。幂等。"""
        if self._closed:
            return
        self._closed = True
        try:
            self._queue.put_nowait(None)
        except Exception:
            pass

    # ----------------------------------------------------------
    # 订阅端调用：async for
    # ----------------------------------------------------------
    def __aiter__(self) -> "StreamSubscriber":
        return self

    async def __anext__(self) -> str:
        if self._sent_sentinel:
            raise StopAsyncIteration
        item = await self._queue.get()
        if item is None:
            self._sent_sentinel = True
            raise StopAsyncIteration
        return item


# ======================================================================
# 每个 thread_id 的聚合状态：来源索引 + final_text 累积 + 工具调用计数
# ======================================================================

@dataclass
class ThreadStreamState:
    thread_id: str
    # 来源索引：按出现顺序编号 1..N
    _source_pool: List[SourceRefItem] = field(default_factory=list)
    # 用于去重：基于 (url, title[:30]) 作为 key → 返回已有 index
    _source_dedup_key: Dict[str, int] = field(default_factory=dict)
    # 累积最终文本（把每个 delta.text 顺序 append），done 事件直接用
    final_text_buffer: str = ""
    tool_call_count: int = 0
    reasoning_count: int = 0
    delta_count: int = 0
    start_monotonic: float = field(default_factory=time.monotonic)
    # 完成态标记：done / error 之后再 publish 一律忽略（幂等收尾）
    finished: bool = False
    finish_reason: str = ""  # "done" / "error" / "cancelled" / "timeout"
    # ---------------- 断点续传专用 ----------------
    # 事件环形缓冲：元素是 (event_id, frame_str)，大小上限 = STREAM_RESUME_EVENT_RING_MAX
    #   - 注：不包含 replay_start/replay_end/gap（回放本身不回放）、heartbeat（重连后用新的）
    event_ring: "Deque[Tuple[str, str]]" = field(default_factory=lambda: deque(maxlen=STREAM_RESUME_EVENT_RING_MAX))
    # 记录每条事件在 ring 里的位置索引（event_id → 环内顺序编号），用来快速找切片起点
    _event_seq: Dict[str, int] = field(default_factory=dict)
    _next_seq: int = 1  # 单调递增，用 seq 切 deque
    # 完成态时间戳（用于 done 的会话 TTL 过期）；None = 还在跑
    finished_at: Optional[float] = None
    # ---------------- 新增：检索/引用元数据专用 ----------------
    # 1) 已注册的"引用角标 → 元数据"（1:1，CitationMetaItem.index 去重）
    _citation_meta: Dict[int, CitationMetaItem] = field(default_factory=dict)
    # 2) 检索池：channel → List[RetrieveResultItem]（用于 §3.5 fallback 关键词重叠分配）
    _retrieved_docs_by_channel: Dict[str, List[RetrieveResultItem]] = field(default_factory=dict)
    # 3) 已推送过的 citation index（避免重复 push）
    _pushed_citation_indices: Set[int] = field(default_factory=set)

    def register_retrieved_documents(self, channel: str, items: List[RetrieveResultItem]) -> None:
        """检索通道结果写入本地池（供 citation fallback 召回使用）。"""
        if not items:
            return
        lst = self._retrieved_docs_by_channel.setdefault(channel, [])
        lst.extend(items)

    def iterate_all_retrieved_items(self) -> List[RetrieveResultItem]:
        out: List[RetrieveResultItem] = []
        for ch_items in self._retrieved_docs_by_channel.values():
            out.extend(ch_items)
        return out

    # ---- 引用角标 API ----
    def set_citation_meta(
        self,
        index: int,
        *,
        title: str,
        url: str = "",
        source_type: str = "web",
        reliability: str = "待验证",
        channel: str = "",
        snippet: str = "",
        published_at: str = "",
    ) -> int:
        """注册一条引用角标 [index] 的元数据。返回 index（便于链式调用）。"""
        if index <= 0:
            return 0
        existing = self._citation_meta.get(index)
        # snippet：按"答案命中句中心窗口"聚焦裁剪，硬上限 CITATION_SNIPPET_MAX_CHARS（默认 100）
        try:
            focus_sentences = self._final_sentences_hint if hasattr(self, "_final_sentences_hint") else None
        except Exception:
            focus_sentences = None
        if snippet:
            merge_snippet = _build_snippet(
                snippet,
                title=title or (existing.title if existing else ""),
                focus_sentences=focus_sentences,
            )
        else:
            raw_old = existing.snippet if existing else ""
            merge_snippet = _build_snippet(
                raw_old,
                title=title or (existing.title if existing else ""),
                focus_sentences=focus_sentences,
            ) if raw_old else ""
        merge_title = (str(title or (existing.title if existing else f"来源{index}"))
                       [:CITATION_TITLE_MAX_CHARS])
        merge_url = (str(url or (existing.url if existing else ""))[:CITATION_URL_MAX_CHARS])
        item = CitationMetaItem(
            index=index,
            title=merge_title,
            url=merge_url,
            source_type=source_type or (existing.source_type if existing else "web"),
            reliability=reliability or (existing.reliability if existing else "待验证"),
            channel=channel or (existing.channel if existing else ""),
            snippet=merge_snippet,
            published_at=published_at or (existing.published_at if existing else ""),
        )
        self._citation_meta[index] = item
        return index

    def get_citation_meta(self, index: int) -> Optional[CitationMetaItem]:
        return self._citation_meta.get(index)

    def pending_new_citation_items(self, indices: List[int]) -> List[CitationMetaItem]:
        """给定一串引用编号，过滤出"尚未推送到前端"的那一批，并同步标记已推送。"""
        out: List[CitationMetaItem] = []
        if not indices:
            return out
        for idx in sorted(set(indices)):
            if idx <= 0:
                continue
            if idx in self._pushed_citation_indices:
                continue
            meta = self._citation_meta.get(idx)
            if meta is None:
                continue
            self._pushed_citation_indices.add(idx)
            out.append(meta)
        return out

    def snapshot_all_citation_meta_items(self) -> List[CitationMetaItem]:
        return [self._citation_meta[k] for k in sorted(self._citation_meta.keys())]

    # ---- 事件缓冲 API ----
    def append_event(self, event_id: str, frame: str) -> None:
        """把一个 SSE 帧（除了 replay/gap/heartbeat）写入环形缓冲。"""
        seq = self._next_seq
        self._next_seq += 1
        ring = self.event_ring  # type: Deque[Tuple[str, str]]
        ring.append((event_id, frame))
        self._event_seq[event_id] = seq
        # deque maxlen 超出会自动抛最旧；但 _event_seq 还要同步清理
        #   这里做一个懒惰清理：如果 seq 数 > 2x maxlen，就用当前 ring 内 id 重建 map
        #   （单次重建 O(N)，摊销 O(1)；避免每条事件 pop _event_seq 的 dict 开销）
        if len(self._event_seq) > STREAM_RESUME_EVENT_RING_MAX * 2:
            keep = {eid: self._event_seq[eid] for (eid, _) in ring if eid in self._event_seq}
            self._event_seq = keep

    def get_events_since(self, last_event_id: str) -> Tuple[bool, int, List[Tuple[str, str]]]:
        """取出 (last_event_id, 此刻] 的所有事件（不含 last 自身）。

        返回 (has_gap, gap_count, events_since)：
          has_gap=True  → last_event_id 已在环外（有缺口）；events_since 是目前能拿到的全部
          has_gap=False → 完美续传，events_since 就是缺失的那一段
          gap_count = 推断缺口条数（仅作提示用，非精确）
        """
        ring: "Deque[Tuple[str, str]]" = self.event_ring
        if not ring:
            # 无任何缓存：100% 断档
            return (True, max(1, self._next_seq - 1), [])
        if not last_event_id:
            # 没传 last：视为需要全部 → 当前实现不做"全量回放"，走 gap 让调用方用 resync
            return (True, len(ring), [])

        last_seq = self._event_seq.get(last_event_id)
        if last_seq is None:
            # last 不在环里 → 完全溢出
            return (True, len(ring), list(ring))

        # 从 last_seq+1 取到末尾
        # 计算环内第一条 seq：ring[0] seq
        first_id_in_ring, _ = ring[0]
        first_seq_in_ring = self._event_seq.get(first_id_in_ring, last_seq)
        gap_count: int
        has_gap: bool
        if first_seq_in_ring > last_seq + 1:
            # 环起始比用户断点还新：中间有一些被 deque 挤出了
            has_gap = True
            gap_count = first_seq_in_ring - (last_seq + 1)
        else:
            has_gap = False
            gap_count = 0
        # 按索引从 (last_seq+1 - first_seq_in_ring) 切到尾
        start = max(0, last_seq + 1 - first_seq_in_ring)
        items = list(ring)[start:]
        return (has_gap, gap_count, items)

    def earliest_event_id(self) -> str:
        ring: "Deque[Tuple[str, str]]" = self.event_ring
        return ring[0][0] if ring else ""

    def latest_event_id(self) -> str:
        ring: "Deque[Tuple[str, str]]" = self.event_ring
        return ring[-1][0] if ring else ""

    def is_finished_and_expired(self, ttl_sec: int = STREAM_RESUME_DONE_SESSION_TTL_SEC) -> bool:
        return bool(self.finished and self.finished_at is not None
                    and (time.monotonic() - self.finished_at) > ttl_sec)

    def mark_finished_at(self) -> None:
        """记录完成时间戳（TTL 过期用）。"""
        self.finished_at = time.monotonic

    # ---- 来源索引 API（原始实现保留） ----
    def add_source(
        self,
        title: str,
        *,
        url: str = "",
        source_type: str = "web",
        reliability: str = "待验证",
        snippet: str = "",
        published_at: str = "",
    ) -> int:
        """新增或查已有来源。返回 1-based 的 index。"""
        dedup_key = f"{(url or '')[:CITATION_URL_MAX_CHARS]}||{(title or '')[:CITATION_TITLE_MAX_CHARS]}"
        if dedup_key in self._source_dedup_key:
            return self._source_dedup_key[dedup_key]
        idx = len(self._source_pool) + 1
        item = SourceRefItem(
            index=idx,
            title=(str(title or "")[:CITATION_TITLE_MAX_CHARS]),
            url=(str(url or "")[:CITATION_URL_MAX_CHARS]),
            source_type=source_type,
            reliability=reliability,
            snippet=_build_snippet(snippet, title=title),
            published_at=(str(published_at or "")[:32]),
        )
        self._source_pool.append(item)
        self._source_dedup_key[dedup_key] = idx
        return idx

    @property
    def source_count(self) -> int:
        return len(self._source_pool)

    def snapshot_source_ref_payload(self) -> SourceRefPayload:
        return SourceRefPayload(items=list(self._source_pool))

    def append_delta(self, text: str, is_reasoning: bool = False) -> None:
        if is_reasoning:
            return
        self.final_text_buffer += text


# ======================================================================
# 主类：StreamEventBus
# ======================================================================

class StreamEventBus:
    """进程内多播事件总线。"""

    def __init__(self, max_subs: int = STREAM_BUS_MAX_SUBS,
                 max_thread_states: int = STREAM_RESUME_MAX_THREAD_STATES):
        self._max_subs = max_subs
        self._max_thread_states = max_thread_states
        # 主循环引用（发布端若处于工作线程，需要 run_coroutine_threadsafe）
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        # LRU：OrderedDict[sub_key=(thread_id,sub_id) -> weakref(sub)]
        self._subs_by_key: "OrderedDict[tuple, weakref.ReferenceType[StreamSubscriber]]" = OrderedDict()
        # 索引：thread_id -> set(sub_ids)
        self._thread_to_subids: Dict[str, Set[str]] = {}
        # 每个线程的聚合状态（LRU：超过 max_thread_states 自动剔除最久不用的）
        self._thread_state: "OrderedDict[str, ThreadStreamState]" = OrderedDict()
        # 锁：asyncio.Lock（惰性创建，因为构造时可能还没 loop）
        self._lock: Optional[asyncio.Lock] = None
        # 后台心跳任务
        self._heartbeat_task: Optional[asyncio.Task] = None
        # 已缓存的事件 ID 正则（提取 SSE 帧里的 "id: X"）
        self._re_extract_id = _re.compile(r"(?:^|\n)id:\s*([^\n]+)")

    # ----------------------------------------------------------
    # 生命周期：loop 绑定 / 心跳启动
    # ----------------------------------------------------------
    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """server.py lifespan 启动时调用一次：绑定主循环 + 启动心跳任务。"""
        self._loop = loop
        if self._lock is None:
            # 在 loop 内创建 asyncio.Lock
            self._lock = asyncio.Lock()
        if self._heartbeat_task is None:
            async def _runner():
                while True:
                    try:
                        await asyncio.sleep(STREAM_BUS_HEARTBEAT_INTERVAL_SEC)
                        await self._publish_heartbeat_all()
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        logger.warning("[StreamBus] heartbeat 异常: %s", e)
            self._heartbeat_task = loop.create_task(_runner())
            logger.info("[StreamBus] heartbeat task 启动，间隔 %ds",
                        STREAM_BUS_HEARTBEAT_INTERVAL_SEC)

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    # ----------------------------------------------------------
    # Thread 状态获取（幂等初始化）
    # ----------------------------------------------------------
    def get_thread_state(self, thread_id: str) -> ThreadStreamState:
        state = self._thread_state.get(thread_id)
        if state is None:
            # LRU 淘汰：先保证 _thread_state 容量不超
            if len(self._thread_state) >= self._max_thread_states:
                self._thread_state.popitem(last=False)
            state = ThreadStreamState(thread_id=thread_id)
            self._thread_state[thread_id] = state
        else:
            # LRU touch（最近使用移到尾部）
            self._thread_state.move_to_end(thread_id)
        return state

    def reset_thread_state(self, thread_id: str) -> None:
        """用户在同一 thread_id 发起"第二轮提问"前调用：重置状态，不重置订阅。"""
        self._thread_state[thread_id] = ThreadStreamState(thread_id=thread_id)
        self._thread_state.move_to_end(thread_id)

    def has_thread_state(self, thread_id: str) -> bool:
        """thread_id 是否已有状态（重连时判断是否存在可续传的会话）。"""
        s = self._thread_state.get(thread_id)
        if s is None:
            return False
        # TTL 过期：把它当作不存在，让 SSE 端点走 gap → resync 流程
        if s.is_finished_and_expired():
            return False
        self._thread_state.move_to_end(thread_id)
        return True

    # ----------------------------------------------------------
    # 断点续传：获取 (last_event_id, now] 事件切片
    # ----------------------------------------------------------
    def get_events_since(self, thread_id: str, last_event_id: str) -> Tuple[bool, int, List[Tuple[str, str]]]:
        """重连回放时调用。返回 (has_gap, gap_count, [(event_id, frame_str), ...])。"""
        state = self._thread_state.get(thread_id)
        if state is None:
            return (True, 0, [])
        self._thread_state.move_to_end(thread_id)
        return state.get_events_since(last_event_id)

    def earliest_event_id(self, thread_id: str) -> str:
        s = self._thread_state.get(thread_id)
        return s.earliest_event_id() if s else ""

    def latest_event_id(self, thread_id: str) -> str:
        s = self._thread_state.get(thread_id)
        return s.latest_event_id() if s else ""

    # ----------------------------------------------------------
    # 订阅 / 取消订阅
    # ----------------------------------------------------------
    def subscribe(self, thread_id: str) -> StreamSubscriber:
        """创建一个订阅者。如果订阅数超限，LRU 淘汰最旧订阅。"""
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                raise RuntimeError("StreamEventBus 必须在 asyncio loop 内使用，或先 bind_loop。")
        loop = self._loop

        import uuid as _uuid
        sub_id = _uuid.uuid4().hex[:10]
        sub = StreamSubscriber(thread_id, sub_id, loop)

        async def _register():
            lock = self._get_lock()
            async with lock:
                # LRU 淘汰
                while len(self._subs_by_key) >= self._max_subs:
                    oldest_key, oldest_ref = self._subs_by_key.popitem(last=False)
                    try:
                        s = oldest_ref()
                        if s is not None:
                            s.close()
                    except Exception:
                        pass
                    old_tid, old_sid = oldest_key
                    t_set = self._thread_to_subids.get(old_tid)
                    if t_set:
                        t_set.discard(old_sid)
                        if not t_set:
                            self._thread_to_subids.pop(old_tid, None)
                    logger.info("[StreamBus] LRU 淘汰订阅 %s@%s（订阅数上限 %d）",
                                old_sid, old_tid, self._max_subs)
                # 注册
                self._subs_by_key[(thread_id, sub_id)] = weakref.ref(sub)
                self._thread_to_subids.setdefault(thread_id, set()).add(sub_id)
                # LRU touch
                self._subs_by_key.move_to_end((thread_id, sub_id))

        # 同步入口兼容：如果 caller 在同一 loop，直接 call_soon；否则 run_coroutine_threadsafe
        try:
            running_loop = asyncio.get_running_loop()
            if running_loop is loop:
                loop.create_task(_register())
            else:
                asyncio.run_coroutine_threadsafe(_register(), loop)
        except RuntimeError:
            asyncio.run_coroutine_threadsafe(_register(), loop)
        return sub

    def unsubscribe(self, thread_id: str, sub: StreamSubscriber) -> None:
        sub.close()
        key = (thread_id, sub.sub_id)

        async def _deregister():
            lock = self._get_lock()
            async with lock:
                self._subs_by_key.pop(key, None)
                t_set = self._thread_to_subids.get(thread_id)
                if t_set:
                    t_set.discard(sub.sub_id)
                    if not t_set:
                        self._thread_to_subids.pop(thread_id, None)
        if self._loop is not None and self._loop.is_running():
            try:
                running_loop = asyncio.get_running_loop()
                if running_loop is self._loop:
                    self._loop.create_task(_deregister())
                else:
                    asyncio.run_coroutine_threadsafe(_deregister(), self._loop)
            except RuntimeError:
                asyncio.run_coroutine_threadsafe(_deregister(), self._loop)

    # ----------------------------------------------------------
    # 发布：统一入口（SSE 帧字符串）
    # ----------------------------------------------------------
    def _publish_frame(self, thread_id: str, frame: str, *, touch_thread: bool = True) -> None:
        """把一帧投递给该 thread_id 下所有订阅者。

        同时做两件事：
          1) 写入订阅者 queue 多播（原逻辑）；
          2) 若 frame 是业务事件（非 heartbeat / 非 replay_* / 非 gap），写入
             ThreadStreamState.event_ring 环形缓冲，供断线重连回放。
        """
        if self._loop is None:
            return
        loop = self._loop

        # --- 事件缓冲双写：心跳 / 回放控制事件不写 ring ---
        try:
            # 用前缀近似判断（避免 JSON 反序列化，每条事件 O(1)）
            first_line = frame[: frame.find("\n")] if "\n" in frame else frame
            # replay_start / replay_end / gap / heartbeat 四个事件不入 ring
            if not (first_line.startswith("event: replay")
                    or first_line.startswith("event: gap")
                    or first_line.startswith("event: heartbeat")):
                state = self.get_thread_state(thread_id)
                # 抽 event_id
                m = self._re_extract_id.search(frame)
                eid = m.group(1) if m else ""
                if eid:
                    state.append_event(eid, frame)
                # 若是 done/error → 标记完成时间戳（TTL 用）
                if first_line.startswith("event: done") or first_line.startswith("event: error"):
                    state.mark_finished_at()
        except Exception as be:
            logger.warning("[StreamBus] 写入环形缓冲失败: %s (tid=%s)", be, thread_id)

        async def _deliver_core():
            lock = self._get_lock()
            async with lock:
                sub_ids = list(self._thread_to_subids.get(thread_id, set()))
                if not sub_ids:
                    return
                alive_refs: List[tuple] = []
                for sid in sub_ids:
                    ref = self._subs_by_key.get((thread_id, sid))
                    if ref is None:
                        continue
                    s = ref()
                    if s is None:
                        continue
                    alive_refs.append((sid, s))
                for sid, s in alive_refs:
                    s.enqueue(frame)
                    k = (thread_id, sid)
                    if k in self._subs_by_key:
                        self._subs_by_key.move_to_end(k)
                if len(alive_refs) != len(sub_ids):
                    alive_ids = {x[0] for x in alive_refs}
                    remain = set()
                    for sid in sub_ids:
                        if sid in alive_ids:
                            remain.add(sid)
                    if remain:
                        self._thread_to_subids[thread_id] = remain
                    else:
                        self._thread_to_subids.pop(thread_id, None)

        try:
            running_loop = asyncio.get_running_loop()
            if running_loop is loop:
                # 同 loop：同步执行（注意 _deliver_core 是 async，create_task 即可）
                #   为了让"发布 → sub 队列有帧"在逻辑上同步可见，这里不立即 await，
                #   但测试场景下 loop 会跑所有 pending task。如果想更"即时"，可直接
                #   asyncio.create_task → 也可以；对 SSE 场景两种都 OK。
                loop.create_task(_deliver_core())
            else:
                asyncio.run_coroutine_threadsafe(_deliver_core(), loop)
        except RuntimeError:
            # 不在 async context（纯同步线程调用）：走 run_coroutine_threadsafe
            asyncio.run_coroutine_threadsafe(_deliver_core(), loop)

    async def _publish_heartbeat_all(self) -> None:
        """心跳：给所有活跃订阅发送一个 heartbeat 帧（用 comment 也可）。"""
        lock = self._get_lock()
        async with lock:
            if not self._subs_by_key:
                return
            ev_id = new_event_id()
            hb_frame = SSEFrame.heartbeat(ev_id) + SSEFrame.comment()
            # 按 thread_id 去重投递：同一 thread_id 的多个订阅者内容相同
            delivered: Set[str] = set()
            for (tid, sid), ref in list(self._subs_by_key.items()):
                if tid in delivered:
                    continue
                sub = ref() if callable(ref) else None
                if sub is None:
                    continue
                delivered.add(tid)
                # 不绕 _publish_frame 双写：直接写内部投递
                sub_ids = list(self._thread_to_subids.get(tid, set()))
                for ssid in sub_ids:
                    ref2 = self._subs_by_key.get((tid, ssid))
                    if not ref2:
                        continue
                    s = ref2()
                    if s:
                        s.enqueue(hb_frame)

    # ----------------------------------------------------------
    # 便捷发布 API（后端端点 / 内部 agent 直接用）—— 每个事件类型一种方法
    #   - 自动生成 event_id
    #   - 更新 thread_state（final_text / source_ref / 计数）
    #   - 产出 SSE 帧并投递
    # ----------------------------------------------------------

    def ev_open(self, thread_id: str, request_id: str, user_id: str = "") -> str:
        state = self.get_thread_state(thread_id)
        p = OpenPayload(request_id=request_id, thread_id=thread_id, user_id=user_id)
        frame = SSEFrame.open(p, new_event_id())
        self._publish_frame(thread_id, frame)
        return frame

    def ev_delta(self, thread_id: str, text: str, *, is_reasoning: bool = False) -> str:
        state = self.get_thread_state(thread_id)
        if state.finished:
            return ""
        state.append_delta(text, is_reasoning=is_reasoning)
        state.delta_count += 1
        p = DeltaPayload(index=state.delta_count, text=text, is_reasoning=is_reasoning)
        frame = SSEFrame.delta(p, new_event_id())
        self._publish_frame(thread_id, frame)
        return frame

    def ev_reasoning(self, thread_id: str, title: str, content: str, elapsed_ms: int = 0,
                     stage: str = "model_coT") -> str:
        state = self.get_thread_state(thread_id)
        if state.finished:
            return ""
        state.reasoning_count += 1
        rid = f"r{state.reasoning_count}"
        p = ReasoningPayload(reasoning_id=rid, title=title, content=content,
                             elapsed_ms=elapsed_ms, stage=stage)
        frame = SSEFrame.reasoning(p, new_event_id())
        self._publish_frame(thread_id, frame)
        return frame

    def ev_tool_call(self, thread_id: str, call_id: str, tool_name: str, tool_category: str = "tool",
                     args_snippet: Optional[Dict[str, Any]] = None) -> str:
        state = self.get_thread_state(thread_id)
        if state.finished:
            return ""
        state.tool_call_count += 1
        p = ToolCallPayload(call_id=call_id, tool_name=tool_name, tool_category=tool_category,
                            args_snippet=args_snippet or {})
        frame = SSEFrame.tool_call(p, new_event_id())
        self._publish_frame(thread_id, frame)
        # 同时 push 一条 progress（进度）事件，前端不用维护两个逻辑
        self.ev_progress(thread_id, stage=f"调用工具: {tool_name}",
                         percent=min(90, 20 + state.tool_call_count * 8),
                         detail=f"开始执行 {tool_name} ...")
        return frame

    def ev_tool_result(
        self,
        thread_id: str,
        call_id: str,
        tool_name: str,
        success: bool,
        *,
        duration_ms: int = 0,
        result_snippet: str = "",
        error_msg: str = "",
        source_candidates: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        state = self.get_thread_state(thread_id)
        if state.finished:
            return ""
        # 先把 source_candidates 注册到引用池 → 返回 (index, 对象) 列表
        refs: List[Dict[str, Any]] = []
        for cand in (source_candidates or []):
            idx = state.add_source(
                title=cand.get("title", ""),
                url=cand.get("url", ""),
                source_type=cand.get("source_type", "web"),
                reliability=cand.get("reliability", "待验证"),
                snippet=cand.get("snippet", ""),
                published_at=cand.get("published_at", ""),
            )
            refs.append({
                "index": idx,
                "title": cand.get("title", ""),
                "url": cand.get("url", ""),
                "source_type": cand.get("source_type", "web"),
                "reliability": cand.get("reliability", "待验证"),
            })
        p = ToolResultPayload(call_id=call_id, tool_name=tool_name, success=success,
                              duration_ms=duration_ms, result_snippet=result_snippet[:200],
                              source_refs=refs, error_msg=error_msg)
        frame = SSEFrame.tool_result(p, new_event_id())
        self._publish_frame(thread_id, frame)
        # 如果新增了来源，顺便 push 一份 snapshot
        if refs:
            self.ev_source_ref(thread_id)
        return frame

    def ev_source_ref(self, thread_id: str) -> str:
        state = self.get_thread_state(thread_id)
        snap = state.snapshot_source_ref_payload()
        frame = SSEFrame.source_ref(snap, new_event_id())
        self._publish_frame(thread_id, frame)
        return frame

    # ----------------------------------------------------------
    # §5 检索 & 引用元数据事件（retrieve_result / citation_meta）
    # ----------------------------------------------------------
    def ev_retrieve_result(
        self,
        thread_id: str,
        channel: str,
        query: str,
        items: List[Dict[str, Any]],
        *,
        duration_ms: int = 0,
        success: bool = True,
        error_msg: str = "",
    ) -> str:
        """发布一路检索通道的候选文档结果（§5.2）。

        同时：(a) 把 RetrieveResultItem 注册到本地引用池（供 fallback 动态分配）；
              (b) 把每个 item 也 add_source 到 SourceRef，统一编号。
        """
        state = self.get_thread_state(thread_id)
        if state.finished:
            return ""
        # 构造 RetrieveResultItem
        parsed_items: List[RetrieveResultItem] = []
        for raw in (items or []):
            doc_id = str(raw.get("doc_id") or f"{channel}-{len(parsed_items)+1}")
            # 精简：标题 / URL 硬上限（常量 TITLE_MAX=80, URL_MAX=256），避免长字段吃 token
            title = (str(raw.get("title") or f"{channel}结果{len(parsed_items)+1}")
                     [:CITATION_TITLE_MAX_CHARS])
            url = str(raw.get("url") or "")[:CITATION_URL_MAX_CHARS]
            # content 仍然保留较完整的 2000 字（给 overlap fallback 动态分配引用用）；
            # 前端/悬停卡片展示只取 snippet，与 content 独立。
            content = str(raw.get("content") or raw.get("snippet") or "")[:2000]
            source_type = str(raw.get("source_type") or "web")
            reliability = str(raw.get("reliability") or "待验证")
            score = float(raw.get("score") or 0.0)
            pub = str(raw.get("published_at") or "")
            meta: Dict[str, Any] = dict(raw.get("meta") or {})
            item = RetrieveResultItem(
                doc_id=doc_id, title=title, url=url, content=content,
                source_type=source_type, reliability=reliability, channel=channel,
                score=score, published_at=pub, meta=meta,
            )
            parsed_items.append(item)
            # 同步写入 state 检索池 & 来源池（来源池统一编号与 citation_meta 对齐）
            # snippet 由 add_source 内部走 _build_snippet → 聚焦 100 字，不必这里硬截
            state.add_source(
                title=title, url=url, source_type=source_type, reliability=reliability,
                snippet=content, published_at=pub,
            )
        state.register_retrieved_documents(channel, parsed_items)
        p = RetrieveResultPayload(
            channel=channel, query=query, items=parsed_items,
            duration_ms=duration_ms, success=success, error_msg=error_msg,
        )
        frame = SSEFrame.retrieve_result(p, new_event_id())
        self._publish_frame(thread_id, frame)
        # 新增来源 push snapshot + citation_meta（增量）
        if parsed_items:
            # 把 parsed_items 的 index 按 pool 顺序映射为 CitationMetaItem
            _new_indices: List[int] = []
            for it in parsed_items:
                idx = state.add_source(
                    title=it.title, url=it.url, source_type=it.source_type,
                    reliability=it.reliability, snippet=it.content,
                    published_at=it.published_at,
                )
                if idx <= 0:
                    continue
                # set_citation_meta 内部会对 snippet 聚焦截到 100 字，这里传原文即可
                state.set_citation_meta(
                    idx, title=it.title, url=it.url,
                    source_type=it.source_type, reliability=it.reliability,
                    channel=it.channel, published_at=it.published_at,
                    snippet=it.content,
                )
                _new_indices.append(idx)
            pending = state.pending_new_citation_items(_new_indices)
            if pending:
                self.ev_citation_meta(thread_id, pending)
            self.ev_source_ref(thread_id)
        return frame

    def ev_citation_meta(
        self,
        thread_id: str,
        items: Optional[List[CitationMetaItem]] = None,
        *,
        indices: Optional[List[int]] = None,
        attach_snippet: str = "",
    ) -> str:
        """推送引用角标映射（增量）。两种入口：
          1) ev_citation_meta(tid, items=[CitationMetaItem(1,...), ...])
          2) ev_citation_meta(tid, indices=[1,3,5], attach_snippet="...")
        """
        state = self.get_thread_state(thread_id)
        if state.finished:
            return ""
        # Case 2: 根据 indices 把已注册的 citation_meta 转 items，并补 snippet
        if indices:
            merged: List[CitationMetaItem] = []
            for idx in sorted(set(indices)):
                existing = state.get_citation_meta(idx)
                if existing is None:
                    # fallback：source_ref 里有没有
                    if 1 <= idx <= len(state._source_pool):
                        src = state._source_pool[idx - 1]
                        merged.append(CitationMetaItem(
                            index=src.index,
                            title=str(src.title or "")[:CITATION_TITLE_MAX_CHARS],
                            url=str(src.url or "")[:CITATION_URL_MAX_CHARS],
                            source_type=src.source_type,
                            reliability=src.reliability,
                            snippet=_build_snippet(attach_snippet or src.snippet,
                                                   title=src.title),
                            published_at=src.published_at,
                        ))
                    continue
                if attach_snippet and not existing.snippet:
                    existing.snippet = _build_snippet(attach_snippet, title=existing.title)
                # 最终统一再卡一遍 100 字硬上限（sorted_items 之后 SSE 发出去的就是最终）
                elif existing.snippet:
                    existing.snippet = _build_snippet(existing.snippet, title=existing.title)
                merged.append(existing)
            items = (items or []) + merged
        if not items:
            return ""
        # 去重 index，按 index 升序；最后统一 clamp title/url/snippet 到用户要求上限（token 安全）
        dedup: Dict[int, CitationMetaItem] = {}
        for it in items:
            dedup[it.index] = it
        sorted_items: List[CitationMetaItem] = []
        for k in sorted(dedup.keys()):
            if k <= 0:
                continue
            it = dedup[k]
            title = str(it.title or "")[:CITATION_TITLE_MAX_CHARS]
            url = str(it.url or "")[:CITATION_URL_MAX_CHARS]
            snippet = _build_snippet(it.snippet, title=title)
            # 只保留用户明确需要的字段：title / url / snippet
            # （其他字段 source_type / reliability / channel / published_at 保留，但不参与展示层渲染）
            sorted_items.append(CitationMetaItem(
                index=it.index,
                title=title,
                url=url,
                source_type=it.source_type,
                reliability=it.reliability,
                channel=it.channel,
                snippet=snippet,
                published_at=it.published_at,
            ))
        if not sorted_items:
            return ""
        p = CitationMetaPayload(items=sorted_items)
        frame = SSEFrame.citation_meta(p, new_event_id())
        self._publish_frame(thread_id, frame)
        return frame

    def ev_progress(self, thread_id: str, stage: str, percent: int = 0, detail: str = "") -> str:
        state = self.get_thread_state(thread_id)
        if state.finished:
            return ""
        p = ProgressPayload(stage=stage, percent=min(100, max(0, percent)), detail=detail)
        frame = SSEFrame.progress(p, new_event_id())
        self._publish_frame(thread_id, frame)
        return frame

    def ev_done(self, thread_id: str, *, usage: Optional[Dict[str, int]] = None,
                force_final_text: Optional[str] = None) -> str:
        state = self.get_thread_state(thread_id)
        if state.finished:
            return ""
        state.finished = True
        state.finish_reason = "done"
        final_text = force_final_text if force_final_text is not None else state.final_text_buffer
        total_ms = int((time.monotonic() - state.start_monotonic) * 1000)
        p = DonePayload(final_text=final_text,
                        usage=usage or {},
                        total_duration_ms=total_ms,
                        source_ref_count=state.source_count,
                        tool_call_count=state.tool_call_count)
        # 最后再推一次完整引用列表（确保末尾参考文献齐）
        if state.source_count > 0:
            self.ev_source_ref(thread_id)
        frame = SSEFrame.done(p, new_event_id())
        self._publish_frame(thread_id, frame)
        return frame

    def ev_error(self, thread_id: str, message: str, *, code: str = "INTERNAL_ERROR",
                 cancelled: bool = False, recoverable: bool = True) -> str:
        state = self.get_thread_state(thread_id)
        if state.finished:
            return ""
        state.finished = True
        state.finish_reason = ("cancelled" if cancelled else ("timeout" if code == "TIMEOUT" else "error"))
        p = ErrorPayload(message=message, code=code, cancelled=cancelled, recoverable=recoverable)
        frame = SSEFrame.error(p, new_event_id())
        self._publish_frame(thread_id, frame)
        return frame


# ======================================================================
# 全局单例
# ======================================================================

_global_bus: Optional[StreamEventBus] = None
_singleton_lock: Optional[asyncio.Lock] = None


async def get_stream_bus() -> StreamEventBus:
    """异步获取全局单例（绑定 lock 到调用方 loop）。"""
    global _global_bus, _singleton_lock
    if _singleton_lock is None:
        _singleton_lock = asyncio.Lock()
    async with _singleton_lock:
        if _global_bus is None:
            _global_bus = StreamEventBus()
    return _global_bus


def get_stream_bus_sync() -> StreamEventBus:
    """同步版本：server lifespan 初始化 / monitor 桥接处用。"""
    global _global_bus
    if _global_bus is None:
        _global_bus = StreamEventBus()
    return _global_bus


# ======================================================================
# Monitor 桥接补丁：把 ToolMonitor 的 WS 事件同步翻译成 bus 事件
#   —— 这样 main_agent 内部的 monitor.report_* 零修改就能同时喂给 SSE。
#   在 server.py lifespan 中调用一次 `install_monitor_bridge_to_bus()`。
# ======================================================================

def install_monitor_bridge_to_bus() -> None:
    """给 monitor._emit 打补丁：在原 WS 发送之前，先把事件路由到 bus。"""
    from api.monitor import monitor  # 延迟 import，避免环
    bus = get_stream_bus_sync()

    # 保存原 _emit
    _orig_emit = monitor._emit  # type: ignore[attr-defined]

    # 每个 tool_call_id 的启动时间，用于 tool_result duration_ms
    _call_started: Dict[str, Dict[str, Any]] = {}

    def _bridged_emit(event_type: str, message: str, data: Optional[Dict[str, Any]] = None):
        data = data or {}
        thread_id = None
        try:
            from api.context import get_thread_context
            thread_id = get_thread_context()
        except Exception:
            thread_id = None

        if thread_id:
            try:
                _route_monitor_to_bus(bus, thread_id, event_type, message, data, _call_started)
            except Exception as be:
                logger.warning("[StreamBus] monitor→bus 桥接失败: %s (event=%s tid=%s)",
                               be, event_type, thread_id)

        # 原逻辑继续走 WS（旧 WS 路径继续生效，不破坏现有前端）
        return _orig_emit(event_type, message, data)

    # 打补丁
    monitor._emit = _bridged_emit  # type: ignore[attr-defined]
    logger.info("[StreamBus] monitor→bus 桥接安装完成")


def _route_monitor_to_bus(
    bus: StreamEventBus,
    thread_id: str,
    event_type: str,
    message: str,
    data: Dict[str, Any],
    call_started: Dict[str, Dict[str, Any]],
) -> None:
    """把 monitor 事件转换成 bus 事件（根据 event_type 分发）。"""
    import uuid as _uuid
    # ===== 双保险脱敏：绝对路径再次过滤（即使 monitor._emit 漏过，这里兜底）=====
    try:
        from api.monitor import sanitize_abs_paths as _sap, sanitize_data_paths as _sdp
        message = _sap(message)
        data = _sdp(data)
    except Exception:
        pass
    if event_type == "thinking":
        stage = data.get("phase") or "智能体思考中"
        bus.ev_progress(thread_id, stage=stage, percent=5, detail=message)
        return
    if event_type == "tool_start":
        tool_name = data.get("tool_name", "")
        args = data.get("args") or {}
        cid = "tc_" + _uuid.uuid4().hex[:8]
        call_started[(thread_id, cid)] = {
            "tool_name": tool_name,
            "started_at": time.monotonic(),
        }
        # args_snippet 裁剪：最多保留 2 个键值，值≤64字
        args_snippet: Dict[str, Any] = {}
        if isinstance(args, dict):
            for i, (k, v) in enumerate(args.items()):
                if i >= 2:
                    break
                sv = str(v)
                if len(sv) > 64:
                    sv = sv[:61] + "..."
                args_snippet[k] = sv
        bus.ev_tool_call(thread_id, call_id=cid, tool_name=tool_name or "unknown_tool",
                         tool_category="tool", args_snippet=args_snippet)
        return
    if event_type == "tool_end":
        tool_name = data.get("tool_name", "")
        result_snippet = data.get("result", "") or ""
        # 找最近一个未完成的同工具名 call_id（近似匹配）
        duration_ms = 0
        matched_cid = None
        for (tid, cid), info in list(call_started.items()):
            if tid == thread_id and info.get("tool_name") == tool_name:
                duration_ms = int((time.monotonic() - info["started_at"]) * 1000)
                matched_cid = cid
                del call_started[(tid, cid)]
                break
        if matched_cid is None:
            matched_cid = "tc_" + _uuid.uuid4().hex[:8]
        # 来源候选：如果 result 中出现了 Markdown 链接 [text](url)，把它们变成 source_ref
        source_candidates: List[Dict[str, Any]] = []
        import re as _re
        if result_snippet:
            for m in _re.finditer(r"\[([^\]]{2,120})\]\((https?://[^)\s]+)\)", result_snippet):
                source_candidates.append({
                    "title": m.group(1),
                    "url": m.group(2),
                    "source_type": "web",
                    "reliability": "待验证",
                    "snippet": "",
                })
        bus.ev_tool_result(thread_id, call_id=matched_cid, tool_name=tool_name or "unknown_tool",
                           success=True, duration_ms=duration_ms,
                           result_snippet=result_snippet,
                           source_candidates=source_candidates[:8])
        return
    if event_type in ("task_result",):
        # task_result → 作为 DONE 的最终文本覆盖，防止 delta 聚合与最终 LLM 文本有差异
        result_text = data.get("result", "") or ""
        bus.ev_done(thread_id, force_final_text=result_text)
        return
    if event_type == "error":
        bus.ev_error(thread_id, message=message or "未知错误", code="INTERNAL_ERROR",
                     cancelled=False, recoverable=True)
        return
    if event_type == "session_created":
        bus.ev_progress(thread_id, stage="初始化会话", percent=1, detail=message)
        return
    if event_type == "assistant_call":
        aname = data.get("assistant_name") or "子智能体"
        bus.ev_tool_call(thread_id, call_id="ag_" + _uuid.uuid4().hex[:8],
                         tool_name=aname, tool_category="subagent",
                         args_snippet=data.get("args") or {})
        return
    # 其他事件：作为 progress.detail
    bus.ev_progress(thread_id, stage=event_type or "处理中", percent=0, detail=message)
