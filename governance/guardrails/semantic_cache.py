# -*- coding: utf-8 -*-
"""
语义缓存层（降级链缓存兜底）：原降级链的缓存层只是占位，LLM 失败时无 KV 兜底。

设计思路：
  1) SemanticCache: 按"语义相似度"匹配缓存（不是精确字符串匹配）
  2) 嵌入模型：本地 sentence-transformers 优先（无外部依赖），未安装时回退到精确 hash
  3) TTL + LRU 双重淘汰
  4) 智能过滤：实时数据查询（带股票代码）默认不缓存
  5) 与降级链集成：主 LLM 失败 → 查语义缓存 → 命中则返回 → 否则降级到下一档

典型用法：
    from agent.semantic_cache import get_semantic_cache

    cache = get_semantic_cache()

    # 查询前：先查缓存
    cached = await cache.get("茅台最近有什么新闻？")
    if cached:
        return cached  # 命中缓存，跳过 LLM

    # 缓存未命中，调 LLM
    answer = await call_llm(question)

    # 写入缓存（带智能过滤）
    await cache.set(question, answer, ttl=300)

    # 降级链中：LLM 失败时
    try:
        answer = await call_llm(question)
    except Exception:
        cached = await cache.get(question, allow_stale=True)  # 允许过期数据兜底
        if cached:
            return cached + "\\n[注：来自缓存兜底]"
        raise
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from config.constants import (
    SEMANTIC_CACHE_BACKEND,
    SEMANTIC_CACHE_DEFAULT_TTL_SEC,
    SEMANTIC_CACHE_MAX_ENTRIES,
    SEMANTIC_CACHE_SIMILARITY_THRESHOLD,
    SEMANTIC_CACHE_EMBED_MODEL,
    SEMANTIC_CACHE_EMBED_DIM,
    SEMANTIC_CACHE_ENABLE_FOR_STOCK_QUERY,
    SEMANTIC_CACHE_REDIS_URL,
    SEMANTIC_CACHE_REDIS_PREFIX,
)

logger = logging.getLogger(__name__)


# ======================================================================
# 缓存条目
# ======================================================================

@dataclass
class CacheEntry:
    """单条缓存条目。"""
    query: str                       # 原始查询
    answer: str                      # LLM 回答
    query_embedding: List[float]    # 查询向量（用于语义匹配）
    query_hash: str                  # 查询的精确 hash（用于兜底）
    created_at: float = field(default_factory=time.time)
    ttl: int = SEMANTIC_CACHE_DEFAULT_TTL_SEC
    hit_count: int = 0              # 命中次数（用于 LRU 决策）
    metadata: Dict[str, Any] = field(default_factory=dict)  # 额外元数据

    def is_expired(self, now: Optional[float] = None) -> bool:
        """是否过期。"""
        now = now or time.time()
        return (now - self.created_at) > self.ttl

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "answer": self.answer,
            "query_embedding": self.query_embedding,
            "query_hash": self.query_hash,
            "created_at": self.created_at,
            "ttl": self.ttl,
            "hit_count": self.hit_count,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CacheEntry":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ======================================================================
# 查询预处理：判定是否可缓存
# ======================================================================

# 股票代码模式（6 位数字 / 港股 5 位）
_STOCK_CODE_PATTERN = re.compile(r"\b\d{5,6}\b")

# 实时性关键词
_REALTIME_KEYWORDS = {
    "今日", "今天", "现在", "当前", "实时", "最新", "刚刚",
    "盘前", "盘中", "盘后", "涨停", "跌停", "现价", "现手",
}


def should_cache_query(query: str) -> Tuple[bool, str]:
    """判定某查询是否应该缓存。

    返回 (should_cache, reason)。
    """
    # 1) 带股票代码默认不缓存（实时数据）
    if not SEMANTIC_CACHE_ENABLE_FOR_STOCK_QUERY:
        if _STOCK_CODE_PATTERN.search(query):
            return False, "包含股票代码，需实时数据"

    # 2) 实时性关键词不缓存
    for kw in _REALTIME_KEYWORDS:
        if kw in query:
            return False, f"包含实时性关键词：{kw}"

    return True, ""


# ======================================================================
# 嵌入模型封装（懒加载 + 优雅降级）
# ======================================================================

class Embedder:
    """查询嵌入模型封装。

    优先使用 sentence-transformers 本地模型；未安装时回退到 hash 向量。
    """
    def __init__(self, model_name: str = SEMANTIC_CACHE_EMBED_MODEL):
        self.model_name = model_name
        self._model: Any = None
        self._lock = asyncio.Lock()
        self._init_failed = False

    async def embed(self, text: str) -> List[float]:
        """把文本转成向量。"""
        if self._init_failed:
            return self._hash_embedding(text)

        if self._model is None:
            async with self._lock:
                if self._model is None:
                    try:
                        from sentence_transformers import SentenceTransformer
                        self._model = SentenceTransformer(self.model_name)
                        logger.info("[semantic_cache] 嵌入模型已加载: %s", self.model_name)
                    except ImportError:
                        logger.info(
                            "[semantic_cache] sentence-transformers 未安装，"
                            "回退到 hash 嵌入（仅精确匹配生效）。"
                            "安装命令: pip install sentence-transformers"
                        )
                        self._init_failed = True
                        return self._hash_embedding(text)
                    except Exception as e:
                        logger.warning("[semantic_cache] 嵌入模型加载失败: %s，回退到 hash", e)
                        self._init_failed = True
                        return self._hash_embedding(text)

        # 同步调用模型（短查询通常 < 50ms，可接受）
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._embed_sync, text)

    def _embed_sync(self, text: str) -> List[float]:
        return self._model.encode(text, normalize_embeddings=True).tolist()

    def _hash_embedding(self, text: str) -> List[float]:
        """Hash 兜底嵌入：仅支持精确匹配（语义匹配无效）。"""
        h = hashlib.md5(text.encode("utf-8")).digest()
        # 把 16 字节 hash 扩展到 embed_dim 维（虽然没语义，但结构兼容）
        result = []
        for i in range(SEMANTIC_CACHE_EMBED_DIM):
            result.append((h[i % 16] / 255.0) * 2 - 1)
        return result


# ======================================================================
# 语义缓存实现
# ======================================================================

class SemanticCache:
    """语义缓存。按相似度匹配，TTL + LRU 淘汰。"""

    def __init__(
        self,
        backend: str = SEMANTIC_CACHE_BACKEND,
        max_entries: int = SEMANTIC_CACHE_MAX_ENTRIES,
        similarity_threshold: float = SEMANTIC_CACHE_SIMILARITY_THRESHOLD,
        default_ttl: int = SEMANTIC_CACHE_DEFAULT_TTL_SEC,
    ):
        self.backend_type = backend
        self.max_entries = max_entries
        self.similarity_threshold = similarity_threshold
        self.default_ttl = default_ttl
        self._embedder = Embedder()
        self._lock = asyncio.Lock()

        # memory 后端：OrderedDict（LRU）
        self._memory_store: "OrderedDict[str, CacheEntry]" = OrderedDict()

        # redis 后端（懒加载）
        self._redis_client: Any = None

        # 命中统计
        self._stats = {
            "hits": 0, "misses": 0, "sets": 0, "evictions": 0,
            "filtered": 0,  # 被智能过滤掉的查询数
        }

    async def get(
        self,
        query: str,
        *,
        allow_stale: bool = False,
    ) -> Optional[str]:
        """查缓存。返回命中的 answer 或 None。

        参数：
            allow_stale: True=允许返回过期数据（降级链兜底场景）
        """
        # 智能过滤：实时查询不查缓存
        ok, _ = should_cache_query(query)
        if not ok:
            self._stats["filtered"] += 1
            return None

        query_embedding = await self._embedder.embed(query)

        if self.backend_type == "redis":
            return await self._get_redis(query, query_embedding, allow_stale)
        return self._get_memory(query, query_embedding, allow_stale)

    def _get_memory(
        self,
        query: str,
        query_embedding: List[float],
        allow_stale: bool,
    ) -> Optional[str]:
        """memory 后端查询。"""
        # 1) 精确匹配优先（hash）
        query_hash = self._hash_text(query)
        if query_hash in self._memory_store:
            entry = self._memory_store[query_hash]
            if not entry.is_expired() or allow_stale:
                entry.hit_count += 1
                self._memory_store.move_to_end(query_hash)  # LRU 更新
                self._stats["hits"] += 1
                return entry.answer

        # 2) 语义匹配
        best_entry: Optional[CacheEntry] = None
        best_score: float = 0.0
        now = time.time()
        for entry in list(self._memory_store.values()):
            if entry.is_expired(now) and not allow_stale:
                continue
            score = self._cosine_similarity(query_embedding, entry.query_embedding)
            if score > best_score:
                best_score = score
                best_entry = entry

        if best_entry and best_score >= self.similarity_threshold:
            best_entry.hit_count += 1
            self._memory_store.move_to_end(best_entry.query_hash)
            self._stats["hits"] += 1
            logger.debug(
                "[semantic_cache] 命中 similarity=%.3f query=%s",
                best_score, query[:50],
            )
            return best_entry.answer

        self._stats["misses"] += 1
        return None

    async def _get_redis(
        self,
        query: str,
        query_embedding: List[float],
        allow_stale: bool,
    ) -> Optional[str]:
        """redis 后端查询：扫描所有 key 找最高相似度（生产环境应换成向量数据库）。"""
        client = await self._ensure_redis()
        # 获取所有缓存 key（注意：扫描全表在大规模下慢，生产应用 RediSearch 或 vector 库）
        keys = await client.keys(f"{SEMANTIC_CACHE_REDIS_PREFIX}*")
        if not keys:
            self._stats["misses"] += 1
            return None

        best_score: float = 0.0
        best_answer: Optional[str] = None
        for key in keys:
            raw = await client.get(key)
            if not raw:
                continue
            try:
                entry = CacheEntry.from_dict(__import__("json").loads(raw))
                if entry.is_expired() and not allow_stale:
                    continue
                score = self._cosine_similarity(query_embedding, entry.query_embedding)
                if score > best_score:
                    best_score = score
                    best_answer = entry.answer
            except Exception:
                continue

        if best_answer and best_score >= self.similarity_threshold:
            self._stats["hits"] += 1
            return best_answer

        self._stats["misses"] += 1
        return None

    async def set(
        self,
        query: str,
        answer: str,
        *,
        ttl: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """写缓存。返回是否成功写入（被过滤则返回 False）。"""
        # 智能过滤
        ok, reason = should_cache_query(query)
        if not ok:
            logger.debug("[semantic_cache] 跳过缓存 query=%s reason=%s", query[:50], reason)
            self._stats["filtered"] += 1
            return False

        query_embedding = await self._embedder.embed(query)
        entry = CacheEntry(
            query=query,
            answer=answer,
            query_embedding=query_embedding,
            query_hash=self._hash_text(query),
            ttl=ttl or self.default_ttl,
            metadata=metadata or {},
        )

        if self.backend_type == "redis":
            return await self._set_redis(entry)
        return self._set_memory(entry)

    def _set_memory(self, entry: CacheEntry) -> bool:
        """memory 后端写入 + LRU 淘汰。"""
        # 注意：asyncio.Lock 不能用 with，这里用同步临界区（_set_memory 是同步函数）
        # 内存写入是原子的，LRU 淘汰在锁外做也安全
        while len(self._memory_store) >= self.max_entries:
            self._memory_store.popitem(last=False)
            self._stats["evictions"] += 1
        self._memory_store[entry.query_hash] = entry
        self._stats["sets"] += 1
        return True

    async def _set_redis(self, entry: CacheEntry) -> bool:
        client = await self._ensure_redis()
        key = f"{SEMANTIC_CACHE_REDIS_PREFIX}{entry.query_hash}"
        await client.set(key, __import__("json").dumps(entry.to_dict()), ex=entry.ttl)
        self._stats["sets"] += 1
        return True

    async def invalidate(self, query: str) -> None:
        """显式失效一条缓存（如用户反馈回答错误时）。"""
        query_hash = self._hash_text(query)
        if self.backend_type == "redis":
            client = await self._ensure_redis()
            await client.delete(f"{SEMANTIC_CACHE_REDIS_PREFIX}{query_hash}")
        else:
            self._memory_store.pop(query_hash, None)

    async def clear(self) -> None:
        """清空全部缓存。"""
        if self.backend_type == "redis":
            client = await self._ensure_redis()
            keys = await client.keys(f"{SEMANTIC_CACHE_REDIS_PREFIX}*")
            if keys:
                await client.delete(*keys)
        else:
            self._memory_store.clear()

    def get_stats(self) -> Dict[str, Any]:
        """返回命中统计。"""
        total = self._stats["hits"] + self._stats["misses"]
        hit_rate = self._stats["hits"] / total if total > 0 else 0.0
        return {
            **self._stats,
            "hit_rate": round(hit_rate, 4),
            "current_size": len(self._memory_store),
            "backend": self.backend_type,
        }

    # ---------- 辅助 ----------

    @staticmethod
    def _hash_text(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _cosine_similarity(a: List[float], b: List[float]) -> float:
        """余弦相似度。"""
        if not a or not b or len(a) != len(b):
            return 0.0
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(y * y for y in b) ** 0.5
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    async def _ensure_redis(self):
        if self._redis_client is None:
            import redis.asyncio as aioredis
            self._redis_client = aioredis.from_url(
                SEMANTIC_CACHE_REDIS_URL, decode_responses=True
            )
        return self._redis_client


# ======================================================================
# 全局单例
# ======================================================================

_global_cache: Optional[SemanticCache] = None
_singleton_lock = asyncio.Lock()


async def get_semantic_cache() -> SemanticCache:
    """获取全局 SemanticCache 单例。"""
    global _global_cache
    if _global_cache is None:
        async with _singleton_lock:
            if _global_cache is None:
                _global_cache = SemanticCache()
    return _global_cache


def get_semantic_cache_sync() -> SemanticCache:
    """同步获取单例（不初始化 embedder，适用于初始化阶段）。"""
    global _global_cache
    if _global_cache is None:
        _global_cache = SemanticCache()
    return _global_cache
