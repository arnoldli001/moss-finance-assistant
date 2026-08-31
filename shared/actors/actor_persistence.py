# -*- coding: utf-8 -*-
"""
Actor 状态持久化与快照恢复：原Actor状态是内存态，单进程挂掉未完成会话丢失。
设计思路：
  1) SnapshotStore 抽象后端：FileBackend / RedisBackend / MemoryBackend
  2) ActorSnapshotter 包装现有 Actor，每处理 N 条消息做一次增量快照
  3) 启动时若 ACTOR_SNAPSHOT_AUTO_RESTORE=True，从最近快照恢复状态
  4) 用 asyncio.Lock 防止并发快照写入冲突

典型用法：
    from agent.actor_persistence import ActorSnapshotter, FileBackend
    backend = FileBackend(base_dir="data/actor_snapshots")
    snapshotter = ActorSnapshotter(
        actor=my_actor,
        backend=backend,
        interval_msgs=ACTOR_SNAPSHOT_INTERVAL_MSGS,
    )

    # 包装 handle_message 调用：每次处理完自动检查是否触发快照
    new_state, reply = await snapshotter.handle_with_snapshot(env)

    # 启动时恢复
    restored_state = await snapshotter.restore()
    if restored_state is not None:
        my_actor.state = restored_state
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.constants import (
    ACTOR_SNAPSHOT_BACKEND,
    ACTOR_SNAPSHOT_FILE_DIR,
    ACTOR_SNAPSHOT_INTERVAL_MSGS,
    ACTOR_SNAPSHOT_FULL_INTERVAL_MSGS,
    ACTOR_SNAPSHOT_KEEP_VERSIONS,
    ACTOR_SNAPSHOT_AUTO_RESTORE,
    ACTOR_SNAPSHOT_REDIS_URL,
    ACTOR_SNAPSHOT_REDIS_PREFIX,
)

logger = logging.getLogger(__name__)


# ======================================================================
# 快照数据结构
# ======================================================================

@dataclass
class SnapshotMeta:
    """快照元数据。"""
    actor_id: str                       # Actor 唯一标识
    version: int                        # 第几次快照（递增）
    msg_seq: int                        # 快照时已处理的消息序号
    timestamp: float = field(default_factory=time.time)
    is_full: bool = True                # 全量 / 增量
    state_type: str = ""                # state 的类型名（便于反序列化）

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SnapshotMeta":
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})


@dataclass
class Snapshot:
    """完整的快照：元数据 + 序列化的状态。"""
    meta: SnapshotMeta
    state_json: str                     # 状态的 JSON 序列化字符串

    def to_dict(self) -> Dict[str, Any]:
        return {"meta": self.meta.to_dict(), "state_json": self.state_json}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Snapshot":
        return cls(
            meta=SnapshotMeta.from_dict(d["meta"]),
            state_json=d["state_json"],
        )


# ======================================================================
# 后端抽象：file / redis / memory
# ======================================================================

class SnapshotBackend(ABC):
    """快照存储后端抽象基类。"""

    @abstractmethod
    async def save(self, actor_id: str, snapshot: Snapshot) -> None:
        """保存快照（保留历史版本，FIFO 淘汰超出 KEEP_VERSIONS 的旧版本）。"""

    @abstractmethod
    async def load_latest(self, actor_id: str) -> Optional[Snapshot]:
        """加载最近一次快照。"""

    @abstractmethod
    async def list_versions(self, actor_id: str) -> List[int]:
        """列出所有历史版本号（升序）。"""

    @abstractmethod
    async def load_version(self, actor_id: str, version: int) -> Optional[Snapshot]:
        """加载指定版本快照。"""

    @abstractmethod
    async def delete_old(self, actor_id: str, keep: int) -> int:
        """删除旧版本，保留最近 keep 份。返回删除数量。"""


class FileBackend(SnapshotBackend):
    """文件后端：每个 Actor 一个目录，每版本一个 JSON 文件。

    目录结构：
        {base_dir}/{actor_id}/v{version:06d}.json
    """

    def __init__(self, base_dir: str = ACTOR_SNAPSHOT_FILE_DIR):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._locks: Dict[str, asyncio.Lock] = {}

    def _actor_dir(self, actor_id: str) -> Path:
        d = self.base_dir / actor_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _lock(self, actor_id: str) -> asyncio.Lock:
        if actor_id not in self._locks:
            self._locks[actor_id] = asyncio.Lock()
        return self._locks[actor_id]

    async def save(self, actor_id: str, snapshot: Snapshot) -> None:
        async with self._lock(actor_id):
            d = self._actor_dir(actor_id)
            path = d / f"v{snapshot.meta.version:06d}.json"
            content = json.dumps(snapshot.to_dict(), ensure_ascii=False, indent=2)
            # 原子写：先写 .tmp 再 rename
            tmp = path.with_suffix(".tmp")
            tmp.write_text(content, encoding="utf-8")
            tmp.replace(path)
            logger.debug("[snapshot:%s] 已保存 v%d (msg_seq=%d)",
                         actor_id, snapshot.meta.version, snapshot.meta.msg_seq)

    async def load_latest(self, actor_id: str) -> Optional[Snapshot]:
        versions = await self.list_versions(actor_id)
        if not versions:
            return None
        return await self.load_version(actor_id, versions[-1])

    async def list_versions(self, actor_id: str) -> List[int]:
        d = self.base_dir / actor_id
        if not d.exists():
            return []
        versions = []
        for f in d.glob("v*.json"):
            try:
                v = int(f.stem[1:])  # 去掉 v 前缀
                versions.append(v)
            except ValueError:
                continue
        return sorted(versions)

    async def load_version(self, actor_id: str, version: int) -> Optional[Snapshot]:
        path = self.base_dir / actor_id / f"v{version:06d}.json"
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return Snapshot.from_dict(data)
        except Exception as e:
            logger.warning("[snapshot:%s] 加载 v%d 失败: %s", actor_id, version, e)
            return None

    async def delete_old(self, actor_id: str, keep: int) -> int:
        versions = await self.list_versions(actor_id)
        if len(versions) <= keep:
            return 0
        to_delete = versions[:-keep] if keep > 0 else versions
        deleted = 0
        for v in to_delete:
            path = self.base_dir / actor_id / f"v{v:06d}.json"
            try:
                path.unlink()
                deleted += 1
            except FileNotFoundError:
                pass
        if deleted:
            logger.info("[snapshot:%s] 清理旧版本 %d 份", actor_id, deleted)
        return deleted


class MemoryBackend(SnapshotBackend):
    """内存后端：仅用于测试或临时缓存，进程退出即丢失。"""
    def __init__(self):
        self._store: Dict[str, List[Snapshot]] = {}

    async def save(self, actor_id: str, snapshot: Snapshot) -> None:
        self._store.setdefault(actor_id, []).append(snapshot)

    async def load_latest(self, actor_id: str) -> Optional[Snapshot]:
        lst = self._store.get(actor_id, [])
        return lst[-1] if lst else None

    async def list_versions(self, actor_id: str) -> List[int]:
        return [s.meta.version for s in self._store.get(actor_id, [])]

    async def load_version(self, actor_id: str, version: int) -> Optional[Snapshot]:
        for s in self._store.get(actor_id, []):
            if s.meta.version == version:
                return s
        return None

    async def delete_old(self, actor_id: str, keep: int) -> int:
        lst = self._store.get(actor_id, [])
        if len(lst) <= keep:
            return 0
        to_keep = lst[-keep:] if keep > 0 else []
        deleted = len(lst) - len(to_keep)
        self._store[actor_id] = to_keep
        return deleted


class RedisBackend(SnapshotBackend):
    """Redis 后端：用 list 存版本号，hash 存快照内容。

    依赖：pip install redis>=4.2 (asyncio 支持)
    """
    def __init__(
        self,
        url: str = ACTOR_SNAPSHOT_REDIS_URL,
        prefix: str = ACTOR_SNAPSHOT_REDIS_PREFIX,
    ):
        self.url = url
        self.prefix = prefix
        self._client = None  # lazy 初始化

    async def _ensure_client(self):
        if self._client is None:
            import redis.asyncio as aioredis
            self._client = aioredis.from_url(self.url, decode_responses=True)
        return self._client

    def _key(self, actor_id: str, version: int) -> str:
        return f"{self.prefix}{actor_id}:v{version:06d}"

    def _versions_key(self, actor_id: str) -> str:
        return f"{self.prefix}{actor_id}:versions"

    async def save(self, actor_id: str, snapshot: Snapshot) -> None:
        client = await self._ensure_client()
        content = json.dumps(snapshot.to_dict(), ensure_ascii=False)
        await client.set(self._key(actor_id, snapshot.meta.version), content)
        await client.rpush(self._versions_key(actor_id), snapshot.meta.version)

    async def load_latest(self, actor_id: str) -> Optional[Snapshot]:
        versions = await self.list_versions(actor_id)
        if not versions:
            return None
        return await self.load_version(actor_id, versions[-1])

    async def list_versions(self, actor_id: str) -> List[int]:
        client = await self._ensure_client()
        raw = await client.lrange(self._versions_key(actor_id), 0, -1)
        return sorted(int(v) for v in raw)

    async def load_version(self, actor_id: str, version: int) -> Optional[Snapshot]:
        client = await self._ensure_client()
        content = await client.get(self._key(actor_id, version))
        if not content:
            return None
        return Snapshot.from_dict(json.loads(content))

    async def delete_old(self, actor_id: str, keep: int) -> int:
        versions = await self.list_versions(actor_id)
        if len(versions) <= keep:
            return 0
        to_delete = versions[:-keep] if keep > 0 else versions
        client = await self._ensure_client()
        for v in to_delete:
            await client.delete(self._key(actor_id, v))
        # 重建 versions list
        keep_v = versions[-keep:] if keep > 0 else []
        await client.delete(self._versions_key(actor_id))
        for v in keep_v:
            await client.rpush(self._versions_key(actor_id), v)
        return len(to_delete)


def get_backend(backend_type: Optional[str] = None) -> SnapshotBackend:
    """工厂方法：根据常量返回对应后端实例。"""
    backend_type = (backend_type or ACTOR_SNAPSHOT_BACKEND).lower()
    if backend_type == "file":
        return FileBackend()
    if backend_type == "redis":
        return RedisBackend()
    if backend_type == "memory":
        return MemoryBackend()
    raise ValueError(f"未知快照后端: {backend_type}")


# ======================================================================
# 状态序列化辅助
# ======================================================================

def serialize_state(state: Any) -> str:
    """把 Actor 状态序列化为 JSON 字符串。

    支持 dataclass / dict / list / 基本类型。
    """
    if state is None:
        return "null"
    if hasattr(state, "__dataclass_fields__"):
        return json.dumps(asdict(state), ensure_ascii=False, default=str)
    if isinstance(state, (dict, list, str, int, float, bool)):
        return json.dumps(state, ensure_ascii=False, default=str)
    # 兜底：转 dict
    try:
        return json.dumps(state.__dict__, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps(str(state), ensure_ascii=False)


def deserialize_state(state_json: str, target_type: Any = None) -> Any:
    """反序列化状态。target_type 可选：若是 dataclass 类，自动重构。"""
    if not state_json or state_json == "null":
        return None
    data = json.loads(state_json)
    if target_type and hasattr(target_type, "__dataclass_fields__") and isinstance(data, dict):
        try:
            valid = {k: v for k, v in data.items() if k in target_type.__dataclass_fields__}
            return target_type(**valid)
        except Exception as e:
            logger.warning("[snapshot] 状态反序列化失败，回退到 dict: %s", e)
    return data


# ======================================================================
# ActorSnapshotter：包装 Actor，自动快照
# ======================================================================

class ActorSnapshotter:
    """包装一个 Actor，自动按消息数间隔做快照。

    使用方式：
        snapshotter = ActorSnapshotter(actor=my_actor, backend=backend)
        # 业务调用时
        new_state, reply = await snapshotter.handle_with_snapshot(env, state, actor)
        # 启动时恢复
        restored = await snapshotter.restore(actor_id, target_type=MyState)
    """

    def __init__(
        self,
        backend: Optional[SnapshotBackend] = None,
        interval_msgs: int = ACTOR_SNAPSHOT_INTERVAL_MSGS,
        full_interval_msgs: int = ACTOR_SNAPSHOT_FULL_INTERVAL_MSGS,
        keep_versions: int = ACTOR_SNAPSHOT_KEEP_VERSIONS,
    ):
        self.backend = backend or get_backend()
        self.interval_msgs = interval_msgs
        self.full_interval_msgs = full_interval_msgs
        self.keep_versions = keep_versions
        self._msg_counter: Dict[str, int] = {}  # actor_id -> 自上次快照以来消息数
        self._total_counter: Dict[str, int] = {}  # actor_id -> 总消息数
        self._version_counter: Dict[str, int] = {}  # actor_id -> 版本号递增
        self._snapshot_lock = asyncio.Lock()

    async def record_and_maybe_snapshot(
        self,
        actor_id: str,
        state: Any,
    ) -> Optional[Snapshot]:
        """记录一次消息处理，达到阈值则触发快照。返回快照（如触发）或 None。"""
        self._msg_counter[actor_id] = self._msg_counter.get(actor_id, 0) + 1
        self._total_counter[actor_id] = self._total_counter.get(actor_id, 0) + 1

        msg_since = self._msg_counter[actor_id]
        total = self._total_counter[actor_id]

        should_snapshot = (
            msg_since >= self.interval_msgs
            or total % self.full_interval_msgs == 0
        )
        if not should_snapshot:
            return None

        async with self._snapshot_lock:
            is_full = (total % self.full_interval_msgs == 0) or self._version_counter.get(actor_id, 0) == 0
            version = self._version_counter.get(actor_id, 0) + 1
            meta = SnapshotMeta(
                actor_id=actor_id,
                version=version,
                msg_seq=total,
                is_full=is_full,
                state_type=type(state).__name__,
            )
            snapshot = Snapshot(
                meta=meta,
                state_json=serialize_state(state),
            )
            await self.backend.save(actor_id, snapshot)
            self._version_counter[actor_id] = version
            self._msg_counter[actor_id] = 0  # 重置计数

            # 清理旧版本
            await self.backend.delete_old(actor_id, self.keep_versions)
            return snapshot

    async def restore(
        self,
        actor_id: str,
        target_type: Any = None,
    ) -> Optional[Any]:
        """从最近快照恢复状态。返回反序列化后的状态对象。"""
        snapshot = await self.backend.load_latest(actor_id)
        if snapshot is None:
            logger.info("[snapshot:%s] 无可恢复快照", actor_id)
            return None
        state = deserialize_state(snapshot.state_json, target_type=target_type)
        # 同步内部计数器
        self._total_counter[actor_id] = snapshot.meta.msg_seq
        self._version_counter[actor_id] = snapshot.meta.version
        logger.info(
            "[snapshot:%s] 已恢复 v%d (msg_seq=%d, type=%s)",
            actor_id, snapshot.meta.version, snapshot.meta.msg_seq,
            snapshot.meta.state_type,
        )
        return state

    async def force_snapshot(self, actor_id: str, state: Any) -> Snapshot:
        """强制做一次全量快照（如应用关闭前调用）。"""
        async with self._snapshot_lock:
            version = self._version_counter.get(actor_id, 0) + 1
            total = self._total_counter.get(actor_id, 0)
            meta = SnapshotMeta(
                actor_id=actor_id,
                version=version,
                msg_seq=total,
                is_full=True,
                state_type=type(state).__name__,
            )
            snapshot = Snapshot(meta=meta, state_json=serialize_state(state))
            await self.backend.save(actor_id, snapshot)
            self._version_counter[actor_id] = version
            return snapshot


# ======================================================================
# 应用级协调：注册所有需要快照的 Actor，统一管理
# ======================================================================

class SnapshotCoordinator:
    """全局快照协调器：管理多个 Actor 的快照与恢复。"""
    def __init__(self, snapshotter: Optional[ActorSnapshotter] = None):
        self.snapshotter = snapshotter or ActorSnapshotter()
        self._actors: Dict[str, Any] = {}  # actor_id -> (actor, get_state_fn)

    def register(self, actor_id: str, actor: Any, get_state_fn=None) -> None:
        """注册一个需要快照的 Actor。

        get_state_fn: 可选，从 actor 提取状态的函数；默认用 actor.state 属性。
        """
        self._actors[actor_id] = (actor, get_state_fn)

    async def snapshot_all(self) -> Dict[str, Optional[Snapshot]]:
        """对所有注册 Actor 做一次强制快照（应用关闭前调用）。"""
        results = {}
        for actor_id, (actor, get_fn) in self._actors.items():
            state = get_fn(actor) if get_fn else getattr(actor, "state", None)
            snap = await self.snapshotter.force_snapshot(actor_id, state)
            results[actor_id] = snap
        return results

    async def restore_all(self, target_types: Optional[Dict[str, Any]] = None) -> Dict[str, Optional[Any]]:
        """恢复所有注册 Actor 的状态。target_types 指定每个 Actor 的状态类型。"""
        target_types = target_types or {}
        results = {}
        for actor_id, (actor, _) in self._actors.items():
            state = await self.snapshotter.restore(
                actor_id,
                target_type=target_types.get(actor_id),
            )
            results[actor_id] = state
            if state is not None:
                # 写回 actor
                if hasattr(actor, "state"):
                    actor.state = state
                elif hasattr(actor, "_state"):
                    actor._state = state
        return results
