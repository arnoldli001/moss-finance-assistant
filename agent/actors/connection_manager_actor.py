"""
Actor 实现 #3 —— ConnectionManagerActor（WebSocket 连接管理）。

替换 api/monitor.py 中的 ConnectionManager.active_connections 共享字典。

原问题（最严重的跨线程状态修改点）：
  ToolMonitor._emit() 中：
    asyncio.run_coroutine_threadsafe(
        self.websocket_manager.send_to_thread(payload, thread_id),
        manager_loop
    )
  这里会从**工作线程**（LangGraph 执行同步工具时用的线程池线程）
  通过 call_soon_threadsafe 投递到主循环，然后 send_to_thread 直接：
    for ws in self.active_connections[thread_id]:
        await ws.send_json(payload)
  而 connect() / disconnect() 在 FastAPI 主协程中也直接操作 active_connections 字典。

虽然 GIL + 单线程事件循环 + dict 单步操作大多数时候没事，
但本质是"跨线程边界修改共享可变对象"——一旦未来改成多 worker 或加锁方式变动，
就会出现 ConcurrentModification 或者推送消息丢失/串台。

Actor 化后：
  - active_connections 是 ConnectionManagerActor 的私有状态
  - connect/disconnect/send_to_thread 全是发消息给 Actor
  - run_coroutine_threadsafe 只负责把消息投递动作放到主循环，真正的状态修改 + WS 发送**只在 Actor 协程内发生**
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from agent.actor_base import Actor, Envelope


# ======================================================================
# 消息类型常量
# ======================================================================

class ConnMsg:
    """ConnectionManagerActor 消息类型。"""
    CONNECT = "connect"                       # 客户端 WS 接入
    DISCONNECT = "disconnect"                 # 客户端 WS 断开
    SEND_TO_THREAD = "send_to_thread"         # 向某 thread_id 的所有 WS 推送 JSON
    BROADCAST = "broadcast"                   # 向所有连接广播（系统消息）
    LIST_CONNECTIONS = "list_connections"     # 监控：列出各 thread_id 的连接数


# ======================================================================
# 私有状态
# ======================================================================

@dataclass
class _ConnState:
    """私有状态：thread_id → [WebSocket_obj, ...]。"""
    # 注意：key=thread_id, value=list（同会话多标签页并存）
    active: Dict[str, List[Any]] = field(default_factory=dict)
    # 总连接计数（监控用）
    total_accepted: int = 0
    total_closed: int = 0


# ======================================================================
# Actor 实现
# ======================================================================

class ConnectionManagerActor(Actor[_ConnState]):
    """
    WebSocket 连接管理 Actor。

    典型调用：
        cm: ConnectionManagerActor = actor_system.get("connection_manager")

        # 新连接接入（FastAPI WS 路由内）
        await cm.send(ConnMsg.CONNECT, {"thread_id": tid, "websocket": ws})

        # 断开
        await cm.send(ConnMsg.DISCONNECT, {"thread_id": tid, "websocket": ws})

        # 跨线程发送 payload（ToolMonitor 中使用）
        #   —— 通过 run_coroutine_threadsafe 或 asyncio.create_task 调用 send 投递消息
        await cm.send(ConnMsg.SEND_TO_THREAD, {"thread_id": tid, "payload": payload_dict})
    """

    def initial_state(self) -> _ConnState:
        return _ConnState()

    # ------------------------------------------------------------------
    # handle_message
    # ------------------------------------------------------------------

    async def handle_message(self, state: _ConnState, env: Envelope):
        msg = env.msg_type
        p = env.payload

        # ==============================================================
        # 1. CONNECT
        # ==============================================================
        if msg == ConnMsg.CONNECT:
            thread_id = p["thread_id"]
            ws = p["websocket"]
            new_active: Dict[str, List[Any]] = {k: list(v) for k, v in state.active.items()}
            bucket = new_active.setdefault(thread_id, [])
            bucket.append(ws)
            new_state = _ConnState(
                active=new_active,
                total_accepted=state.total_accepted + 1,
                total_closed=state.total_closed,
            )
            print(f"[ConnActor] Client connected: {thread_id} (total this thread: {len(bucket)})")
            return new_state, None

        # ==============================================================
        # 2. DISCONNECT
        # ==============================================================
        if msg == ConnMsg.DISCONNECT:
            thread_id = p["thread_id"]
            ws = p["websocket"]
            if thread_id not in state.active:
                return state, None
            new_active: Dict[str, List[Any]] = {k: list(v) for k, v in state.active.items()}
            bucket = new_active[thread_id]
            if ws in bucket:
                bucket.remove(ws)
            # 空 bucket 可以删，减少内存
            if not bucket:
                del new_active[thread_id]
            new_state = _ConnState(
                active=new_active,
                total_accepted=state.total_accepted,
                total_closed=state.total_closed + 1,
            )
            return new_state, None

        # ==============================================================
        # 3. SEND_TO_THREAD —— 核心：向指定会话的所有 WS 推送 JSON
        #    发送失败（连接已关）的 WS 会被顺手从状态中移除
        # ==============================================================
        if msg == ConnMsg.SEND_TO_THREAD:
            thread_id = p["thread_id"]
            payload: Dict[str, Any] = p["payload"]
            bucket = state.active.get(thread_id)
            if not bucket:
                # 没有连接：状态不变，静默丢弃（避免前端日志被打爆）
                return state, {"sent": 0, "reason": "no_connections"}

            # 做一次发送，统计哪些连接失败（失败的下次 DISCONNECT 清理或这里就地清理）
            sent_count = 0
            dead_ws = []
            for ws in bucket:
                try:
                    await ws.send_json(payload)
                    sent_count += 1
                except Exception as exc:
                    # 常见：WebSocketDisconnect / 连接重置
                    dead_ws.append(ws)

            if not dead_ws:
                return state, {"sent": sent_count}

            # 有死掉的连接：从 state 中移除它们
            new_active: Dict[str, List[Any]] = {k: list(v) for k, v in state.active.items()}
            live = [w for w in new_active[thread_id] if w not in dead_ws]
            if live:
                new_active[thread_id] = live
            else:
                del new_active[thread_id]
            new_state = _ConnState(
                active=new_active,
                total_accepted=state.total_accepted,
                total_closed=state.total_closed + len(dead_ws),
            )
            print(f"[ConnActor] thread {thread_id}: 发送 {sent_count}, 清理死连接 {len(dead_ws)}")
            return new_state, {"sent": sent_count, "dead_removed": len(dead_ws)}

        # ==============================================================
        # 4. BROADCAST —— 广播（系统公告，少用）
        # ==============================================================
        if msg == ConnMsg.BROADCAST:
            payload = p["payload"]
            sent_count = 0
            dead_ws_by_thread: Dict[str, List[Any]] = {}
            for tid, bucket in state.active.items():
                for ws in bucket:
                    try:
                        await ws.send_json(payload)
                        sent_count += 1
                    except Exception:
                        dead_ws_by_thread.setdefault(tid, []).append(ws)
            if not dead_ws_by_thread:
                return state, {"sent": sent_count}
            # 清理死亡连接
            new_active: Dict[str, List[Any]] = {k: list(v) for k, v in state.active.items()}
            total_dead = 0
            for tid, dead in dead_ws_by_thread.items():
                live = [w for w in new_active[tid] if w not in dead]
                total_dead += len(dead)
                if live:
                    new_active[tid] = live
                else:
                    del new_active[tid]
            new_state = _ConnState(
                active=new_active,
                total_accepted=state.total_accepted,
                total_closed=state.total_closed + total_dead,
            )
            return new_state, {"sent": sent_count, "dead_removed": total_dead}

        # ==============================================================
        # 5. LIST_CONNECTIONS —— 监控
        # ==============================================================
        if msg == ConnMsg.LIST_CONNECTIONS:
            per_thread = {tid: len(bucket) for tid, bucket in state.active.items()}
            total = sum(per_thread.values())
            return state, {
                "thread_count": len(per_thread),
                "total_connections": total,
                "total_accepted": state.total_accepted,
                "total_closed": state.total_closed,
                "per_thread": per_thread,
            }

        return state, None
