"""
Actor 实现 #1 —— SessionRegistryActor（会话任务注册表）。

替换 server.py 中以下 3 个全局可变字典：
  - _active_agent_tasks[thread_id]    = asyncio.Task  # 普通聊天任务
  - _active_background_tasks[thread_id] = asyncio.Task # 后台任务（盘前小作文等）
  - _background_tasks: set              # 防 GC 的任务引用集合

原问题：
  1. run_task() → _on_done callback 中直接修改字典
  2. stop_task() / _register_background_task() 也直接改
  3. task.add_done_callback 在**任意时机**被回调（可能与主流程并发修改）

Actor 化后：
  所有对注册表的读写必须发消息给 SessionRegistryActor，
  由 Actor 邮箱串行执行 `next_state = f(current_state, msg)`。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

from agent.actor_base import Actor, Envelope, Msg


# ======================================================================
# 消息类型常量
# ======================================================================

class SRMsg:
    """SessionRegistryActor 消息类型。"""
    # ---- 写操作 ----
    REGISTER_AGENT_TASK = "register_agent_task"           # 注册聊天任务
    REGISTER_BG_TASK = "register_bg_task"                 # 注册后台任务
    UNREGISTER_IF_SELF = "unregister_if_self"             # 任务完成时，若仍是自己则清掉（来自 done_callback）
    STOP_AND_REMOVE_TASK = "stop_and_remove_task"         # 用户点停止按钮
    # ---- 读操作 ----
    GET_TASK_INFO = "get_task_info"                       # 查询某会话当前任务情况
    LIST_ALL = "list_all"                                 # 列出所有活跃任务（监控）


# ======================================================================
# 私有状态结构
# ======================================================================

@dataclass
class _RegistryState:
    """Actor 私有状态。所有字段只能在 handle_message 中通过返回新对象修改。"""
    # thread_id -> (task_id, task对象, 类型: 'agent'|'bg')
    active_agent_tasks: Dict[str, Any] = field(default_factory=dict)
    active_background_tasks: Dict[str, Any] = field(default_factory=dict)
    # 防 GC 的任务引用（保存 id(task) -> task，因为 dict 的 value 可能被替换）
    gc_guard: Set[int] = field(default_factory=set)


# ======================================================================
# Actor 实现
# ======================================================================

class SessionRegistryActor(Actor[_RegistryState]):
    """
    会话任务注册表 Actor。

    典型外部调用：
        reg: SessionRegistryActor = actor_system.get("session_registry")

        # 注册新聊天任务（send，不等待）
        await reg.send(SRMsg.REGISTER_AGENT_TASK, {
            "thread_id": thread_id,
            "task": task_object,
            "on_done_unregister": True,
        })

        # 用户停止（ask，等待确认被取消了哪个任务）
        result = await reg.ask(SRMsg.STOP_AND_REMOVE_TASK, {"thread_id": thread_id})

        # done_callback 中（send，避免回调阻塞）
        def _on_done(t):
            asyncio.create_task(reg.send(SRMsg.UNREGISTER_IF_SELF, {
                "thread_id": thread_id,
                "task_id": id(t),
                "task_type": "agent",
            }))
    """

    def initial_state(self) -> _RegistryState:
        return _RegistryState()

    # ------------------------------------------------------------------
    # handle_message —— 唯一状态转换入口
    # ------------------------------------------------------------------

    async def handle_message(self, state: _RegistryState, env: Envelope):
        msg = env.msg_type
        p = env.payload

        # ==============================================================
        # 1. REGISTER_AGENT_TASK —— 注册新聊天任务
        #    语义：若同 thread_id 已有旧任务，先 cancel 旧任务再注册新的
        # ==============================================================
        if msg == SRMsg.REGISTER_AGENT_TASK:
            thread_id = p["thread_id"]
            new_task = p["task"]
            tid = id(new_task)

            # ---- 产生副作用：cancel 旧任务（可重入、幂等）----
            old_task = state.active_agent_tasks.get(thread_id)
            if old_task is not None and not old_task.done():
                old_task.cancel("session_actor_replaced: new agent task started on same thread")
                # 旧任务的 done_callback 会通过 UNREGISTER_IF_SELF 清理；
                # 但既然这里已经要替换，直接同步从 gc_guard 移除旧 id 引用
                state.gc_guard.discard(id(old_task))

            # ---- 构造新状态（不就地修改旧 state！）----
            new_agent_tasks = dict(state.active_agent_tasks)
            new_agent_tasks[thread_id] = new_task
            new_gc_guard = set(state.gc_guard)
            new_gc_guard.add(tid)

            new_state = _RegistryState(
                active_agent_tasks=new_agent_tasks,
                active_background_tasks=dict(state.active_background_tasks),
                gc_guard=new_gc_guard,
            )
            return new_state, {"ok": True, "replaced_old": old_task is not None}

        # ==============================================================
        # 2. REGISTER_BG_TASK —— 注册后台任务
        #    额外语义：同时取消同会话的**聊天任务**（防止 checkpointer 并发写入竞态）
        # ==============================================================
        if msg == SRMsg.REGISTER_BG_TASK:
            thread_id = p["thread_id"]
            new_task = p["task"]
            tid = id(new_task)

            new_agent_tasks = dict(state.active_agent_tasks)
            new_bg_tasks = dict(state.active_background_tasks)
            new_gc_guard = set(state.gc_guard)

            # (a) 取消同会话旧后台任务
            old_bg = new_bg_tasks.get(thread_id)
            if old_bg is not None and not old_bg.done():
                old_bg.cancel("session_actor_replaced: new bg task started on same thread")
                new_gc_guard.discard(id(old_bg))

            # (b) 取消同会话聊天任务（防止 checkpointer 并发写入）
            old_agent = new_agent_tasks.get(thread_id)
            if old_agent is not None and not old_agent.done():
                old_agent.cancel("session_actor_replaced: bg task replaces running agent task on same thread")
                new_gc_guard.discard(id(old_agent))
                del new_agent_tasks[thread_id]

            # (c) 注册新后台任务
            new_bg_tasks[thread_id] = new_task
            new_gc_guard.add(tid)

            new_state = _RegistryState(
                active_agent_tasks=new_agent_tasks,
                active_background_tasks=new_bg_tasks,
                gc_guard=new_gc_guard,
            )
            return new_state, {"ok": True}

        # ==============================================================
        # 3. UNREGISTER_IF_SELF —— 任务完成回调中调用：只有仍然指向自己时才移除
        #    幂等：task 已经被新任务覆盖 → 什么都不做
        # ==============================================================
        if msg == SRMsg.UNREGISTER_IF_SELF:
            thread_id = p["thread_id"]
            task_id = p["task_id"]  # id(task_object)
            task_type = p.get("task_type", "agent")  # "agent" or "bg"

            new_gc_guard = set(state.gc_guard)
            new_gc_guard.discard(task_id)

            if task_type == "agent":
                cur = state.active_agent_tasks.get(thread_id)
                if cur is not None and id(cur) == task_id:
                    new_agent = dict(state.active_agent_tasks)
                    del new_agent[thread_id]
                    new_state = _RegistryState(
                        active_agent_tasks=new_agent,
                        active_background_tasks=dict(state.active_background_tasks),
                        gc_guard=new_gc_guard,
                    )
                    return new_state, {"removed": True}
            else:  # bg
                cur = state.active_background_tasks.get(thread_id)
                if cur is not None and id(cur) == task_id:
                    new_bg = dict(state.active_background_tasks)
                    del new_bg[thread_id]
                    new_state = _RegistryState(
                        active_agent_tasks=dict(state.active_agent_tasks),
                        active_background_tasks=new_bg,
                        gc_guard=new_gc_guard,
                    )
                    return new_state, {"removed": True}

            # 不是自己（已被新任务替换）→ 状态不变，只移除 gc_guard 引用
            new_state = _RegistryState(
                active_agent_tasks=dict(state.active_agent_tasks),
                active_background_tasks=dict(state.active_background_tasks),
                gc_guard=new_gc_guard,
            )
            return new_state, {"removed": False, "reason": "not_self"}

        # ==============================================================
        # 4. STOP_AND_REMOVE_TASK —— 用户点停止按钮
        # ==============================================================
        if msg == SRMsg.STOP_AND_REMOVE_TASK:
            thread_id = p["thread_id"]
            # 聊天任务优先停止；后台任务也尝试停止
            stopped_any = False

            new_agent = dict(state.active_agent_tasks)
            new_bg = dict(state.active_background_tasks)
            new_gc = set(state.gc_guard)

            t = new_agent.get(thread_id)
            if t is not None:
                if not t.done():
                    t.cancel()
                    stopped_any = True
                new_gc.discard(id(t))
                del new_agent[thread_id]

            t2 = new_bg.get(thread_id)
            if t2 is not None:
                if not t2.done():
                    t2.cancel()
                    stopped_any = True
                new_gc.discard(id(t2))
                del new_bg[thread_id]

            new_state = _RegistryState(
                active_agent_tasks=new_agent,
                active_background_tasks=new_bg,
                gc_guard=new_gc,
            )
            return new_state, {"stopped": stopped_any}

        # ==============================================================
        # 5. GET_TASK_INFO —— 只读查询
        # ==============================================================
        if msg == SRMsg.GET_TASK_INFO:
            thread_id = p["thread_id"]
            agent_t = state.active_agent_tasks.get(thread_id)
            bg_t = state.active_background_tasks.get(thread_id)
            info = {
                "thread_id": thread_id,
                "has_agent_task": agent_t is not None,
                "agent_done": agent_t.done() if agent_t else None,
                "has_bg_task": bg_t is not None,
                "bg_done": bg_t.done() if bg_t else None,
            }
            return state, info  # 状态不变

        # ==============================================================
        # 6. LIST_ALL —— 监控面板
        # ==============================================================
        if msg == SRMsg.LIST_ALL:
            summary = {
                "agent_task_count": len(state.active_agent_tasks),
                "bg_task_count": len(state.active_background_tasks),
                "gc_guard_size": len(state.gc_guard),
                "agent_threads": sorted(state.active_agent_tasks.keys()),
                "bg_threads": sorted(state.active_background_tasks.keys()),
            }
            return state, summary

        # 未识别消息：状态不变（但对内部 __actor_* sentinel 静默，不打日志）
        if not isinstance(msg, str) or not msg.startswith("__actor_"):
            print(f"[SessionRegistryActor] 未知消息类型: {msg}")
        return state, None
