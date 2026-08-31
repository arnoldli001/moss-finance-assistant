"""
Actor 模型并发安全测试（替代原有全局可变状态）。

测试覆盖：
  1. Actor 基类：消息 FIFO 顺序、纯函数转换边界、并发 ask/send 不冲突
  2. SessionRegistryActor：同会话并发注册任务 → 旧任务被 cancel，不产生孤儿
  3. SessionRegistryActor：UNREGISTER_IF_SELF done_callback 竞态 → 不误删新任务
  4. CircuitBreakerActor：并发 success/failure → 计数准确，状态转换顺序确定
  5. SLOMonitorActor：并发 record_event → 事件不丢，聚合统计一致

运行： python -m pytest tests/test_actor_concurrency.py -v
      或   python tests/test_actor_concurrency.py
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

# 确保项目根在 sys.path 中
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pytest

from agent.actor_base import Actor, Envelope, ActorSystem, Msg
from agent.actors import (
    SessionRegistryActor, SRMsg,
    CircuitBreakerActor, CBMsg,
    SLOMonitorActor, SLOMsg,
)


# ======================================================================
# 辅助：一个简单的 CounterActor 用于基类行为验证
# ======================================================================

class CounterActor(Actor[int]):
    def initial_state(self) -> int:
        return 0

    async def handle_message(self, state: int, env: Envelope):
        if env.msg_type == "inc":
            n = env.payload.get("n", 1)
            return state + n, None
        if env.msg_type == "dec":
            n = env.payload.get("n", 1)
            return state - n, None
        if env.msg_type == "get":
            return state, state
        return state, None


# ======================================================================
# Test 1: Actor 基类 —— 消息顺序 + 纯函数转换
# ======================================================================

@pytest.mark.asyncio
async def test_counter_fifo_order_and_pure_transition():
    """100 次并发 inc(1) → 结果必须精确等于 100，顺序 = 投递顺序。"""
    actor = CounterActor("counter_test")
    await actor.start()

    N = 100
    # 并发 send 100 条（asyncio.gather 并发）
    await asyncio.gather(*[actor.send("inc", {"n": 1}) for _ in range(N)])
    # 再问一次当前值
    value = await actor.ask("get", {})
    assert value == N, f"并发 inc 丢失更新：期望 {N}, 实际 {value}"

    # 纯函数边界：Actor 不允许就地修改 state
    # 验证：发 inc 后再发 dec，每次转换的中间态都应正确
    await actor.send("dec", {"n": 30})
    value2 = await actor.ask("get", {})
    assert value2 == N - 30

    await actor.stop()


# ======================================================================
# Test 2: Actor 基类 —— 内置 PING / SNAPSHOT 消息
# ======================================================================

@pytest.mark.asyncio
async def test_actor_builtin_ping_and_snapshot():
    actor = CounterActor("ping_test")
    await actor.start()

    pong = await actor.ask(Msg.PING, {})
    assert pong["ok"] is True
    assert pong["name"] == "ping_test"
    assert isinstance(pong["queue"], int)

    snap = await actor.ask(Msg.SNAPSHOT, {})
    # CounterActor 私有状态是 int，SNAPSHOT 返回深拷贝
    assert snap == 0  # 初始值

    await actor.send("inc", {"n": 42})
    snap2 = await actor.ask(Msg.SNAPSHOT, {})
    assert snap2 == 42

    await actor.stop()


# ======================================================================
# Test 3: SessionRegistryActor —— 同会话并发注册任务 → 旧任务被 cancel
# ======================================================================

@pytest.mark.asyncio
async def test_session_registry_concurrent_register_cancels_old():
    """并发为同 thread_id 注册 N 个任务 → 只有最后一个存活，其他 N-1 个被 cancel。"""
    actor = SessionRegistryActor("session_reg_test")
    await actor.start()

    N = 10
    tid = "session_concurrent_test"
    tasks: list[asyncio.Task] = []

    # 创建 N 个"永不完成"的 task（模拟耗时 LLM 调用）
    async def _infinite():
        try:
            await asyncio.sleep(9999)
        except asyncio.CancelledError:
            # 被 cancel 是预期行为
            raise

    for _ in range(N):
        t = asyncio.create_task(_infinite())
        tasks.append(t)
        # 串行注册（actor 内部串行处理）
        await actor.send(SRMsg.REGISTER_AGENT_TASK, {"thread_id": tid, "task": t})
        # 让 Actor 消费队列有机会执行（每个 REGISTER 都会 cancel 旧的）
        await asyncio.sleep(0.001)

    # 等所有 actor 邮箱消息消费完
    await asyncio.sleep(0.05)

    # 查询当前状态：应当只注册了最后一个 task（tasks[-1]）
    info = await actor.ask(SRMsg.GET_TASK_INFO, {"thread_id": tid})
    assert info["has_agent_task"] is True

    # 前 N-1 个任务必须被 cancel
    for i, t in enumerate(tasks[:-1]):
        assert t.cancelled() or t.done(), f"task[{i}] 未被 cancel（应被新任务替换）"

    # 最后一个任务应在运行（未 cancel，未 done）
    assert not tasks[-1].done(), "最后一个任务不应被 cancel"
    assert not tasks[-1].cancelled()

    # 清理：最后一个任务 cancel + 清理 actor
    tasks[-1].cancel()
    try:
        await tasks[-1]
    except asyncio.CancelledError:
        pass
    await actor.stop()


# ======================================================================
# Test 4: SessionRegistryActor —— UNREGISTER_IF_SELF 身份校验（防误删）
# ======================================================================

@pytest.mark.asyncio
async def test_session_registry_unregister_if_self_race():
    """典型竞态：旧 task 的 done_callback 在新 task 已注册后才到达 → 不应误删新 task。"""
    actor = SessionRegistryActor("unreg_self_test")
    await actor.start()
    tid = "tid_race_unreg"

    # task_old: 注册后立即"完成"
    async def _quick():
        return 42
    task_old = asyncio.create_task(_quick())
    await task_old  # 保证它 done
    await actor.send(SRMsg.REGISTER_AGENT_TASK, {"thread_id": tid, "task": task_old})
    await asyncio.sleep(0.01)

    # task_new: 注册（内部会移除 task_old，因为 task_old.done() 检查返回 True 不会 cancel，但会覆盖）
    task_new = asyncio.create_task(asyncio.sleep(9999))
    await actor.send(SRMsg.REGISTER_AGENT_TASK, {"thread_id": tid, "task": task_new})
    await asyncio.sleep(0.01)

    # 现在：模拟 task_old 的 done_callback 迟到了 → 发 UNREGISTER_IF_SELF
    # task_old.id(task_old) 与 actor 中登记的 task_new 不同 → 应不移除
    result = await actor.ask(SRMsg.UNREGISTER_IF_SELF, {
        "thread_id": tid,
        "task_id": id(task_old),  # 身份：旧 task
        "task_type": "agent",
    })
    assert result["removed"] is False, "旧 task 的回调不应误删新 task"
    assert result["reason"] == "not_self"

    # 验证：当前登记的仍是 task_new
    info = await actor.ask(SRMsg.GET_TASK_INFO, {"thread_id": tid})
    assert info["has_agent_task"] is True

    # 清理
    task_new.cancel()
    try:
        await task_new
    except asyncio.CancelledError:
        pass
    await actor.stop()


# ======================================================================
# Test 5: CircuitBreakerActor —— 并发 success/failure 计数准确
# ======================================================================

@pytest.mark.asyncio
async def test_cb_actor_concurrent_failure_count():
    """并发 100 次 record_failure → 熔断器应准确计数并触发 OPEN。"""
    actor = CircuitBreakerActor("cb_test")
    await actor.start()
    name = "concurrent_failure_svc"

    # 先 GET_OR_CREATE 初始化
    await actor.ask(CBMsg.GET_OR_CREATE, {
        "name": name,
        "failure_threshold": 10,  # 10 次失败就熔断
        "failure_window_sec": 999999,  # 无限窗口，避免 GC 干扰
        "recovery_cooldown_sec": 1,
    })

    N_FAIL = 50
    # 并发发 N_FAIL 次失败
    await asyncio.gather(*[
        actor.send(CBMsg.RECORD_FAILURE, {"name": name})
        for _ in range(N_FAIL)
    ])
    await asyncio.sleep(0.05)  # 让邮箱消费完

    snap = await actor.ask(CBMsg.SNAPSHOT_ONE, {"name": name})
    assert snap is not None
    assert snap["state"] == "OPEN", f"{N_FAIL} 次失败后应为 OPEN，实际 {snap['state']}"
    assert snap["failures_in_window"] == N_FAIL
    assert snap["total_failures"] == N_FAIL

    # 探测：allow_request 在冷却期应返回 False
    ok = await actor.ask(CBMsg.ALLOW_REQUEST, {"name": name})
    assert ok is False, "OPEN 冷却期不应允许请求"

    # 等冷却过去 → HALF_OPEN → 成功探测 2 次 → CLOSED
    await asyncio.sleep(1.2)  # 等 cooldown + 小 buffer
    ok2 = await actor.ask(CBMsg.ALLOW_REQUEST, {"name": name})
    assert ok2 is True, "冷却后 HALF_OPEN 应允许探测请求"

    snap2 = await actor.ask(CBMsg.SNAPSHOT_ONE, {"name": name})
    assert snap2["state"] == "HALF_OPEN"

    # 2 次成功 → 回 CLOSED
    await actor.send(CBMsg.RECORD_SUCCESS, {"name": name})
    await actor.send(CBMsg.RECORD_SUCCESS, {"name": name})
    await asyncio.sleep(0.05)
    snap3 = await actor.ask(CBMsg.SNAPSHOT_ONE, {"name": name})
    assert snap3["state"] == "CLOSED", f"2 次探测成功后应为 CLOSED，实际 {snap3['state']}"
    assert snap3["failures_in_window"] == 0, "CLOSED 后窗口应清零"

    await actor.stop()


# ======================================================================
# Test 6: SLOMonitorActor —— 并发 record_event 无丢失
# ======================================================================

@pytest.mark.asyncio
async def test_slo_actor_concurrent_record():
    """并发 200 次 record_event → 统计 total = 200，成功数与成功比例一致。"""
    actor = SLOMonitorActor("slo_test", persist_db=None)
    await actor.start()

    N = 200
    N_SUCCESS = 150  # 150 成功，50 失败（150/200 = 0.75 可用性）

    async def _record(i: int):
        success = i < N_SUCCESS
        return await actor.send(SLOMsg.RECORD_EVENT, {
            "session_id": f"sess_{i % 5}",
            "timestamp": time.time(),
            "success": success,
            "latency_sec": float(i) * 0.01,
            "token_count": i * 100,
            "final_tier": 1 + (i % 4),
            "hit_hard_limit": i == N - 1,  # 最后一个标记硬上限
            "hallucination_passed": success,
            "error_quadrant": "A" if success else "C",
            "circuit_open": not success and i % 3 == 0,
        })

    await asyncio.gather(*[_record(i) for i in range(N)])
    await asyncio.sleep(0.05)

    snap = await actor.ask(SLOMsg.SNAPSHOT, {})
    assert snap is not None
    # 窗口事件数 = N
    assert snap["window_events"] == N, f"事件丢失：期望 {N}, 实际 {snap['window_events']}"
    # 可用性 = 150 / 200
    assert abs(snap["metrics"]["availability"] - 0.75) < 1e-6
    # 幻觉通过率：150 过 50 未过 = 75%
    # 但我们这里 hallucination_passed = success 所以也是 75%
    assert abs(snap["metrics"]["hallucination_pass_rate"] - 0.75) < 1e-2
    # 硬上限：1 次
    assert snap["metrics"]["hard_limit_hits"] == 1
    # 降级链：1-4 tier 均匀分布（i%4 → 0,1,2,3），所以 tier_1=50, tier_2=50, tier_3=50, tier_4=50
    tier_hits = snap["degradation_chain"]["tier_hits"]
    assert tier_hits.get("tier_1", 0) == 50
    assert tier_hits.get("tier_2", 0) == 50
    assert tier_hits.get("tier_3", 0) == 50
    assert tier_hits.get("tier_4", 0) == 50

    await actor.stop()


# ======================================================================
# Test 7: ActorSystem —— 批量启动/停止
# ======================================================================

@pytest.mark.asyncio
async def test_actor_system_lifecycle():
    system = ActorSystem()
    a1 = CounterActor("sys_a1")
    a2 = CircuitBreakerActor("sys_a2")
    a3 = SessionRegistryActor("sys_a3")
    system.register("a1", a1)
    system.register("a2", a2)
    system.register("a3", a3)

    await system.start_all()
    # 启动后都 running
    assert a1.is_running and a2.is_running and a3.is_running
    # a1 可用
    await a1.send("inc", {"n": 7})
    assert await a1.ask("get", {}) == 7

    status = system.snapshot_all()
    assert len(status) == 3
    for name in ("a1", "a2", "a3"):
        assert status[name]["running"] is True

    await system.stop_all()
    assert not a1.is_running and not a2.is_running and not a3.is_running


# ======================================================================
# 直接脚本运行入口
# ======================================================================

async def _run_all():
    tests = [
        ("test_counter_fifo_order_and_pure_transition", test_counter_fifo_order_and_pure_transition),
        ("test_actor_builtin_ping_and_snapshot", test_actor_builtin_ping_and_snapshot),
        ("test_session_registry_concurrent_register_cancels_old", test_session_registry_concurrent_register_cancels_old),
        ("test_session_registry_unregister_if_self_race", test_session_registry_unregister_if_self_race),
        ("test_cb_actor_concurrent_failure_count", test_cb_actor_concurrent_failure_count),
        ("test_slo_actor_concurrent_record", test_slo_actor_concurrent_record),
        ("test_actor_system_lifecycle", test_actor_system_lifecycle),
    ]
    pass_count = 0
    fail_count = 0
    for name, fn in tests:
        t0 = time.time()
        try:
            await fn()
            dt = time.time() - t0
            print(f"✓ PASS {name}  ({dt*1000:.1f}ms)")
            pass_count += 1
        except Exception as e:
            dt = time.time() - t0
            print(f"✗ FAIL {name}  ({dt*1000:.1f}ms): {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            fail_count += 1
    print(f"\n===== 总计: PASS {pass_count} / FAIL {fail_count} =====")
    return fail_count == 0


if __name__ == "__main__":
    ok = asyncio.run(_run_all())
    sys.exit(0 if ok else 1)
