# -*- coding: utf-8 -*-
"""
CancellationToken + RequestContext 三位一体机制验证测试。

覆盖场景：
  1. 基础：cancel / check / wait / is_cancelled 语义正确
  2. 级联取消：register_child_task → 父 token.cancel() → 子任务立即 CancelledError
  3. thread_id 反向索引：create_request_context → cancel_by_thread_id 能找到并取消
  4. 超时自动取消：deadline 到期后 is_cancelled 自动变 True
  5. 取消速度（核心指标）：同步密集循环（无 await）靠 check_cancelled() 感知 → <1ms 抛错
  6. DISCONNECT 场景模拟：令牌取消 → 运行中的同步 for 循环在下次 check 立即退出
  7. 子任务级联（孤儿任务验证）：压缩模拟任务被 register_child_task → 父取消 → 子也取消
  8. 取消回调：register_callback 触发顺序与幂等

运行:  python tests/test_cancellation.py
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from agent.request_context import (
    CancellationToken,
    RequestContext,
    RequestCancelledError,
    create_request_context,
    bind_request_context,
    unbind_request_context,
    current_context,
    current_token,
    check_cancelled,
    cancel_by_thread_id,
)


# ======================================================================
# Test 1. 基础语义
# ======================================================================
async def test_token_basic():
    tok = CancellationToken()
    assert tok.is_cancelled is False
    assert tok.reason == ""
    tok.cancel("user_stop")
    assert tok.is_cancelled is True
    assert tok.reason == "user_stop"
    # 幂等
    assert tok.cancel("again") is False
    # check 抛异常
    thrown = False
    try:
        tok.check("somewhere")
    except RequestCancelledError as e:
        thrown = True
        assert e.reason == "user_stop"
        assert tok.token_id in str(e)
    assert thrown, "cancel 后 check 必须抛 RequestCancelledError"
    # wait 立即返回
    t0 = time.monotonic()
    assert await tok.wait(timeout=5.0) is True
    dt = time.monotonic() - t0
    assert dt < 0.1, f"cancel 后的 wait 应立即返回，用时 {dt*1000:.1f}ms"
    tok.dispose()
    print("  ✓ 基础语义（cancel/check/wait/powerup）")


# ======================================================================
# Test 2. 级联取消子任务
# ======================================================================
async def test_cancel_child_task():
    tok = CancellationToken()

    # 子任务：跑 sleep(9999)（永不完成，除非被 cancel）
    async def _child():
        try:
            await asyncio.sleep(9999)
            return "unexpected"
        except asyncio.CancelledError:
            return "cancelled_ok"

    child = asyncio.create_task(_child())
    tok.register_child_task(child)
    # 立即取消父令牌
    tok.cancel("father_dead")
    # 等待子任务被取消。注意：child 内部 except CancelledError 返回 cancelled_ok，
    # 但如果直接 await child，父 cancel 也会级联 child.task.cancel() → child 内部捕获后 return。
    try:
        result = await child  # 不用 wait_for，避免 CancelledError 从 wait_for 冒出来
    except asyncio.CancelledError:
        # 也可能 wait_for 的位置没 wait_for 但 asyncio 自身取消语义向外冒 → 也算成功（被取消即 OK）
        result = "cancelled_ok"
    assert result == "cancelled_ok", f"子任务应该返回 cancelled_ok，实际 {result!r}"
    assert child.done()
    tok.dispose()
    print("  ✓ 级联取消：register_child_task → 父 cancel → 子立即 CancelledError")


# ======================================================================
# Test 3. thread_id 反向索引
# ======================================================================
async def test_thread_id_index():
    tid = "test_thread_" + str(int(time.time() * 1000))
    # 模拟请求链路：创建 ctx → bind → 用 cancel_by_thread_id 取消
    ctx = create_request_context(thread_id=tid, timeout_sec=None)
    bind_tok = bind_request_context(ctx)
    try:
        # 给 index_put 一点时间调度（它是 create_task 异步登记的）
        await asyncio.sleep(0.02)
        # 用 thread_id 触发取消
        info = await cancel_by_thread_id(tid, "remote_stop")
        assert info["found"] is True, f"cancel_by_thread_id 应能找到刚登记的 token, info={info}"
        assert ctx.is_cancelled is True
        assert ctx.token.reason == "remote_stop"
        # 再次：已取消 → info 仍然找到，但 already_cancelled=True
        info2 = await cancel_by_thread_id(tid, "again")
        assert info2["found"] is True
        assert info2["already_cancelled"] is True
    finally:
        unbind_request_context(bind_tok)
        ctx.dispose()
    # 不存在的 thread_id → found=False
    miss = await cancel_by_thread_id("this_thread_does_not_exist_xyz")
    assert miss["found"] is False
    print("  ✓ thread_id 反向索引：cancel_by_thread_id 远程取消成功")


# ======================================================================
# Test 4. 超时自动取消
# ======================================================================
async def test_timeout_cancel():
    # 0.1s 超时
    tok = CancellationToken(timeout_sec=0.1)
    assert tok.is_cancelled is False
    await asyncio.sleep(0.15)
    assert tok.is_cancelled is True
    assert "timeout" in tok.reason
    tok.dispose()
    # remaining_sec / elapsed_sec
    ctx = create_request_context(timeout_sec=10.0)
    try:
        await asyncio.sleep(0.05)
        assert ctx.elapsed_sec > 0
        # remaining_sec 应该在 9.9 附近
        assert ctx.remaining_sec is not None and 9.0 < ctx.remaining_sec <= 10.0
    finally:
        ctx.dispose()
    print("  ✓ 超时自动取消：deadline 到期后 is_cancelled 自动变 True")


# ======================================================================
# Test 5. 取消速度（核心）: 循环中主动 check_cancelled → call_later 触发后 <100ms 感知
# ======================================================================
async def test_cancel_speed_sync_loop():
    """模拟 LLM 后处理 / 大 JSON 解析这类"每轮都能检查取消"的代码：
    check_cancelled() 仅为 ns 级 bool 读 + monotonic()，开销极低。
    取消信号到达后，下一次 check 立即抛出 RequestCancelledError → 毫秒级停止。
    """
    ctx = create_request_context(thread_id="speed_test")
    bind_tok = bind_request_context(ctx)
    iterations_until_cancel: int = 0
    cancelled_hit: bool = False
    loop = asyncio.get_event_loop()

    # 异步密集循环：每 100 轮 yield 一次（让 call_later 有机会触发 cancel）
    async def _async_dense():
        nonlocal iterations_until_cancel, cancelled_hit
        i = 0
        while True:
            i += 1
            iterations_until_cancel = i
            check_cancelled(f"async_loop.iter{i}")
            if i % 100 == 0:
                await asyncio.sleep(0)  # 让出事件循环，call_later 在这里有机会触发
            sum(range(10))  # 模拟一点同步工作

    t0 = time.perf_counter()
    # 2ms 后取消
    loop.call_later(0.002, lambda: ctx.cancel("trigger_cancel"))
    try:
        await _async_dense()
    except RequestCancelledError:
        cancelled_hit = True
    except Exception:
        pass
    dt_ms = (time.perf_counter() - t0) * 1000.0

    unbind_request_context(bind_tok)
    ctx.dispose()

    assert cancelled_hit is True, "循环必须通过 check_cancelled() 感知取消"
    # 断言：总耗时 < 200ms（大部分情况 5-30ms 级别）
    assert dt_ms < 200, f"取消感知过慢：总耗时 {dt_ms:.1f}ms，应 <200ms"
    # 断言：取消前迭代不能过多（粒度过粗），2ms 约跑 20000~200000 轮
    assert iterations_until_cancel < 500_000, (
        f"check_cancelled 感知粒度过粗：取消前跑了 {iterations_until_cancel} 轮"
    )
    print(f"  ✓ 取消速度：感知取消总耗时 {dt_ms:.1f}ms，取消前迭代 {iterations_until_cancel} 轮")


# ======================================================================
# Test 6. DISCONNECT 场景模拟
# ======================================================================
async def test_disconnect_scenario():
    """模拟：一个协程正在密集循环写文件/后处理 → WebSocket断开 → cancel_by_thread_id
    → 立即在下次 check 停止，不再继续写。"""
    tid = "disconnect_sess_" + str(int(time.time() * 1000))
    ctx = create_request_context(thread_id=tid)
    bind_tok = bind_request_context(ctx)
    written_items: list[int] = []

    async def _worker():
        i = 0
        try:
            while True:
                i += 1
                # 模型执行间隙主动检查：模拟每生成一段输出就检查一次
                check_cancelled(f"worker.i{i}")
                written_items.append(i)
                # 模拟小处理（每 100 轮 yield 给事件循环，让 DISCONNECT 有机会）
                if i % 200 == 0:
                    await asyncio.sleep(0)
        except RequestCancelledError:
            # 预期取消
            return i
        return -1

    worker_task = asyncio.create_task(_worker())
    # 等 worker 启动，积累一小段数据
    await asyncio.sleep(0.02)
    # ==== 模拟 WebSocket DISCONNECT：先 cancel_by_thread_id（令牌级取消）====
    info = await cancel_by_thread_id(tid, "websocket_disconnected")
    assert info["found"] is True
    # 等待 worker 结束
    final_i = await asyncio.wait_for(worker_task, timeout=1.0)
    unbind_request_context(bind_tok)
    ctx.dispose()

    # 验证：worker 在取消后没有继续写入（written_items 长度应 <= final_i）
    # 注：可能相差 0 或 1 —— 取决于"最后一次 check 通过 → append"还是"最后一次 check 抛错"。
    assert final_i > 0
    assert len(written_items) <= final_i <= len(written_items) + 1, (
        f"取消后写入计数异常：len={len(written_items)}, final_i={final_i}（应相差≤1）"
    )
    # 验证：取消后 written_items 不再增长（再等 30ms 看，若未被取消会继续写数万条）
    size_after = len(written_items)
    await asyncio.sleep(0.03)
    assert len(written_items) == size_after, (
        f"取消后 written_items 仍在增长（取消不生效）：{size_after}→{len(written_items)}"
    )
    print(f"  ✓ DISCONNECT 场景：worker 已在 i≈{final_i} 处停止，取消后不再继续写")


# ======================================================================
# Test 7. 孤儿任务验证：父取消 → 子压缩任务也取消
# ======================================================================
async def test_orphan_child_cancel():
    ctx = create_request_context(thread_id="orphan_test")
    bind_tok = bind_request_context(ctx)
    # 模拟 memory_manager 中：add_turn 创建后台压缩任务并登记为 child_task
    comp_finished_normally = False
    comp_cancelled = False

    async def _compress_summaries():
        """模拟压缩任务：内部有 5 次大循环（每次 50ms sleep），每次循环前 check_cancelled"""
        nonlocal comp_finished_normally, comp_cancelled
        try:
            for seg in range(5):
                check_cancelled(f"compress.seg_{seg}")
                await asyncio.sleep(0.05)
            comp_finished_normally = True
        except asyncio.CancelledError:
            comp_cancelled = True
            raise

    comp_task = asyncio.create_task(_compress_summaries())
    # 登记为父令牌的子任务 → 父取消 → 子任务也会被 cancel
    ctx.token.register_child_task(comp_task)
    # 只等 30ms（正常压缩要 250ms），然后立即取消父令牌
    await asyncio.sleep(0.03)
    ok = ctx.cancel("user_clicked_stop")
    assert ok is True
    # 等待子任务结束（如果 comp_task 内 except CancelledError 并 raise → await 时向外冒 → 捕获）
    try:
        await comp_task
    except asyncio.CancelledError:
        # 预期：级联取消生效
        pass
    except asyncio.TimeoutError:
        raise AssertionError("压缩子任务在父取消后 1 秒内未停止（孤儿任务！）")
    unbind_request_context(bind_tok)
    ctx.dispose()

    assert comp_cancelled is True, "压缩子任务必须收到 CancelledError（否则成为孤儿任务）"
    assert comp_finished_normally is False, "压缩任务不应正常结束（父已取消）"
    print("  ✓ 孤儿任务：父取消 → 登记的 child_task 压缩任务立即 CancelledError，无孤儿")


# ======================================================================
# Test 8. 取消回调（幂等 + 异步回调）
# ======================================================================
async def test_cancel_callback():
    tok = CancellationToken()
    calls: list[str] = []

    def _sync_cb(t: CancellationToken):
        calls.append(f"sync:{t.reason}")

    async def _async_cb(t: CancellationToken):
        calls.append(f"async:{t.reason}")
        await asyncio.sleep(0.001)

    tok.register_callback(_sync_cb)
    tok.register_callback(_async_cb)
    tok.cancel("cb_test")
    # 给异步回调 create_task 一点时间跑
    await asyncio.sleep(0.02)
    # 幂等：再次 cancel 不触发回调
    tok.cancel("cb_again")
    await asyncio.sleep(0.01)
    assert "sync:cb_test" in calls
    assert "async:cb_test" in calls
    assert "sync:cb_again" not in calls, "再次 cancel 不应再次触发回调（幂等）"
    tok.dispose()
    print("  ✓ 取消回调：同步/异步回调都触发，再次 cancel 幂等不重复")


# ======================================================================
# main
# ======================================================================
async def _run_all():
    tests = [
        ("test_token_basic", test_token_basic),
        ("test_cancel_child_task", test_cancel_child_task),
        ("test_thread_id_index", test_thread_id_index),
        ("test_timeout_cancel", test_timeout_cancel),
        ("test_cancel_speed_sync_loop", test_cancel_speed_sync_loop),
        ("test_disconnect_scenario", test_disconnect_scenario),
        ("test_orphan_child_cancel", test_orphan_child_cancel),
        ("test_cancel_callback", test_cancel_callback),
    ]
    pass_n = 0
    fail_n = 0
    for name, fn in tests:
        t0 = time.time()
        try:
            await fn()
            dt_ms = (time.time() - t0) * 1000
            print(f"✓ PASS {name}  ({dt_ms:.1f}ms)")
            pass_n += 1
        except Exception as e:
            dt_ms = (time.time() - t0) * 1000
            print(f"✗ FAIL {name}  ({dt_ms:.1f}ms): {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            fail_n += 1
    print(f"\n===== 总计: PASS {pass_n} / FAIL {fail_n} =====")
    return fail_n == 0


if __name__ == "__main__":
    ok = asyncio.run(_run_all())
    sys.exit(0 if ok else 1)
