#coding: utf-8
"""并发闸单测：限流 / 排队 / 忙闲观测 / 排队回调 / 异常传播。运行：python tests/test_concurrency_gate.py"""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.utils.concurrency_gate import ConcurrencyGate


async def _main():
    failures = []

    def check(name, cond):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        if not cond:
            failures.append(name)

    # 1) 限流：并发 2，3 个任务 → 第 3 个排队，总时长 >= 2 个串行槽
    gate = ConcurrencyGate("t", 2)
    started, done = [], []

    async def worker(i, dur):
        async def _f():
            started.append(i)
            await asyncio.sleep(dur)
            return i
        r = await gate.run(_f)
        done.append(r)

    t0 = time.monotonic()
    await asyncio.gather(worker(1, 0.3), worker(2, 0.3), worker(3, 0.3))
    elapsed = time.monotonic() - t0
    check("3 任务/并发2 → 全部完成", done == [1, 2, 3])
    check("第 3 任务被排队（总时长 >= 0.6s）", elapsed >= 0.55)
    check("闸内 active 峰值不超限（started 1,2 先于 3）",
          started[:2] == [1, 2] and started[2] == 3)

    # 2) on_wait 回调 + waiting 计数
    gate2 = ConcurrencyGate("o", 1)
    wait_cb = []

    async def hold():
        await asyncio.sleep(0.4)
        return "ok"

    async def queued():
        async def _f():
            await asyncio.sleep(0.05)
            return "ok2"
        return await gate2.run(_f, on_wait=lambda n: wait_cb.append(n))

    t = asyncio.create_task(gate2.run(hold))
    await asyncio.sleep(0.05)
    q = asyncio.create_task(queued())
    await asyncio.sleep(0.1)
    check("排队中 waiting=1", gate2.waiting == 1)
    await asyncio.gather(t, q)
    check("on_wait 回调触发", len(wait_cb) == 1 and wait_cb[0] >= 1)
    check("完成后 waiting/active 归零", gate2.waiting == 0 and gate2.active == 0)

    # 3) 异常传播：factory 抛错 → run 抛错，且许可释放（后续可再用）
    gate3 = ConcurrencyGate("e", 1)

    async def boom():
        raise ValueError("boom")

    try:
        await gate3.run(boom)
        check("异常应传播", False)
    except ValueError:
        check("异常传播", True)
    r = await gate3.run(lambda: asyncio.sleep(0, "reusable"))
    check("异常后许可释放可复用", r == "reusable" and gate3.active == 0)

    print()
    if failures:
        print(f"结果：{len(failures)} 项失败 -> {failures}")
        sys.exit(1)
    print("结果：全部通过")


if __name__ == "__main__":
    asyncio.run(_main())
