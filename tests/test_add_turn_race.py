# -*- coding: utf-8 -*-
"""
竞态复现与验证：add_turn 的 turn_index 读-改-写竞态。

复现场景（修复前会触发）：
  后台盘前小作文任务 与 主聊天流程 几乎同时调用 add_turn(同一 session_id)。
  两个协程并发执行 SELECT MAX(turn_index)+1 → 拿到相同的值 →
  第二个 INSERT 触发 PRIMARY KEY(session_id, turn_index) 冲突 → 该轮对话丢失。

修复后：每个 session_id 一把 asyncio.Lock 串行化 SELECT+INSERT，无丢失更新。

运行：python tests/test_add_turn_race.py
"""
import sys
import asyncio
from pathlib import Path

# 强制 utf-8，避免 Windows gbk 编码错误
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except AttributeError:
    pass

# 加入项目根
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agent.memory_manager import MemoryManager, get_memory_manager  # noqa: E402


async def run_race(n_concurrent: int = 20) -> None:
    # 用独立 DB，避免污染正式数据：临时实例
    mm = MemoryManager()
    # 复用其 DB（memory.db）。为隔离测试，清理目标 session
    sid = "race-test-session"
    await mm.clear_session(sid)

    print(f">> 并发写入 {n_concurrent} 轮 add_turn（同一会话 {sid}）...")

    tasks = []
    for i in range(n_concurrent):
        tasks.append(mm.add_turn(sid, f"用户消息#{i}", f"助手回复#{i}"))

    # 全部并发触发 —— 修复前这里会抛 IntegrityError
    results = await asyncio.gather(*tasks, return_exceptions=True)

    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        print(f"   ❌ 出现 {len(errors)} 个异常（竞态未修复！）：")
        for e in errors[:3]:
            print(f"      {type(e).__name__}: {e}")
        await mm.clear_session(sid)
        sys.exit(1)

    # 验证无丢失更新：应有且仅有 n_concurrent 条记录，turn_index 1..N 连续无缺
    stats = await mm.get_stats(sid)
    print(f"   预期 {n_concurrent} 轮，实际 {stats['turn_count']} 轮")
    assert stats["turn_count"] == n_concurrent, (
        f"丢失更新！预期 {n_concurrent}，实际 {stats['turn_count']}"
    )

    # 校验 turn_index 连续无重复
    conn = await mm._ensure_async_conn()
    cur = await conn.execute(
        "SELECT turn_index FROM memory_turns WHERE session_id = ? ORDER BY turn_index ASC",
        (sid,),
    )
    rows = [int(r["turn_index"]) for r in await cur.fetchall()]
    expected = list(range(1, n_concurrent + 1))
    assert rows == expected, f"turn_index 不连续/有重复：{rows[:10]}...（预期 1..{n_concurrent}）"

    print(f"   ✅ 全部 {n_concurrent} 轮写入成功，turn_index 连续 1..{n_concurrent}，无丢失更新")

    await mm.clear_session(sid)


async def run_compress_race() -> None:
    """复现 _compress_summaries 并发执行导致重复 segment 的竞态。

    修复前：两个并发压缩任务交叉执行 DELETE+INSERT，memory_summaries 出现
    重复 segment_index，build_prompt_context 读到双倍内容。
    修复后：每个 session_id 一把 asyncio.Lock，压缩任务串行执行。
    """
    from agent.memory_manager import SUMMARY_TRIGGER_TURNS, SUMMARY_SEGMENTS

    mm = MemoryManager()
    sid = "compress-race-session"
    await mm.clear_session(sid)

    # 写入超过触发阈值的轮次（每轮都触发后台压缩）
    n_turns = SUMMARY_TRIGGER_TURNS + 5
    print(f">> 写入 {n_turns} 轮（阈值 {SUMMARY_TRIGGER_TURNS}），并直接并发触发压缩...")
    for i in range(n_turns):
        await mm.add_turn(sid, f"用户#{i}", f"助手回复#{i} 涉及600519茅台目标价")

    # 并发触发多次压缩（模拟连续 add_turn 各自 create_task 的压缩）
    tasks = [mm._compress_summaries(sid) for _ in range(5)]
    await asyncio.gather(*tasks, return_exceptions=True)
    # 给后台 create_task 触发的压缩留点时间（add_turn 内部 fire-and-forget）
    await asyncio.sleep(0.5)

    # 校验：每个 segment_index 只能出现一次（PRIMARY KEY 约束 + 锁保证）
    conn = await mm._ensure_async_conn()
    cur = await conn.execute(
        "SELECT segment_index, COUNT(*) AS c FROM memory_summaries "
        "WHERE session_id = ? GROUP BY segment_index",
        (sid,),
    )
    seg_counts = {int(r["segment_index"]): int(r["c"]) for r in await cur.fetchall()}
    print(f"   segment 分布：{seg_counts}")
    for seg_idx, cnt in seg_counts.items():
        assert cnt == 1, f"segment_index={seg_idx} 出现 {cnt} 次（应为1，存在重复压缩）"

    print(f"   ✅ 并发压缩无重复 segment（{len(seg_counts)} 段，各 1 次）")
    await mm.clear_session(sid)


if __name__ == "__main__":
    asyncio.run(run_race(20))
    asyncio.run(run_compress_race())
    print("\n>> 竞态测试通过")
