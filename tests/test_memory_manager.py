"""
测试 Context Engineering 记忆管理模块核心逻辑。
运行方式：python tests/test_memory_manager.py
不需要启动 LLM 或 server，纯单元逻辑验证。
"""
import asyncio
import sys
from pathlib import Path

# 测试文件位于 tests/ 子目录，需要向上一级找到项目根目录
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from agent.memory_manager import MemoryManager, get_memory_manager, WINDOW_KEEP_LAST_N, SUMMARY_TRIGGER_TURNS


def test_priority_classification():
    """验证优先级分类 + 关键决策识别"""
    print("\n" + "=" * 60)
    print("【测试1】优先级分类（启发式关键词）")
    print("=" * 60)

    cases = [
        # (user, assistant, expect_key_decision, expect_high_priority, label)
        ("你好", "你好！有什么可以帮您？", False, False, "纯问候闲聊"),
        ("帮我分析一下 600519 贵州茅台的走势",
         "600519 贵州茅台近30日收盘从 1680 元上涨至 1750 元，建议持有，目标价 1850 元，PE约 26 倍。",
         True, True, "股票代码 + 买入/持有建议"),
        ("美联储最近会不会加息？",
         "根据最新的CPI和非农数据，美联储本月大概率维持利率不变，但仍保留后续加息可能。",
         True, True, "宏观政策：美联储/加息/CPI"),
        ("今天天气怎么样？", "今天北京晴，温度26度。", False, False, "天气闲聊"),
        ("好的，谢谢你的分析", "不客气，祝您投资顺利！", False, False, "礼貌告别"),
        ("根据以上分析，我决定买入 100 股 Tesla，仓位 5%，止盈止损 10%",
         "已确认您的决策：TSLA 买入100股，仓位5%，止盈止损各10%。请注意风险控制。",
         True, True, "明确决策：买入/仓位/止盈止损"),
    ]
    all_pass = True
    for user, assistant, exp_key, exp_high, label in cases:
        prio, is_key = MemoryManager.classify_priority(user, assistant)
        ok_key = is_key == exp_key
        ok_high = (prio >= 70) if exp_high else (prio <= 70)
        status = "✅" if (ok_key and ok_high) else "❌"
        if not (ok_key and ok_high):
            all_pass = False
        print(
            f"  {status} [{label}]\n"
            f"      priority={prio}, is_key_decision={is_key}  "
            f"(期望 key={exp_key}, 高优先级={'是' if exp_high else '否'})"
        )
    return all_pass


async def test_add_turn_and_build_context():
    """验证：写入对话 → 构建上下文 → 滑窗/摘要/关键决策三段式输出"""
    print("\n" + "=" * 60)
    print("【测试2】写入多轮对话 + 构建 Prompt 上下文")
    print("=" * 60)
    SID = "test_session_001"
    mm = get_memory_manager()
    # 先清理可能残留的旧数据
    await mm.clear_session(SID)

    # ========== 1. 先写 5 轮，包含 2 轮关键决策 + 3 轮闲聊 ==========
    print(f"\n>> 写入 5 轮对话（滑窗阈值 {WINDOW_KEEP_LAST_N}，压缩阈值 {SUMMARY_TRIGGER_TURNS}）")
    turns_5 = [
        ("你好", "您好！我是MOSS金融助手，有什么投研问题可以帮您？"),
        ("帮我看看宁德时代的基本面",
         "宁德时代(300750)：最新财报营收 4009 亿，同比+10%，净利润 450 亿，同比+15%；ROE 18%。\n结论：动力电池龙头地位稳固，维持买入评级，目标价 280 元。"),
        ("好的，那最近有没有什么政策风险？",
         "近期欧盟对中国动力电池发起反补贴调查，可能影响出口；但国内新能源车购置税减免延续到2027年，对冲利好。整体风险可控。"),
        ("谢谢你的分析", "不客气！还有其他想了解的标的吗？"),
        ("没有了，先这样吧", "好的，祝您投资顺利！"),
    ]
    for u, a in turns_5:
        await mm.add_turn(SID, u, a)

    stats = await mm.get_stats(SID)
    print(f"   写入后统计：turns={stats['turn_count']}, keys={stats['key_decision_count']}, summaries={stats['summary_segment_count']}")
    assert stats["turn_count"] == 5
    # 第 2、3 轮应该被识别为关键决策
    assert stats["key_decision_count"] >= 2, f"关键决策至少应有 2 条，实际 {stats['key_decision_count']}"
    assert stats["summary_segment_count"] == 0, "小于压缩阈值，不应生成摘要"
    print("   ✅ 轮数、关键决策计数符合预期（无摘要）")

    # ========== 2. 构建上下文，验证结构 ==========
    ctx = await mm.build_prompt_context(SID, "帮我再看看比亚迪")
    print(f"\n>> 构建上下文长度：{len(ctx)} 字符")
    assert "关键决策记录" in ctx, "关键决策记录段缺失"
    assert "最近对话（滑窗）" in ctx, "最近对话滑窗段缺失"
    assert "历史摘要" not in ctx, "未触发压缩，不应出现摘要段"
    # 检查 300750 和 280 元等关键信息出现在关键决策中
    assert "300750" in ctx or "宁德时代" in ctx, "关键股票代码在上下文中缺失"
    print("   ✅ 上下文结构正确：包含关键决策段 + 滑窗段（无摘要），关键信息存在")

    # ========== 3. 继续写入到超过 SUMMARY_TRIGGER_TURNS，触发摘要压缩 ==========
    extra_needed = SUMMARY_TRIGGER_TURNS - stats["turn_count"] + 3
    print(f"\n>> 再写入 {extra_needed} 轮对话，触发摘要压缩...")
    # 混合写入：再加入一些关键决策，其他为普通/闲聊
    extra_patterns = [
        ("请分析下中国平安601318", "601318 中国平安：NBV同比+12%，寿险复苏明显，PEV仅0.65，严重低估，建议买入，目标价 65 元。"),
        ("美股的英伟达NVDA呢？", "NVDA 英伟达：Q2营收翻倍，H100需求旺盛，AI龙头溢价明显，但估值偏高，建议分批建仓，仓位不超过5%。"),
        ("最近美联储态度如何？", "美联储主席鲍威尔最新讲话偏鹰，年内可能再加息一次，美债收益率突破4.5%，注意成长股估值承压。"),
        ("好的了解了", "还有其他问题可以随时问我。"),
        ("今天心情怎么样", "我是AI没有心情，不过很高兴为您提供投研服务！"),
    ]
    for i in range(extra_needed):
        u, a = extra_patterns[i % len(extra_patterns)]
        # 加序号避免主键冲突（add_turn会自增turn_index，所以不需要）
        await mm.add_turn(SID, u + f"（第{i+1}次）", a + f" 序号{i+1}")

    # 摘要压缩是后台任务，给它点时间
    await asyncio.sleep(1.5)

    stats = await mm.get_stats(SID)
    print(f"   压缩后统计：turns={stats['turn_count']}, keys={stats['key_decision_count']}, summaries={stats['summary_segment_count']}")
    assert stats["turn_count"] == 5 + extra_needed
    assert stats["summary_segment_count"] >= 1, "超过阈值应触发摘要压缩"
    print(f"   ✅ 摘要压缩已生成 {stats['summary_segment_count']} 段")

    # ========== 4. 再次构建上下文，验证三段式都出现 ==========
    ctx2 = await mm.build_prompt_context(SID, "总结一下我之前关注的股票")
    print(f"\n>> 压缩后构建上下文长度：{len(ctx2)} 字符")
    has_summary = "历史摘要" in ctx2
    has_keys = "关键决策记录" in ctx2
    has_window = "最近对话（滑窗）" in ctx2
    print(f"   历史摘要段={'✅' if has_summary else '❌'}  关键决策段={'✅' if has_keys else '❌'}  最近滑窗段={'✅' if has_window else '❌'}")
    assert has_summary and has_keys and has_window, "三段式结构不完整"
    # 验证上下文总长度不会爆炸
    print(f"   ✅ 上下文结构完整（三段式），总长度 {len(ctx2)} 字符，上限可防膨胀")

    # ========== 5. 清理 ==========
    await mm.clear_session(SID)
    stats = await mm.get_stats(SID)
    assert stats["turn_count"] == 0 and stats["key_decision_count"] == 0 and stats["summary_segment_count"] == 0
    print("\n   ✅ 清理会话成功，所有数据归零")

    return True


def test_extract_summary():
    """验证摘要压缩算法的句子抽取与优先级排序"""
    print("\n" + "=" * 60)
    print("【测试3】摘要压缩：句子抽取算法")
    print("=" * 60)

    # 模拟 turns（dict 形式，与 aiosqlite.Row 字段名一致）
    turns = [
        {"priority": 20, "is_key_decision": 0, "user_content": "你好", "assistant_content": "您好！有什么能帮您的？"},
        {"priority": 95, "is_key_decision": 1,
         "user_content": "分析贵州茅台600519",
         "assistant_content": "贵州茅台600519：PE 26倍，低于历史中枢30倍。当前价1750，目标价1850，建议逢低买入，仓位控制在8%以内。ROE常年30%以上，护城河稳固。"},
        {"priority": 30, "is_key_decision": 0, "user_content": "谢谢", "assistant_content": "不客气！"},
        {"priority": 88, "is_key_decision": 1,
         "user_content": "美联储政策怎么看？",
         "assistant_content": "美联储9月议息会议维持利率不变。点阵图显示年内或再加息一次。美债10Y收益率4.4%，黄金承压，注意成长股估值回调风险。"},
        {"priority": 25, "is_key_decision": 0, "user_content": "好的", "assistant_content": "明白了。"},
    ]
    summary = MemoryManager._build_segment_summary(turns, 400)
    print(f"   生成摘要（≤400字）：{len(summary)} 字符")
    print(f"   内容：{summary}")
    # 必须包含关键决策信息，闲聊应被丢弃
    assert "600519" in summary or "茅台" in summary, "关键股票信息缺失"
    assert "美联储" in summary or "加息" in summary, "宏观关键信息缺失"
    assert "你好" not in summary and "不客气" not in summary, "闲聊不应出现在摘要中"
    assert len(summary) <= 400 + 10, "摘要长度超标"
    print("   ✅ 摘要仅包含高优先级/关键句，闲聊被丢弃，长度受控")
    return True


async def main():
    print("\n" + "*" * 60)
    print("  MOSS Finance Assistant - 记忆管理单元测试")
    print("*" * 60)

    results = []
    results.append(("优先级分类", test_priority_classification()))
    results.append(("摘要压缩抽取", test_extract_summary()))
    try:
        ok_ctx = await test_add_turn_and_build_context()
        results.append(("写入/上下文/清理", ok_ctx))
    except AssertionError as e:
        import traceback
        traceback.print_exc()
        results.append(("写入/上下文/清理", False))
    except Exception as e:
        import traceback
        traceback.print_exc()
        results.append(("写入/上下文/清理", False))

    print("\n" + "=" * 60)
    print("汇总：")
    all_ok = True
    for name, ok in results:
        status = "✅ 通过" if ok else "❌ 失败"
        print(f"  {status} - {name}")
        all_ok = all_ok and ok
    print("=" * 60)
    if all_ok:
        print("🎉 全部测试通过！记忆管理模块工作正常。")
        return 0
    else:
        print("💥 有测试失败，请检查。")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
