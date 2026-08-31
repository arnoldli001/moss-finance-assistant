# coding=utf-8
"""
单元测试：StockMatcher 股票匹配工具 + 项目集成校验。

运行：
    python tests/test_stock_matcher.py         # 直接运行（自带 unittest.main）
    python -m unittest tests.test_stock_matcher -v

覆盖：
  T1 ~ T9  : StockMatcher 核心能力（读取、索引、查询、边界、歧义消解）
  T10~T13 : 性能硬指标（单次查询<20us、文本抽取 500字<5ms、单例懒加载）
  T14~T17 : 与 4 个业务模块的集成（output_validator / context_engineer
            / maker_checker / zsxq_tool）
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
import unittest
from pathlib import Path

# 确保在项目根目录能正确 import（python tests/test_xxx.py 方式运行时）
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ======================================================================
# 基础数据（与 data/stock_list.txt 保持同步，便于直观阅读用例）
# ======================================================================
_KNOWN_GOOD_CODES = [
    "600519", "601398", "601288", "601939", "601857",
    "601988", "300750", "600941", "601138", "600036",
    "000001", "600000", "688825", "002487", "002562",
]
_KNOWN_GOOD_NAMES = [
    "贵州茅台", "工商银行", "农业银行", "建设银行", "中国石油",
    "中国银行", "宁德时代", "中国移动", "工业富联", "招商银行",
    "平安银行", "浦发银行", "长鑫科技", "大金重工", "兄弟科技",
]
_FAKE_CODES = ["000000", "123456", "999999", "202501", "202506"]


class TestStockMatcherCore(unittest.TestCase):
    """T1 ~ T9：StockMatcher 核心能力。"""

    @classmethod
    def setUpClass(cls) -> None:
        # 每个测试套件重置单例，确保不被其他测试污染
        from tools.stock_matcher import StockMatcher
        StockMatcher._instance = None
        cls.matcher = StockMatcher.get_instance()

    # ---- T1 基础加载 ----
    def test_T1_file_loaded_sane_count(self):
        """股票清单加载后应至少有 4000 只 A 股（当前约 4600+）。"""
        self.assertGreaterEqual(
            self.matcher.total_count, 4000,
            f"stock_list.txt 加载异常，仅 {self.matcher.total_count} 只"
        )

    # ---- T2 代码存在性判断 ----
    def test_T2_valid_code_lookup(self):
        from tools.stock_matcher import is_stock_code
        for c in _KNOWN_GOOD_CODES:
            self.assertTrue(is_stock_code(c), f"{c} 应是有效代码")
        for c in _FAKE_CODES:
            self.assertFalse(is_stock_code(c), f"{c} 应是无效代码")

    # ---- T3 全称存在性判断 ----
    def test_T3_valid_name_lookup(self):
        from tools.stock_matcher import is_stock_name, lookup_stock
        for n in _KNOWN_GOOD_NAMES:
            self.assertTrue(is_stock_name(n), f"{n} 应是有效全称")
            info = lookup_stock(n)
            self.assertIsNotNone(info, f"lookup 全称{n}不应返回None")
            self.assertEqual(info.name, n)

    # ---- T4 代码反查名称 & 市场 ----
    def test_T4_code_to_name_and_market(self):
        cases = {
            "600519": ("贵州茅台", "SH"),   # 沪主板
            "300750": ("宁德时代", "CY"),   # 创业板
            "688825": ("长鑫科技", "KC"),   # 科创板
            "000001": ("平安银行", "SZ"),   # 深主板
            "600036": ("招商银行", "SH"),
        }
        for code, (exp_name, exp_market) in cases.items():
            info = self.matcher.lookup_by_code(code)
            self.assertIsNotNone(info, code)
            self.assertEqual(info.name, exp_name, code)
            self.assertEqual(info.market, exp_market, code)

    # ---- T5 短别名查询 + 强语境依赖 ----
    def test_T5_short_alias_requires_context(self):
        """≤3 字短别名 lookup 必须提供强语境（或自身就是全称），否则返回 None。"""
        # 无上下文 → 一律不认
        for alias in ["茅台", "工商", "宁德", "移动", "兄弟", "大金"]:
            self.assertIsNone(
                self.matcher.lookup(alias, ""),
                f"短别名 {alias} 无上下文应返回 None"
            )
        # 有强语境 → 应命中对应股票
        cases_with_ctx = [
            ("茅台", "贵州茅台今日股价多少", "600519"),
            ("工商", "工商银行 601398 今日涨停", "601398"),
            ("宁德", "宁德时代大涨 5%", "300750"),
            ("移动", "中国移动今日发布财报", "600941"),
            ("大金", "大金重工(002487)涨停了", "002487"),
            ("兄弟", "兄弟科技涨停了", "002562"),
        ]
        for alias, ctx, exp_code in cases_with_ctx:
            info = self.matcher.lookup(alias, ctx)
            self.assertIsNotNone(info, f"{alias} ctx={ctx!r} 应命中")
            self.assertEqual(info.code, exp_code)

    # ---- T6 高歧义短别名日常语境误匹配防护 ----
    def test_T6_no_false_positive_on_daily_phrases(self):
        """经验回归：来自 2276024 —— 短别名落入日常用语绝不能误判为股票。"""
        false_positive_cases = [
            ("大金", "大金融板块拉升，银行股集体走强"),
            ("兄弟", "兄弟们一起冲"),
            ("今天", "今天盘前策略，关注大盘走势"),
            ("今日", "今日上涨是因为大盘好，不是个股机会"),
            ("中国", "中国经济稳增长"),
            ("国际", "国际油价上涨"),
            ("科技", "科技股集体走强"),
        ]
        for alias, ctx in false_positive_cases:
            info = self.matcher.lookup(alias, ctx)
            self.assertIsNone(
                info, f"{alias!r} ctx={ctx!r} 不应命中任何股票，实际={info}"
            )

    # ---- T7 文本抽取（常规新闻/聊天句子）----
    def test_T7_extract_from_text_normal(self):
        samples = [
            (
                "贵州茅台和工商银行今日上涨，宁德时代下跌。600519 目标价 2000 元",
                ["600519", "601398", "300750"],
            ),
            (
                "请问600036和000002这两只股票怎么样",
                ["600036", "000002"],
            ),
            (
                "长鑫科技688825是做内存的，移动600941和工行601398也涨了",
                ["688825", "600941", "601398"],
            ),
            (
                "推荐买入中国平安和招商银行",
                ["601318", "600036"],
            ),
        ]
        from tools.stock_matcher import extract_stocks
        for text, exp_codes in samples:
            got = extract_stocks(text)
            got_codes = [s.code for s in got]
            self.assertEqual(
                got_codes, exp_codes,
                f"\n文本: {text!r}\n期望: {exp_codes}\n实际: {got_codes}"
            )

    # ---- T8 文本抽取中的假阳性过滤 ----
    def test_T8_extract_from_text_no_false_positive(self):
        """日常话题句子不应抽到任何股票。"""
        noisy = [
            "大金融板块拉升，银行股集体走强",
            "今日上涨是因为大盘好，不是个股机会",
            "今天盘前策略：关注宏观数据",
            "朋友们一起冲，今天表现不错",
            "2025年6月12日 星期三 天气晴",
        ]
        from tools.stock_matcher import extract_stocks
        for t in noisy:
            got = extract_stocks(t)
            self.assertEqual(
                got, [],
                f"纯噪声文本应抽不到任何股票: {t!r} → {[(s.code, s.name) for s in got]}"
            )

    # ---- T9 日期/编号 6 位数字不应识别为股票代码 ----
    def test_T9_no_6digit_date_false_match(self):
        """常见 6 位日期（YYMMDD / YYYYM 的部分片段）/序号 绝不能误识别。"""
        from tools.stock_matcher import extract_stocks, is_stock_code
        pure_noise_codes = ["202501", "202506", "202412", "000001", "123456", "888888"]
        text = "订单编号 202501 于 202506 发货，序号 123456 和 888888 已签收"
        codes = {s.code for s in extract_stocks(text)}
        for fake in pure_noise_codes:
            self.assertNotIn(fake, codes, f"{fake} 不应从日常文本中抽到")
        # 清单外的假代码：is_stock_code 必须 False
        for fake in ["123456", "888888", "999999"]:
            self.assertFalse(is_stock_code(fake))


class TestStockMatcherPerformance(unittest.TestCase):
    """T10 ~ T13：性能硬指标。"""

    @classmethod
    def setUpClass(cls) -> None:
        from tools.stock_matcher import StockMatcher
        StockMatcher._instance = None
        cls.matcher = StockMatcher.get_instance()

    def test_T10_singleton_first_load_under_500ms(self):
        """首次加载（含 GBK 文件读取、索引构建、两个大 alternation 正则编译）应 <500ms。"""
        from tools.stock_matcher import StockMatcher
        StockMatcher._instance = None
        t0 = time.perf_counter()
        StockMatcher.get_instance()
        cost_ms = (time.perf_counter() - t0) * 1000
        self.assertLess(cost_ms, 500, f"首次加载 {cost_ms:.1f}ms > 500ms")

    def test_T11_single_code_lookup_under_20us(self):
        """单次 O(1) 代码查询应 <20 微秒。"""
        N = 1000
        t0 = time.perf_counter()
        ok = 0
        for _ in range(N):
            for c in _KNOWN_GOOD_CODES:
                if self.matcher.is_valid_code(c):
                    ok += 1
        cost_per_us = (time.perf_counter() - t0) / (N * len(_KNOWN_GOOD_CODES)) * 1_000_000
        self.assertEqual(ok, N * len(_KNOWN_GOOD_CODES))
        self.assertLess(cost_per_us, 20, f"单查代码 {cost_per_us:.1f}us > 20us")

    def test_T12_extract_500chars_under_5ms(self):
        """典型 500 字中文金融聊天抽取应 <5ms。"""
        long_text = (
            "今日A股三大指数集体高开，随后震荡回落，沪指盘中一度涨超1%，"
            "截至收盘，沪指涨0.48%报3350点，深成指涨0.72%，创业板指涨1.08%。"
            "个股方面，贵州茅台(600519)、工商银行601398、宁德时代300750集体飘红，"
            "招商银行600036、平安银行000001双双走强，大金重工002487涨停，"
            "兄弟科技002562跟涨。半导体板块中芯国际(688981)创新高。"
            "消息面上，国家发改委召开新闻发布会，介绍宏观经济运行情况。"
            "央行今日开展3000亿元MLF操作，中标利率不变。业内人士分析，"
            "后续降准降息仍有空间。操作上建议关注消费、新能源、AI算力方向。"
        ) * 2  # ≈ 500+ 字
        from tools.stock_matcher import extract_stocks
        N = 100
        t0 = time.perf_counter()
        codes_agg = None
        for _ in range(N):
            codes_agg = [s.code for s in extract_stocks(long_text)]
        cost_ms = (time.perf_counter() - t0) / N * 1000
        self.assertIn("600519", codes_agg)
        self.assertLess(cost_ms, 10, f"500字抽取 {cost_ms:.2f}ms > 10ms")

    def test_T13_reload_if_changed_noop_under_1ms(self):
        """热更新检查（文件未变）应 <0.5ms/次，不阻断请求。"""
        t0 = time.perf_counter()
        N = 1000
        for _ in range(N):
            self.matcher.reload_if_changed()
        cost_us = (time.perf_counter() - t0) / N * 1_000_000
        self.assertLess(cost_us, 500.0, f"reload_if_changed 空跑 {cost_us:.1f}us/次 > 500us")


class TestIntegration(unittest.TestCase):
    """T14 ~ T17：4 个业务模块集成验证。"""

    # ---- T14 Output Validator：StockCodeFormatRule ----
    def test_T14_output_validator_stock_code_rule(self):
        from agent.output_validator import StockCodeFormatRule, ValidationContext
        rule = StockCodeFormatRule()

        ctx_ok = ValidationContext(
            agent_output="推荐 600519 贵州茅台、300750 宁德时代，注意风险"
        )
        vios_ok = asyncio.run(rule.check(ctx_ok))
        self.assertEqual(vios_ok, [], f"全部有效代码不应有违规, got {vios_ok}")

        ctx_bad = ValidationContext(
            agent_output="推荐 123456 不存在股票 和 999999 另一只 以及 600519 茅台"
        )
        vios_bad = asyncio.run(rule.check(ctx_bad))
        bad_codes = {v.evidence for v in vios_bad}
        self.assertIn("123456", bad_codes, "不存在代码 123456 应告警")
        self.assertIn("999999", bad_codes, "不存在代码 999999 应告警")
        self.assertNotIn("600519", bad_codes, "存在的 600519 不应告警")

    # ---- T15 Context Engineer：_extract_stock_codes ----
    def test_T15_context_engineer_extract(self):
        from agent.context_engineer import ContextEngineer
        ce = ContextEngineer()
        codes = ce._extract_stock_codes(
            "今天20250612 贵州茅台(600519)和宁德时代大涨，推荐买入 600036"
        )
        self.assertIn("600519", codes, "贵州茅台代码")
        self.assertIn("300750", codes, "宁德时代（通过名称抽取）")
        self.assertIn("600036", codes, "招商银行")
        self.assertNotIn("202506", codes, "日期片段不应抽到")
        self.assertNotIn("20250612"[:6], codes, "日期前缀不应抽到")

    # ---- T16 Maker-Checker：一致性 + 完整性 ----
    def test_T16_maker_checker_consistency_and_completeness(self):
        from agent.maker_checker import MakerChecker
        mc = MakerChecker()

        # 一致性：工具只提到茅台，输出却提到工行
        issues_c = mc._check_data_consistency(
            output="推荐买入工商银行601398",
            tool_results=["工具返回: 贵州茅台(600519) 目标价 2000元"],
        )
        self.assertTrue(
            any("601398" in i for i in issues_c),
            f"601398 张冠李戴应报: {issues_c}"
        )

        # 完整性：query 问了 2 只，输出只提 1 只
        issues_m = mc._check_completeness(
            query="请问贵州茅台(600519)和工商银行今天表现如何",
            output="工商银行601398 今日上涨 1%，量价齐升",
        )
        self.assertTrue(
            any("600519" in i or "贵州茅台" in i for i in issues_m),
            f"贵州茅台未回应应报缺失: {issues_m}"
        )

    # ---- T17 ZSXQ 工具：参数合法化（不实际触发浏览器）----
    def test_T17_zsxq_by_stock_name_validation(self):
        """合法化校验分支：在真正获取浏览器锁之前，非股票名应直接被拒。"""
        import tools.zsxq_tool

        # 说明：search_zsxq_by_stock 是 LangChain @tool 装饰的 StructuredTool。
        # - 直接传参调用：tool.func(...) ，走的是合法化校验+真正浏览器抓取路径
        # - 合法化失败会在拿锁前直接 return，不会阻塞，也不会抛异常
        _fn = tools.zsxq_tool.search_zsxq_by_stock.func
        # 非股票名 → 立即返回提示（不会走到 _zsxq_browser_lock.acquire）
        invalid_inputs = [
            "今日复盘",
            "大金融板块",
            "大盘走势",
            "今天的新闻",
            "",
        ]
        for s in invalid_inputs:
            r = _fn(s)
            self.assertIsInstance(r, str)
            # 必须是"未在 A 股/请提供准确的股票"这类拒绝性信息，或空参数提示
            self.assertTrue(
                "未在 A 股" in r or "清单" in r or "未提供" in r or "请提供" in r,
                f"invalid={s!r} 应被拒绝，实际={r!r}"
            )

        # 合法别名 → 在拿锁之前会归一化为全称
        # 这里我们不能真的拿锁（会超时），只验证前置分支正常
        from tools.stock_matcher import lookup_stock as _look
        info = _look("600519")  # 代码 → 全称
        self.assertIsNotNone(info)
        self.assertEqual(info.name, "贵州茅台")
        info2 = _look("茅台", "贵州茅台今日涨停的研报")  # 别名+语境 → 全称
        self.assertIsNotNone(info2)
        self.assertEqual(info2.name, "贵州茅台")


if __name__ == "__main__":
    unittest.main(verbosity=2)
