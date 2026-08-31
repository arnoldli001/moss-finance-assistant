"""tests/test_router_smoke.py — Router Agent 规则阶段冒烟（纯 decide()，不调本地 LLM，秒级）。
运行：
    pytest tests/test_router_smoke.py -v
    python -m pytest tests/

覆盖 RouteBranch 7 个显式分支（通过 11 条用例 × 方案 A-a 对齐）：
    PRE_MARKET_NEWS / PRESET_SHORTCUT_OTHER / CODE_GENERATION / IMPACT_ANALYSIS
    STOCK_QUERY(代码+别名) / GENERAL_QUERY(通用+兜底)
注：VISION 分支由 has_visual_input=True（图像附件/URL）触发；
    当 query 只带"图/K线图"等文字但附带金融走势研判语义时，
    依据路由优先级 IMPACT > GENERAL 命中 IMPACT_ANALYSIS（见 R11）。
"""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import shared.compat_bootstrap  # noqa: F401  兼容层优先加载

import pytest
from agents.router.agent import decide
from shared.models import RouteBranch


# ------------------------------------------------------------------
# 11 条固化冒烟用例（稳定 ID：R1 ~ R11）
# ------------------------------------------------------------------
# 元组结构：(case_id, query, expect_branch, extra_field_assertions_dict)
# extra_field_assertions_dict：除 branch 之外的字段断言（字段 == 值），
#   或字段 -> callable(value)->bool（用于 "600519" in codes 这类断言）
ROUTER_SMOKE_CASES = [
    # --- R1 / R2：快捷按钮分支 ---
    pytest.param(
        "R1", "盘前新闻",
        RouteBranch.PRE_MARKET_NEWS,
        {"decided_by": "rule_based", "confidence": 1.0},
        id="R1_premarket_news_shortcut",
    ),
    pytest.param(
        "R2", "复盘预测",
        RouteBranch.PRESET_SHORTCUT_OTHER,
        {"decided_by": "rule_based", "confidence": 1.0},
        id="R2_review_forecast_shortcut",
    ),

    # --- R3 / R4：CODE 关键词 ---
    pytest.param(
        "R3", "写个Python脚本爬取茅台股价",
        RouteBranch.CODE_GENERATION,
        {"has_code_keywords": True},
        id="R3_code_write_script_keyword",
    ),
    pytest.param(
        "R4", "帮我写个爬虫抓取东方财富沪深300历史数据",
        RouteBranch.CODE_GENERATION,
        {"has_code_keywords": True},
        id="R4_code_crawler_keyword",
    ),

    # --- R5 / R6：IMPACT（影响/多空/风险/走势）---
    pytest.param(
        "R5", "分析美联储加息对A股影响",
        RouteBranch.IMPACT_ANALYSIS,
        {"has_analysis_keywords": True},
        id="R5_impact_fed_rate_hike",
    ),
    pytest.param(
        "R6", "评估白酒板块近期下跌风险，利多还是利空？",
        RouteBranch.IMPACT_ANALYSIS,
        {"has_analysis_keywords": True},
        id="R6_impact_liquor_risk_long_short",
    ),

    # --- R7 / R8：STOCK（6位代码 / 中文别名）---
    pytest.param(
        "R7", "贵州茅台 600519 今日行情 PE PB 估值分析",
        RouteBranch.STOCK_QUERY,
        {"has_stock_keywords": True,
         "extracted_stock_codes": lambda v: "600519" in v},
        id="R7_stock_by_6digit_code_600519",
    ),
    pytest.param(
        "R8", "宁德时代 + 比亚迪对比分析护城河、ROE、行业对比",
        RouteBranch.STOCK_QUERY,
        {"has_stock_keywords": True,
         "extracted_stock_names": lambda v: any(("宁德时代" in n) for n in v) and any(("比亚迪" in n) for n in v)},
        id="R8_stock_by_chinese_alias_CATL_BYD",
    ),

    # --- R9 / R10：GENERAL 兜底 ---
    pytest.param(
        "R9", "今日有什么值得关注的消息？",
        RouteBranch.GENERAL_QUERY,
        {"has_stock_keywords": False, "has_code_keywords": False, "has_analysis_keywords": False},
        id="R9_general_today_news",
    ),
    pytest.param(
        "R10", "今天北京天气怎么样？",
        RouteBranch.GENERAL_QUERY,
        {"has_stock_keywords": False, "has_code_keywords": False, "has_analysis_keywords": False},
        id="R10_general_non_finance_weather_strong_fallback",
    ),

    # --- R11：VISION 关键词但附带 IMPACT 语义 → 路由优先级 IMPACT > GENERAL（方案 A-a）
    # 严格 VISION 分支需 has_visual_input=True（真实图像附件/URL/.png 后缀等），
    # 无附件仅"K线图走势研判"等文字属于金融研判问题，按 IMPACT 走 deepseek-r1 更合理。
    pytest.param(
        "R11", "请帮我看一下这张K线图后续走势研判（无附件）",
        RouteBranch.IMPACT_ANALYSIS,
        {"has_analysis_keywords": True, "has_visual_input": False},
        id="R11_vision_text_only_kline_then_impact_A_a",
    ),
]


@pytest.mark.parametrize("case_id,query,expect_branch,extras", ROUTER_SMOKE_CASES)
def test_router_rule_smoke(case_id, query, expect_branch, extras):
    """11 条 Router 规则冒烟：断言 branch + 附加字段。"""
    d = decide(query)
    # 1) branch 主断言
    assert d.branch == expect_branch, (
        f"[{case_id}] route branch mismatch: expect={expect_branch.value} got={d.branch.value}"
        f" | decided_by={d.decided_by} reason={d.reason!r}"
    )
    # 2) 附加字段断言（等值 / callable）
    for field, expected in (extras or {}).items():
        actual = getattr(d, field, None)
        if callable(expected):
            ok = bool(expected(actual))
            msg_extra = f"predicate({field}={actual!r}) returned False"
        else:
            ok = actual == expected
            msg_extra = f"expect {expected!r}, got {actual!r}"
        assert ok, f"[{case_id}] field {field!r} failed: {msg_extra}"
