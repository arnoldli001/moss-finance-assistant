# -*- coding: utf-8 -*-
"""2026-09-06 修复回归集：
1. 复盘预测超时预算常量自洽（阶段2 并行段 + 阶段3 ≤ 后台总预算）
2. analysis_prompt 模板保留 {zsxq_result}/{news_result} 插槽（防格式化缺参）
3. SkillManager 注入：frontmatter 剥离 + 单/总体积硬上限（防 skill 原文复读到前端）
4. trading-reliability 触发词收紧回归：宽泛单词（如"交易"）不再单字触发
5. zsxq_tool.py 源码脱敏回归：token/群组ID 不得硬编码（防 IDE 旧缓冲回滚）
"""
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ======================================================================
# 1) 复盘预测预算自洽
# ======================================================================
def test_review_prediction_budget_constants():
    from config.constants import (
        REVIEW_PREDICTION_BG_TIMEOUT_SEC,
        REVIEW_STAGE3_DEEPSEEK_TIMEOUT_SEC,
    )
    # 阶段3 实测 120.5s → 150s 预算
    assert 145 <= REVIEW_STAGE3_DEEPSEEK_TIMEOUT_SEC <= 200
    # 阶段2 280（并行段最坏）+ 阶段3 150 = 430，后台预算必须留余量
    assert REVIEW_PREDICTION_BG_TIMEOUT_SEC >= 430 + 10, (
        f"后台预算 {REVIEW_PREDICTION_BG_TIMEOUT_SEC}s 不足以覆盖最坏链路 430s")


def test_server_stage2_timeout_literal():
    """server.py 阶段2 上限须 ≥ 双源120s+直连综答120s+路由裕量。"""
    src = (PROJECT_ROOT / "interfaces" / "api" / "server.py").read_text(encoding="utf-8")
    assert "_news_timeout_sec = 280.0" in src, "阶段2 超时应为 280s（复用按钮工作流预算）"
    assert "run_analysis_workflow(" in src, "阶段2 应复用盘前新闻按钮工作流"


# ======================================================================
# 2) analysis_prompt 模板插槽完整性
# ======================================================================
def test_analysis_prompt_template_slots():
    tpl_doc = yaml.safe_load(
        (PROJECT_ROOT / "prompt" / "prompts.yml").read_text(encoding="utf-8"))
    # 真源结构：runtime_prompts 下扁平点号键
    tpl = tpl_doc["runtime_prompts"]["server.review_prediction.analysis_prompt"]
    assert "{zsxq_result}" in tpl, "模板缺少小作文插槽 {zsxq_result}"
    assert "{news_result}" in tpl, "模板缺少盘前新闻插槽 {news_result}"
    assert "风险" in tpl, "预测模板必须包含风险声明要求"


# ======================================================================
# 3) SkillManager 注入安全
# ======================================================================
def _manager():
    from agent.skill_manager import get_skill_manager
    return get_skill_manager()


def test_skill_prefix_strips_frontmatter_and_caps():
    m = _manager()
    prefix = m.build_skill_prefix("交易系统故障处理和下单重试机制")
    assert prefix, "复合触发词应命中 trading-reliability"
    assert "trigger-keywords" not in prefix, "frontmatter 元数据不得注入"
    assert "allowed-tools" not in prefix, "frontmatter 元数据不得注入"
    # 总量硬上限 8000 + 少量包装头尾
    assert len(prefix) <= m._MAX_TOTAL_CHARS + 400


def test_skill_no_overbroad_single_word_trigger():
    m = _manager()
    # "交易"/"策略"/"复盘" 等宽泛单词命中曾导致 17KB 规范注入无关查询
    for broad in ("交易", "策略", "复盘", "报错", "超时", "监控"):
        hits = [sd.name for sd in m.match_skills(broad)]
        assert "trading-reliability" not in hits, f"宽泛词「{broad}」不应触发 trading-reliability"


def test_trading_skill_keywords_are_compound():
    from agent.skill_manager import _split_frontmatter
    md = (PROJECT_ROOT / "skills" / "trading-reliability" / "SKILL.md").read_text(
        encoding="utf-8")
    fm, _ = _split_frontmatter(md)
    kws = [k.strip().lower() for k in (fm.get("trigger-keywords") or [])]
    assert kws, "trading-reliability 必须声明 trigger-keywords"
    assert "交易系统" in kws, "复合触发词「交易系统」应保留"
    assert "交易" not in kws, "宽泛单词「交易」应已从触发词移除"


# ======================================================================
# 4) zsxq_tool.py 源码脱敏回归
# ======================================================================
def test_zsxq_tool_source_has_no_hardcoded_secrets():
    # 拆分字面量：本测试文件入库后自身不得包含真实群组ID
    _leaked_group_id = "4884848" "4411448"
    src = (PROJECT_ROOT / "tools" / "zsxq_tool.py").read_text(encoding="utf-8")
    assert "FACF01DE" not in src, "真实 access token 不得硬编码（已泄露需轮换）"
    assert _leaked_group_id not in src, "真实群组ID 不得硬编码"
    assert 'os.environ.get("ZSXQ_ACCESS_TOKEN", "")' in src
    assert 'os.environ.get("ZSXQ_GROUP_ID", "")' in src
