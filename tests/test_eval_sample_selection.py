# -*- coding: utf-8 -*-
"""tests.eval.run_eval 选样逻辑的离线单测（不触网、不调 LLM）。

守护 CI 评测门的正确性：direct 模式（无联网/工具）必须剔除依赖实时数据的样本，
否则裸模型对实时题只能诚实拒答，却被误判为质量回归（历史 CI 红灯根因）。
"""
from __future__ import annotations

import json
from pathlib import Path

from tests.eval.run_eval import _sample_requires_live_data, select_samples

_GOLDEN = json.loads(
    (Path(__file__).parent / "eval" / "golden_set.json").read_text(encoding="utf-8")
)
_BY_ID = {s["id"]: s for s in _GOLDEN}


def test_category_default_live_classification():
    # 定性/行为类默认可离线
    assert _sample_requires_live_data(_BY_ID["eval_003"]) is False  # moat
    assert _sample_requires_live_data(_BY_ID["eval_004"]) is False  # hallucination_guard
    assert _sample_requires_live_data(_BY_ID["eval_005"]) is False  # risk_disclaimer
    # 实时数据类默认需要联网
    assert _sample_requires_live_data(_BY_ID["eval_001"]) is True   # 新闻
    assert _sample_requires_live_data(_BY_ID["eval_002"]) is True   # 当前估值
    assert _sample_requires_live_data(_BY_ID["eval_006"]) is True   # 今日大盘
    assert _sample_requires_live_data(_BY_ID["eval_017"]) is True   # 行业当期数据


def test_explicit_flag_overrides_category():
    # eval_010 属 stock_compare（默认实时），但显式标 requires_live_data=false
    assert _sample_requires_live_data(_BY_ID["eval_010"]) is False
    # 显式 true 永远剔除，即便类别在离线白名单
    forced = {"id": "x", "category": "moat", "requires_live_data": True}
    assert _sample_requires_live_data(forced) is True


def test_direct_filters_before_limit():
    # 这是历史 bug：--limit 3 先截断，前三条恰是 001/002(实时)+003，导致结构性必挂
    chosen, skipped = select_samples(_GOLDEN, "direct", limit=3)
    chosen_ids = [s["id"] for s in chosen]
    assert chosen_ids == ["eval_003", "eval_004", "eval_005"]
    assert "eval_001" in skipped and "eval_002" in skipped and "eval_006" in skipped
    assert "eval_003" not in skipped


def test_direct_full_offline_subset():
    chosen, skipped = select_samples(_GOLDEN, "direct")
    chosen_ids = {s["id"] for s in chosen}
    # 当前离线可公平评测的 4 条：护城河、反幻觉、风险声明、定性对比
    assert chosen_ids == {"eval_003", "eval_004", "eval_005", "eval_010"}
    assert len(skipped) == len(_GOLDEN) - 4


def test_http_mode_keeps_all_samples():
    # 全系统联网模式不做实时数据剔除
    chosen, skipped = select_samples(_GOLDEN, "http")
    assert len(chosen) == len(_GOLDEN)
    assert skipped == []


def test_category_and_ids_filters():
    chosen, skipped = select_samples(_GOLDEN, "direct", category_filter="moat")
    assert [s["id"] for s in chosen] == ["eval_003"]
    chosen2, _ = select_samples(_GOLDEN, "http", ids_filter=["eval_001", "eval_003"])
    assert {s["id"] for s in chosen2} == {"eval_001", "eval_003"}
    # direct + 只选实时 ID → 空集（上层 main 必须据此判失败，不能空转通过）
    empty, skipped3 = select_samples(_GOLDEN, "direct", ids_filter=["eval_001"])
    assert empty == [] and skipped3 == ["eval_001"]
