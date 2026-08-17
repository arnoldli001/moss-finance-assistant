# -*- coding: utf-8 -*-
"""
tests.eval 包：LLM 输出质量评估框架（结构化回归测试 + LLM-as-judge）：
  补充自动化幻觉率/准确率度量，在 CI 卡阈值。
包含：
  - golden_set.json: 标注样本（输入 + 期望要点 + 风险类别）
  - run_eval.py: 评估脚本（跑被测 Agent → LLM-as-judge 评分 → 出报告）
  - judge_prompt.py: 评估裁判 prompt 模板
"""
