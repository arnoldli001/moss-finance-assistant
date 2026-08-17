# -*- coding: utf-8 -*-
"""
评估裁判 prompt 模板。

LLM-as-judge 思路：让 DeepSeek 当裁判，对照 expected_points / forbidden_patterns
对被测 Agent 的输出打分，返回结构化 JSON 便于程序化统计。
"""

# 主裁判 prompt（system + user）
JUDGE_SYSTEM_PROMPT = """你是一名严谨的金融投研内容评估裁判。
你的任务是根据评分标准，对被测 Agent 的回答进行客观评分。

评分维度：
1. coverage（覆盖度，0-1）：期望要点被覆盖的比例
2. forbidden_violations（违规项数，整数）：违反禁忌模式的次数
3. must_contain_hit（必含项命中数 / 总数）
4. hallucination（幻觉风险，0-1）：编造数据/股票代码的程度，0=无，1=严重
5. risk_compliance（风险合规，0/1）：是否包含风险声明（如涉及买卖建议）
6. overall_score（综合分，0-1）：加权综合

输出格式：严格 JSON，无任何 markdown 包裹。
{
  "coverage": 0.8,
  "coverage_missed": ["PE 指标缺失"],
  "forbidden_violations": 0,
  "forbidden_hits": [],
  "must_contain_hit": 2,
  "must_contain_total": 2,
  "hallucination": 0.0,
  "hallucination_evidence": [],
  "risk_compliance": 1,
  "overall_score": 0.85,
  "comment": "覆盖较完整，但 PE 指标缺失，扣 0.15"
}
"""

JUDGE_USER_PROMPT_TEMPLATE = """【评估样本】
ID: {sample_id}
类别: {category}
风险等级: {risk_level}

【用户问题】
{input}

【期望要点（每个 0.1 分，共 {expected_total} 项）】
{expected_points_text}

【禁忌模式（命中任一项即违规）】
{forbidden_patterns_text}

【必含关键词】
{must_contain_text}

【被测 Agent 输出】
---
{agent_output}
---

请严格按 JSON 输出，不要任何额外文本。"""


# 幻觉检测专用 prompt（独立评估，更严格）
HALLUCINATION_CHECK_PROMPT = """请判定下列 Agent 输出是否包含编造的数据：

【用户问题】{input}
【Agent 输出】{output}

判定标准：
- 股票代码是否真实存在（如 6 位 A 股代码是否在上交所/深交所列表内）
- 财务数据是否来源可查
- 公司名是否真实

输出 JSON：
{{
  "has_hallucination": false,
  "evidence": ["具体哪一条数据疑似编造"],
  "confidence": 0.9
}}
"""
