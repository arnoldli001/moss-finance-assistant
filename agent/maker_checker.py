"""
Layer 4 - Loop Engineering: Maker-Checker 质量校验子智能体。

Maker（主智能体）生成回答，Checker（独立校验）在交付用户前审查，形成"制作-校验"分离：
1. 数据准确性：回答是否与工具结果矛盾（股票代码是否张冠李戴）
2. 完整性：是否覆盖用户查询的全部要点（询问的标的是否都被提及）
3. 风险免责：涉及买卖/估值/建议时是否附带风险提示
4. 来源归属：引用新闻/数据时是否标注来源
5. 幻觉检测：回答中的具体数字（百分比/价格/金额）是否可在工具结果中找到

默认走规则校验（确定性、零依赖）；传入 checker_model 时可通过
build_checker_prompt 进行 LLM 辅助校验。
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict, List, Optional, Tuple

# 运行时提示词模板访问器（校验员提示词抽取到 prompts.yml）
from agent.prompts import format_prompt


# ======================================================================
# 关键词模式
# ======================================================================
# 触发风险免责要求的关键词
_RISK_TRIGGER_PATTERNS = [
    r"买入", r"卖出", r"目标价", r"估值", r"建议", r"推荐",
    r"加仓", r"减仓", r"建仓", r"清仓", r"止盈", r"止损",
    r"看多", r"看空", r"评级",
]
_RISK_TRIGGER_REGEX = re.compile("|".join(_RISK_TRIGGER_PATTERNS))

# 风险免责声明关键词（出现任一即视为已声明）
_RISK_DISCLAIMER_PATTERNS = [
    r"风险", r"投资建议", r"盈亏自负", r"仅供参考",
    r"不构成投资建议", r"谨慎决策", r"风险自担",
]
_RISK_DISCLAIMER_REGEX = re.compile("|".join(_RISK_DISCLAIMER_PATTERNS))

# 6 位股票代码（前后均非数字，避免匹配到长数字串片段）
_STOCK_CODE_REGEX = re.compile(r"(?<!\d)\d{6}(?!\d)")

# 来源引用关键词
_SOURCE_ATTR_PATTERNS = [
    r"来源[：:]", r"来源[于自]", r"据.*报道", r"据.*数据",
    r"根据", r"引自", r"参考", r"消息源", r"Source",
    r"知识星球", r"zsxq",
]
_SOURCE_ATTR_REGEX = re.compile("|".join(_SOURCE_ATTR_PATTERNS), re.IGNORECASE)

# 新闻/数据引用触发词
_NEWS_DATA_TRIGGER_PATTERNS = [
    r"新闻", r"消息", r"报道", r"资讯", r"公告", r"数据显示",
    r"财报", r"业绩",
]
_NEWS_DATA_TRIGGER_REGEX = re.compile("|".join(_NEWS_DATA_TRIGGER_PATTERNS))

# 具体数字提取：百分比、价格、金额、财务比率
_PERCENT_REGEX = re.compile(r"-?\d+(?:\.\d+)?\s*%")
_PRICE_REGEX = re.compile(r"-?\d+(?:\.\d+)?\s*(?:元|块钱|美元|港元|US\$|HK\$|\$)")
_AMOUNT_REGEX = re.compile(r"-?\d+(?:\.\d+)?\s*(?:亿|万|千万|百万|万亿|亿元|万元)")
_RATIO_REGEX = re.compile(r"-?\d+(?:\.\d+)?\s*(?:倍|PE|PB|ROE|EPS|倍数)")


class MakerChecker:
    """
    Maker-Checker 质量校验器。

    使用方式：
        checker = get_maker_checker()
        is_valid, issues = await checker.check_output(query, output, tool_results)
        if not is_valid:
            # 退回 Maker 重写或补充免责声明
    """

    def __init__(self, checker_model: Any = None):
        """
        Args:
            checker_model: 可选的独立校验模型（LLM）。传入时可用于 build_checker_prompt
                           进行 LLM 辅助校验；为 None 时仅使用规则校验。
        """
        self.checker_model = checker_model

    async def check_output(self, user_query: str, agent_output: str,
                           tool_results: Optional[List[str]] = None) -> Tuple[bool, str]:
        """
        校验主智能体输出，返回 (是否通过, 问题描述)。

        校验维度：
        1. 数据准确性：输出是否与工具结果矛盾
        2. 完整性：是否覆盖用户查询的全部要点
        3. 风险免责：涉及买卖/估值时是否附带风险提示
        4. 来源归属：引用新闻/数据时是否标注来源
        5. 幻觉检测：输出中的具体数字是否可在工具结果中找到

        tool_results 为空时跳过数据一致性 / 幻觉检测。
        """
        tool_results = tool_results or []
        issues: List[str] = []

        # 1. 数据准确性
        for item in self._check_data_consistency(agent_output, tool_results):
            issues.append(f"[数据准确性] {item}")

        # 2. 完整性
        for item in self._check_completeness(user_query, agent_output):
            issues.append(f"[完整性] {item}")

        # 3. 风险免责
        if not self._check_risk_disclaimer(agent_output):
            issues.append("[风险免责] 输出涉及买卖/估值/建议等，但缺少风险免责声明"
                          "（风险/投资建议/盈亏自负/仅供参考）")

        # 4. 来源归属
        for item in self._check_source_attribution(agent_output):
            issues.append(f"[来源归属] {item}")

        # 5. 幻觉检测
        for item in self._detect_hallucination(agent_output, tool_results):
            issues.append(f"[幻觉检测] {item}")

        is_valid = len(issues) == 0
        return is_valid, "\n".join(issues)

    # ------------------------------------------------------------------
    # 风险免责检查
    # ------------------------------------------------------------------
    def _check_risk_disclaimer(self, output: str) -> bool:
        """
        输出提及 买入/卖出/目标价/估值/建议/推荐 等时，
        检查是否包含风险免责声明（风险/投资建议/盈亏自负/仅供参考）。
        无需免责时返回 True。
        """
        if not output:
            return True
        if not _RISK_TRIGGER_REGEX.search(output):
            return True
        return bool(_RISK_DISCLAIMER_REGEX.search(output))

    # ------------------------------------------------------------------
    # 数据一致性检查
    # ------------------------------------------------------------------
    def _check_data_consistency(self, output: str, tool_results: List[str]) -> List[str]:
        """
        从工具结果中提取 6 位股票代码，校验输出中出现的代码是否与工具结果一致。
        输出中出现工具结果未提及的代码 → 可能编造或张冠李戴。
        返回不一致项列表。
        """
        issues: List[str] = []
        if not output or not tool_results:
            return issues

        tool_text = "\n".join(tool_results)
        tool_codes = set(_STOCK_CODE_REGEX.findall(tool_text))
        output_codes = set(_STOCK_CODE_REGEX.findall(output))

        # 仅当工具结果中存在至少一个代码时才报告，避免对纯日期数字误报
        if tool_codes:
            unsupported = output_codes - tool_codes
            for code in sorted(unsupported):
                issues.append(f"输出中的股票代码 {code} 未在工具结果中找到，可能存在张冠李戴")

        return issues

    # ------------------------------------------------------------------
    # 完整性检查
    # ------------------------------------------------------------------
    def _check_completeness(self, query: str, output: str) -> List[str]:
        """
        从用户查询中提取股票名称/代码，检查输出是否对每一项作出回应。
        返回缺失项列表。
        """
        issues: List[str] = []
        if not query or not output:
            return issues

        # 提取查询中的 6 位股票代码
        query_codes = set(_STOCK_CODE_REGEX.findall(query))
        for code in sorted(query_codes):
            if code not in output:
                issues.append(f"用户询问的股票代码 {code} 未在输出中提及")

        # 提取查询中「《》」/ 引号 / 「」 / <> 包裹的专有名词（如股票/公司名）
        quoted = re.findall(r"[《<「\"'](.+?)[》>」\"']", query)
        for name in quoted:
            if name and name not in output:
                issues.append(f"用户询问的对象「{name}」未在输出中提及")

        return issues

    # ------------------------------------------------------------------
    # 幻觉检测
    # ------------------------------------------------------------------
    def _detect_hallucination(self, output: str, tool_results: List[str]) -> List[str]:
        """
        从输出中提取具体数字（百分比、价格、金额、财务比率），
        检查是否可在工具结果中找到。返回疑似幻觉的数字列表。
        """
        issues: List[str] = []
        if not output or not tool_results:
            return issues

        tool_text = "\n".join(tool_results)
        tool_norm = re.sub(r"\s+", "", tool_text)

        # 汇总输出中的所有具体数字
        output_numbers: List[str] = []
        for regex in (_PERCENT_REGEX, _PRICE_REGEX, _AMOUNT_REGEX, _RATIO_REGEX):
            output_numbers.extend(regex.findall(output))

        # 去重并规范化（去除空白）
        seen = set()
        for num in output_numbers:
            norm = re.sub(r"\s+", "", num)
            if norm in seen:
                continue
            seen.add(norm)
            if norm not in tool_norm:
                issues.append(f"输出中的数字「{num}」未在工具结果中找到，疑似幻觉")

        return issues

    # ------------------------------------------------------------------
    # 来源归属检查
    # ------------------------------------------------------------------
    def _check_source_attribution(self, output: str) -> List[str]:
        """输出提及新闻/数据时，检查是否标注来源。返回问题列表。"""
        issues: List[str] = []
        if not output:
            return issues
        if _NEWS_DATA_TRIGGER_REGEX.search(output) and not _SOURCE_ATTR_REGEX.search(output):
            issues.append("输出引用了新闻/数据，但未标注来源")
        return issues

    # ------------------------------------------------------------------
    # 构建 LLM 校验提示词
    # ------------------------------------------------------------------
    def build_checker_prompt(self, user_query: str, agent_output: str,
                             tool_results: List[str]) -> str:
        """
        为独立校验 LLM 构建提示词（当 checker_model 可用时使用）。
        提示 LLM 按 JSON 返回 is_valid 与 issues。
        """
        tool_text = "\n---\n".join(tool_results) if tool_results else "（无工具结果）"
        # 校验员提示词从 prompts.yml runtime_prompts 段加载，动态填入查询/工具结果/输出
        return format_prompt(
            "maker_checker.checker_prompt",
            user_query=user_query,
            tool_text=tool_text,
            agent_output=agent_output,
        )


# ======================================================================
# 全局单例
# ======================================================================
_checker: Optional[MakerChecker] = None


def get_maker_checker() -> MakerChecker:
    global _checker
    if _checker is None:
        _checker = MakerChecker()
    return _checker
