"""
Layer 3 - Harness Engineering: 模型幻觉防护（Hallucination Guard）。

本模块实现用户要求的"避免模型幻觉"三重防护：
    1. RAG + 引用追踪（强制标注信息来源，缺失来源的陈述视为可疑）
    2. LLM-as-Judge（轻量模型二次校验：独立模型审查 Maker 输出是否与工具结果一致）
    3. 输出验证管道（JSON Schema 结构校验 + 语义比对：关键数字 / 股票代码必须可在工具结果中找到）

设计原则：
    - 失败默认保守：无法判定时按"疑似幻觉"处理，附加警示而非直接交付。
    - 与 MakerChecker 协同：MakerChecker 负责通用规则校验（风险声明 / 完整性），
      本模块聚焦于幻觉特定防护，提供更细粒度的可追溯审计。
    - 可观测：每次校验产出 HallucinationReport，供 trace 与 SLO 监控记录。
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from agent.prompts import format_prompt


# ======================================================================
# 正则：引用标记 / 关键实体提取
# ======================================================================

# 引用来源标记（必须出现至少一次才视为"已标注来源"）
_SOURCE_MARK_PATTERNS = [
    re.compile(r"来源[：:]\s*\S", re.IGNORECASE),
    re.compile(r"引自[：:]\s*\S", re.IGNORECASE),
    re.compile(r"参考[：:]\s*\S", re.IGNORECASE),
    re.compile(r"消息源[：:]\s*\S", re.IGNORECASE),
    re.compile(r"根据.{0,20}(?:公告|报道|数据|财报|研报)", re.IGNORECASE),
    re.compile(r"据.{0,10}(?:报道|数据|公告)", re.IGNORECASE),
    re.compile(r"\[来源[:：]", re.IGNORECASE),
    re.compile(r"Source[：:]\s*\S", re.IGNORECASE),
]

# 具体数字提取（与 maker_checker 对齐，但本模块独立维护以便扩展）
_PERCENT_RE = re.compile(r"-?\d+(?:\.\d+)?\s*%")
_PRICE_RE = re.compile(r"-?\d+(?:\.\d+)?\s*(?:元|块钱|美元|港元|US\$|HK\$|\$)")
_AMOUNT_RE = re.compile(r"-?\d+(?:\.\d+)?\s*(?:亿|万|千万|百万|万亿|亿元|万元)")
_RATIO_RE = re.compile(r"-?\d+(?:\.\d+)?\s*(?:倍|PE|PB|ROE|EPS|倍数)")

# 股票代码：6 位数字（前后非数字）
_STOCK_CODE_RE = re.compile(r"(?<!\d)\d{6}(?!\d)")

# 触发"需要引用来源"的关键词
_NEEDS_SOURCE_RE = re.compile(
    r"新闻|消息|报道|资讯|公告|数据显示|财报|业绩|研报|"
    r"PE|PB|ROE|营收|净利|涨|跌|涨幅|跌幅|成交",
    re.IGNORECASE,
)

# 风险声明触发词
_RISK_TRIGGER_RE = re.compile(
    r"买入|卖出|目标价|估值|建议|推荐|加仓|减仓|建仓|清仓|止盈|止损|看多|看空|评级"
)


# ======================================================================
# 数据结构
# ======================================================================

@dataclass
class CitationGap:
    """缺少引用来源的陈述项。"""
    statement_snippet: str       # 陈述片段（前后文截取）
    evidence_type: str          # "number" / "stock_code" / "factual_claim"
    expected_source: str        # 期望的来源类型说明


@dataclass
class HallucinationReport:
    """幻觉防护报告。"""
    passed: bool                       # 是否通过（无幻觉嫌疑）
    confidence: float                 # 置信度 0.0 ~ 1.0
    citation_gaps: List[CitationGap] = field(default_factory=list)
    unverified_numbers: List[str] = field(default_factory=list)
    unverified_stock_codes: List[str] = field(default_factory=list)
    llm_judge_verdict: Optional[Dict[str, Any]] = None   # LLM-as-Judge 返回结果
    schema_valid: bool = True
    schema_errors: List[str] = field(default_factory=list)
    latency_sec: float = 0.0
    tiers_run: List[str] = field(default_factory=list)  # 已执行的校验层

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "confidence": round(self.confidence, 3),
            "citation_gaps": [
                {"snippet": g.statement_snippet[:120], "type": g.evidence_type,
                 "expected_source": g.expected_source}
                for g in self.citation_gaps
            ],
            "unverified_numbers": self.unverified_numbers[:10],
            "unverified_stock_codes": self.unverified_stock_codes[:10],
            "llm_judge_verdict": self.llm_judge_verdict,
            "schema_valid": self.schema_valid,
            "schema_errors": self.schema_errors[:5],
            "latency_sec": round(self.latency_sec, 3),
            "tiers_run": self.tiers_run,
        }

    def render_warning(self) -> str:
        """若未通过，渲染用户可见的警示文本（附加到输出末尾）。"""
        if self.passed:
            return ""
        lines = ["\n\n---", "⚠️ 幻觉防护提示（以下陈述未经工具结果完全验证，请谨慎参考）："]
        if self.unverified_numbers:
            lines.append("- 未在工具结果中找到的数字：" +
                          "、".join(self.unverified_numbers[:5]))
        if self.unverified_stock_codes:
            lines.append("- 未在工具结果中找到的股票代码：" +
                          "、".join(self.unverified_stock_codes[:5]))
        if self.citation_gaps:
            lines.append("- 缺少来源标注的关键陈述：")
            for g in self.citation_gaps[:3]:
                lines.append(f"  · 「{g.statement_snippet[:80]}…」"
                             f"（期望来源：{g.expected_source}）")
        if not self.schema_valid:
            lines.append(f"- 输出结构校验失败：{'; '.join(self.schema_errors[:2])}")
        if self.llm_judge_verdict and not self.llm_judge_verdict.get("is_valid", True):
            judge_issues = self.llm_judge_verdict.get("issues", [])
            if judge_issues:
                lines.append("- LLM-as-Judge 发现：" + "；".join(judge_issues[:2]))
        return "\n".join(lines)


# ======================================================================
# 核心防护器
# ======================================================================

class HallucinationGuard:
    """
    幻觉防护三重管道。

    使用方式：
        guard = get_hallucination_guard()
        report = await guard.verify(
            user_query=query,
            agent_output=output,
            tool_results=tool_texts,
            json_schema=schema,            # 可选
            judge_fn=llm_judge_callable,  # 可选
        )
        if not report.passed:
            output += report.render_warning()
    """

    def __init__(self, judge_model: Any = None):
        """
        Args:
            judge_model: 可选的独立轻量模型，用于 LLM-as-Judge 二次校验。
                         若为 None 则跳过该层（仅走规则与 Schema 校验）。
        """
        self.judge_model = judge_model

    async def verify(
        self,
        user_query: str,
        agent_output: str,
        tool_results: Optional[List[str]] = None,
        json_schema: Optional[Dict[str, Any]] = None,
        judge_fn: Optional[Callable[[str, str, str], Awaitable[str]]] = None,
    ) -> HallucinationReport:
        """
        执行三重校验，返回 HallucinationReport。

        三层（顺序执行，互不短路，便于完整审计）：
          1) RAG 引用追踪：检测陈述是否标注来源、关键数字/代码是否可在工具结果中找到
          2) JSON Schema 输出验证：若提供 schema，校验输出结构
          3) LLM-as-Judge：若提供 judge_fn 或 judge_model，独立模型二次审查
        """
        start = time.time()
        tool_results = tool_results or []
        report = HallucinationReport(passed=True, confidence=1.0)

        # ===== Tier 1: RAG + 引用追踪 =====
        report.tiers_run.append("rag_citation")
        self._check_rag_citation(agent_output, tool_results, report)

        # ===== Tier 2: JSON Schema 输出验证 =====
        if json_schema is not None:
            report.tiers_run.append("json_schema")
            self._check_json_schema(agent_output, json_schema, report)

        # ===== Tier 3: LLM-as-Judge =====
        if judge_fn is not None or self.judge_model is not None:
            report.tiers_run.append("llm_judge")
            try:
                await self._run_llm_judge(
                    user_query, agent_output, tool_results, judge_fn, report,
                )
            except Exception as e:
                # Judge 失败不致命，仅记录
                report.llm_judge_verdict = {
                    "is_valid": True,
                    "issues": [],
                    "error": f"judge_failed: {type(e).__name__}: {e}",
                }

        # 汇总结论：任一层失败则 passed=False
        report.passed = (
            not report.unverified_numbers
            and not report.unverified_stock_codes
            and not report.citation_gaps
            and report.schema_valid
            and (report.llm_judge_verdict is None
                 or report.llm_judge_verdict.get("is_valid", True))
        )
        # 置信度：按未通过项数量衰减
        total_issues = (
            len(report.unverified_numbers)
            + len(report.unverified_stock_codes)
            + len(report.citation_gaps)
            + (0 if report.schema_valid else 2)
            + (0 if (report.llm_judge_verdict is None
                     or report.llm_judge_verdict.get("is_valid", True)) else 2)
        )
        report.confidence = max(0.0, 1.0 - 0.15 * total_issues)
        report.latency_sec = time.time() - start
        return report

    # ------------------------------------------------------------------
    # Tier 1: RAG 引用追踪 + 关键数字 / 代码校验
    # ------------------------------------------------------------------
    def _check_rag_citation(
        self,
        output: str,
        tool_results: List[str],
        report: HallucinationReport,
    ) -> None:
        if not output:
            return

        tool_text = "\n".join(tool_results)
        tool_norm = re.sub(r"\s+", "", tool_text)

        # 1. 关键数字校验：输出中的具体数字必须可在工具结果中找到
        output_numbers: List[str] = []
        for regex in (_PERCENT_RE, _PRICE_RE, _AMOUNT_RE, _RATIO_RE):
            output_numbers.extend(regex.findall(output))
        seen = set()
        for num in output_numbers:
            norm = re.sub(r"\s+", "", num)
            if norm in seen:
                continue
            seen.add(norm)
            if tool_text and norm not in tool_norm:
                report.unverified_numbers.append(num)

        # 2. 股票代码校验：输出中的 6 位代码必须可在工具结果中找到
        if tool_text:
            tool_codes = set(_STOCK_CODE_RE.findall(tool_text))
            output_codes = set(_STOCK_CODE_RE.findall(output))
            if tool_codes:
                unsupported = output_codes - tool_codes
                report.unverified_stock_codes.extend(sorted(unsupported))

        # 3. 引用来源标注检查：涉及新闻/数据/财报时必须标注来源
        if _NEEDS_SOURCE_RE.search(output):
            has_source = any(p.search(output) for p in _SOURCE_MARK_PATTERNS)
            if not has_source:
                # 截取含触发词的句子片段
                snippets = self._extract_claim_snippets(output)
                for snip in snippets:
                    report.citation_gaps.append(CitationGap(
                        statement_snippet=snip,
                        evidence_type="factual_claim",
                        expected_source="官方公告/交易所/权威媒体/知识星球研报",
                    ))

    @staticmethod
    def _extract_claim_snippets(output: str) -> List[str]:
        """提取包含数字 / 财务术语的句子片段，用于 citation_gaps 报告。"""
        sentences = re.split(r"[。！？\n]", output)
        snippets: List[str] = []
        for s in sentences:
            s = s.strip()
            if not s:
                continue
            # 含数字或财务术语的陈述句
            if re.search(r"\d", s) and _NEEDS_SOURCE_RE.search(s):
                snippets.append(s[:120])
            if len(snippets) >= 3:
                break
        return snippets

    # ------------------------------------------------------------------
    # Tier 2: JSON Schema 输出验证
    # ------------------------------------------------------------------
    def _check_json_schema(
        self,
        output: str,
        schema: Dict[str, Any],
        report: HallucinationReport,
    ) -> None:
        """
        若输出包含 JSON 块（```json ... ```），校验是否符合 schema。
        采用轻量级字段存在性校验（不引入 jsonschema 依赖）。
        """
        # 提取首个 JSON 代码块
        json_match = re.search(r"```json\s*([\s\S]*?)```", output)
        if not json_match:
            # 也尝试整体解析
            candidate = output.strip()
            if not (candidate.startswith("{") or candidate.startswith("[")):
                report.schema_valid = True
                return
            json_str = candidate
        else:
            json_str = json_match.group(1).strip()

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            report.schema_valid = False
            report.schema_errors.append(f"JSON 解析失败: {e}")
            return

        # 轻量级字段存在性校验
        required = schema.get("required", [])
        if isinstance(data, dict):
            missing = [f for f in required if f not in data]
            if missing:
                report.schema_valid = False
                report.schema_errors.append(f"缺少必填字段: {missing}")
            # 类型校验（仅基础类型）
            properties = schema.get("properties", {})
            for field_name, field_schema in properties.items():
                if field_name not in data:
                    continue
                expected_type = field_schema.get("type")
                actual_value = data[field_name]
                if not self._check_type(actual_value, expected_type):
                    report.schema_valid = False
                    report.schema_errors.append(
                        f"字段 '{field_name}' 类型错误：期望 {expected_type}，"
                        f"实际 {type(actual_value).__name__}"
                    )
        elif isinstance(data, list) and isinstance(schema.get("items"), dict):
            item_schema = schema["items"]
            item_required = item_schema.get("required", [])
            for idx, item in enumerate(data[:5]):  # 只校验前5项
                if not isinstance(item, dict):
                    report.schema_valid = False
                    report.schema_errors.append(f"数组第 {idx} 项不是对象")
                    continue
                missing = [f for f in item_required if f not in item]
                if missing:
                    report.schema_valid = False
                    report.schema_errors.append(
                        f"数组第 {idx} 项缺少字段: {missing}"
                    )

    @staticmethod
    def _check_type(value: Any, expected_type: Optional[str]) -> bool:
        if expected_type is None:
            return True
        type_map = {
            "string": str, "integer": int, "number": (int, float),
            "boolean": bool, "array": list, "object": dict,
            "null": type(None),
        }
        py_type = type_map.get(expected_type)
        if py_type is None:
            return True  # 未知类型不校验
        # 注意：bool 是 int 的子类，需特殊处理
        if expected_type == "integer" and isinstance(value, bool):
            return False
        if expected_type == "boolean":
            return isinstance(value, bool)
        return isinstance(value, py_type)

    # ------------------------------------------------------------------
    # Tier 3: LLM-as-Judge 二次校验
    # ------------------------------------------------------------------
    async def _run_llm_judge(
        self,
        user_query: str,
        agent_output: str,
        tool_results: List[str],
        judge_fn: Optional[Callable[[str, str, str], Awaitable[str]]],
        report: HallucinationReport,
    ) -> None:
        """
        调用独立的轻量模型审查 Maker 输出。
        judge_fn(query, output, tool_text) -> str(JSON)
        若未提供 judge_fn 但有 judge_model，则用 build_judge_prompt 构造提示词。
        """
        tool_text = "\n---\n".join(tool_results) if tool_results else "（无工具结果）"
        if judge_fn is not None:
            raw = await judge_fn(user_query, agent_output, tool_text)
        else:
            prompt = format_prompt(
                "hallucination_guard.judge_prompt",
                user_query=user_query,
                agent_output=agent_output,
                tool_text=tool_text,
            )
            resp = await self.judge_model.ainvoke(prompt)  # type: ignore[union-attr]
            raw = getattr(resp, "content", str(resp))

        # 解析 JSON 返回
        try:
            # 容忍模型前后多余文本
            json_match = re.search(r"\{[\s\S]*\}", raw)
            verdict = json.loads(json_match.group(0)) if json_match else {}
        except json.JSONDecodeError:
            verdict = {"is_valid": True, "issues": [],
                       "error": "judge_response_not_json"}
        report.llm_judge_verdict = verdict


# ======================================================================
# 全局单例
# ======================================================================

_guard: Optional[HallucinationGuard] = None


def get_hallucination_guard() -> HallucinationGuard:
    global _guard
    if _guard is None:
        _guard = HallucinationGuard()
    return _guard


def reset_hallucination_guard() -> None:
    """测试用：重置全局单例。"""
    global _guard
    _guard = None
