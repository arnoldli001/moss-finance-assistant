# -*- coding: utf-8 -*-
"""
输出 schema 校验器 + 自动拦截：
AGENTS.md 规定"必须输出风险声明"靠prompt约束，无 output validator自动拦截，违规输出可能直接到用户。

设计思路：
  1) 一组独立的校验规则，每条规则返回 Violation 列表
  2) OutputValidator 聚合所有规则，返回 ValidationResult
  3) block 严重度违规 → 拦截，可选自动重试（带修正提示）
  4) warn 严重度违规 → 告警但放行（适合非致命问题）
  5) 所有违规落盘 JSONL，便于审计

典型用法：
    from agent.output_validator import get_output_validator, ValidationContext

    validator = get_output_validator()

    # 校验
    ctx = ValidationContext(
        user_input="茅台能买吗？目标价多少？",
        agent_output="建议买入茅台，目标价 2000 元...",
        category="risk_disclaimer",
    )
    result = await validator.validate(ctx)

    if result.is_blocked:
        # 自动重试
        if validator.can_retry():
            retry_prompt = validator.build_retry_prompt(result.violations)
            agent_output = await call_llm(prompt + retry_prompt)
        else:
            agent_output = "抱歉，本次回答违反了内容规范，请重新提问。"

    elif result.has_warnings:
        logger.warning("[output] 校验告警: %s", result.violations)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from config.constants import (
    OUTPUT_VALIDATOR_AUTO_RETRY,
    OUTPUT_VALIDATOR_MAX_RETRIES,
    OUTPUT_VALIDATOR_RETRY_HINT,
    OUTPUT_VALIDATOR_RISK_KEYWORDS,
    OUTPUT_VALIDATOR_RISK_DISCLAIMER,
    OUTPUT_VALIDATOR_VIOLATION_SEVERITY,
    OUTPUT_VALIDATOR_LOG_PATH,
)
from tools.stock_matcher import is_stock_code as _matcher_is_valid_code

logger = logging.getLogger(__name__)


# ======================================================================
# 数据结构
# ======================================================================

@dataclass
class Violation:
    """单条校验违规。"""
    rule_name: str              # 规则名（如 risk_disclaimer_missing）
    severity: str               # block / warn
    message: str                # 违规描述
    evidence: str = ""          # 违规证据（如命中的关键词）
    fix_hint: str = ""          # 修正建议


@dataclass
class ValidationContext:
    """校验上下文。"""
    user_input: str = ""
    agent_output: str = ""
    category: str = ""          # 输出类别（news_summary / valuation / moat / 等）
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """校验结果。"""
    is_clean: bool = True       # 完全无违规
    has_warnings: bool = False
    is_blocked: bool = False    # 是否拦截
    violations: List[Violation] = field(default_factory=list)
    retry_count: int = 0        # 已重试次数

    @property
    def block_violations(self) -> List[Violation]:
        return [v for v in self.violations if v.severity == "block"]

    @property
    def warn_violations(self) -> List[Violation]:
        return [v for v in self.violations if v.severity == "warn"]

    def __bool__(self) -> bool:
        """True = 可放行，False = 被拦截。"""
        return not self.is_blocked


# ======================================================================
# 校验规则基类
# ======================================================================

class ValidationRule(ABC):
    """校验规则抽象基类。"""
    name: str = "base_rule"
    default_severity: str = "warn"

    @abstractmethod
    async def check(self, ctx: ValidationContext) -> List[Violation]:
        """执行校验，返回违规列表。"""


# ======================================================================
# 内置规则
# ======================================================================

class RiskDisclaimerRule(ValidationRule):
    """风险声明校验：涉及买卖建议必须附带风险声明。

    对应 AGENTS.md：涉及买卖价格/目标价/评级建议时，必须输出风险声明。
    """
    name = "risk_disclaimer"

    async def check(self, ctx: ValidationContext) -> List[Violation]:
        violations: List[Violation] = []
        output = ctx.agent_output or ""

        # 1) 检查是否包含买卖建议关键词
        hit_keywords = [
            kw for kw in OUTPUT_VALIDATOR_RISK_KEYWORDS if kw in output
        ]
        if not hit_keywords:
            return violations  # 无买卖建议，不需要风险声明

        # 2) 检查是否包含风险声明
        disclaimer_keywords = ["仅供参考", "不构成投资建议", "投资有风险"]
        has_disclaimer = any(kw in output for kw in disclaimer_keywords)

        if not has_disclaimer:
            violations.append(Violation(
                rule_name=self.name,
                severity=OUTPUT_VALIDATOR_VIOLATION_SEVERITY,
                message=(
                    f"输出包含买卖建议关键词 {hit_keywords}，但未附带风险声明。"
                    f"必须输出：{OUTPUT_VALIDATOR_RISK_DISCLAIMER}"
                ),
                evidence=",".join(hit_keywords),
                fix_hint=f"请在输出末尾添加：{OUTPUT_VALIDATOR_RISK_DISCLAIMER}",
            ))
        return violations


class StockCodeFormatRule(ValidationRule):
    """股票代码格式 + 存在性校验：防止编造不存在的代码。

    使用 StockMatcher 清单（data/stock_list.txt）验证：
      * 6 位数字代码不在清单中 → 报 warn，提示可能不存在。
      * 彻底屏蔽 LLM 瞎编的格式合规但不存在的代码（AGENTS.md 数据准确性规则）。
    """
    name = "stock_code_format"
    default_severity = "block"

    # 回退：用正则兜底抽取 6 位 A 股代码（避免 StockMatcher 的前瞻后顾边界漏掉少数情况）
    _A_STOCK_CODE_FALLBACK = re.compile(r"(?<!\d)(\d{6})(?!\d)")

    async def check(self, ctx: ValidationContext) -> List[Violation]:
        violations: List[Violation] = []
        output = ctx.agent_output or ""
        if not output:
            return violations

        seen_code = set()
        for m in self._A_STOCK_CODE_FALLBACK.finditer(output):
            code = m.group(1)
            if code in seen_code:
                continue
            seen_code.add(code)
            # 集成 StockMatcher：真正查询清单，而不止是正则格式判断
            if not _matcher_is_valid_code(code):
                violations.append(Violation(
                    rule_name=self.name,
                    severity="warn",
                    message=f"股票代码 {code} 未在官方股票清单中找到，可能是编造/不存在的代码",
                    evidence=code,
                    fix_hint="所有股票代码必须来自检索结果，未检索到的不要写入；请核实后修正。",
                ))
        return violations


class HallucinationGuardRule(ValidationRule):
    """幻觉防护：检查"未找到"场景下是否编造数据。

    对应 AGENTS.md：检索不到就明确说"未找到相关数据"，不能瞎猜。
    """
    name = "hallucination_guard"
    default_severity = "block"

    # 用户输入包含"不存在"或冷门公司名时触发
    _NON_EXISTENT_HINTS = ["不存在", "不知名", "没听过", "找不到"]

    # 数字模式
    _NUM_PATTERN = re.compile(r"\d+(?:\.\d+)?\s*(?:亿|万|元|%|%)")

    async def check(self, ctx: ValidationContext) -> List[Violation]:
        violations: List[Violation] = []
        user_input = ctx.user_input or ""
        output = ctx.agent_output or ""

        # 判定是否是"幻觉防护"场景
        is_guard_scenario = any(h in user_input for h in self._NON_EXISTENT_HINTS)
        if not is_guard_scenario:
            return violations

        # 在幻觉防护场景下，检查是否出现了具体数字（疑似编造）
        nums = self._NUM_PATTERN.findall(output)
        has_not_found = "未找到" in output or "未检索到" in output or "无相关数据" in output

        if nums and not has_not_found:
            violations.append(Violation(
                rule_name=self.name,
                severity="block",
                message=(
                    f"用户询问不存在的实体，但输出包含具体数字 {nums[:3]}，"
                    f"疑似编造数据。必须明确说'未找到相关数据'。"
                ),
                evidence=",".join(nums[:3]),
                fix_hint="请把输出改为：未找到相关数据",
            ))
        return violations


class LengthLimitRule(ValidationRule):
    """长度限制校验：个股新闻汇总 ≤ 200 字等。"""
    name = "length_limit"

    # 各类别长度上限
    _LENGTH_LIMITS = {
        "news_summary": 200,       # 个股新闻速览 ≤ 200 字
        "valuation": 3000,
        "moat": 3000,
    }

    async def check(self, ctx: ValidationContext) -> List[Violation]:
        violations: List[Violation] = []
        if not ctx.category:
            return violations
        limit = self._LENGTH_LIMITS.get(ctx.category)
        if not limit:
            return violations
        actual_len = len(ctx.agent_output or "")
        if actual_len > limit * 1.2:  # 允许 20% 溢出
            violations.append(Violation(
                rule_name=self.name,
                severity="warn",
                message=(
                    f"{ctx.category} 输出 {actual_len} 字符，"
                    f"超出建议上限 {limit}（{int(actual_len/limit*100)}%）"
                ),
                evidence=f"{actual_len}/{limit}",
                fix_hint=f"请精简到 {limit} 字以内",
            ))
        return violations


class ForbiddenContentRule(ValidationRule):
    """禁忌内容校验：输出不能包含特定模式。

    对应 AGENTS.md：来自股吧/论坛/自媒体的信息必须标注"信息来源可靠性待验证"。
    """
    name = "forbidden_content"

    _UNVERIFIED_SOURCE_HINTS = ["股吧", "论坛", "自媒体", "网友爆料", "内部消息"]

    async def check(self, ctx: ValidationContext) -> List[Violation]:
        violations: List[Violation] = []
        output = ctx.agent_output or ""

        for hint in self._UNVERIFIED_SOURCE_HINTS:
            if hint in output:
                # 检查是否标注了"待验证"
                if "待验证" not in output and "可靠性" not in output:
                    violations.append(Violation(
                        rule_name=self.name,
                        severity="warn",
                        message=(
                            f"输出引用了 {hint} 的信息，但未标注"
                            f"'信息来源可靠性待验证'"
                        ),
                        evidence=hint,
                        fix_hint="请在该信息后添加：信息来源可靠性待验证",
                    ))
                    break  # 一次违规足够
        return violations


# ======================================================================
# 主校验器
# ======================================================================

class OutputValidator:
    """输出校验器：聚合所有规则，提供统一接口。"""

    def __init__(self, auto_retry: bool = OUTPUT_VALIDATOR_AUTO_RETRY):
        self.auto_retry = auto_retry
        self.rules: List[ValidationRule] = [
            RiskDisclaimerRule(),
            StockCodeFormatRule(),
            HallucinationGuardRule(),
            LengthLimitRule(),
            ForbiddenContentRule(),
        ]
        self._retry_counts: Dict[str, int] = {}  # request_id -> retry_count

    def add_rule(self, rule: ValidationRule) -> None:
        """注册自定义规则。"""
        self.rules.append(rule)

    async def validate(self, ctx: ValidationContext) -> ValidationResult:
        """执行所有规则校验。"""
        result = ValidationResult()
        for rule in self.rules:
            try:
                violations = await rule.check(ctx)
                result.violations.extend(violations)
            except Exception as e:
                logger.error("[output_validator] 规则 %s 执行异常: %s", rule.name, e)

        result.has_warnings = any(v.severity == "warn" for v in result.violations)
        result.is_blocked = any(v.severity == "block" for v in result.violations)
        result.is_clean = not result.violations

        # 落盘审计
        if result.violations:
            await self._log_violations(ctx, result)

        return result

    def can_retry(self, request_id: str = "") -> bool:
        """检查是否还能重试。"""
        if not self.auto_retry:
            return False
        return self._retry_counts.get(request_id, 0) < OUTPUT_VALIDATOR_MAX_RETRIES

    def increment_retry(self, request_id: str = "") -> None:
        """记录一次重试。"""
        self._retry_counts[request_id] = self._retry_counts.get(request_id, 0) + 1

    def build_retry_prompt(self, violations: List[Violation]) -> str:
        """构建重试时的修正提示。"""
        violation_texts = []
        for v in violations:
            text = f"- [{v.rule_name}] {v.message}"
            if v.fix_hint:
                text += f"\n  修正建议：{v.fix_hint}"
            violation_texts.append(text)
        return OUTPUT_VALIDATOR_RETRY_HINT.format(
            violations="\n".join(violation_texts)
        )

    async def validate_and_maybe_retry(
        self,
        ctx: ValidationContext,
        llm_caller_fn,  # async def(ctx) -> str
        request_id: str = "",
    ) -> str:
        """校验 + 自动重试的完整流程。返回最终输出。

        用法：
            final = await validator.validate_and_maybe_retry(
                ctx=ctx,
                llm_caller_fn=lambda c: call_llm(c.user_input),
                request_id="r1",
            )
        """
        while True:
            output = await llm_caller_fn(ctx)
            ctx.agent_output = output
            result = await self.validate(ctx)

            if not result.is_blocked:
                return output

            if not self.can_retry(request_id):
                logger.warning(
                    "[output_validator] 已重试 %d 次仍违规，拒绝输出: %s",
                    self._retry_counts.get(request_id, 0),
                    [v.rule_name for v in result.block_violations],
                )
                return "抱歉，本次回答未能通过内容校验，请重新提问或换种方式询问。"

            self.increment_retry(request_id)
            # 把违规提示塞进 user_input 重试
            retry_hint = self.build_retry_prompt(result.block_violations)
            ctx.user_input = f"{ctx.user_input}\n\n{retry_hint}"
            logger.info(
                "[output_validator] 自动重试 %d/%d: %s",
                self._retry_counts[request_id], OUTPUT_VALIDATOR_MAX_RETRIES,
                [v.rule_name for v in result.block_violations],
            )

    async def _log_violations(
        self,
        ctx: ValidationContext,
        result: ValidationResult,
    ) -> None:
        """违规落盘 JSONL 审计日志。"""
        log_dir = os.path.dirname(OUTPUT_VALIDATOR_LOG_PATH)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        event = {
            "timestamp": time.time(),
            "ts_human": time.strftime("%Y-%m-%d %H:%M:%S"),
            "category": ctx.category,
            "user_input": (ctx.user_input or "")[:200],
            "agent_output": (ctx.agent_output or "")[:500],
            "is_blocked": result.is_blocked,
            "violations": [
                {
                    "rule": v.rule_name,
                    "severity": v.severity,
                    "message": v.message,
                    "evidence": v.evidence,
                    "fix_hint": v.fix_hint,
                }
                for v in result.violations
            ],
        }
        try:
            with open(OUTPUT_VALIDATOR_LOG_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error("[output_validator] 审计日志写入失败: %s", e)


# ======================================================================
# 全局单例
# ======================================================================

_global_validator: Optional[OutputValidator] = None
_singleton_lock = asyncio.Lock()


async def get_output_validator() -> OutputValidator:
    """获取全局 OutputValidator 单例。"""
    global _global_validator
    if _global_validator is None:
        async with _singleton_lock:
            if _global_validator is None:
                _global_validator = OutputValidator()
    return _global_validator


def get_output_validator_sync() -> OutputValidator:
    """同步获取单例。"""
    global _global_validator
    if _global_validator is None:
        _global_validator = OutputValidator()
    return _global_validator
