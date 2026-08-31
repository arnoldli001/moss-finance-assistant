"""
Layer 3 - Harness Engineering: 四级降级链（Four-Level Degradation Chain）。

主模型 DeepSeek 不可用 / 超时 / 熔断时，按下列顺序自动降级：
    Tier 1 DeepSeek （主模型，最强推理能力）
    Tier 2 IMA 知识库（基于检索的回答，覆盖专有文档）
    Tier 3 Qwen3-8B 本地模型（离线兜底，无网络依赖）
    Tier 4 静态模板（无 LLM 可用时的最终兜底，结构化提示）

双重硬上限（任一超限即触发降级）：
    单任务执行时间 ≤ 150 秒
    单任务 Token 消耗 ≤ 1,000,000

与 circuit_breaker.py 协同：
    每层调用前查询对应熔断器（cb.allow_request()），
    若熔断中则跳过该层直接进入下一层，避免无效等待。
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Awaitable, Callable, Dict, List, Optional

from agent.circuit_breaker import get_circuit_registry
from agent.error_classifier import ErrorClassifier, ErrorQuadrant, get_error_classifier

# ===== 全局常量集中引用（替代魔鬼数字，统一修改一处即全局生效）=====
from config.constants import (
    DEGRADE_MAX_TASK_SECONDS,
    DEGRADE_MAX_TASK_TOKENS,
)


# ======================================================================
# 常量：硬上限
# ======================================================================
MAX_TASK_SECONDS: float = DEGRADE_MAX_TASK_SECONDS
MAX_TASK_TOKENS: int = DEGRADE_MAX_TASK_TOKENS


# ======================================================================
# 降级层级
# ======================================================================

class DegradationTier(IntEnum):
    """降级链层级。数值越大，能力越弱。"""
    TIER_1_DEEPSEEK = 1    # 主模型 DeepSeek（联网搜索 + 推理）
    TIER_2_IMA = 2          # IMA 知识库检索（专有文档问答）
    TIER_3_QWEN8B = 3       # 本地 Qwen3-8B 模型（离线兜底）
    TIER_4_STATIC = 4       # 静态模板（结构化兜底回复）


@dataclass
class TierResult:
    """单层执行结果。"""
    tier: DegradationTier
    success: bool
    output: str = ""
    latency_sec: float = 0.0
    token_count: int = 0
    error: Optional[str] = None
    skipped: bool = False              # 因熔断或前置失败而跳过
    fallback_reason: Optional[str] = None  # 触发降级的原因


@dataclass
class DegradationReport:
    """整条降级链的执行报告，供 SLO 监控与 trace 记录使用。"""
    final_tier: DegradationTier
    final_output: str
    total_latency_sec: float
    total_tokens: int
    tier_results: List[TierResult] = field(default_factory=list)
    hit_hard_limit: bool = False       # 是否触达硬上限
    limit_breached: Optional[str] = None  # "TIME" / "TOKEN" / None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "final_tier": int(self.final_tier),
            "final_tier_name": self.final_tier.name,
            "total_latency_sec": round(self.total_latency_sec, 3),
            "total_tokens": self.total_tokens,
            "hit_hard_limit": self.hit_hard_limit,
            "limit_breached": self.limit_breached,
            "tiers": [
                {
                    "tier": int(r.tier),
                    "name": r.tier.name,
                    "success": r.success,
                    "skipped": r.skipped,
                    "latency_sec": round(r.latency_sec, 3),
                    "token_count": r.token_count,
                    "error": r.error,
                    "fallback_reason": r.fallback_reason,
                }
                for r in self.tier_results
            ],
        }


# ======================================================================
# 单层执行的通用包装：超时 + Token 上限 + 熔断 + 错误分类
# ======================================================================

async def _execute_tier(
    tier: DegradationTier,
    cb_name: str,
    call_fn: Callable[[], Awaitable[str]],
    *,
    timeout_sec: float = MAX_TASK_SECONDS,
    token_counter: Optional[Callable[[], int]] = None,
) -> TierResult:
    """
    执行单层调用，统一处理：
      - 熔断器准入判断（cb.allow_request）
      - 执行超时（asyncio.wait_for）
      - Token 硬上限检查
      - 错误分类（决定是否可重试 / 是否降级）

    Args:
        tier: 当前层级
        cb_name: 对应的熔断器名称（deepseek/ima/qwen8b）
        call_fn: 实际的异步调用函数，返回 str 输出
        timeout_sec: 本层超时上限（默认 150s）
        token_counter: 可选的 Token 计数函数，调用后返回该层消耗的 Token 数
    """
    # 1) 熔断器准入检查
    cb = get_circuit_registry().get_or_create(cb_name)
    if not cb.allow_request():
        return TierResult(
            tier=tier, success=False, skipped=True,
            fallback_reason=f"circuit_breaker_{cb_name}_OPEN",
        )

    # 2) 执行（超时保护）
    start = time.time()
    try:
        output = await asyncio.wait_for(call_fn(), timeout=timeout_sec)
        latency = time.time() - start
        tokens = token_counter() if token_counter else 0

        # 3) Token 硬上限检查
        if tokens > MAX_TASK_TOKENS:
            cb.record_failure()
            return TierResult(
                tier=tier, success=False, latency_sec=latency, token_count=tokens,
                error=f"Token 超限: {tokens} > {MAX_TASK_TOKENS}",
                fallback_reason="TOKEN_LIMIT_BREACHED",
            )

        cb.record_success()
        return TierResult(
            tier=tier, success=True, output=output,
            latency_sec=latency, token_count=tokens,
        )

    except asyncio.TimeoutError:
        latency = time.time() - start
        cb.record_failure()
        return TierResult(
            tier=tier, success=False, latency_sec=latency,
            error=f"执行超时 (>{timeout_sec}s)",
            fallback_reason="TIMEOUT",
        )
    except Exception as e:
        latency = time.time() - start
        # 错误分类：决定是否计入熔断器
        classifier = get_error_classifier()
        cls_err = classifier.classify(e)
        # 配置类错误（D）不重试，但也不计入熔断（不是服务端故障）
        # 可重试硬错误（A）与不可重试错误（C）计入熔断
        # 软错误（B）如模型幻觉不计入熔断（重试无意义）
        if cls_err.quadrant in (ErrorQuadrant.A_RETRYABLE_HARD, ErrorQuadrant.C_PERMANENT):
            cb.record_failure()
        return TierResult(
            tier=tier, success=False, latency_sec=latency,
            error=f"{type(e).__name__}: {e}",
            fallback_reason=f"{cls_err.quadrant.value}_{cls_err.error_type}",
        )


# ======================================================================
# 四级降级链
# ======================================================================

class DegradationChain:
    """
    四级降级链执行器。

    使用方式：
        chain = get_degradation_chain()
        report = await chain.execute(
            user_query=query,
            deepseek_fn=lambda: deepseek_model.ainvoke(query),
            ima_fn=lambda: ima_kb.search(query),
            qwen8b_fn=lambda: qwen8b.ainvoke(query),
        )
        if report.final_output:
            send_to_user(report.final_output)
        log_slo(report.to_dict())
    """

    def __init__(self):
        self.classifier = get_error_classifier()

    async def execute(
        self,
        user_query: str,
        deepseek_fn: Optional[Callable[[], Awaitable[str]]] = None,
        ima_fn: Optional[Callable[[], Awaitable[str]]] = None,
        qwen8b_fn: Optional[Callable[[], Awaitable[str]]] = None,
        deepseek_token_counter: Optional[Callable[[], int]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> DegradationReport:
        """
        按顺序执行四级降级链，返回首个成功层的输出。

        任一层失败或被熔断即自动进入下一层。
        若触达时间/Token 硬上限，立即终止后续层级并标记。
        """
        chain_start = time.time()
        cumulative_tokens = 0
        tier_results: List[TierResult] = []
        context = context or {}

        # ===== Tier 1: DeepSeek =====
        if deepseek_fn is not None:
            # 剩余时间预算
            remaining_time = MAX_TASK_SECONDS - (time.time() - chain_start)
            if remaining_time <= 0:
                tier_results.append(TierResult(
                    tier=DegradationTier.TIER_1_DEEPSEEK, success=False, skipped=True,
                    fallback_reason="NO_TIME_BUDGET",
                ))
            else:
                r1 = await _execute_tier(
                    DegradationTier.TIER_1_DEEPSEEK, "deepseek", deepseek_fn,
                    timeout_sec=remaining_time,
                    token_counter=deepseek_token_counter,
                )
                tier_results.append(r1)
                cumulative_tokens += r1.token_count
                if r1.success and r1.output:
                    return self._build_report(
                        DegradationTier.TIER_1_DEEPSEEK, r1.output,
                        chain_start, cumulative_tokens, tier_results,
                    )
                # Token 超限则立即终止
                if r1.fallback_reason == "TOKEN_LIMIT_BREACHED":
                    return self._build_report(
                        DegradationTier.TIER_1_DEEPSEEK, "",
                        chain_start, cumulative_tokens, tier_results,
                        hit_hard_limit=True, limit_breached="TOKEN",
                    )
        else:
            tier_results.append(TierResult(
                tier=DegradationTier.TIER_1_DEEPSEEK, success=False, skipped=True,
                fallback_reason="NO_CALLABLE",
            ))

        # ===== Tier 2: IMA 知识库 =====
        if ima_fn is not None:
            remaining_time = MAX_TASK_SECONDS - (time.time() - chain_start)
            if remaining_time <= 0:
                tier_results.append(TierResult(
                    tier=DegradationTier.TIER_2_IMA, success=False, skipped=True,
                    fallback_reason="NO_TIME_BUDGET",
                ))
            else:
                r2 = await _execute_tier(
                    DegradationTier.TIER_2_IMA, "ima", ima_fn,
                    timeout_sec=remaining_time,
                )
                tier_results.append(r2)
                if r2.success and r2.output:
                    return self._build_report(
                        DegradationTier.TIER_2_IMA, r2.output,
                        chain_start, cumulative_tokens, tier_results,
                    )
        else:
            tier_results.append(TierResult(
                tier=DegradationTier.TIER_2_IMA, success=False, skipped=True,
                fallback_reason="NO_CALLABLE",
            ))

        # ===== Tier 3: Qwen3-8B 本地模型 =====
        if qwen8b_fn is not None:
            remaining_time = MAX_TASK_SECONDS - (time.time() - chain_start)
            if remaining_time <= 0:
                tier_results.append(TierResult(
                    tier=DegradationTier.TIER_3_QWEN8B, success=False, skipped=True,
                    fallback_reason="NO_TIME_BUDGET",
                ))
            else:
                r3 = await _execute_tier(
                    DegradationTier.TIER_3_QWEN8B, "qwen8b", qwen8b_fn,
                    timeout_sec=remaining_time,
                )
                tier_results.append(r3)
                if r3.success and r3.output:
                    return self._build_report(
                        DegradationTier.TIER_3_QWEN8B, r3.output,
                        chain_start, cumulative_tokens, tier_results,
                    )
        else:
            tier_results.append(TierResult(
                tier=DegradationTier.TIER_3_QWEN8B, success=False, skipped=True,
                fallback_reason="NO_CALLABLE",
            ))

        # ===== Tier 4: 静态模板兜底 =====
        static_output = self._build_static_template(user_query, context)
        tier_results.append(TierResult(
            tier=DegradationTier.TIER_4_STATIC, success=True,
            output=static_output, latency_sec=0.0, token_count=0,
        ))
        return self._build_report(
            DegradationTier.TIER_4_STATIC, static_output,
            chain_start, cumulative_tokens, tier_results,
        )

    # ------------------------------------------------------------------
    # 内部：构建最终报告（统一检查硬上限）
    # ------------------------------------------------------------------
    def _build_report(
        self,
        final_tier: DegradationTier,
        final_output: str,
        chain_start: float,
        total_tokens: int,
        tier_results: List[TierResult],
        *,
        hit_hard_limit: bool = False,
        limit_breached: Optional[str] = None,
    ) -> DegradationReport:
        total_latency = time.time() - chain_start
        # 总耗时超 150s 标记为触达硬上限
        if not hit_hard_limit and total_latency > MAX_TASK_SECONDS:
            hit_hard_limit = True
            limit_breached = "TIME"
        # 总 Token 超 1M 标记为触达硬上限
        if not hit_hard_limit and total_tokens > MAX_TASK_TOKENS:
            hit_hard_limit = True
            limit_breached = "TOKEN"
        return DegradationReport(
            final_tier=final_tier,
            final_output=final_output,
            total_latency_sec=total_latency,
            total_tokens=total_tokens,
            tier_results=tier_results,
            hit_hard_limit=hit_hard_limit,
            limit_breached=limit_breached,
        )

    # ------------------------------------------------------------------
    # 内部：静态模板兜底
    # ------------------------------------------------------------------
    def _build_static_template(self, user_query: str, context: Dict[str, Any]) -> str:
        """四级静态兜底模板：无 LLM 可用时的最终回复。"""
        session_id = context.get("session_id", "unknown")
        return (
            "⚠️ 当前所有 AI 模型均不可用（DeepSeek / IMA / Qwen8B 依次降级失败）。\n\n"
            f"您的问题：{user_query}\n\n"
            "可能的原因：\n"
            "1. 网络波动导致 API 超时\n"
            "2. 模型服务熔断保护已触发\n"
            "3. Token 预算或时间预算超限\n\n"
            "建议操作：\n"
            "- 稍后重试（30 秒后熔断器将进入半开探测状态）\n"
            "- 简化问题后再次提问\n"
            "- 若持续失败，请联系管理员检查 API Key 与网络配置\n\n"
            f"会话标识：{session_id}\n"
            "⚠️ 以上信息来自系统降级链，不构成投资建议。投资有风险，入市需谨慎，盈亏自负。"
        )


# ======================================================================
# 全局单例
# ======================================================================

_chain: Optional[DegradationChain] = None


def get_degradation_chain() -> DegradationChain:
    global _chain
    if _chain is None:
        _chain = DegradationChain()
    return _chain
