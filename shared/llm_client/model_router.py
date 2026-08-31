# -*- coding: utf-8 -*-
"""
多模型动态路由（成本/SLA 感知）：所有问题不能都打到 DeepSeek 主模型，要进行成本控制。
设计思路：
  1) 三种策略：simple / cost_aware / sla_aware
     - simple: 简单问题路由到便宜模型（如闲聊）
     - cost_aware: 在 simple 基础上，监控每模型日预算，超限降级
     - sla_aware: P95 延迟超过阈值，下一轮切到更快模型
  2) 输入分类：基于字符数/关键词/代码片段判定问题复杂度
  3) 失败降级链：主模型 → 备模型 → 本地兜底
  4) 调用统计：每日调用次数、累计 token、累计 USD 成本

典型用法：
    from agent.model_router import ModelRouter

    router = ModelRouter()

    # 路由决策
    decision = await router.route(
        prompt="对比茅台和五粮液的护城河",
        user_id="u123",
    )
    # decision.model = "deepseek-reasoner"
    # decision.reason = "命中复杂关键词：对比,护城河"

    # 调用 LLM（带自动降级）
    result = await router.call_with_fallback(
        prompt=prompt,
        user_id="u123",
        llm_caller_fn=my_caller,  # async def(prompt, model) -> LLMResponse
    )
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from config.constants import (
    MODEL_ROUTER_STRATEGY,
    MODEL_ROUTER_CHEAP_MODEL,
    MODEL_ROUTER_STRONG_MODEL,
    MODEL_ROUTER_LOCAL_MODEL,
    MODEL_ROUTER_SIMPLE_MAX_CHARS,
    MODEL_ROUTER_SIMPLE_NO_CODE,
    MODEL_ROUTER_COMPLEX_KEYWORDS,
    MODEL_ROUTER_DAILY_BUDGET_USD,
    MODEL_ROUTER_DAILY_CALL_LIMIT,
    MODEL_ROUTER_FALLBACK_CHAIN,
    MODEL_ROUTER_SLA_LATENCY_THRESHOLD_SEC,
)

logger = logging.getLogger(__name__)


# ======================================================================
# 路由决策结果
# ======================================================================

@dataclass
class RouteDecision:
    """路由决策结果。"""
    model: str                         # 选定的模型名
    reason: str                        # 选择原因
    complexity: str = "medium"        # simple / medium / complex
    is_fallback: bool = False         # 是否降级选择
    fallback_chain_used: List[str] = field(default_factory=list)
    estimated_cost_usd: float = 0.0   # 预估成本


# ======================================================================
# 调用统计
# ======================================================================

@dataclass
class ModelStats:
    """单模型的当日调用统计。"""
    model: str
    date: str                                  # YYYY-MM-DD
    call_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0
    latencies: List[float] = field(default_factory=list)  # 最近 N 次延迟

    def p95_latency(self) -> float:
        if not self.latencies:
            return 0.0
        sorted_l = sorted(self.latencies)
        idx = int(len(sorted_l) * 0.95)
        return sorted_l[min(idx, len(sorted_l) - 1)]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model": self.model,
            "date": self.date,
            "call_count": self.call_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_cost_usd": round(self.total_cost_usd, 6),
            "p95_latency_sec": round(self.p95_latency(), 2),
        }


# ======================================================================
# 成本表（USD / 1K token）—— 实际从环境变量或配置中心读取
# ======================================================================

# DeepSeek 2024 定价（USD / 1M tokens），实际请按官方为准
_MODEL_PRICING_USD_PER_M_TOKENS = {
    "deepseek-chat":      {"input": 0.14, "output": 0.28},
    "deepseek-reasoner":  {"input": 0.55, "output": 2.19},
    "qwen3:8b":            {"input": 0.0, "output": 0.0},  # 本地模型
}


def calc_cost_usd(model: str, input_tokens: int, output_tokens: int) -> float:
    """计算单次调用成本（USD）。"""
    p = _MODEL_PRICING_USD_PER_M_TOKENS.get(model, {"input": 0.0, "output": 0.0})
    return (input_tokens / 1_000_000 * p["input"]) + (output_tokens / 1_000_000 * p["output"])


# ======================================================================
# 输入复杂度分类器
# ======================================================================

class ComplexityClassifier:
    """根据输入特征判定问题复杂度。"""

    # 代码片段检测
    _CODE_PATTERN = re.compile(r"```|def\s+\w+\s*\(|class\s+\w+|import\s+\w+|function\s+\w+", re.MULTILINE)

    @classmethod
    def classify(cls, prompt: str) -> Tuple[str, str]:
        """返回 (complexity, reason)。

        complexity: simple / medium / complex
        """
        if not prompt:
            return "simple", "空输入"

        char_count = len(prompt)
        has_code = bool(cls._CODE_PATTERN.search(prompt))
        complex_kw_hits = [kw for kw in MODEL_ROUTER_COMPLEX_KEYWORDS if kw in prompt]

        # 复杂：含复杂关键词 或 代码片段
        if complex_kw_hits:
            return "complex", f"命中复杂关键词：{','.join(complex_kw_hits)}"
        if has_code and not MODEL_ROUTER_SIMPLE_NO_CODE:
            return "complex", "包含代码片段"

        # 简单：短输入无关键词
        if char_count <= MODEL_ROUTER_SIMPLE_MAX_CHARS:
            return "simple", f"短输入 ({char_count} 字符)，无复杂关键词"

        return "medium", f"中等输入 ({char_count} 字符)"


# ======================================================================
# 主路由器
# ======================================================================

class ModelRouter:
    """多模型动态路由器。

    线程安全：所有状态修改通过 asyncio.Lock 串行化。
    持久化：每日统计通过 stats_backend 落盘（默认仅内存，重启清零）。
    """

    def __init__(
        self,
        strategy: str = MODEL_ROUTER_STRATEGY,
        fallback_chain: Tuple[str, ...] = MODEL_ROUTER_FALLBACK_CHAIN,
    ):
        self.strategy = strategy
        self.fallback_chain = list(fallback_chain)
        self.classifier = ComplexityClassifier()
        self._lock = asyncio.Lock()
        self._stats: Dict[str, ModelStats] = {}  # model -> ModelStats
        self._sla_blacklist: Dict[str, float] = {}  # model -> 黑名单到期时间戳
        self._init_stats()

    def _init_stats(self) -> None:
        """初始化所有已知模型的统计条目。"""
        today = date.today().isoformat()
        for model in set(self.fallback_chain + [
            MODEL_ROUTER_CHEAP_MODEL, MODEL_ROUTER_STRONG_MODEL, MODEL_ROUTER_LOCAL_MODEL
        ]):
            if model not in self._stats:
                self._stats[model] = ModelStats(model=model, date=today)

    async def route(
        self,
        prompt: str,
        user_id: str = "",
        **kwargs: Any,
    ) -> RouteDecision:
        """路由决策。返回选定的模型。"""
        complexity, reason = self.classifier.classify(prompt)

        # 1) 基于复杂度选主模型
        if complexity == "simple":
            primary = MODEL_ROUTER_CHEAP_MODEL
        elif complexity == "complex":
            primary = MODEL_ROUTER_STRONG_MODEL
        else:
            primary = MODEL_ROUTER_CHEAP_MODEL  # medium 也用 cheap（成本优先）

        # 2) cost_aware：检查预算，超限则降级
        if self.strategy in ("cost_aware", "sla_aware"):
            primary = await self._check_budget_and_adjust(primary)

        # 3) sla_aware：检查 SLA 黑名单
        if self.strategy == "sla_aware":
            primary = self._check_sla_blacklist(primary)

        # 4) 估算成本
        est_cost = calc_cost_usd(primary, len(prompt) // 4, 500)  # 粗估

        return RouteDecision(
            model=primary,
            reason=reason,
            complexity=complexity,
            estimated_cost_usd=est_cost,
        )

    async def _check_budget_and_adjust(self, primary: str) -> str:
        """检查预算是否超限。超限则降级到 fallback_chain 中下一个。"""
        async with self._lock:
            self._rollover_if_new_day()
            stats = self._stats.get(primary)
            if stats is None:
                return primary

            # 调用次数超限
            if stats.call_count >= MODEL_ROUTER_DAILY_CALL_LIMIT:
                logger.warning(
                    "[router] %s 日调用 %d 超限 %d，降级",
                    primary, stats.call_count, MODEL_ROUTER_DAILY_CALL_LIMIT,
                )
                return self._pick_next_in_chain(primary)

            # 成本超限
            if stats.total_cost_usd >= MODEL_ROUTER_DAILY_BUDGET_USD:
                logger.warning(
                    "[router] %s 日成本 $%.2f 超预算 $%.2f，降级",
                    primary, stats.total_cost_usd, MODEL_ROUTER_DAILY_BUDGET_USD,
                )
                return self._pick_next_in_chain(primary)

            return primary

    def _check_sla_blacklist(self, primary: str) -> str:
        """SLA 检查：若模型在黑名单中（P95 超阈值），降级。"""
        now = time.time()
        if primary in self._sla_blacklist:
            expire = self._sla_blacklist[primary]
            if now < expire:
                logger.info("[router] %s 在 SLA 黑名单中（P95 超阈值），降级", primary)
                return self._pick_next_in_chain(primary)
            else:
                # 黑名单到期，移除
                del self._sla_blacklist[primary]
        return primary

    def _pick_next_in_chain(self, excluded: str) -> str:
        """从 fallback_chain 中挑下一个非 excluded 的模型。"""
        for m in self.fallback_chain:
            if m != excluded:
                return m
        # 兜底：本地模型
        return MODEL_ROUTER_LOCAL_MODEL

    def _rollover_if_new_day(self) -> None:
        """跨日则重置统计。"""
        today = date.today().isoformat()
        for stats in self._stats.values():
            if stats.date != today:
                stats.date = today
                stats.call_count = 0
                stats.success_count = 0
                stats.failure_count = 0
                stats.total_input_tokens = 0
                stats.total_output_tokens = 0
                stats.total_cost_usd = 0.0
                stats.latencies = []

    # ==================================================================
    # 调用 + 自动降级
    # ==================================================================

    async def call_with_fallback(
        self,
        prompt: str,
        llm_caller_fn: Callable[[str, str], Awaitable[Any]],
        user_id: str = "",
        **kwargs: Any,
    ) -> Tuple[Any, RouteDecision]:
        """按路由决策调用 LLM，失败时按 fallback_chain 自动降级。

        参数：
            prompt: 用户 prompt
            llm_caller_fn: async def(prompt: str, model: str) -> LLMResponse
                           返回的对象需有 .content / .usage.input_tokens / .usage.output_tokens

        返回 (llm_response, decision)
        """
        decision = await self.route(prompt, user_id=user_id, **kwargs)
        chain_to_try = [decision.model] + [
            m for m in self.fallback_chain if m != decision.model
        ]

        last_err: Optional[Exception] = None
        for i, model in enumerate(chain_to_try):
            t0 = time.time()
            try:
                resp = await llm_caller_fn(prompt, model)
                elapsed = time.time() - t0
                await self._record_success(model, resp, elapsed)

                # SLA 检查：若 P95 超阈值，下轮黑名单
                if self.strategy == "sla_aware":
                    self._maybe_blacklist_for_sla(model, elapsed)

                if i > 0:
                    decision.is_fallback = True
                    decision.fallback_chain_used = chain_to_try[:i + 1]
                    decision.reason += f"；前 {i} 个模型失败，已降级到 {model}"
                return resp, decision

            except Exception as e:
                elapsed = time.time() - t0
                await self._record_failure(model, elapsed)
                last_err = e
                logger.warning(
                    "[router] %s 调用失败 (%.1fs): %s: %s",
                    model, elapsed, type(e).__name__, str(e)[:200],
                )
                continue

        # 所有模型都失败
        raise RuntimeError(
            f"所有模型都失败（{len(chain_to_try)} 个）：last_err={last_err}"
        )

    async def _record_success(self, model: str, resp: Any, elapsed: float) -> None:
        """记录成功调用。"""
        async with self._lock:
            stats = self._stats.setdefault(model, ModelStats(model=model, date=date.today().isoformat()))
            stats.call_count += 1
            stats.success_count += 1
            stats.latencies.append(elapsed)
            if len(stats.latencies) > 100:  # 只保留最近 100 次
                stats.latencies = stats.latencies[-100:]
            # 提取 token 用量
            usage = getattr(resp, "usage", None) or {}
            if isinstance(usage, dict):
                stats.total_input_tokens += usage.get("input_tokens", usage.get("prompt_tokens", 0))
                stats.total_output_tokens += usage.get("output_tokens", usage.get("completion_tokens", 0))
            # 计算成本
            cost = calc_cost_usd(model, stats.total_input_tokens, stats.total_output_tokens)
            stats.total_cost_usd = cost

    async def _record_failure(self, model: str, elapsed: float) -> None:
        """记录失败调用。"""
        async with self._lock:
            stats = self._stats.setdefault(model, ModelStats(model=model, date=date.today().isoformat()))
            stats.call_count += 1
            stats.failure_count += 1
            stats.latencies.append(elapsed)

    def _maybe_blacklist_for_sla(self, model: str, elapsed: float) -> None:
        """若该模型 P95 超阈值，加入 5 分钟黑名单。"""
        stats = self._stats.get(model)
        if stats and len(stats.latencies) >= 10:
            p95 = stats.p95_latency()
            if p95 > MODEL_ROUTER_SLA_LATENCY_THRESHOLD_SEC:
                self._sla_blacklist[model] = time.time() + 300  # 5 分钟
                logger.warning(
                    "[router] %s P95=%.1fs 超阈值 %.1fs，加入 5 分钟黑名单",
                    model, p95, MODEL_ROUTER_SLA_LATENCY_THRESHOLD_SEC,
                )

    # ==================================================================
    # 监控接口
    # ==================================================================

    async def get_stats(self) -> Dict[str, Any]:
        """返回所有模型的当日统计。"""
        async with self._lock:
            return {
                "strategy": self.strategy,
                "daily_budget_usd": MODEL_ROUTER_DAILY_BUDGET_USD,
                "daily_call_limit": MODEL_ROUTER_DAILY_CALL_LIMIT,
                "sla_blacklist": {
                    m: time.strftime("%H:%M:%S", time.localtime(exp))
                    for m, exp in self._sla_blacklist.items()
                },
                "models": {m: s.to_dict() for m, s in self._stats.items()},
            }


# ======================================================================
# 全局单例
# ======================================================================

_global_router: Optional[ModelRouter] = None


def get_model_router() -> ModelRouter:
    """获取全局 ModelRouter 单例。"""
    global _global_router
    if _global_router is None:
        _global_router = ModelRouter()
    return _global_router
