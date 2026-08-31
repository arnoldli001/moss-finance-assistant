# -*- coding: utf-8 -*-
"""
企业级增强模块的统一集成入口。

把增强防御模块组装成"请求处理流水线"，
让现有 server.py / main_agent.py 一行代码即可接入所有增强能力。

接入步骤：
  1) 应用启动时调用 init_enterprise_extensions()
  2) 应用关闭时调用 shutdown_enterprise_extensions()
  3) 在请求入口（WebSocket handler / FastAPI route）调用
     enter_request_pipeline(user_input, user_id, session_id, request_id)
     → 返回 RequestContext 对象，包含所有增强能力的入口
  4) 业务代码用 ctx 调用 LLM（自动走 model_router + 语义缓存 + OTel 追踪）
  5) 业务代码用 ctx.validate_output() 校验 LLM 输出（自动拦截 + 重试）
  6) 在 finally 中调用 exit_request_pipeline(ctx)

典型用法：
    from agent.enterprise_hooks import (
        init_enterprise_extensions,
        shutdown_enterprise_extensions,
        enter_request_pipeline,
        exit_request_pipeline,
    )

    # 应用启动
    @app.on_event("startup")
    async def _startup():
        await init_enterprise_extensions()

    @app.on_event("shutdown")
    async def _shutdown():
        await shutdown_enterprise_extensions()

    # 请求处理
    async def handle_chat(user_input: str, user_id: str, session_id: str):
        ctx = await enter_request_pipeline(
            user_input=user_input,
            user_id=user_id,
            session_id=session_id,
            request_id=uuid.uuid4().hex,
        )
        try:
            # prompt 注入检测
            if ctx.is_input_blocked:
                return ctx.block_reason

            # 调用 LLM（带 model_router + 语义缓存 + OTel）
            answer = await ctx.call_llm_with_enhancements()

            # 校验输出
            final = await ctx.validate_output(answer)
            return final
        finally:
            await exit_request_pipeline(ctx)
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from agent.observability import (
    init_tracing,
    shutdown_tracing,
    agent_span,
    llm_span,
    set_request_trace_context,
    clear_request_trace_context,
)
from agent.semantic_cache import get_semantic_cache, SemanticCache
from agent.model_router import get_model_router, ModelRouter
from agent.output_validator import get_output_validator, OutputValidator, ValidationContext
from agent.stream_resume import get_stream_resume_store, StreamResumeStore
from agent.actor_persistence import SnapshotCoordinator, ActorSnapshotter
from api.middleware.prompt_sanitizer import sanitize_user_input_async, SanitizeResult
from api.middleware.rbac import RBACPolicy, UserContext

logger = logging.getLogger(__name__)


# ======================================================================
# 全局组件句柄
# ======================================================================

class _Globals:
    """全局组件句柄（init 后赋值）。"""
    semantic_cache: Optional[SemanticCache] = None
    model_router: Optional[ModelRouter] = None
    output_validator: Optional[OutputValidator] = None
    stream_resume_store: Optional[StreamResumeStore] = None
    snapshot_coordinator: Optional[SnapshotCoordinator] = None
    rbac_policy: Optional[RBACPolicy] = None
    _initialized: bool = False


async def init_enterprise_extensions() -> None:
    """应用启动时调用。初始化所有企业级增强模块。"""
    if _Globals._initialized:
        return

    # 1) OpenTelemetry（同步初始化）
    init_tracing()
    logger.info("[enterprise] OTel 追踪已启用")

    # 2) 语义缓存（懒加载 embedder）
    _Globals.semantic_cache = await get_semantic_cache()
    logger.info("[enterprise] 语义缓存已就绪 backend=%s",
                _Globals.semantic_cache.backend_type)

    # 3) 模型路由器
    _Globals.model_router = get_model_router()
    logger.info("[enterprise] 模型路由器已就绪 strategy=%s",
                _Globals.model_router.strategy)

    # 4) 输出校验器
    _Globals.output_validator = await get_output_validator()
    logger.info("[enterprise] 输出校验器已就绪 rules=%d",
                len(_Globals.output_validator.rules))

    # 5) 流式续传存储
    _Globals.stream_resume_store = await get_stream_resume_store()
    logger.info("[enterprise] 流式续传存储已就绪 backend=%s",
                _Globals.stream_resume_store.backend_type)

    # 6) Actor 快照协调器
    _Globals.snapshot_coordinator = SnapshotCoordinator()
    logger.info("[enterprise] Actor 快照协调器已就绪")

    # 7) RBAC 策略（同步加载）
    _Globals.rbac_policy = RBACPolicy()
    logger.info("[enterprise] RBAC 策略已加载 roles=%d",
                len(_Globals.rbac_policy._roles))

    _Globals._initialized = True
    logger.info("[enterprise] 所有企业级增强模块初始化完成")


async def shutdown_enterprise_extensions() -> None:
    """应用关闭时调用。flush 状态 + 持久化快照。"""
    if not _Globals._initialized:
        return

    # 1) 强制快照所有注册的 Actor
    if _Globals.snapshot_coordinator:
        try:
            await _Globals.snapshot_coordinator.snapshot_all()
            logger.info("[enterprise] Actor 快照已落盘")
        except Exception as e:
            logger.warning("[enterprise] Actor 快照失败: %s", e)

    # 2) 关闭 OTel
    shutdown_tracing()
    logger.info("[enterprise] OTel 已关闭")

    _Globals._initialized = False
    logger.info("[enterprise] 所有企业级增强模块已关闭")


# ======================================================================
# 请求级流水线
# ======================================================================

@dataclass
class RequestPipelineContext:
    """单次请求的流水线上下文。

    包含所有增强能力的入口，业务代码通过此对象访问：
      - safe_input: 经 prompt 注入检测的安全输入
      - user_context: RBAC 用户上下文
      - call_llm_with_enhancements(): 一站式 LLM 调用
        （自动走 model_router + 语义缓存 + OTel span）
      - validate_output(): 输出 schema 校验
    """
    # 原始输入
    user_input: str = ""
    user_id: str = ""
    session_id: str = ""
    request_id: str = ""

    # 处理后的安全输入
    safe_input: str = ""
    is_input_blocked: bool = False
    block_reason: str = ""
    sanitize_result: Optional[SanitizeResult] = None

    # RBAC 用户上下文
    user_context: Optional[UserContext] = None

    # OTel 上下文（已注入）
    _otel_token: Any = None

    # 错误收集
    errors: list = field(default_factory=list)

    async def call_llm_with_enhancements(
        self,
        llm_caller_fn: Callable,
        category: str = "",
    ) -> str:
        """一站式 LLM 调用：语义缓存 → model_router → OTel span。

        参数：
            llm_caller_fn: async def(prompt: str, model: str) -> str
            category: 输出类别（用于长度校验等）

        用法：
            answer = await ctx.call_llm_with_enhancements(
                llm_caller_fn=my_caller,
                category="news_summary",
            )
        """
        # 1) 查语义缓存
        if _Globals.semantic_cache:
            cached = await _Globals.semantic_cache.get(self.safe_input)
            if cached:
                logger.info("[pipeline] 命中语义缓存 request_id=%s", self.request_id)
                return cached

        # 2) 路由 + 调用（带自动降级）
        if _Globals.model_router:
            resp, decision = await _Globals.model_router.call_with_fallback(
                prompt=self.safe_input,
                llm_caller_fn=llm_caller_fn,
                user_id=self.user_id,
            )
            # 兼容字符串或对象返回
            answer = resp if isinstance(resp, str) else getattr(resp, "content", str(resp))
            logger.info(
                "[pipeline] LLM 调用完成 model=%s cost=$%.4f complexity=%s",
                decision.model, decision.estimated_cost_usd, decision.complexity,
            )
        else:
            # 未启用路由器，直接调用
            with agent_span("llm.call") as span:
                span.set_attribute("user_id", self.user_id)
                answer = await llm_caller_fn(self.safe_input, "deepseek-chat")

        # 3) 写入语义缓存
        if _Globals.semantic_cache and answer:
            await _Globals.semantic_cache.set(
                self.safe_input, answer,
                metadata={"category": category, "model": decision.model if _Globals.model_router else "default"},
            )

        return answer

    async def validate_output(
        self,
        agent_output: str,
        category: str = "",
    ) -> str:
        """校验 LLM 输出。返回最终（可能修正后）的输出。"""
        if not _Globals.output_validator:
            return agent_output

        ctx = ValidationContext(
            user_input=self.user_input,
            agent_output=agent_output,
            category=category,
        )
        result = await _Globals.output_validator.validate(ctx)

        if result.is_blocked:
            logger.warning(
                "[pipeline] 输出被拦截 request_id=%s violations=%s",
                self.request_id,
                [v.rule_name for v in result.block_violations],
            )
            # 简化：直接拒绝，不在此自动重试（业务自行决定是否重试）
            return (
                "抱歉，本次回答未能通过内容校验，请重新提问或换种方式询问。\n"
                f"违规规则：{', '.join(v.rule_name for v in result.block_violations)}"
            )
        if result.has_warnings:
            logger.info(
                "[pipeline] 输出告警 request_id=%s violations=%s",
                self.request_id,
                [v.rule_name for v in result.warn_violations],
            )
        return agent_output

    async def begin_stream(
        self,
        msg_id: str,
        llm_context: Optional[dict] = None,
        continue_fn: Optional[Callable] = None,
    ):
        """开始一次流式输出（用于断点续传）。"""
        if not _Globals.stream_resume_store:
            return None
        return await _Globals.stream_resume_store.begin(
            session_id=self.session_id,
            msg_id=msg_id,
            user_id=self.user_id,
            llm_context=llm_context or {},
            continue_fn=continue_fn,
        )

    async def append_stream(self, msg_id: str, chunk: str) -> None:
        """追加流式 chunk（用于断点续传）。"""
        if _Globals.stream_resume_store:
            await _Globals.stream_resume_store.append(
                session_id=self.session_id, msg_id=msg_id, chunk=chunk,
            )

    async def complete_stream(self, msg_id: str, final_output: Optional[str] = None) -> None:
        """完成流式输出。"""
        if _Globals.stream_resume_store:
            await _Globals.stream_resume_store.complete(
                session_id=self.session_id, msg_id=msg_id, final_output=final_output,
            )


# ======================================================================
# 流水线入口 / 出口
# ======================================================================

async def enter_request_pipeline(
    user_input: str,
    user_id: str = "anonymous",
    session_id: str = "",
    request_id: str = "",
) -> RequestPipelineContext:
    """请求入口：执行所有前置增强。

    顺序：
      1) 注入 OTel trace context（所有下游 span 自动继承）
      2) RBAC 用户上下文构建
      3) prompt 注入检测
      4) 返回 ctx 供业务代码使用
    """
    ctx = RequestPipelineContext(
        user_input=user_input,
        user_id=user_id,
        session_id=session_id,
        request_id=request_id,
    )

    # 1) OTel 注入
    set_request_trace_context(
        request_id=request_id,
        user_id=user_id,
        session_id=session_id,
    )

    # 2) RBAC 用户上下文
    if _Globals.rbac_policy:
        ctx.user_context = _Globals.rbac_policy.build_user_context(user_id)

    # 3) prompt 注入检测（双层：正则快路 + LLM 分类器慢路）
    result = await sanitize_user_input_async(user_input)
    ctx.sanitize_result = result
    if result.is_rejected:
        ctx.is_input_blocked = True
        ctx.block_reason = result.reason
        logger.warning(
            "[pipeline] 输入被拒绝 request_id=%s reason=%s violations=%s",
            request_id, result.reason, result.violations,
        )
    else:
        ctx.safe_input = result.sanitized_text if result.has_warning else user_input

    return ctx


async def exit_request_pipeline(ctx: RequestPipelineContext) -> None:
    """请求出口：清理 ContextVar。"""
    clear_request_trace_context()


# ======================================================================
# 健康检查接口
# ======================================================================

async def get_enterprise_health() -> dict:
    """返回所有企业级模块的健康状态。用于 /health 端点。"""
    return {
        "initialized": _Globals._initialized,
        "modules": {
            "tracing": "ok" if _Globals._initialized else "not_initialized",
            "semantic_cache": (
                _Globals.semantic_cache.get_stats()
                if _Globals.semantic_cache else "disabled"
            ),
            "model_router": (
                await _Globals.model_router.get_stats()
                if _Globals.model_router else "disabled"
            ),
            "output_validator": "ok" if _Globals.output_validator else "disabled",
            "stream_resume": (
                await _Globals.stream_resume_store.get_stats()
                if _Globals.stream_resume_store else "disabled"
            ),
            "rbac_roles": len(_Globals.rbac_policy._roles) if _Globals.rbac_policy else 0,
        },
    }
