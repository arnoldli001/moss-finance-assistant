# -*- coding: utf-8 -*-
"""
企业级增强模块接入示例，本文件给出"最小侵入"的接入示例。
实际接入时把以下代码片段复制到对应位置：
  1) app startup / shutdown 钩子
  2) WebSocket handler 或 FastAPI route 入口
  3) main_agent.run_agent 内部调用 LLM 处
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ======================================================================
# 示例 1：FastAPI 应用启动/关闭钩子
# ======================================================================
EXAMPLE_FASTAPI_STARTUP = """
# === 在 api/server.py 顶部加导入 ===
from agent.enterprise_hooks import (
    init_enterprise_extensions,
    shutdown_enterprise_extensions,
    enter_request_pipeline,
    exit_request_pipeline,
    get_enterprise_health,
)
from api.middleware.rbac import RBACMiddleware


# === 在 create_app() 中注册中间件（在限流中间件之后）===
def create_app() -> FastAPI:
    app = FastAPI(...)
    # 1) RBAC 中间件
    app.add_middleware(RBACMiddleware)
    # 2) 限流中间件（已有）
    # app.add_middleware(RateLimitMiddleware, ...)
    return app


# === 注册 startup / shutdown 钩子 ===
@app.on_event("startup")
async def _startup_enterprise():
    await init_enterprise_extensions()
    logger.info("企业级增强模块已启动")


@app.on_event("shutdown")
async def _shutdown_enterprise():
    await shutdown_enterprise_extensions()
    logger.info("企业级增强模块已关闭")


# === 新增健康检查端点 ===
@app.get("/health/enterprise")
async def health_enterprise():
    return await get_enterprise_health()
"""


# ======================================================================
# 示例 2：WebSocket chat handler 改造
# ======================================================================
EXAMPLE_WEBSOCKET_HANDLER = """
# === 改造前 ===
@app.websocket("/ws")
async def ws_handle(websocket: WebSocket):
    await websocket.accept()
    while True:
        user_input = await websocket.receive_text()
        answer = await run_agent(user_input, session_id, user_id)
        await websocket.send_text(answer)


# === 改造后 ===
@app.websocket("/ws")
async def ws_handle(websocket: WebSocket):
    await websocket.accept()
    while True:
        user_input = await websocket.receive_text()
        user_id = websocket.headers.get("X-User-Id", "anonymous")
        session_id = websocket.headers.get("X-Session-Id", "")
        request_id = uuid.uuid4().hex

        # 1) 进入企业级流水线（注入 OTel / RBAC / prompt 检测）
        ctx = await enter_request_pipeline(
            user_input=user_input,
            user_id=user_id,
            session_id=session_id,
            request_id=request_id,
        )
        try:
            # 2) prompt 注入拦截
            if ctx.is_input_blocked:
                await websocket.send_text(f"⚠️ 输入被拒绝：{ctx.block_reason}")
                continue

            # 3) 调用 LLM（自动走 语义缓存 + 模型路由 + OTel）
            msg_id = uuid.uuid4().hex

            # 3a) 启动流式续传会话（断线重连可续）
            await ctx.begin_stream(
                msg_id=msg_id,
                llm_context={"messages": [{"role": "user", "content": ctx.safe_input}]},
            )

            # 3b) 流式输出（每个 chunk 同步写 store）
            answer_parts = []
            async for chunk in stream_agent(ctx.safe_input, session_id, user_id):
                await ctx.append_stream(msg_id, chunk)
                await websocket.send_text(chunk)
                answer_parts.append(chunk)
            answer = "".join(answer_parts)

            # 3c) 完成流式
            await ctx.complete_stream(msg_id, final_output=answer)

            # 4) 输出校验（自动拦截违规）
            final = await ctx.validate_output(answer, category="news_summary")
            if final != answer:
                # 被拦截，发修正后的内容
                await websocket.send_text(f"\\n[校验修正]\\n{final}")

        finally:
            await exit_request_pipeline(ctx)
"""


# ======================================================================
# 示例 3：main_agent.run_agent 内部改造
# ======================================================================
EXAMPLE_MAIN_AGENT = """
# === 在 agent/main_agent.py 顶部加导入 ===
from agent.enterprise_hooks import _Globals
from agent.observability import agent_span, llm_span


# === 在 run_agent 函数内 ===
async def run_agent(user_input: str, session_id: str, user_id: str) -> str:
    # 1) 用 OTel span 包裹整个流程
    with agent_span("agent.run", user_id=user_id, session_id=session_id) as span:
        span.set_attribute("input_chars", len(user_input))

        # 2) 检查语义缓存（如果上游未做）
        if _Globals.semantic_cache:
            cached = await _Globals.semantic_cache.get(user_input)
            if cached:
                span.set_attribute("cache_hit", True)
                return cached

        # 3) 调用 LLM（带模型路由 + 自动降级）
        if _Globals.model_router:
            resp, decision = await _Globals.model_router.call_with_fallback(
                prompt=user_input,
                llm_caller_fn=_call_llm_with_model,  # async def(prompt, model) -> str
                user_id=user_id,
            )
            span.set_attribute("model", decision.model)
            span.set_attribute("cost_usd", decision.estimated_cost_usd)
            answer = resp if isinstance(resp, str) else getattr(resp, "content", str(resp))
        else:
            answer = await _call_llm(user_input)

        # 4) 写入语义缓存
        if _Globals.semantic_cache and answer:
            await _Globals.semantic_cache.set(user_input, answer)

        # 5) 输出校验（自动拦截 + 可重试）
        if _Globals.output_validator:
            from agent.output_validator import ValidationContext
            val_ctx = ValidationContext(
                user_input=user_input,
                agent_output=answer,
                category="news_summary",
            )
            answer = await _Globals.output_validator.validate_and_maybe_retry(
                ctx=val_ctx,
                llm_caller_fn=lambda c: _call_llm(c.user_input),
                request_id=session_id,
            )

        return answer
"""


# ======================================================================
# 示例 4：Actor 状态持久化接入
# ======================================================================
EXAMPLE_ACTOR_PERSISTENCE = """
# === 在 agent/actor_base.py 或启动脚本中 ===
from agent.actor_persistence import SnapshotCoordinator, ActorSnapshotter


# 应用启动时注册需要快照的 Actor
async def setup_actor_snapshots():
    coordinator = SnapshotCoordinator()

    # 注册所有需要持久化的 Actor
    coordinator.register(
        actor_id="session_registry",
        actor=session_registry_actor,
        get_state_fn=lambda a: a.state,
    )
    coordinator.register(
        actor_id="connection_manager",
        actor=connection_manager_actor,
        get_state_fn=lambda a: a.state,
    )
    coordinator.register(
        actor_id="circuit_breakers",
        actor=circuit_breaker_registry_actor,
        get_state_fn=lambda a: a.state,
    )

    # 启动时恢复
    if ACTOR_SNAPSHOT_AUTO_RESTORE:
        await coordinator.restore_all()

    # 保存到全局，便于 shutdown 时调用
    app.state.snapshot_coordinator = coordinator


# Actor 消息处理时调用快照
async def actor_handle_with_snapshot(actor_id: str, actor, env, state):
    new_state, reply = await actor.handle_message(env, state)

    # 处理 N 条消息后自动快照
    snapshotter = app.state.snapshotter
    await snapshotter.record_and_maybe_snapshot(actor_id, new_state)

    return new_state, reply
"""


# ======================================================================
# 示例 5：CI 集成评估
# ======================================================================
EXAMPLE_CI_EVAL = """
# === 在 .github/workflows/ci.yml 或类似 CI 配置 ===
# - name: Run LLM eval (regression)
#   run: |
#     pip install -r requirements.txt
#     # 启动 server（后台）
#     python -m api.server &
#     SERVER_PID=$!
#     # 等待 server 就绪
#     sleep 5
#     # 跑评估（失败则 CI 阻断）
#     python -m tests.eval.run_eval --agent-url http://localhost:8000/chat
#     EVAL_EXIT=$?
#     # 关闭 server
#     kill $SERVER_PID
#     exit $EVAL_EXIT

# === 或本地手动跑 ===
# python -m tests.eval.run_eval --category valuation --limit 3
"""


def print_examples() -> None:
    """打印所有接入示例。"""
    print("=" * 70)
    print("企业级增强模块接入示例")
    print("=" * 70)
    for name, code in [
        ("示例1 FastAPI 启动/关闭钩子", EXAMPLE_FASTAPI_STARTUP),
        ("示例2 WebSocket handler 改造", EXAMPLE_WEBSOCKET_HANDLER),
        ("示例3 main_agent 改造", EXAMPLE_MAIN_AGENT),
        ("示例4 Actor 状态持久化", EXAMPLE_ACTOR_PERSISTENCE),
        ("示例5 CI 集成评估", EXAMPLE_CI_EVAL),
    ]:
        print(f"\n{'='*70}\n{name}\n{'='*70}")
        print(code)


if __name__ == "__main__":
    print_examples()
