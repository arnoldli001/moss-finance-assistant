# -*- coding: utf-8 -*-
"""
OpenTelemetry 分布式追踪接入层：SLO 指标但无分布式追踪，跨Agent调用链路无法可视化。本模块封装 OTel SDK，提供：
  - init_tracing(): 应用启动时调用，注册 TracerProvider + 导出器（console / otlp）
  - agent_span / llm_span / tool_span: 三个上下文管理器装饰器
  - set_request_trace_context(): 把 RequestContext 的 request_id 升级为 trace_id，全链路贯通

设计要点：
  1) 优雅降级：未安装 opentelemetry-* 包时，回退到 no-op 实现，业务代码不报错
  2) 采样可调：通过 OTEL_TRACE_SAMPLE_RATIO 控制生产环境采样率
  3) 与 RequestContext 集成：每次请求入口注入 trace_id，下游 LLM/工具调用自动继承

典型用法：
    # 应用启动时
    from agent.observability import init_tracing, shutdown_tracing, agent_span, llm_span
    init_tracing()

    # 主流程入口（FastAPI middleware / WebSocket handler）
    set_request_trace_context(request_id="abc123", user_id="u1", session_id="s1")

    # 业务代码
    with agent_span("pre_market_news") as span:
        span.set_attribute("user_id", "u1")
        with llm_span("deepseek", prompt_chars=500) as lspan:
            result = await call_deepseek(...)
            lspan.set_attribute("tokens", result.usage.total_tokens)

    # 应用关闭
    shutdown_tracing()
"""
from __future__ import annotations

import contextlib
import functools
import logging
import os
import threading
import time
import uuid
from contextvars import ContextVar
from typing import Any, Callable, Dict, Optional

from config.constants import (
    OTEL_SERVICE_NAME,
    OTEL_SERVICE_VERSION,
    OTEL_EXPORTER_TYPE,
    OTEL_OTLP_ENDPOINT,
    OTEL_TRACE_SAMPLE_RATIO,
    OTEL_LLM_SPAN_NAME_PREFIX,
    OTEL_TOOL_SPAN_NAME_PREFIX,
    OTEL_AGENT_SPAN_NAME,
)

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# 全局 tracer 句柄（init 后赋值，未 init 时为 None，自动走 no-op 路径）
# ----------------------------------------------------------------------
_tracer: Any = None
_provider: Any = None
_init_lock = threading.Lock()
_initialized: bool = False

# ----------------------------------------------------------------------
# 请求级 trace context：与 RequestContext 对齐，用于跨 span 关联
# ----------------------------------------------------------------------
_current_trace: ContextVar[Optional[Dict[str, str]]] = ContextVar(
    "otel_current_trace", default=None
)


# ======================================================================
# No-op 实现：未安装 opentelemetry 或未 init 时的回退路径
# ======================================================================

class _NoOpSpan:
    """无操作 Span，仅记录属性到内存（用于本地调试）。"""
    def __init__(self, name: str):
        self.name = name
        self.start_ts = time.time()
        self.attrs: Dict[str, Any] = {}
        self._exc_recorded = False

    def set_attribute(self, key: str, value: Any) -> None:
        self.attrs[key] = value

    def set_attributes(self, attrs: Dict[str, Any]) -> None:
        self.attrs.update(attrs)

    def record_exception(self, exc: BaseException) -> None:
        self._exc_recorded = True
        self.attrs["exception.type"] = type(exc).__name__
        self.attrs["exception.message"] = str(exc)

    def set_status(self, status: str, description: str = "") -> None:
        self.attrs["otel.status_code"] = status
        if description:
            self.attrs["otel.status_description"] = description

    def end(self) -> None:
        self.end_ts = time.time()
        if logger.isEnabledFor(logging.DEBUG):
            dur_ms = (self.end_ts - self.start_ts) * 1000
            logger.debug("[noop-span] %s dur=%.1fms attrs=%s", self.name, dur_ms, self.attrs)

    def __enter__(self) -> "_NoOpSpan":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_val is not None:
            self.record_exception(exc_val)
            self.set_status("ERROR", str(exc_val))
        else:
            self.set_status("OK")
        self.end()


class _NoOpTracer:
    """无操作 Tracer，未安装 OTel 时回退。"""
    def start_as_current_span(self, name: str, **kwargs: Any) -> _NoOpSpan:
        return _NoOpSpan(name)

    def start_span(self, name: str, **kwargs: Any) -> _NoOpSpan:
        return _NoOpSpan(name)


# ======================================================================
# 初始化与关闭
# ======================================================================

def init_tracing() -> None:
    """应用启动时调用一次。多次调用幂等（用 _init_lock 保护）。

    根据 OTEL_EXPORTER_TYPE 选择导出方式：
      - console: 控制台打印（开发调试用）
      - otlp: 通过 OTLP gRPC/HTTP 上报到 Jaeger/Tempo/OTel Collector
      - none: 仅注册 TracerProvider，不上报（用于采样但本地 span）
    """
    global _tracer, _provider, _initialized
    with _init_lock:
        if _initialized:
            return
        try:
            from opentelemetry import trace
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.sampling import (
                ParentBased, TraceIdRatioBased,
            )
        except ImportError:
            logger.info(
                "[tracing] opentelemetry-sdk 未安装，回退到 no-op tracer。"
                "安装命令: pip install opentelemetry-api opentelemetry-sdk "
                "opentelemetry-exporter-otlp"
            )
            _tracer = _NoOpTracer()
            _initialized = True
            return

        resource = Resource.create({
            "service.name": OTEL_SERVICE_NAME,
            "service.version": os.environ.get("MOSS_VERSION", OTEL_SERVICE_VERSION),
            "deployment.environment": os.environ.get("ENV", "dev"),
        })

        sampler = ParentBased(TraceIdRatioBased(OTEL_TRACE_SAMPLE_RATIO))
        provider = TracerProvider(resource=resource, sampler=sampler)

        exporter_type = OTEL_EXPORTER_TYPE.lower()
        if exporter_type == "otlp":
            try:
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                    OTLPSpanExporter,
                )
                provider.add_span_processor(
                    _get_processor_class()(OTLPSpanExporter(endpoint=OTEL_OTLP_ENDPOINT))
                )
                logger.info("[tracing] OTLP 导出已启用，endpoint=%s", OTEL_OTLP_ENDPOINT)
            except ImportError:
                logger.warning(
                    "[tracing] opentelemetry-exporter-otlp 未安装，回退到 console 导出。"
                )
                _install_console_exporter(provider)
        elif exporter_type == "console":
            _install_console_exporter(provider)
        else:
            logger.info("[tracing] 导出器类型=none，span 仅本地不导出")

        trace.set_tracer_provider(provider)
        _provider = provider
        _tracer = trace.get_tracer(OTEL_SERVICE_NAME)
        _initialized = True
        logger.info(
            "[tracing] 初始化完成 exporter=%s sample_ratio=%.2f",
            exporter_type, OTEL_TRACE_SAMPLE_RATIO,
        )


def _install_console_exporter(provider: Any) -> None:
    """安装 ConsoleSpanExporter（开发调试用）。"""
    try:
        from opentelemetry.sdk.trace.export import (
            BatchSpanProcessor, ConsoleSpanExporter,
        )
        provider.add_span_processor(
            BatchSpanProcessor(ConsoleSpanExporter())
        )
    except ImportError:
        logger.warning("[tracing] ConsoleSpanExporter 安装失败")


def _get_processor_class() -> Any:
    """返回 BatchSpanProcessor（生产用）的类引用。"""
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    return BatchSpanProcessor


def shutdown_tracing() -> None:
    """应用退出时调用，flush 剩余 span。"""
    global _provider, _tracer, _initialized
    with _init_lock:
        if _provider is not None:
            try:
                _provider.shutdown()
            except Exception as e:
                logger.warning("[tracing] shutdown 异常: %s", e)
        _provider = None
        _tracer = None
        _initialized = False


def get_tracer() -> Any:
    """获取全局 tracer。未初始化时返回 no-op tracer。"""
    global _tracer
    if _tracer is None:
        return _NoOpTracer()
    return _tracer


# ======================================================================
# 请求级 trace context（与 RequestContext 对齐）
# ======================================================================

def set_request_trace_context(
    request_id: Optional[str] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    thread_id: Optional[str] = None,
) -> None:
    """在请求入口调用，把元数据塞进 ContextVar，所有下游 span 自动继承。

    与 agent.request_context.RequestContext 配合使用——
    RequestContext 管取消/超时，本函数管追踪元数据，两者独立但互补。
    """
    rid = request_id or uuid.uuid4().hex
    ctx_meta = {
        "request.id": rid,
        "user.id": user_id or "anonymous",
        "session.id": session_id or "",
        "thread.id": thread_id or "",
    }
    _current_trace.set(ctx_meta)
    # 同步到 OTel baggage（若安装了）
    try:
        from opentelemetry import baggage
        ctx = baggage.set_baggage("request.id", rid)
        for k, v in ctx_meta.items():
            ctx = baggage.set_baggage(k, v, context=ctx)
    except ImportError:
        pass


def clear_request_trace_context() -> None:
    """请求结束时清理 ContextVar。"""
    _current_trace.set(None)


def _attach_request_attrs(span: Any) -> None:
    """把请求级元数据附加到 span 上（如果存在）。"""
    meta = _current_trace.get()
    if meta:
        for k, v in meta.items():
            try:
                span.set_attribute(k, v)
            except Exception:
                pass


# ======================================================================
# 三个上下文管理器：agent_span / llm_span / tool_span
# ======================================================================

@contextlib.contextmanager
def agent_span(name: str = OTEL_AGENT_SPAN_NAME, **attrs: Any) -> Any:
    """Agent 主流程 span。覆盖整个 run_agent 调用。

    用法：
        with agent_span("pre_market_news", task_type="zsxq") as span:
            span.set_attribute("user_id", user_id)
            result = await run_agent()
            span.set_attribute("result_chars", len(result))
    """
    tracer = get_tracer()
    with tracer.start_as_current_span(name) as span:
        _attach_request_attrs(span)
        if attrs:
            try:
                span.set_attributes(attrs)
            except Exception:
                pass
        yield span


@contextlib.contextmanager
def llm_span(model: str, prompt_chars: int = 0, **attrs: Any) -> Any:
    """LLM 调用 span。在 chat_deepseek / chat_ollama 等位置包裹。

    用法：
        with llm_span("deepseek-chat", prompt_chars=len(prompt)) as span:
            resp = await llm.chat(prompt)
            span.set_attribute("tokens.input", resp.usage.input_tokens)
            span.set_attribute("tokens.output", resp.usage.output_tokens)
            span.set_attribute("tokens.total", resp.usage.total_tokens)
            span.set_attribute("cost_usd", _calc_cost(resp.usage))
    """
    tracer = get_tracer()
    span_name = f"{OTEL_LLM_SPAN_NAME_PREFIX}.{model}"
    with tracer.start_as_current_span(span_name) as span:
        _attach_request_attrs(span)
        try:
            span.set_attribute("llm.model", model)
            span.set_attribute("llm.prompt_chars", prompt_chars)
            if attrs:
                span.set_attributes(attrs)
        except Exception:
            pass
        yield span


@contextlib.contextmanager
def tool_span(tool_name: str, **attrs: Any) -> Any:
    """工具调用 span。在 tools/*.py 的每个工具入口包裹。

    用法：
        with tool_span("internet_search", query=query) as span:
            result = tavily_client.search(query)
            span.set_attribute("results_count", len(result.get("results", [])))
    """
    tracer = get_tracer()
    span_name = f"{OTEL_TOOL_SPAN_NAME_PREFIX}.{tool_name}"
    with tracer.start_as_current_span(span_name) as span:
        _attach_request_attrs(span)
        try:
            span.set_attribute("tool.name", tool_name)
            if attrs:
                span.set_attributes(attrs)
        except Exception:
            pass
        yield span


# ======================================================================
# 装饰器版本（便于给现有函数加 span，不改函数体）
# ======================================================================

def traced_llm_call(model: str) -> Callable:
    """装饰器：给 LLM 调用函数自动加 llm_span。

    用法：
        @traced_llm_call("deepseek-chat")
        async def chat_deepseek(prompt: str) -> str:
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            prompt_chars = 0
            # 粗略提取 prompt 长度
            if args and isinstance(args[0], str):
                prompt_chars = len(args[0])
            with llm_span(model, prompt_chars=prompt_chars):
                return await func(*args, **kwargs)
        return async_wrapper
    return decorator


def traced_tool_call(tool_name: str) -> Callable:
    """装饰器：给工具函数自动加 tool_span。"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            with tool_span(tool_name):
                return await func(*args, **kwargs)
        return async_wrapper
    return decorator
