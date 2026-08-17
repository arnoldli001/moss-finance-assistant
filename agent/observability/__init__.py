# -*- coding: utf-8 -*-
"""
agent.observability 包：分布式追踪 / 指标 / 日志的统一接入层。
"""
from agent.observability.tracing import (
    init_tracing,
    shutdown_tracing,
    get_tracer,
    agent_span,
    llm_span,
    tool_span,
    set_request_trace_context,
    clear_request_trace_context,
)
