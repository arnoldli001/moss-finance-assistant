# governance/monitor: SLO + Trace + 可观测性 + Stream Resume 断点续传
#   slo_monitor.py — 硬上限（150s 单任务 / 1M tokens）检查
#   trace.py       — request_id 贯穿的 Trace 埋点
#   tracing.py     — OTel 兼容 observability（迁移自 agent/observability/）
#   stream_resume.py — SSE 环形缓冲 + Last-Event-ID 回放
