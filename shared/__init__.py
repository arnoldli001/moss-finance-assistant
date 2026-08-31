# Shared：跨层共享，4 层架构全部可引用。
# 子模块：
#   models/       — Pydantic 统一模型（请求/响应/路由/检索/流式）
#   config/       — constants.py、rbac_policy.json、prompts.yml、prompt 模板
#   llm_client/   — Ollama + DeepSeek 云端 6 模型统一封装 + model_router + stream_adapters + tool_router
#   data_sources/ — 4 数据源：web_search(Tavily) / zhishixingqiu(ZSXQ Playwright) / ima_knowledge(RAGFlow) / local_sql(MySQL) + stock_matcher
#   aggregator/   — 独立信息池：去重/可靠性/共享记忆池/Prompt 上下文段
#   actors/       — Actor Model：actor_base + session_registry / circuit_breaker / connection_manager / slo_monitor + persistence + observability tracing
#   utils/        — 通用工具：path_utils / word_converter / markdown / pdf / upload read
