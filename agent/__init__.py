# agent 包 —— 主 Agent 协调层：
# - subagents/：三个子 Agent（network_search / database_query / knowledge_base）
# - request_context.py：跨层请求上下文与取消令牌（RequestContext / CancellationToken）
# - skill_manager.py：SKILL 自动加载管理器（Layer 4 Loop Engineering）
# - state_store.py：任务状态存储（TaskState / StateStore）
