# orchestration/workflows: 显式 DAG 工作流（LangGraph 风格）
#   analysis_workflow.py — 核心工作流：Router 分支 → 盘前缓存命中短路 / 4源 asyncio.gather(180s) / 2源并发 →
#                          Aggregator 去重整合 → 级联路由到对应 Agent（coder/reasoning/analyst/vision）
