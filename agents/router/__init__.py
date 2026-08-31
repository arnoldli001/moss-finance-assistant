# agents/router: gemma4:e4b 本地轻量路由 Agent
# 路由规则 = Python 正则/关键词/缓存命中 优先（超高速）+ gemma4 语义兜底（含歧义时）+ 级联降级链
# 输出：shared.models.RouterDecision （RouteBranch 枚举）
