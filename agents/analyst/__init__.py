# agents/analyst: DEEPSEEK_V4_FLASH 云端核心分析师 Agent（原 main_agent 全量功能）
# 金融投研最终回答输出：新闻速览(≤200字+利空/利多) + PE/PB/ROE/营收增速估值 + 护城河五维度 + 散户数据
# 包含：治理链（熔断/错误四象限/降级/幻觉/Maker-Checker/输出校验/RBAC）+ PTD + 记忆管理 + Citation 悬停卡片
# 对外入口：run_deep_agent(task_query, session_id, user_id, quiet)
