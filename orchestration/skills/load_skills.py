# orchestration/skills/load_skills.py —— 技能加载（包装 agents.analyst.enterprise_hooks 中的相关函数）
# 保守迁移：直接从原 enterprise_hooks 重导出，对外 API 不变。
from agents.analyst.enterprise_hooks import (  # noqa: F401
    discover_skill_packages,
    load_skill_context,
    attach_skill_tools_to_agent,
    get_skill_summary_block,
    SKILLS_ROOT,
)
