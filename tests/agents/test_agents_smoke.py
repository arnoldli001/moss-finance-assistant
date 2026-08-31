"""tests/agents 子目录最小 smoke：验证 Agent 层核心模块可正常 import（不启动、不跑模型）。"""
from __future__ import annotations

import sys
from pathlib import Path

# python tests/agents/smoke.py 直接运行时的项目根注入
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def test_agents_import_chain():
    """核心 Agents + prompts/context_engineer compat 层双路径都能 import。"""
    # 新目录（AGENTS.md 架构：5 专业 Agent 位置）
    from agents.analyst.agent import run_deep_agent as _  # noqa: F401
    from agents.reasoning.memory_manager import MemoryManager  # noqa: F401
    # compat 层短路径（保证 `from agent.main_agent` 不挂）
    from agent.prompts import format_prompt  # noqa: F401
    from agent.context_engineer import get_context_engineer  # type: ignore  # noqa: F401
    # 能运行到这里说明 import chain 闭合
    assert callable(get_context_engineer)
