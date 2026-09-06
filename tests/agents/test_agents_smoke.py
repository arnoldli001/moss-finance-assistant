"""tests/agents 子目录最小 smoke：验证 Agent 层核心模块可正常 import（不启动、不跑模型）。"""
from __future__ import annotations

import sys
from pathlib import Path

# python tests/agents/smoke.py 直接运行时的项目根注入
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def test_agents_import_chain():
    """核心 Agents + prompts/context_engineer legacy 模块都能 import。"""
    # 新目录（AGENTS.md 架构：5 专业 Agent 位置）
    from agents.analyst.agent import run_deep_agent as _  # noqa: F401
    from agents.reasoning.memory_manager import MemoryManager  # noqa: F401
    # agents 层 legacy 模块（prompts_legacy / context_engineer_legacy）
    from agents.analyst.prompts_legacy import format_prompt  # noqa: F401
    from agents.reasoning.context_engineer_legacy import get_context_engineer  # type: ignore  # noqa: F401
    # 能运行到这里说明 import chain 闭合
    assert callable(get_context_engineer)
