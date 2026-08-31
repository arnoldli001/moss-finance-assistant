"""tests/orchestration 子目录最小 smoke：验证调度层 DAG/workflow/Scheduler 能正常 import。"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def test_orchestration_import_chain():
    """显式 DAG workflow、错误四象限 loop、Scheduler 都能 import。"""
    from orchestration.workflows.analysis_workflow import (  # noqa: F401
        run_analysis_workflow, WorkflowResult, RouteBranch,
    )
    from orchestration.loop import GLOBAL_SLO_MAX_TASK_SEC  # noqa: F401
    from orchestration.scheduler.scheduler import TaskScheduler, setup_preset_tasks  # type: ignore  # noqa: F401
    # 关键常量可见性（防止 import * stub 没正确 re-export）
    assert RouteBranch.PRE_MARKET_NEWS is not None
    assert WorkflowResult.__name__ == "WorkflowResult"
    assert TaskScheduler is not None and callable(setup_preset_tasks)
    assert GLOBAL_SLO_MAX_TASK_SEC == 150.0  # 对齐 AGENTS.md 150s 硬墙
