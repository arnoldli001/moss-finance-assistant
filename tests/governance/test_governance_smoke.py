"""tests/governance 子目录最小 smoke：验证治理层 Guardrails/Monitor/Feedback 可 import。"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def test_governance_import_chain():
    """断路器 / SLO / MakerChecker / HallucinationGuard + Feedback Handler。"""
    from governance.guardrails.circuit_breaker import (  # noqa: F401
        TimeWindowCircuitBreaker, CircuitState, _set_cb_actor,
    )
    from governance.monitor.slo_monitor import SLOMonitor  # noqa: F401
    from governance.guardrails.maker_checker import MakerChecker  # noqa: F401
    from governance.guardrails.hallucination_guard import HallucinationGuard  # noqa: F401
    from governance.feedback.feedback_handler import FeedbackHandler  # type: ignore  # noqa: F401
    assert CircuitState is not None  # 断路器状态快照类可用
    assert TimeWindowCircuitBreaker is not None
    assert callable(_set_cb_actor)  # server lifespan 注入桥
    assert SLOMonitor is not None
