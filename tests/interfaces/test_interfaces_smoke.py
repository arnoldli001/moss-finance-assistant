"""tests/interfaces 子目录最小 smoke：验证接入层 REST/SSE 接口可正常 import 并能创建 FastAPI app。"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def test_interfaces_app_and_middleware():
    """FastAPI app 对象 + SSE Router + 关键治理模块能正常 import（不启动 lifespan）。"""
    from interfaces.api.server import app  # noqa: F401
    from interfaces.api.stream_bus import StreamEventBus, get_stream_bus_sync  # noqa: F401
    # 三个治理模块（compat 层 api.middleware.XXX 会落到 governance.XXX 真实实现）—— 只验证模块级 import 闭合 + 导出最核心符号
    from api.middleware import audit_logger as audit_mod  # type: ignore  # noqa: F401
    from api.middleware.rbac import RBACMiddleware  # type: ignore  # noqa: F401
    from api.middleware.prompt_sanitizer import SanitizeResult, sanitize_user_input  # type: ignore  # noqa: F401
    # 应用可调用（FastAPI 实例本身就是 ASGI callable）
    assert callable(app)
    # 方案一两 POST 接口必须已挂载到路由表
    routes = {getattr(r, "path", None) for r in getattr(app, "routes", [])}
    assert "/api/task" in routes, "方案一非流式 POST /api/task 路由必须已注册"
    assert "/api/task/stream" in routes, "方案一流式 POST /api/task/stream 路由必须已注册"
    # RBACMiddleware 真实类（最常用的 HTTP 中间件），确保类对象可实例化检查
    assert RBACMiddleware is not None and callable(getattr(RBACMiddleware, "dispatch", None))
