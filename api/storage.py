#coding = utf-8
"""薄壳 alias：从真实实现 interfaces.api.storage 重导出全部公共 API。

薄壳目的：维持旧引用路径 `from api import storage` / `from api.storage import ...`
的兼容性，把调用直接转发到单真相源 interfaces/api/storage.py。

P0 升级新增的 password_hash / role / last_login 三列、verify/update_password、
assign_role 等函数，全部通过薄壳透明暴露，调用方无需修改 import 路径。
"""

# --- 薄壳同一性声明：以下 * 导入与 interfaces.api.storage 模块 100% 同址 ---
from interfaces.api.storage import *  # noqa: F401,F403

# Thin Shell 校验：确保 import 的目标对象与真实模块是同一引用
# （否则说明 sys.modules alias 未生效或薄壳导入失败）
import sys as _sys
_true = _sys.modules.get("interfaces.api.storage")
if _true is None:  # pragma: no cover - 极端保护
    raise ImportError("thin-shell api.storage: interfaces.api.storage 尚未加载")

# 将本模块的 __dict__ 中每个公共符号与真实模块做同一性验证（完整 public API 清单）
_self = _sys.modules[__name__]
for _name in ("init_db", "get_or_create_user", "get_user",
              "verify_user_password", "update_password", "assign_role",
              "touch_last_login", "create_session", "ensure_session",
              "get_session", "list_sessions", "verify_session_owner",
              "delete_session", "generate_default_title",
              "update_session_title", "touch_session"):
    _mine = getattr(_self, _name, None)
    _his = getattr(_true, _name, None)
    if _mine is None or _his is None:
        # 允许将来新增缺省可选 API 未对齐时报 warning 不 raise
        import warnings as _w
        _w.warn(f"thin-shell api.storage: {_name} not found (self={_mine is not None}, real={_his is not None})")
        continue
    assert _mine is _his, (
        f"thin-shell identity FAIL: storage.{_name} 与真实模块不同对象，"
        f"可能 compat_bootstrap 重复注册或模块加载顺序异常")
