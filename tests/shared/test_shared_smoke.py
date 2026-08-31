"""tests/shared 子目录最小 smoke：验证跨层 Shared（Models/DataSources/Actors/Config）import 闭合并一致。"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def test_shared_import_chain_and_alias_consistency():
    """shared.actors + shared.config + shared.data_sources 新路径 & compat 别名一致。"""
    # 新路径（Shared 层真实文件）
    from shared.actors.session_registry_actor import SessionRegistryActor  # noqa: F401
    from shared.config.constants import OLLAMA_DEFAULT_BASE_URL  # 全局模型网关 URL，保证 config 常量可见
    # 新 shared.data_sources.zhishixingqiu 必须单例等价于 tools.zsxq_tool（ZSXQ 登录修复正确性锚点）
    import importlib
    s = importlib.import_module("shared.data_sources.zhishixingqiu")
    t = importlib.import_module("tools.zsxq_tool")
    assert s is t, "shared.data_sources.zhishixingqiu 与 tools.zsxq_tool 必须是同一个模块对象"
