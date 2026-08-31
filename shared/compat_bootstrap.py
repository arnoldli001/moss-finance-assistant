from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# 注册别名：(new_module_fullname, old_alias_fullname) 列表
# ---------------------------------------------------------------------------
_ALIAS_PAIRS: List[Tuple[str, str]] = [
    # config 子模块（config 顶层目录真实存在，不再整体 set alias "shared.config"→"config"，
    # 否则磁盘 config/__init__.py 不会执行，其子模块 config.rbac_policy.json 路径定位也会错）
    ("shared.config.constants", "config.constants"),

    # adapter → shared/llm_client 子模块（adapter 顶层真实目录保留走磁盘 __init__.py，防止 adapter/*.py 重导）
    ("shared.llm_client.ollama_client", "adapter.ollama_client"),
    ("shared.llm_client.stream_adapters", "adapter.stream_adapters"),

    # tools → shared/data_sources 子模块（tools 顶层必须保留真实磁盘包：tools/markdown_tools.py 等 stub 要能找到）
    ("shared.data_sources.web_search", "tools.tavily_tool"),
    ("shared.data_sources.zhishixingqiu", "tools.zsxq_tool"),
    ("shared.data_sources.ima_knowledge", "tools.ragflow_tools"),
    ("shared.data_sources.local_sql", "tools.db_tools"),
    ("shared.data_sources.MyRAGFlow", "tools.MyRAGFlow"),
    ("shared.utils.stock_matcher", "tools.stock_matcher"),

    # agent.actor / agent.actors → shared/actors
    ("shared.actors.actor_base", "agent.actor_base"),
    ("shared.actors.actor_persistence", "agent.actor_persistence"),
    ("shared.actors", "agent.actors"),
    ("shared.actors.session_registry_actor", "agent.actors.session_registry_actor"),
    ("shared.actors.circuit_breaker_actor", "agent.actors.circuit_breaker_actor"),
    ("shared.actors.connection_manager_actor", "agent.actors.connection_manager_actor"),
    ("shared.actors.slo_monitor_actor", "agent.actors.slo_monitor_actor"),

    # agent.llm / model_router / tool_router / prompts / scheduler → 子模块级别名
    # （agent 顶层真实包必须来自磁盘，保证 agent/main_agent.py stub 能被 Python 加载）
    ("shared.llm_client.deepseek_client", "agent.llm"),
    ("shared.llm_client.model_router", "agent.model_router"),
    ("shared.llm_client.tool_router", "agent.tool_router"),
    ("agents.analyst.prompts_legacy", "agent.prompts"),
    ("orchestration.scheduler.scheduler", "agent.scheduler"),

    # agent submodules (治理层) → governance/guardrails 子模块级别名
    ("governance.guardrails.circuit_breaker", "agent.circuit_breaker"),
    ("governance.guardrails.error_classifier", "agent.error_classifier"),
    ("governance.guardrails.degradation_chain", "agent.degradation_chain"),
    ("governance.guardrails.hallucination_guard", "agent.hallucination_guard"),
    ("governance.guardrails.output_validator", "agent.output_validator"),
    ("governance.guardrails.maker_checker", "agent.maker_checker"),
    ("governance.guardrails.semantic_cache", "agent.semantic_cache"),
    ("agents.analyst.enterprise_hooks", "agent.enterprise_hooks"),

    # agent (memory / context_engineer) → agents/reasoning / analyst
    ("agents.reasoning.memory_manager", "agent.memory_manager"),
    ("agents.reasoning.context_engineer_legacy", "agent.context_engineer"),

    # agent observability → governance.monitor
    ("governance.monitor.tracing", "agent.observability.tracing"),

    # governance.monitor → agent (slo/trace/stream_resume/feedback)
    ("governance.monitor.slo_monitor", "agent.slo_monitor"),
    ("governance.monitor.trace", "agent.trace"),
    ("governance.monitor.stream_resume", "agent.stream_resume"),
    ("governance.feedback.feedback_handler", "agent.feedback_handler"),

    # api.middleware → governance 子模块级（api 顶层真实目录保留）
    ("governance.logger.audit_logger", "api.middleware.audit_logger"),
    ("governance.guardrails.prompt_sanitizer", "api.middleware.prompt_sanitizer"),
    ("governance.guardrails.rbac", "api.middleware.rbac"),

    # cache → orchestration.skills 子模块级
    ("orchestration.skills.stock_cache", "cache.stock_cache"),
    ("orchestration.skills.hot_stock_warmup", "cache.hot_stock_warmup"),

    # main agent → agents/analyst（agent.main_agent 文件级 stub 也同时存在，双保险）
    ("agents.analyst.agent", "agent.main_agent"),
]


def _ensure_module_loaded(fullname: str) -> bool:
    """try import a module; return True on success."""
    if fullname in sys.modules:
        return True
    try:
        importlib.import_module(fullname)
        return True
    except Exception:
        # 有些模块是"引用旧符号名"才会用到（比如 agent/subagents 根本不存在），失败不阻塞
        return False


def _ensure_parent_packages(old_alias: str) -> None:
    """为多级 alias 创建空的父包 **只在磁盘目录不存在时**。

    关键规则：若 `parent` 对应的真实目录包存在（磁盘有 __init__.py 或 目录），
    绝对不要用空 ModuleType 抢占 sys.modules[parent]——会导致 Python 跳过磁盘
    agent/__init__.py 的 compat bootstrap + stub 链，引发 ImportError (unknown location)。
    只对 `api.middleware` 这种"子目录包"（父级 api/ 真实存在但子目录可能清空）
    或 `config.sub.subsub` 这种不存在的多级才创建 namespace stub。
    """
    import os as _os
    parts = old_alias.split(".")
    # 项目根目录（shared/compat_bootstrap.py 的父目录的父目录 = 项目根）
    _project_root = Path(__file__).resolve().parents[1]

    for i in range(1, len(parts)):
        parent = ".".join(parts[:i])
        if parent in sys.modules:
            continue
        parent_path = _project_root / Path(*parts[:i])
        # 磁盘存在目录 或 存在 .py 文件（当作包/模块）→ 不创建 fake；交给 Python 正常 import
        if parent_path.exists() and (parent_path.is_dir() or parent_path.suffix == ".py"):
            continue
        if (parent_path.parent / (parent_path.name + ".py")).exists():
            continue
        # 确实不存在 → 创建空的 namespace module stub
        m = types.ModuleType(parent)
        m.__path__ = []  # type: ignore
        sys.modules[parent] = m


_ALREADY_BOOTSTRAPPED = False


def bootstrap() -> None:
    """执行兼容别名注册。幂等，多次调用安全。"""
    global _ALREADY_BOOTSTRAPPED
    if _ALREADY_BOOTSTRAPPED:
        return
    _ALREADY_BOOTSTRAPPED = True

    for new_name, old_name in _ALIAS_PAIRS:
        # 先 import 新模块（失败就跳过这个别名，不抛）
        ok = _ensure_module_loaded(new_name)
        if not ok or new_name not in sys.modules:
            continue
        mod = sys.modules[new_name]
        # 再建中间空父包
        _ensure_parent_packages(old_name)
        # 写入旧别名 → 指向同一个模块对象
        sys.modules[old_name] = mod

    # 旧路径 "agent" 主包：把常见的子符号一起 re-export 到 agent 包，保证 `from agent import x` 可行
    if "agent" not in sys.modules:
        _ensure_parent_packages("agent.main_agent")
    # 从 agent.main_agent（已经是 agents.analyst.agent 别名）导出 run_deep_agent 等符号
    if "agent" in sys.modules and isinstance(sys.modules["agent"], types.ModuleType):
        agent_pkg = sys.modules["agent"]
        # 不重复覆盖，避免多次 bootstrap 冲突
        if not getattr(agent_pkg, "__compat_injected__", False):
            # 常见子模块：先在 agent 主包上加属性（让 `agent.actor_base` 这种 `import agent; agent.actor_base` 能走）
            for attr_name in [
                "actor_base", "actor_persistence", "actors",
                "llm", "model_router", "tool_router", "prompts", "scheduler",
                "circuit_breaker", "error_classifier", "degradation_chain",
                "hallucination_guard", "output_validator", "maker_checker",
                "semantic_cache", "enterprise_hooks",
                "memory_manager", "context_engineer",
                "slo_monitor", "trace", "stream_resume",
                "feedback_handler", "main_agent",
            ]:
                try:
                    sub_mod = importlib.import_module(f"agent.{attr_name}")
                    setattr(agent_pkg, attr_name, sub_mod)
                except Exception:
                    pass
            agent_pkg.__compat_injected__ = True  # type: ignore


# bootstrap 默认立即执行（import shared.compat_bootstrap 时生效）
bootstrap()
