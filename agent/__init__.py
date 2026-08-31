# compat bootstrap: 保证 from agent.main_agent import ... 能走 新架构 agents.analyst.agent
import shared.compat_bootstrap as _cb  # noqa: F401  (side-effect: sys.modules alias inject)
import sys as _sys

def _ensure_attr_chain(root_alias: str) -> None:
    """扫描已注册的 sys.modules，把 'root.sub' 这种 key 对应的对象 setattr 到 root 模块上。"""
    root_mod = _sys.modules.get(root_alias)
    if root_mod is None:
        return
    prefix = root_alias + "."
    for k, v in list(_sys.modules.items()):
        if not k.startswith(prefix) or v is None:
            continue
        sub = k[len(prefix):]
        if "." in sub:
            continue  # 只处理一级子属性（嵌套子包 Python 会自己走下一层 import）
        if not hasattr(root_mod, sub):
            try:
                setattr(root_mod, sub, v)
            except Exception:
                pass

_ensure_attr_chain("agent")
_ensure_attr_chain("config")
_ensure_attr_chain("tools")
_ensure_attr_chain("adapter")
_ensure_attr_chain("api")
_ensure_attr_chain("cache")


# ----------------------------------------------------------------------
# __getattr__ 延迟加载（彻底解决循环 import：agent.main_agent / agents.*
# 引用 agent.xxx 时才真实 import 对应新模块；在 __init__.py 顶不提前加载）
# ----------------------------------------------------------------------

_LAZY_ALIASES = {
    # agent.<sub>   ->  (new_module_fullname)
    "main_agent":         "agents.analyst.agent",
    "llm":                "shared.llm_client.deepseek_client",
    "prompts":            "agents.analyst.prompts_legacy",
    "model_router":       "shared.llm_client.model_router",
    "tool_router":        "shared.llm_client.tool_router",
    "scheduler":          "orchestration.scheduler.scheduler",
    "actor_base":         "shared.actors.actor_base",
    "actor_persistence":  "shared.actors.actor_persistence",
    "observability":      "governance.monitor.tracing",  # 简化：observability 直接指向 tracing 模块
    "circuit_breaker":    "governance.guardrails.circuit_breaker",
    "error_classifier":   "governance.guardrails.error_classifier",
    "degradation_chain":  "governance.guardrails.degradation_chain",
    "hallucination_guard": "governance.guardrails.hallucination_guard",
    "output_validator":   "governance.guardrails.output_validator",
    "maker_checker":      "governance.guardrails.maker_checker",
    "semantic_cache":     "governance.guardrails.semantic_cache",
    "enterprise_hooks":   "agents.analyst.enterprise_hooks",
    "memory_manager":     "agents.reasoning.memory_manager",
    "context_engineer":   "agents.reasoning.context_engineer_legacy",
    "slo_monitor":        "governance.monitor.slo_monitor",
    "trace":              "governance.monitor.trace",
    "stream_resume":      "governance.monitor.stream_resume",
    "feedback_handler":   "governance.feedback.feedback_handler",
    # actors 子包延迟加载（import agent.actors 再触发）
    "actors":             "shared.actors",
}


def __getattr__(name: str):
    """延迟加载：agent.<name> 不存在时，按 _LAZY_ALIASES 找到新模块，注册别名并返回。"""
    if name in _LAZY_ALIASES:
        new_name = _LAZY_ALIASES[name]
        import importlib as _importlib
        try:
            mod = _importlib.import_module(new_name)
        except Exception as _e:
            raise ModuleNotFoundError(
                f"agent.{name}（懒加载别名 → {new_name}）真实 import 失败：{type(_e).__name__}: {_e}"
            ) from None
        # 写入 sys.modules 别名，下次直接走快路径
        _sys.modules[f"agent.{name}"] = mod
        # 同时 setattr 给 agent 包对象（方便 dir / hasattr）
        try:
            setattr(_sys.modules["agent"], name, mod)
        except Exception:
            pass
        return mod
    raise AttributeError(f"module 'agent' has no attribute {name!r}")


# 原注释（保留）：
# agent 包
