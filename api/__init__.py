# compat bootstrap: api.* → interfaces.api.* / governance.*
import shared.compat_bootstrap as _cb  # noqa: F401
import sys as _sys

def _ensure_attr_chain(root_alias: str) -> None:
    root_mod = _sys.modules.get(root_alias)
    if root_mod is None:
        return
    prefix = root_alias + "."
    for k, v in list(_sys.modules.items()):
        if not k.startswith(prefix) or v is None:
            continue
        sub = k[len(prefix):]
        if "." in sub:
            continue
        if not hasattr(root_mod, sub):
            try:
                setattr(root_mod, sub, v)
            except Exception:
                pass

_ensure_attr_chain("api")
_ensure_attr_chain("agent")
_ensure_attr_chain("tools")
_ensure_attr_chain("config")
_ensure_attr_chain("cache")
