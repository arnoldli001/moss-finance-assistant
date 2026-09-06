# coding = utf-8
"""shared 层常量聚合：flat re-export + 分组视图。

与 config/constants.py 的关系：
    - 平铺常量的唯一真源是 config/constants.py（修改常量只改那里，新常量自动 re-export 可见）；
    - 本文件负责两件事：
      1) 从真源 config.constants re-export 全部平铺常量；
      2) 提供分组视图字典（TIMEOUTS / SLO_TARGETS），供 orchestration/loop.py
         等按 dict 键读取的调用方使用。键值与 loop.py 内置默认保持一致（行为中立）。
"""
from __future__ import annotations

from config import constants as _src

# 把真源模块的所有公开符号（非 dunder）一次性注入本模块全局，等价于 `from config.constants import *`
for _k, _v in vars(_src).items():
    if _k.startswith("__") and _k.endswith("__"):
        continue
    globals().setdefault(_k, _v)
del _src


# ======================================================================
# 分组视图：TIMEOUTS（loop.py 策略覆写用；值对齐 loop.DEFAULT_LOOP_POLICY）
# ======================================================================
def _G(name: str, fallback=None):  # type: ignore[no-untyped-def]
    val = globals().get(name, fallback)
    if val is None and fallback is None:
        raise RuntimeError(
            f"[shared/config/constants] 常量 {name} 未从 config/constants.py 真源注入成功，"
            "请检查真源文件是否定义了该常量。"
        )
    return val


TIMEOUTS = {
    "MAIN_AGENT": _G("DEFAULT_AGENT_TIMEOUT_SEC"),   # 180.0 → stock_query / general_query
    "PREMARKET_ANALYSIS": 120.0,                     # 对齐 loop.py pre_market_news 默认 120s
}

# ======================================================================
# 分组视图：SLO_TARGETS（slo_monitor 同构 + loop.py 使用的旧大写键并存）
# ======================================================================
SLO_TARGETS = {
    "availability": _G("SLO_AVAILABILITY_TARGET"),                # 0.99
    "latency_p95_sec": _G("SLO_LATENCY_P95_SEC"),                 # 30.0
    "hallucination_pass_rate": _G("SLO_HALLUCINATION_PASS_RATE"), # 0.95
    "max_task_sec": _G("SLO_MAX_TASK_SEC"),                       # 150.0
    "max_tokens": _G("SLO_MAX_TOKENS"),                           # 1_000_000
    # —— 旧大写键（orchestration/loop.py 兼容读取，勿删）——
    "SLO_MAX_TASK_SEC": _G("SLO_MAX_TASK_SEC"),
    "SLO_MAX_TOKENS": _G("SLO_MAX_TOKENS"),
}

del _G
