# coding = utf-8
"""shared 层常量聚合：flat re-export + 分组视图。

与 config/constants.py 的关系：
    - 平铺常量的唯一真源是 config/constants.py（修改常量只改那里）；
    - 本文件负责两件事：
      1) `from config.constants import *` 全量 re-export 平铺常量
         （shared/compat_bootstrap.py 会把 sys.modules["config.constants"]
          指向本模块，因此旧路径 `from config.constants import X` 也必须能拿到全部平铺名）；
      2) 提供分组视图字典（TIMEOUTS / SLO_TARGETS），供 orchestration/loop.py
         等按 dict 键读取的调用方使用。键值与 loop.py 内置默认保持一致（行为中立）。
"""
from config.constants import *  # noqa: F401,F403

# ======================================================================
# 分组视图：TIMEOUTS（loop.py 策略覆写用；值对齐 loop.DEFAULT_LOOP_POLICY）
# ======================================================================
TIMEOUTS = {
    "MAIN_AGENT": DEFAULT_AGENT_TIMEOUT_SEC,        # 180.0 → stock_query / general_query
    "PREMARKET_ANALYSIS": 120.0,                    # 对齐 loop.py pre_market_news 默认 120s
}

# ======================================================================
# 分组视图：SLO_TARGETS（slo_monitor 同构 + loop.py 使用的旧大写键并存）
# ======================================================================
SLO_TARGETS = {
    "availability": SLO_AVAILABILITY_TARGET,                # 0.99
    "latency_p95_sec": SLO_LATENCY_P95_SEC,                 # 30.0
    "hallucination_pass_rate": SLO_HALLUCINATION_PASS_RATE, # 0.95
    "max_task_sec": SLO_MAX_TASK_SEC,                       # 150.0
    "max_tokens": SLO_MAX_TOKENS,                           # 1_000_000
    # —— 旧大写键（orchestration/loop.py 兼容读取，勿删）——
    "SLO_MAX_TASK_SEC": SLO_MAX_TASK_SEC,
    "SLO_MAX_TOKENS": SLO_MAX_TOKENS,
}
