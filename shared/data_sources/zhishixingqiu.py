"""
shared.data_sources.zhishixingqiu —— 知识星球抓取工具（compat stub，不重复维护实代码）

设计说明：
  tools/zsxq_tool.py 是**生产唯一实代码**（带鲁棒的 AGENTS.md 项目根定位 + storage_state.json 路径）。
  本文件是 analysis_workflow.py 等新代码 `from shared.data_sources.zhishixingqiu import search_zsxq_by_stock`
  时的兼容入口：它直接把自身 sys.modules 条目替换为 tools.zsxq_tool 同一个模块对象，从而保证：
    1. 新、旧链路共享同一个 storage_state.json 路径（不出现"旧链路已登录，新链路判定未登录"）
    2. 浏览器互斥锁 _zsxq_browser_lock 全局唯一（不会各自一把锁导致并发冲突）
    3. 不依赖 shared.compat_bootstrap 的 alias 方向（alias 对不上也没关系，自己能解析）
"""
from __future__ import annotations

import sys
import importlib

# 1) 加载生产实代码（tools/zsxq_tool.py 是唯一实代码来源）
_real_mod = importlib.import_module("tools.zsxq_tool")

# 2) 把本模块在 sys.modules 中的对象替换为实模块，保证 import 两侧得到同一对象
#    这是 Python import 重导向的标准安全写法（比 from X import * 更彻底，
#    能保留 is 判定、单例、模块级锁）。
sys.modules[__name__] = _real_mod
