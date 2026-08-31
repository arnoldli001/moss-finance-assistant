# -*- coding: utf-8 -*-
"""[兼容垫片] 唯一真源：api/monitor.py。

历史说明：本文件曾是 api/monitor.py 的全量物理副本（ConnectionManager 单例双份），
存在 split-brain 风险（两个 manager 实例、事件推送分裂）。
现已去重为 re-export：两条 import 路径拿到同一个 manager 单例。
"""
from api.monitor import *  # noqa: F401,F403
from api.monitor import manager  # noqa: F401  高频单例显式导出
