# -*- coding: utf-8 -*-
"""[兼容垫片] 唯一真源：api/stream_bus.py。

历史说明：本文件曾是 api/stream_bus.py 的全量物理副本（StreamEventBus 单例双份），
存在"两个事件总线、SSE 与 WS 收到不同事件流"的 split-brain 风险。
现已去重为 re-export：两条 import 路径拿到同一个总线单例。
"""
from api.stream_bus import *  # noqa: F401,F403
from api.stream_bus import (  # noqa: F401  高频符号显式导出
    StreamEventBus,
    get_stream_bus_sync,
)
