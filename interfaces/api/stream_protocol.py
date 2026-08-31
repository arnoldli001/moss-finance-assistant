# -*- coding: utf-8 -*-
"""[兼容垫片] 唯一真源：api/stream_protocol.py。

历史说明：本文件曾是 api/stream_protocol.py 的全量物理副本（SSE/WS 事件协议双份），
协议字段漂移会导致前端解析不一致。现已去重为 re-export。
"""
from api.stream_protocol import *  # noqa: F401,F403
