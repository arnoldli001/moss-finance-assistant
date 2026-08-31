# -*- coding: utf-8 -*-
"""[兼容垫片] 唯一真源：api/context.py。

历史说明：本文件曾是 api/context.py 的全量物理副本（ContextVar 定义双份），
存在"两条 import 路径各持一个 ContextVar 实例"的隐性 split-brain 风险。
现已去重为 re-export：两条路径共享同一组 ContextVar 对象。
"""
from api.context import *  # noqa: F401,F403
from api.context import (  # noqa: F401  显式列出高频符号
    set_session_context,
    reset_session_context,
    set_thread_context,
    get_thread_context,
)
