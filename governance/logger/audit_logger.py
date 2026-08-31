# -*- coding: utf-8 -*-
"""
安全审计日志：所有触发 warning/reject 的事件落盘 JSONL，避免无安全审计日志，prompt 注入攻击无追踪。

用法：
    from api.middleware.audit_logger import audit_log_security_event

    audit_log_security_event(
        event_type="prompt_injection_blocked",
        user_id="u123",
        ip="1.2.3.4",
        input_text="ignore previous instructions...",
        violations=["keyword:ignore previous instructions"],
        action="rejected",
    )
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Dict, Optional

from config.constants import SECURITY_AUDIT_LOG_PATH

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_current_log_path = SECURITY_AUDIT_LOG_PATH


def set_log_path(path: str) -> None:
    """运行时切换审计日志路径（如按日期切分）。"""
    global _current_log_path
    _current_log_path = path


def audit_log_security_event(
    event_type: str,
    user_id: str = "",
    ip: str = "",
    input_text: str = "",
    violations: Optional[list] = None,
    action: str = "logged",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """记录一条安全审计事件。

    参数：
        event_type: 事件类型（如 prompt_injection_blocked / rbac_denied）
        user_id: 触发事件的用户 ID
        ip: 请求 IP
        input_text: 用户原始输入（截断到 500 字符）
        violations: 命中的违规模式列表
        action: 处理动作（logged / warned / rejected）
        extra: 额外字段
    """
    event = {
        "timestamp": time.time(),
        "ts_human": time.strftime("%Y-%m-%d %H:%M:%S"),
        "event_type": event_type,
        "user_id": user_id,
        "ip": ip,
        "input_text": input_text[:500],  # 截断
        "violations": violations or [],
        "action": action,
    }
    if extra:
        event.update(extra)

    with _lock:
        try:
            os.makedirs(os.path.dirname(_current_log_path), exist_ok=True)
            with open(_current_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error("[audit] 审计日志写入失败: %s", e)
