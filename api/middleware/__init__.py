# -*- coding: utf-8 -*-
"""
api.middleware 包：FastAPI 中间件层。

包含：
  - rbac.py: RBAC 角色权限 + 数据行级权限
  - prompt_sanitizer.py: prompt 注入检测与防护
  - audit_logger.py: 安全审计日志（落盘 JSONL）
"""
