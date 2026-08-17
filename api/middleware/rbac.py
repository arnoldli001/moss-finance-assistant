# -*- coding: utf-8 -*-
"""
RBAC 权限中间件 + 数据行级权限：限流的基础上，新增RBAC/数据行级权限，避免散户 A 能看到散户 B 的持仓数据。

设计思路：
  1) RBACPolicy: 从 config/rbac_policy.json 加载角色 → 权限映射（60s 热加载）
  2) RBACMiddleware: FastAPI 中间件，从请求 header 提取 user_id → 查角色 → 鉴权
  3) RowLevelContext: ContextVar 注入 (user_id, role, max_rows)，下游查询自动加 LIMIT
  4) require_permission: 装饰器/依赖，在 endpoint 上声明所需权限

典型用法：
    from api.middleware.rbac import RBACMiddleware, require_permission, get_current_user_context
    app.add_middleware(RBACMiddleware)
    @app.post("/api/retail/{user_id}")
    @require_permission("retail_data:read:self")
    async def get_retail(user_id: str):
        ctx = get_current_user_context()
        # 行级权限：user_id 必须 == ctx.user_id（除非 admin）
        if user_id != ctx.user_id and not ctx.has_permission("*"):
            raise HTTPException(403, "无权访问他人数据")
        ...

    # 查询时自动加 LIMIT
    ctx = get_current_user_context()
    sql = f"SELECT * FROM retail_data WHERE user_id=? LIMIT {ctx.max_rows}"
"""
from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
import os
import threading
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from fastapi import HTTPException, Request, status
from fastapi.security import APIKeyHeader
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from config.constants import (
    RBAC_POLICY_FILE,
    RBAC_DEFAULT_ROLE,
    RBAC_ROW_LEVEL_MAX_ROWS,
    HTTP_CODE_UNAUTHORIZED,
)

logger = logging.getLogger(__name__)


# ======================================================================
# 用户上下文（ContextVar 注入，跨层零参访问）
# ======================================================================

@dataclass
class UserContext:
    """请求级用户上下文，下游通过 ContextVar 访问。"""
    user_id: str
    role: str
    permissions: Set[str] = field(default_factory=set)
    max_rows: int = RBAC_ROW_LEVEL_MAX_ROWS
    rate_limit_per_min: int = 60
    allowed_endpoints: List[str] = field(default_factory=list)

    def has_permission(self, perm: str) -> bool:
        """检查是否拥有某权限。* 通配。"""
        if "*" in self.permissions:
            return True
        if perm in self.permissions:
            return True
        # 支持 news:* 这类通配
        for p in self.permissions:
            if p.endswith(":*") and perm.startswith(p[:-1]):
                return True
        return False

    def can_access_endpoint(self, path: str) -> bool:
        """检查是否能访问某 endpoint 路径。"""
        if "*" in self.allowed_endpoints:
            return True
        for pattern in self.allowed_endpoints:
            if fnmatch.fnmatch(path, pattern):
                return True
        return False


_current_user: ContextVar[Optional[UserContext]] = ContextVar(
    "rbac_current_user", default=None
)


def get_current_user_context() -> Optional[UserContext]:
    """获取当前请求的用户上下文。"""
    return _current_user.get()


def clear_current_user_context() -> None:
    _current_user.set(None)


# ======================================================================
# RBAC 策略加载（带热加载）
# ======================================================================

class RBACPolicy:
    """RBAC 策略：从 JSON 文件加载角色配置，支持热重载。"""

    def __init__(self, policy_file: str = RBAC_POLICY_FILE):
        self.policy_file = policy_file
        self._raw: Dict[str, Any] = {}
        self._roles: Dict[str, Dict[str, Any]] = {}
        self._user_role_map: Dict[str, str] = {}
        self._last_load_ts: float = 0
        self._lock = threading.RLock()
        self._load()

    def _load(self) -> None:
        """从文件加载策略。"""
        with self._lock:
            try:
                if not os.path.exists(self.policy_file):
                    logger.warning("[rbac] 策略文件不存在: %s，使用空策略", self.policy_file)
                    self._roles = {}
                    self._user_role_map = {}
                    return
                with open(self.policy_file, "r", encoding="utf-8") as f:
                    self._raw = json.load(f)
                self._roles = self._raw.get("roles", {})
                self._user_role_map = self._raw.get("user_role_map", {})
                self._last_load_ts = time.time()
                logger.info("[rbac] 策略已加载 roles=%d users=%d",
                            len(self._roles), len(self._user_role_map))
            except Exception as e:
                logger.error("[rbac] 策略加载失败: %s", e)
                if not self._roles:
                    # 首次加载失败，给个最小默认
                    self._roles = {RBAC_DEFAULT_ROLE: {
                        "permissions": ["news:read"],
                        "max_rows_per_query": 10,
                        "rate_limit_per_min": 10,
                        "allowed_endpoints": ["/api/news/*"],
                    }}

    def maybe_reload(self, max_age_sec: float = 60.0) -> None:
        """检查文件是否更新，超过 max_age_sec 则重载。"""
        try:
            mtime = os.path.getmtime(self.policy_file)
            if mtime > self._last_load_ts:
                self._load()
        except OSError:
            pass

    def get_role_for_user(self, user_id: str) -> str:
        """查询用户对应的角色。"""
        self.maybe_reload()
        return self._user_role_map.get(user_id, RBAC_DEFAULT_ROLE)

    def get_role_config(self, role: str) -> Dict[str, Any]:
        """获取角色配置。"""
        self.maybe_reload()
        return self._roles.get(role, self._roles.get(RBAC_DEFAULT_ROLE, {}))

    def build_user_context(self, user_id: str) -> UserContext:
        """根据 user_id 构建 UserContext。"""
        role = self.get_role_for_user(user_id)
        cfg = self.get_role_config(role)
        return UserContext(
            user_id=user_id,
            role=role,
            permissions=set(cfg.get("permissions", [])),
            max_rows=cfg.get("max_rows_per_query", RBAC_ROW_LEVEL_MAX_ROWS),
            rate_limit_per_min=cfg.get("rate_limit_per_min", 60),
            allowed_endpoints=cfg.get("allowed_endpoints", []),
        )


# ======================================================================
# FastAPI 中间件
# ======================================================================

class RBACMiddleware(BaseHTTPMiddleware):
    """RBAC 中间件：解析 user_id → 注入 UserContext → 鉴权 endpoint。

    从 header `X-User-Id` 提取 user_id（缺失则用 guest 角色）。
    """

    # 不需要鉴权的路径（健康检查、login 等）
    PUBLIC_PATHS: Set[str] = {"/", "/health", "/docs", "/openapi.json", "/redoc"}

    def __init__(self, app, policy: Optional[RBACPolicy] = None):
        super().__init__(app)
        self.policy = policy or RBACPolicy()

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # 公开路径放行
        if path in self.PUBLIC_PATHS:
            return await call_next(request)

        # 提取 user_id（生产应从 JWT/Session 提取，这里简化为 header）
        user_id = request.headers.get("X-User-Id", "anonymous")
        ctx = self.policy.build_user_context(user_id)

        # 注入 ContextVar
        token = _current_user.set(ctx)
        try:
            # 鉴权 endpoint 访问权限
            if not ctx.can_access_endpoint(path):
                logger.warning("[rbac] 拒绝访问 user=%s role=%s path=%s",
                              user_id, ctx.role, path)
                return JSONResponse(
                    status_code=status.HTTP_403_FORBIDDEN,
                    content={"detail": f"角色 {ctx.role} 无权访问 {path}"},
                )

            response = await call_next(request)
            # 把 user 信息回写到响应 header（便于前端/调试）
            response.headers["X-User-Id"] = user_id
            response.headers["X-User-Role"] = ctx.role
            return response
        finally:
            _current_user.reset(token)


# ======================================================================
# 权限校验装饰器（FastAPI 依赖）
# ======================================================================

def require_permission(permission: str):
    """FastAPI 依赖：声明某 endpoint 需要的权限。

    用法：
        from fastapi import Depends

        @app.get("/api/retail/{user_id}")
        async def get_retail(user_id: str, _: None = Depends(require_permission("retail_data:read:self"))):
            ...
    """
    def _check() -> None:
        ctx = get_current_user_context()
        if ctx is None:
            raise HTTPException(
                status_code=HTTP_CODE_UNAUTHORIZED,
                detail="未认证（RBAC 中间件未注入用户上下文）",
            )
        if not ctx.has_permission(permission):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"权限不足：需要 {permission}，当前角色 {ctx.role} 仅有 {ctx.permissions}",
            )
        return None
    return _check


def require_role(role: str):
    """FastAPI 依赖：声明某 endpoint 仅特定角色可访问。"""
    def _check() -> None:
        ctx = get_current_user_context()
        if ctx is None or ctx.role != role:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"需要角色 {role}",
            )
        return None
    return _check


def assert_row_level_access(target_user_id: str) -> None:
    """行级权限断言：确保当前用户只能访问自己的数据（除非 admin）。

    在所有按 user_id 查询数据的 endpoint 内调用。
    """
    ctx = get_current_user_context()
    if ctx is None:
        raise HTTPException(
            status_code=HTTP_CODE_UNAUTHORIZED,
            detail="未认证",
        )
    if ctx.has_permission("*"):
        return  # admin 放行
    if target_user_id != ctx.user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"无权访问用户 {target_user_id} 的数据（行级隔离）",
        )
