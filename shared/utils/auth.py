# -*- coding: utf-8 -*-
"""企业级鉴权工具：JWT 签发/校验 + 安全密码哈希 + FastAPI 依赖注入。

设计目标：
- 零新增第三方依赖首选：密码哈希优先用 PBKDF2-HMAC-SHA256（hashlib 内建），
  环境若安装 bcrypt 则自动升级为 bcrypt（更高强度）。
- JWT 优先用 PyJWT（PyJWT 未安装时提供纯 HS256 回退，仅用于开发/演示；
  生产建议 pip install PyJWT cryptography）。
- 兼容「游客模式」：允许用户以 guest_<uuid> 方式零注册进入，
  自动签发 guest JWT，绑定固定 role=guest，接受严格限流。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
import uuid as _uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from fastapi import Depends, HTTPException, Request, WebSocket, status
from pydantic import BaseModel

# ------------------------------ 可选依赖探测 ------------------------------
try:  # PyJWT：首选真实 JWT 库
    import jwt as _pyjwt  # type: ignore
    _HAS_PYJWT = True
except Exception:  # pragma: no cover - 演示环境回退
    _pyjwt = None  # type: ignore
    _HAS_PYJWT = False

try:  # bcrypt：更强密码哈希，可选
    import bcrypt as _bcrypt  # type: ignore
    _HAS_BCRYPT = True
except Exception:  # pragma: no cover - 标准库回退
    _bcrypt = None  # type: ignore
    _HAS_BCRYPT = False


# ------------------------------ 配置常量（读取 env，硬编码兜底） ------------------------------
JWT_SECRET: str = os.getenv("JWT_SECRET", "").strip() or secrets.token_urlsafe(48)
JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "120"))
JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("JWT_REFRESH_TOKEN_EXPIRE_DAYS", "14"))

PBKDF2_ITERATIONS: int = int(os.getenv("PBKDF2_ITERATIONS", "200_000"))
PBKDF2_ALGO: str = "sha256"
PBKDF2_SALT_BYTES: int = 16
PBKDF2_HASH_BYTES: int = 32

# 前缀用于区分 hash 算法：$pbkdf2$... / $2b$...（bcrypt）
PREFIX_PBKDF2 = "pbkdf2"
PREFIX_BCRYPT = "2b"

GUEST_USER_PREFIX = "guest_"


# ------------------------------ 数据模型 ------------------------------
@dataclass
class CurrentUser:
    """登录后注入到每个业务端点的「当前用户上下文」。

    role 取值与 config/rbac_policy.json 对齐：owner / admin / user / guest。
    """
    user_id: str
    role: str
    display_name: str
    is_guest: bool
    token_type: str  # access / refresh

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "role": self.role,
            "display_name": self.display_name,
            "is_guest": self.is_guest,
        }


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # 秒
    user: Dict[str, Any]


# ------------------------------ 密码哈希（双算法兼容 + 升级） ------------------------------
def _b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii").rstrip("=")


def _b64d(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.b64decode(s + pad)


def hash_password(password: str) -> str:
    """安全密码哈希。优先 bcrypt；否则 PBKDF2-HMAC-SHA256。

    返回格式：
      bcrypt  -> "$2b$12$...."  (直接由 bcrypt.hashpw 产出)
      PBKDF2  -> "$pbkdf2$<iterations>$<salt_b64>$<hash_b64>"
    """
    if not isinstance(password, str) or not password:
        raise ValueError("password 必须是非空字符串")
    if _HAS_BCRYPT:
        pw_bytes = password.encode("utf-8")
        # 12 rounds ≈ 250ms，OWASP 推荐
        salted = _bcrypt.hashpw(pw_bytes, _bcrypt.gensalt(rounds=12))
        return salted.decode("utf-8")
    salt = os.urandom(PBKDF2_SALT_BYTES)
    dk = hashlib.pbkdf2_hmac(
        PBKDF2_ALGO, password.encode("utf-8"), salt,
        PBKDF2_ITERATIONS, dklen=PBKDF2_HASH_BYTES,
    )
    return (
        f"${PREFIX_PBKDF2}${PBKDF2_ITERATIONS}$"
        f"{_b64e(salt)}${_b64e(dk)}"
    )


def verify_password(password: str, hashed: Optional[str]) -> Tuple[bool, bool]:
    """验证密码。返回 (是否通过, 是否需要升级哈希)。

    need_upgrade=True 表示用户登录成功但密码还是旧算法（或低迭代 PBKDF2），
    调用方应立刻用 hash_password() 重新生成覆盖到 DB。
    """
    if not hashed or not password:
        return False, False
    if hashed.startswith("$2b$") or hashed.startswith("$2a$"):
        if not _HAS_BCRYPT:
            # 用户密码是 bcrypt 但本环境没装 bcrypt → 无法校验，返回 False 不升级
            return False, False
        ok = _bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
        # 若 rounds 低于 12，建议升级
        need = False
        try:
            rounds = int(hashed.split("$")[2])
            need = rounds < 12
        except Exception:
            pass
        return ok, need
    if hashed.startswith(f"${PREFIX_PBKDF2}$"):
        try:
            parts = hashed.split("$")
            # ["", "pbkdf2", iters, salt, hash] -> len 5
            if len(parts) != 5:
                return False, False
            iters = int(parts[2])
            salt = _b64d(parts[3])
            expected = _b64d(parts[4])
        except Exception:
            return False, False
        dk = hashlib.pbkdf2_hmac(
            PBKDF2_ALGO, password.encode("utf-8"), salt,
            max(iters, 1), dklen=PBKDF2_HASH_BYTES,
        )
        ok = hmac.compare_digest(dk, expected)
        need = iters < PBKDF2_ITERATIONS
        # 如果已装 bcrypt，登录成功后建议升级到 bcrypt
        if ok and _HAS_BCRYPT:
            need = True
        return ok, need
    # 未知格式 -> 一律不通过
    return False, False


# ------------------------------ JWT 签发 / 校验 ------------------------------
def _now_ts() -> int:
    return int(time.time())


def _create_jwt_payload(user_id: str, role: str, display_name: str,
                        is_guest: bool, minutes: int, type_: str) -> Dict[str, Any]:
    now = _now_ts()
    exp = now + minutes * 60
    return {
        "sub": user_id,
        "role": role,
        "name": display_name,
        "guest": bool(is_guest),
        "type": type_,
        "iat": now,
        "nbf": now - 5,
        "exp": exp,
        "jti": _uuid.uuid4().hex,
        "iss": "MOSS-Finance-Assistant",
    }


def _encode_jwt(payload: Dict[str, Any]) -> str:
    if _HAS_PYJWT:
        return _pyjwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    # ====== 纯手写 HS256 回退（仅用于演示环境）======
    header = {"alg": JWT_ALGORITHM, "typ": "JWT"}
    h_b64 = _b64e(json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    p_b64 = _b64e(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    signing_input = f"{h_b64}.{p_b64}".encode("ascii")
    sig = hmac.new(JWT_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
    s_b64 = _b64e(sig)
    return f"{h_b64}.{p_b64}.{s_b64}"


def _decode_jwt(token: str, *, type_required: Optional[str] = None) -> Dict[str, Any]:
    """校验 JWT 并返回 payload。失败抛 HTTPException(401)。"""
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "TOKEN_MISSING", "message": "缺少认证令牌"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        if _HAS_PYJWT:
            payload = _pyjwt.decode(
                token, JWT_SECRET, algorithms=[JWT_ALGORITHM],
                issuer="MOSS-Finance-Assistant",
                options={"require": ["exp", "iat", "sub", "role", "type"]},
            )
        else:
            # 手写回退：校验签名 + exp/nbf
            parts = token.split(".")
            if len(parts) != 3:
                raise ValueError("malformed")
            h_b64, p_b64, s_b64 = parts
            signing_input = f"{h_b64}.{p_b64}".encode("ascii")
            expected_sig = hmac.new(JWT_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
            if not hmac.compare_digest(_b64d(s_b64), expected_sig):
                raise ValueError("sig mismatch")
            payload = json.loads(_b64d(p_b64).decode("utf-8"))
            for k in ("exp", "iat", "sub", "role", "type"):
                if k not in payload:
                    raise ValueError(f"missing {k}")
            now = _now_ts()
            if int(payload["exp"]) < now:
                raise _pyjwt.ExpiredSignatureError() if _HAS_PYJWT else Exception("expired")
            if int(payload.get("nbf", 0)) > now + 60:
                raise ValueError("nbf future")
            if payload.get("iss") != "MOSS-Finance-Assistant":
                raise ValueError("bad iss")
    except HTTPException:
        raise
    except Exception as e:
        name = type(e).__name__
        if "ExpiredSignature" in name or "expired" in str(e):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"code": "TOKEN_EXPIRED", "message": "认证令牌已过期，请重新登录"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "TOKEN_INVALID", "message": f"认证令牌无效：{name}"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    if type_required and payload.get("type") != type_required:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "TOKEN_TYPE_INVALID", "message": f"令牌类型错误，期望 {type_required}"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload


# ------------------------------ 对外签发（登录/注册/游客/刷新） ------------------------------
def create_token_pair(user_id: str, role: str, display_name: str,
                      is_guest: bool = False) -> TokenResponse:
    access_payload = _create_jwt_payload(
        user_id, role, display_name, is_guest,
        JWT_ACCESS_TOKEN_EXPIRE_MINUTES, "access",
    )
    refresh_payload = _create_jwt_payload(
        user_id, role, display_name, is_guest,
        JWT_REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60, "refresh",
    )
    return TokenResponse(
        access_token=_encode_jwt(access_payload),
        refresh_token=_encode_jwt(refresh_payload),
        expires_in=JWT_ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        user={
            "user_id": user_id,
            "role": role,
            "display_name": display_name,
            "is_guest": is_guest,
        },
    )


def refresh_access_token(refresh_token: str) -> TokenResponse:
    p = _decode_jwt(refresh_token, type_required="refresh")
    return create_token_pair(
        user_id=p["sub"], role=p["role"], display_name=p.get("name", p["sub"]),
        is_guest=bool(p.get("guest", False)),
    )


def create_guest_token() -> Tuple[TokenResponse, str]:
    """生成游客账号 + JWT 对。返回 (token_pair, user_id)。

    游客 user_id = guest_<uuid8>，role=guest，display_name=游客<uuid6>。
    """
    u = _uuid.uuid4().hex
    user_id = f"{GUEST_USER_PREFIX}{u[:12]}"
    display_name = f"游客{u[:6]}"
    pair = create_token_pair(user_id, role="guest", display_name=display_name, is_guest=True)
    return pair, user_id


# ------------------------------ FastAPI 依赖：解析当前用户 ------------------------------
def _extract_bearer_from_header(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    # 兼容裸 token
    return authorization.strip() or None


async def get_current_user(request: Request) -> CurrentUser:
    """HTTP 业务端点默认依赖。从 Authorization: Bearer <jwt> 取 token。

    返回 CurrentUser；任何校验失败抛 HTTP 401。
    """
    auth = request.headers.get("Authorization", "")
    token = _extract_bearer_from_header(auth)
    if not token:
        # 兼容 WebSocket 升级前握手场景：GET /ws?token=xxx（虽然 ws 端点用另一个依赖）
        token = request.query_params.get("token") or None
    p = _decode_jwt(token, type_required="access")
    return CurrentUser(
        user_id=p["sub"], role=p.get("role", "user"),
        display_name=p.get("name", p["sub"]),
        is_guest=bool(p.get("guest", False)),
        token_type="access",
    )


async def get_current_user_websocket(websocket: WebSocket) -> Optional[CurrentUser]:
    """WS 端点依赖：先拿 token（header 或 query）再校验。

    任何校验失败不抛 HTTPException（WS 协议需要 close(code=4401)），
    调用方接收 None 后自行决定 accept 还是 close。
    """
    auth = websocket.headers.get("Authorization", "")
    token = _extract_bearer_from_header(auth)
    if not token:
        qp = websocket.query_params
        token = (qp.get("access_token") or qp.get("token") or "").strip() or None
    if not token:
        return None
    try:
        p = _decode_jwt(token, type_required="access")
    except HTTPException:
        return None
    return CurrentUser(
        user_id=p["sub"], role=p.get("role", "user"),
        display_name=p.get("name", p["sub"]),
        is_guest=bool(p.get("guest", False)),
        token_type="access",
    )


async def get_current_user_optional(request: Request) -> Optional[CurrentUser]:
    """健康检查、根页面、静态资源等场景：有 token 解析，无也放行。"""
    auth = request.headers.get("Authorization", "")
    token = _extract_bearer_from_header(auth)
    if not token:
        return None
    try:
        p = _decode_jwt(token, type_required="access")
    except HTTPException:
        return None
    return CurrentUser(
        user_id=p["sub"], role=p.get("role", "user"),
        display_name=p.get("name", p["sub"]),
        is_guest=bool(p.get("guest", False)),
        token_type="access",
    )


def current_user_id_must_match(current: CurrentUser, target_user_id: Optional[str]) -> None:
    """垂直越权校验：owner/admin 角色可跳过（运营视角），普通 user/guest 必须 user_id 相等。

    失败抛 HTTP 403。
    """
    if current.role in ("owner", "admin"):
        return
    if not target_user_id or target_user_id == current.user_id:
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={"code": "FORBIDDEN_USER_MISMATCH",
                "message": "无权访问其他用户的资源"},
    )
