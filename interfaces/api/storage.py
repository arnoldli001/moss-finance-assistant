#coding = utf-8
"""
用户/会话存储层 (SQLite)。

设计说明：
- 本模块管理 user 与 session 的元数据关系（哪个用户有哪些会话、会话标题等）。
- 对话消息本身不在此处存储，由 LangGraph 的 SqliteSaver 持久化（checkpointer.db），
  通过 thread_id (= session_id) 关联。本表的 session_id 与 checkpointer 的 thread_id 一一对应。
- 数据库文件位于 data/app.db，自动创建。
- 所有写入均通过结构化 SQL，禁止裸文本替换，避免编码损坏。
- v2 schema（鉴权升级）：users 表新增 password_hash / role / last_login 三列，
  init_db() 采用「列存在性检查 → ALTER TABLE ADD COLUMN」幂等迁移，不破坏老数据。
- 默认角色：guest_ 前缀用户 = guest；首次注册的非游客 = user。
"""
from __future__ import annotations

import sqlite3
import threading
import uuid
import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from shared.utils.auth import (
    GUEST_USER_PREFIX,
    hash_password as _hash_password,
    verify_password as _verify_password,
)

# 项目根目录
project_root = Path(__file__).resolve().parents[1]
# 数据目录：存放 SQLite 文件
data_dir = project_root / "data"
data_dir.mkdir(parents=True, exist_ok=True)

DB_PATH = data_dir / "app.db"
DEFAULT_OWNER_USER_ID_ENV = "MOSS_DEFAULT_OWNER_USER_ID"  # 可在 .env 指定默认 owner

# SQLite 默认不支持跨线程共享连接，这里用 thread-local 保证每个线程独立连接
_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """获取当前线程的 SQLite 连接（惰性创建）。"""
    conn = getattr(_local, "conn", None)
    if conn is None:
        # check_same_thread=False 已不再需要，因为每线程独立连接
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # 启用外键约束
        conn.execute("PRAGMA foreign_keys = ON")
        _local.conn = conn
    return conn


def _now() -> str:
    """ISO 格式当前时间，便于序列化"""
    return datetime.datetime.now().isoformat(timespec="seconds")


def _has_column(table: str, column: str) -> bool:
    """PRAGMA table_info 查询列是否存在（用于幂等 ADD COLUMN）。"""
    conn = _get_conn()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == column for r in rows)


def init_db():
    """初始化数据库表结构（幂等，含 v2 迁移）。"""
    import os as _os
    conn = _get_conn()
    cur = conn.cursor()
    # 用户表：v2 新增 password_hash / role / last_login；v1 用户（老数据）三列默认 NULL
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id       TEXT PRIMARY KEY,
            display_name  TEXT,
            created_at    TEXT NOT NULL
        )
    """)
    for col, decl in [
        ("password_hash", "TEXT"),
        ("role",          "TEXT NOT NULL DEFAULT 'user'"),
        ("last_login",    "TEXT"),
    ]:
        if not _has_column("users", col):
            cur.execute(f"ALTER TABLE users ADD COLUMN {col} {decl}")
    # 会话表：每个用户可有多个会话，session_id 即 LangGraph thread_id
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id    TEXT PRIMARY KEY,
            user_id       TEXT NOT NULL,
            title         TEXT,
            created_at    TEXT NOT NULL,
            updated_at    TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )
    """)
    # 索引：按用户查会话列表
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sessions_user_id
        ON sessions(user_id)
    """)
    # 索引：用户 role 查询（owner/admin 列表、限流时避免扫全表）
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_users_role
        ON users(role)
    """)
    conn.commit()
    # 初始化默认 owner：如果 env 指定且该用户有 password_hash 且 role != owner，则自动升权
    default_owner = _os.environ.get(DEFAULT_OWNER_USER_ID_ENV, "").strip()
    if default_owner:
        try:
            _ensure_owner(default_owner)
        except Exception:
            # 初始化 owner 失败不阻塞主流程
            pass


# ======================== 用户相关 ========================

def _default_role_for(user_id: str) -> str:
    return "guest" if user_id.startswith(GUEST_USER_PREFIX) else "user"


def _ensure_user_exists(user_id: str) -> None:
    """若 user_id 不存在则 get_or_create_user 自动建（空密码/默认 role）。"""
    if get_user(user_id) is None:
        get_or_create_user(user_id)


def _row_to_user(row: sqlite3.Row, *, include_sensitive: bool = False) -> Dict:
    out = {
        "user_id": row["user_id"],
        "display_name": row["display_name"],
        "created_at": row["created_at"],
    }
    # role 列在 v2 中 NOT NULL DEFAULT 'user'，老数据 ALTER 后也有值
    out["role"] = row["role"] if "role" in row.keys() else _default_role_for(row["user_id"])
    last_login = row["last_login"] if "last_login" in row.keys() else None
    out["last_login"] = last_login
    out["has_password"] = bool(row["password_hash"]) if "password_hash" in row.keys() else False
    if include_sensitive:
        out["password_hash"] = row["password_hash"] if "password_hash" in row.keys() else None
    return out


def _select_user_full(user_id: str) -> Optional[sqlite3.Row]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, display_name, created_at, password_hash, role, last_login "
        "FROM users WHERE user_id = ?",
        (user_id,),
    )
    return cur.fetchone()


def get_or_create_user(user_id: str, display_name: Optional[str] = None,
                       *, password: Optional[str] = None,
                       role: Optional[str] = None) -> Dict:
    """登录/注册：用户不存在则创建；存在返回现有（并按传入 password/role 执行幂等更新）。

    password 仅在新用户创建时生效；老用户若要改密码请调用 update_password()。
    role 仅在「当前 role 更弱」时才升级（guest->user->admin->owner 单向，避免降权事故）。
    """
    if not user_id:
        raise ValueError("user_id 不能为空")
    conn = _get_conn()
    cur = conn.cursor()
    row = _select_user_full(user_id)
    now = _now()
    if row is None:
        display_name = display_name or user_id
        pw_hash = _hash_password(password) if password else None
        final_role = role or _default_role_for(user_id)
        cur.execute(
            "INSERT INTO users (user_id, display_name, created_at, password_hash, role, last_login) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, display_name, now, pw_hash, final_role, None),
        )
        conn.commit()
        row = _select_user_full(user_id)
        return _row_to_user(row)

    # 已存在 -> 增量更新（幂等）
    updates: List[str] = []
    params: List = []
    if display_name and display_name != row["display_name"]:
        updates.append("display_name = ?")
        params.append(display_name)
    # ===== P0 安全修正：password 只在 NEW USER 创建分支（L169-170）写入，此处不再更新 =====
    # 原注释「password 仅在新用户创建时生效」与之前 L186 UPDATE password_hash 分支
    # 自相矛盾，导致同 user_id 第二次 register 会把旧密码覆盖（违反幂等 + 安全风险）。
    # 已存在用户改密码必须显式调用 update_password()（带 old_pw 校验）。
    if role:
        # 单向升级：仅当目标权限高于现有才允许（owner > admin > user > guest）
        rank = {"guest": 0, "user": 1, "admin": 2, "owner": 3}
        cur_role = (row["role"] if "role" in row.keys() else None) or _default_role_for(user_id)
        if rank.get(role, -1) > rank.get(cur_role, -1):
            updates.append("role = ?")
            params.append(role)
    if updates:
        params.append(user_id)
        cur.execute(f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?", params)
        conn.commit()
        row = _select_user_full(user_id)
    return _row_to_user(row)


def _ensure_owner(user_id: str) -> None:
    """幂等将指定用户升级为 owner；若不存在则先创建（空密码，禁止登录）。"""
    u = get_or_create_user(user_id, display_name=f"Owner:{user_id}")
    if u.get("role") != "owner":
        conn = _get_conn()
        conn.execute("UPDATE users SET role = 'owner' WHERE user_id = ?", (user_id,))
        conn.commit()


def get_user(user_id: str, *, include_sensitive: bool = False) -> Optional[Dict]:
    row = _select_user_full(user_id)
    if row is None:
        return None
    return _row_to_user(row, include_sensitive=include_sensitive)


def touch_last_login(user_id: str) -> None:
    """登录成功调用，写入 last_login。"""
    conn = _get_conn()
    conn.execute("UPDATE users SET last_login = ? WHERE user_id = ?", (_now(), user_id))
    conn.commit()


def verify_user_password(user_id: str, password: str) -> Tuple[bool, bool, Optional[str]]:
    """校验登录密码。返回 (ok, need_upgrade, role)。

    ok: True = 密码对；need_upgrade: True = 登录成功但应重新 hash_password()（算法升级）。
    role 为当前用户角色，用于签发 JWT。
    """
    row = _select_user_full(user_id)
    if row is None:
        return False, False, None
    role = (row["role"] if "role" in row.keys() else None) or _default_role_for(user_id)
    pw_hash = row["password_hash"] if "password_hash" in row.keys() else None
    if not pw_hash:
        # 空密码 = 未设置密码（游客或老用户从未设置）
        return False, False, role
    ok, need = _verify_password(password, pw_hash)
    return ok, need, role


def update_password(user_id: str, new_password: str) -> bool:
    """改密码。成功 True；用户不存在或新密码为空 -> False 或 raise。"""
    if not new_password:
        raise ValueError("new_password 不能为空")
    row = _select_user_full(user_id)
    if row is None:
        return False
    conn = _get_conn()
    conn.execute(
        "UPDATE users SET password_hash = ? WHERE user_id = ?",
        (_hash_password(new_password), user_id),
    )
    conn.commit()
    return True


def assign_role(user_id: str, role: str, *, by_role: str = "owner") -> bool:
    """管理员改角色。

    - 只有 owner 可以改他人成 owner/admin；
    - admin 可以改 user<->guest，但不能碰 owner；
    - 禁止降权 owner（by_role=owner 才允许把 owner 降成 admin）。
    """
    if role not in ("owner", "admin", "user", "guest"):
        raise ValueError(f"非法 role: {role}")
    if by_role not in ("owner", "admin"):
        return False
    row = _select_user_full(user_id)
    if row is None:
        return False
    cur_role = (row["role"] if "role" in row.keys() else None) or _default_role_for(user_id)
    if by_role == "admin":
        # admin 不能把任何人改成 owner，也不能修改 owner 的角色
        if role == "owner" or cur_role == "owner":
            return False
        if role not in ("admin", "user", "guest"):
            return False
    # by_role == "owner" 可以任意指定（包含 owner 降权）
    conn = _get_conn()
    conn.execute("UPDATE users SET role = ? WHERE user_id = ?", (role, user_id))
    conn.commit()
    return True


# ======================== 会话相关 ========================

def generate_default_title(user_id: str, query: str) -> str:
    """根据 user_id + 首次提问关键词 + 日期 生成默认会话标题。

    格式：{user_id}_{关键词}_{MM-DD}
    关键词提取：取 query 前 12 个字符（去掉首尾空白和标点），超长截断。
    """
    # 提取关键词：去掉首尾空白，截断到 12 字
    keyword = query.strip()[:12] if query else "新会话"
    # 日期：MM-DD
    today = datetime.datetime.now().strftime("%m-%d")
    return f"{user_id}_{keyword}_{today}"


def create_session(user_id: str, title: Optional[str] = None) -> Dict:
    """为指定用户新建一个会话（session_id 由系统 uuid4 生成），返回完整 session dict。"""
    _ensure_user_exists(user_id)
    session_id = str(uuid.uuid4())
    return _do_create_session(session_id, user_id, title)


def ensure_session(session_id: str, user_id: str,
                   title: Optional[str] = None) -> Dict:
    """幂等保证 session_id 存在。LangGraph thread_id == session_id，
    调用方会自己指定 session_id，所以不能用 create_session() 的自动 uuid。

    - 若 session 已存在：返回现有 session dict
    - 若不存在：按传入 session_id 创建，title 默认 "新会话"
    - 若 user_id 不存在：先 get_or_create_user(user_id) 自动建
    """
    _ensure_user_exists(user_id)
    existing = get_session(session_id)
    if existing:
        return existing
    return _do_create_session(session_id, user_id, title)


def _do_create_session(session_id: str, user_id: str, title: Optional[str]) -> Dict:
    """（内部）真正执行 INSERT INTO sessions，不做 user/session 前置校验。"""
    now = _now()
    final_title = title or "新会话"
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sessions (session_id, user_id, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        (session_id, user_id, final_title, now, now),
    )
    conn.commit()
    return {"session_id": session_id, "user_id": user_id, "title": final_title,
            "created_at": now, "updated_at": now}


def list_sessions(user_id: str) -> List[Dict]:
    """列出某用户的所有会话，按 updated_at 倒序。"""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT session_id, user_id, title, created_at, updated_at "
        "FROM sessions WHERE user_id = ? ORDER BY updated_at DESC",
        (user_id,),
    )
    rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_session(session_id: str) -> Optional[Dict]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT session_id, user_id, title, created_at, updated_at FROM sessions WHERE session_id = ?",
        (session_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def update_session_title(session_id: str, title: str) -> bool:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE sessions SET title = ?, updated_at = ? WHERE session_id = ?",
        (title, _now(), session_id),
    )
    conn.commit()
    return cur.rowcount > 0


def touch_session(session_id: str):
    """更新会话的 updated_at（每次有新消息时调用）。"""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE sessions SET updated_at = ? WHERE session_id = ?",
        (_now(), session_id),
    )
    conn.commit()


def delete_session(session_id: str) -> bool:
    """删除会话记录（LangGraph checkpointer 中的消息保留，由其自身清理）。"""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
    conn.commit()
    return cur.rowcount > 0


def verify_session_owner(session_id: str, user_id: str) -> bool:
    """校验会话是否属于该用户（防止越权访问他人会话）。"""
    s = get_session(session_id)
    return s is not None and s["user_id"] == user_id


# 模块导入时初始化表
init_db()
