#coding = utf-8
"""
用户/会话存储层 (SQLite)。

设计说明：
- 本模块管理 user 与 session 的元数据关系（哪个用户有哪些会话、会话标题等）。
- 对话消息本身不在此处存储，由 LangGraph 的 SqliteSaver 持久化（checkpointer.db），
  通过 thread_id (= session_id) 关联。本表的 session_id 与 checkpointer 的 thread_id 一一对应。
- 数据库文件位于 data/app.db，自动创建。
- 所有写入均通过结构化 SQL，禁止裸文本替换，避免编码损坏。
"""
import sqlite3
import threading
import uuid
import datetime
from pathlib import Path
from typing import List, Dict, Optional

# 项目根目录
project_root = Path(__file__).resolve().parents[1]
# 数据目录：存放 SQLite 文件
data_dir = project_root / "data"
data_dir.mkdir(parents=True, exist_ok=True)

DB_PATH = data_dir / "app.db"

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


def init_db():
    """初始化数据库表结构（幂等）。"""
    conn = _get_conn()
    cur = conn.cursor()
    # 用户表：简单 user_id 标识，无密码
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id       TEXT PRIMARY KEY,
            display_name  TEXT,
            created_at    TEXT NOT NULL
        )
    """)
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
    conn.commit()


# ======================== 用户相关 ========================

def get_or_create_user(user_id: str, display_name: Optional[str] = None) -> Dict:
    """登录/注册：用户不存在则创建，存在则返回现有。"""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, display_name, created_at FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if row is None:
        display_name = display_name or user_id
        created_at = _now()
        cur.execute(
            "INSERT INTO users (user_id, display_name, created_at) VALUES (?, ?, ?)",
            (user_id, display_name, created_at),
        )
        conn.commit()
        return {"user_id": user_id, "display_name": display_name, "created_at": created_at}
    return {"user_id": row["user_id"], "display_name": row["display_name"], "created_at": row["created_at"]}


def get_user(user_id: str) -> Optional[Dict]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, display_name, created_at FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if row is None:
        return None
    return {"user_id": row["user_id"], "display_name": row["display_name"], "created_at": row["created_at"]}


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
    """为指定用户新建一个会话，返回 session_id（同时作为 LangGraph thread_id）。"""
    if get_user(user_id) is None:
        # 自动注册用户
        get_or_create_user(user_id)
    session_id = str(uuid.uuid4())
    now = _now()
    title = title or "新会话"
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sessions (session_id, user_id, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        (session_id, user_id, title, now, now),
    )
    conn.commit()
    return {"session_id": session_id, "user_id": user_id, "title": title,
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
