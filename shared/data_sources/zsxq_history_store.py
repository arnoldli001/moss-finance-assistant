"""知识星球抓取结果持久化（独立 IO 层，无 Playwright/@tool 依赖）。

职责单一：
  1) MySQL zsxq_posts 表写入（参数化 SQL 防注入）
  2) .zsxq_history.json 历史增量判断读写
  3) 文本内容 hash 摘要去重辅助

仅依赖 stdlib + mysql.connector + 同层 db_tools 配置查询。
所有文件路径通过参数显式传入，不依赖调用方的全局常量，保证可独立单测。
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, List


# ---------------------------------------------------------------------------
# 1. MySQL 持久化
# ---------------------------------------------------------------------------
def save_topics_to_db(topics_info: List[Dict]) -> str:
    """将批量抓取到的主题信息写入本地 MySQL zsxq_posts 表。

    表不存在会自动建表；遇到重复 PRIMARY KEY 走 ON DUPLICATE KEY UPDATE 以
    最新抓取到的点赞数/评论数/正文覆盖旧值。
    """
    try:
        from mysql.connector import connect
        from tools.db_tools import get_db_config  # type: ignore

        cfg = get_db_config()
        conn = connect(**cfg)
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zsxq_posts (
                id VARCHAR(64) PRIMARY KEY,
                group_id VARCHAR(64) NOT NULL,
                title VARCHAR(255),
                content TEXT,
                author_name VARCHAR(128),
                author_id VARCHAR(64),
                create_time VARCHAR(32),
                create_time_ts BIGINT,
                like_count INT DEFAULT 0,
                comment_count INT DEFAULT 0,
                digested TINYINT DEFAULT 0,
                raw_json LONGTEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uk_id (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        sql = """
            INSERT INTO zsxq_posts
            (id, group_id, title, content, author_name, author_id,
             create_time, create_time_ts, like_count, comment_count, digested, raw_json)
            VALUES (%(id)s, %(group_id)s, %(title)s, %(content)s, %(author_name)s,
                    %(author_id)s, %(create_time)s, %(create_time_ts)s, %(like_count)s,
                    %(comment_count)s, %(digested)s, %(raw_json)s)
            ON DUPLICATE KEY UPDATE
                title=VALUES(title), content=VALUES(content),
                like_count=VALUES(like_count), comment_count=VALUES(comment_count)
        """
        for info in topics_info:
            db_data = {
                "id": info["topic_id"],
                "group_id": info["group_id"],
                "title": info["title"],
                "content": info["content"],
                "author_name": info["author_name"],
                "author_id": info["author_id"],
                "create_time": info["create_time"],
                "create_time_ts": info["create_time_ts"],
                "like_count": info["like_count"],
                "comment_count": info["comment_count"],
                "digested": 1 if info["digested"] else 0,
                "raw_json": info["raw_json"],
            }
            cur.execute(sql, db_data)

        conn.commit()
        cur.close()
        conn.close()
        return f"已写入 {len(topics_info)} 条到数据库"
    except Exception as e:  # pragma: no cover - 运行时 IO 错误不应该阻断主流程
        return f"写入数据库失败: {e}"


# ---------------------------------------------------------------------------
# 2. 增量抓取历史（JSON 文件）读写
# ---------------------------------------------------------------------------
def load_history(history_file_path: Path) -> Dict:
    """加载已抓取历史记录；文件不存在或损坏时返回 {"topics": {}}。"""
    if not history_file_path.exists():
        return {"topics": {}}
    try:
        return json.loads(history_file_path.read_text(encoding="utf-8"))
    except Exception:  # pragma: no cover - 损坏 JSON 兜底
        return {"topics": {}}


def save_history(history_file_path: Path, history: Dict) -> None:
    """保存已抓取历史记录到指定 JSON 文件；出错只打印不抛。"""
    try:
        history_file_path.write_text(
            json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:  # pragma: no cover
        print(f"[ZSXQ] 保存历史记录失败: {e}")


# ---------------------------------------------------------------------------
# 3. 内容去重辅助
# ---------------------------------------------------------------------------
def content_hash(text: str, head_chars: int = 16) -> str:
    """返回内容 MD5 十六进制摘要；默认截取前 head_chars 位（足够做抓取去重）。"""
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:head_chars]
