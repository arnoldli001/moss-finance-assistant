"""知识星球抓取文本处理纯工具（无全局状态、可独立单测）。

对外：
  - clean_text(text)        → HTML 标签、URL 编码、空白归一化清理
  - extract_topic_info(raw_api_topic_dict, *, group_id, ...) → 原始 API topic JSON
        → 标准化 topic_info Dict（topic_id/标题/正文/作者/点赞/评论/原始 JSON …）
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

# ---------------------------------------------------------------------------
# 默认截断参数（与 config/constants.py 的 ZSXQ_EXTRACT_* 保持一致；
# 调用方会显式传入常量值，这里仅作为裸导入时的兜底）
# ---------------------------------------------------------------------------
_DEFAULT_TITLE_TRUNCATE = 128
_DEFAULT_AUTHOR_TRUNCATE = 64


def clean_text(text: str) -> str:
    """清理知识星球正文中的 HTML 标签和多余空白。纯函数。"""
    if not text:
        return ""
    # 移除 <e type="hashtag" ... /> 等自闭合标签
    text = re.sub(r"<e\s+[^>]*/?>", "", text)
    # 移除其他常见 HTML 标签
    text = re.sub(r"<[^>]+>", "", text)
    # 移除末尾不完整的 HTML 标签（API 截断导致缺少闭合 >）
    text = re.sub(r"<[a-zA-Z][^>]*$", "", text)
    # URL 解码（%23 → #），只解码看起来像 URL 编码的部分
    try:
        from urllib.parse import unquote

        text = re.sub(
            r"%[0-9A-Fa-f]{2}",
            lambda m: unquote(m.group(0)),
            text,
        )
    except Exception:
        pass
    # 合并多余空行
    text = re.sub(r"\n{3,}", "\n\n", text)
    # 去除首尾空白
    return text.strip()


def extract_topic_info(
    topic: Dict,
    *,
    group_id: str,
    title_truncate_chars: int = _DEFAULT_TITLE_TRUNCATE,
    author_name_truncate_chars: int = _DEFAULT_AUTHOR_TRUNCATE,
) -> Dict:
    """从知识星球 API 返回的原始 topic JSON 中提取标准化字段字典。纯函数。

    Args:
        topic: API 返回的原始 topic 对象（含 talk/owner/create_time/... 字段）
        group_id: 所属群组 ID（写回结果中的 group_id 字段）
        title_truncate_chars: 标题截断长度
        author_name_truncate_chars: 作者名截断长度
    """
    talk = topic.get("talk", {}) or {}
    owner = (talk.get("owner", {}) or topic.get("owner", {}) or {})

    title = clean_text(talk.get("title") or "")
    text = clean_text(talk.get("text") or "")
    full_content = f"{title}\n\n{text}" if title else text

    # 处理时间：ISO 8601 / 毫秒戳 两种格式兜底
    create_time_raw = topic.get("create_time", "")
    create_time_str = str(create_time_raw)
    create_time_ts = 0
    try:
        if isinstance(create_time_raw, str) and "T" in create_time_raw:
            ts_str = create_time_raw.replace("+0800", "+08:00").replace("+0000", "+00:00")
            dt = datetime.fromisoformat(ts_str)
            create_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            create_time_ts = int(dt.timestamp())
        elif create_time_raw:
            create_time_ts = int(create_time_raw)
            dt = datetime.fromtimestamp(create_time_ts / 1000)
            create_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    return {
        "topic_id": str(topic.get("topic_id", "")),
        "title": title[:title_truncate_chars] if title else "",
        "content": full_content,
        "author_name": (owner.get("name") or "")[:author_name_truncate_chars],
        "author_id": str(owner.get("user_id", "")),
        "create_time": create_time_str,
        "create_time_ts": create_time_ts,
        "like_count": int(topic.get("likes_count", 0) or 0),
        "comment_count": int(topic.get("comments_count", 0) or 0),
        "digested": bool(topic.get("digested", False)),
        "group_id": group_id,
        "raw_json": json.dumps(topic, ensure_ascii=False),
    }
