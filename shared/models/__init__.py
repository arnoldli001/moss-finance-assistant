# shared/models: 跨层通用 Pydantic 数据模型（请求/响应/路由决策/检索结果/流式事件等）
#
# 本层包含四类模型：
#   1. api_models.py      — HTTP / SSE / WebSocket 的请求与响应 Schema（来自原 server.py 内联 BaseModel）
#   2. router_models.py   — Router Agent 路由决策结构化输出（gemma4:e4b 返回，供 orchestration 调度）
#   3. retrieval_models.py — 4 数据源统一检索条目、citation 元数据、可靠性字段（供 Aggregator 聚合）
#   4. stream_models.py   — StreamBus 流式事件帧 / SSE 帧 / Reasoning 事件 等协议模型（来自 stream_protocol.py）
#
# 对外导出统一放在此处，避免各层直接依赖具体文件名，后续可随时拆合。

# ============================================================
# 1. API 请求 / 响应模型（原 server.py TaskRequest/UserRequest/SessionRequest/_StreamTaskRequest）
# ============================================================
from __future__ import annotations

from typing import List, Optional, Dict, Any
from pydantic import BaseModel


# ---------------- HTTP 业务请求 ----------------
class TaskRequest(BaseModel):
    """POST /api/task 的请求体。"""
    query: str
    thread_id: Optional[str] = None
    user_id: Optional[str] = None


class StreamTaskRequest(BaseModel):
    """POST /api/task/stream 的请求体（SSE 流式）。"""
    query: str
    thread_id: Optional[str] = None
    user_id: Optional[str] = None
    incremental: bool = True
    max_silence_sec: float = 60.0
    last_event_id: Optional[str] = None


class UserRequest(BaseModel):
    user_id: str
    display_name: Optional[str] = None


class SessionRequest(BaseModel):
    title: Optional[str] = None


# ============================================================
# 2. Router Agent 路由决策结构（gemma4:e4b 输出 + Python 规则双重）
# ============================================================
from enum import Enum as _Enum


class RouteBranch(str, _Enum):
    """Router 最终分发的一级分支枚举。与重构.md §智能路由设计 1~5 一一对应：

    PRE_MARKET_NEWS        = 分支1 盘前新闻（查6h缓存→并发搜索zsxq→Aggregator→deepseek-r1→保存）
    PRESET_SHORTCUT_OTHER  = 分支2 盘前小作文热度 / 复盘预测（复用原逻辑）
    STOCK_QUERY            = 分支3.1 含股票（并发4任务:1周缓存+联网+zsxq2条+IMA，180s超时）
    GENERAL_QUERY          = 分支3.2 不含股票（联网+zsxq并发）
    CODE_GENERATION        = 分支3.3 "写个脚本 / 爬取数据" → coder(qwen2.5-coder)
    IMPACT_ANALYSIS        = 分支3.4 "分析影响 / 评估风险 / 利多利空" → reasoning(deepseek-r1)
    VISION                 = 分支5 图片/图表 → vision(qwen3-vl)
    FALLBACK               = 兜底（无法判定，走 GENERAL_QUERY）
    """
    PRE_MARKET_NEWS = "pre_market_news"
    PRESET_SHORTCUT_OTHER = "preset_shortcut_other"
    STOCK_QUERY = "stock_query"
    GENERAL_QUERY = "general_query"
    CODE_GENERATION = "code_generation"
    IMPACT_ANALYSIS = "impact_analysis"
    VISION = "vision"
    FALLBACK = "fallback"


class RouterDecision(BaseModel):
    """Router Agent 结构化决策（Python 规则优先，gemma4 兜底时双写 source）。"""
    # 必选：一级分支
    branch: RouteBranch
    # 命中方式：'rule_based' = Python 正则/关键词/缓存命中直接判；'gemma4' = 本地 gemma4:e4b 推理；'cascade' = 级联兜底
    decided_by: str = "rule_based"
    # 判定原因（中文，便于前端进度条展示）
    reason: str = ""
    # 是否识别出个股 / 板块
    has_stock_keywords: bool = False
    extracted_stock_names: List[str] = []
    extracted_stock_codes: List[str] = []
    # 是否识别出"写脚本/爬虫/抓数据"关键词
    has_code_keywords: bool = False
    # 是否识别出"分析影响/评估风险/利多利空"关键词
    has_analysis_keywords: bool = False
    # 是否包含图片 / 图表输入（多模态触发）
    has_visual_input: bool = False
    # 是否来自前端快捷按钮（盘前新闻/小作文热度/复盘预测）
    from_shortcut_button: bool = False
    shortcut_type: Optional[str] = None  # 'premarket_news' | 'essay_heat' | 'review_forecast' | None
    # 路由置信度 0.0~1.0（级联路由使用）
    confidence: float = 1.0
    # 级联需要升级时，下一级候选模型
    cascade_upgrade_suggestion: Optional[str] = None
    # 附加透传字段（原 prompt 或原始 query）
    extras: Dict[str, Any] = {}


# ============================================================
# 3. 统一检索条目（4 数据源规范化后输出，Aggregator 输入）
# ============================================================
class SourceReliability(str, _Enum):
    RELIABLE = "可靠"
    UNVERIFIED = "待验证"
    UNKNOWN = "未知"


class RetrievalItem(BaseModel):
    """Aggregator / Citation 池 / Prompt 上下文的统一条目结构。

    4 数据源工具（web_search / zsxq / ima / local_sql）输出原始文本后，
    先通过 shared/aggregator.normalize_item() 转为此结构，保证后续各层字段一致。
    """
    # 条目标题（可空）
    title: str = ""
    # 正文片段（≤ CITATION_PROMPT_DOC_CONTENT_MAX，超过截断）
    content: str
    # 原文 URL（可空，IMA / 本地 SQL 可能没有）
    url: str = ""
    # 来源类型枚举：web | zsxq | ima | sql | cache | other
    source_type: str = "other"
    # 来源渠道细分：如 "韭研社区"/"上交所"/"茅台2024年报.pdf"
    channel: str = ""
    # 发布时间（ISO 字符串或空；Aggregator 做时效性过滤）
    published_at: str = ""
    # 可靠性评级
    reliability: SourceReliability = SourceReliability.UNVERIFIED
    # 作者 / 发布者（ZSXQ/IMA 等有用）
    author: str = ""
    # 利多 / 利空 / 中性（可选，工具层或 reasoning Agent 预判）
    sentiment: str = "中性"
    # 原始结构化字典（未裁剪，保留给 hallucination_guard / 前端悬停卡片用）
    raw_dict: Dict[str, Any] = {}


# ============================================================
# 4. 对外统一 re-export（保持未来灵活性：若拆分为多个文件只需改动此处 import）
# ============================================================
__all__ = [
    # API
    "TaskRequest", "StreamTaskRequest", "UserRequest", "SessionRequest",
    # Router
    "RouteBranch", "RouterDecision",
    # Retrieval
    "SourceReliability", "RetrievalItem",
]
