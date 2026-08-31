"""agents.router.agent —— Router Agent（gemma4:e4b 本地轻量 + Python 规则优先双保险）。

对外函数：
    decide(query: str, *, has_visual_input: bool = False) -> shared.models.RouterDecision

执行顺序（严格按重构.md §智能路由设计 + 用户决策②"规则+gemma4级联"）：
  1. Rule-based Phase（毫秒级，规则优先）：
     - PREMARKET_SHORTCUTS（盘前新闻/小作文/复盘预测快捷按钮文本精确匹配或前缀）→ RouteBranch.PRESET_SHORTCUT_OTHER
     - query contains "盘前新闻"（中文+英文+各种变体） + 未命中快捷按钮 → RouteBranch.PRE_MARKET_NEWS
     - stock_matcher.extract_stocks(query) 非空 → has_stock_keywords=True
     - CODE_KEYWORDS 正则命中 → has_code_keywords=True → RouteBranch.CODE_GENERATION
     - ANALYSIS_KEYWORDS 正则命中 → has_analysis_keywords=True → RouteBranch.IMPACT_ANALYSIS
     - has_visual_input=True → RouteBranch.VISION
     - has_stock_keywords → RouteBranch.STOCK_QUERY
     - else → RouteBranch.GENERAL_QUERY
  2. Cascade Phase（仅当 Phase1 confidence < 0.8 时；gemma4 语义兜底）：
     - 调本地 gemma4:e4b（Ollama 默认 http://localhost:11434），强制输出 JSON，解析回填到 RouterDecision
     - gemma4 不可用 → 静默 fallback，保留 Phase1 结果（不抛异常、不影响主流程）
  3. 结果输出：decided_by 记录来源；confidence 打分；cascade_upgrade_suggestion 供升级 Agent 用
"""
from __future__ import annotations

import asyncio
import json as _json
import re
from typing import List, Optional

from shared.models import RouteBranch, RouterDecision

# ---------------------------------------------------------------------------
# Phase 1：Python 规则关键字（与重构.md §路由规则 1~7 对齐）
# ---------------------------------------------------------------------------

# 快捷按钮（前端点击时会把 query 写成下面这些精确文本）
PRESET_SHORTCUT_MAP = {
    "PREMARKET_NEWS_HINT": [
        "盘前新闻", "pre_market_news", "盘前新闻速览", "获取今日盘前新闻", "今日盘前新闻",
    ],
    "ESSAY_HEAT_HINT": [
        "盘前小作文热度", "小作文热度", "今日小作文热度", "essay_heat",
    ],
    "REVIEW_FORECAST_HINT": [
        "复盘预测", "复盘+预测", "今日复盘预测", "review_forecast",
    ],
}
# 规则2说：除了"盘前新闻"另外两个复用原逻辑 → 归到 RouteBranch.PRESET_SHORTCUT_OTHER
PRESET_OTHER_KEYWORDS = (
    PRESET_SHORTCUT_MAP["ESSAY_HEAT_HINT"] + PRESET_SHORTCUT_MAP["REVIEW_FORECAST_HINT"]
)

# 代码生成（规则3.4）：宽松"写*脚本"、"爬*数据"、"*代码"、"自动化"等
CODE_KEYWORDS = re.compile(
    r"(写.*脚本|脚本|写.*代码|python.*代码|爬.*数据|爬取|爬虫|抓.*数据|抓取|自动化脚本|生成脚本|自动.*化)",
    re.I,
)
# 影响分析/风险评估/多空判断（规则3.5）：宽松"分析*影响"、"评估*风险"、"利多/利空/利好"、"涨跌/牛熊"
ANALYSIS_KEYWORDS = re.compile(
    r"(分析.*影响|影响.*分析|评估.*风险|风险.*评估|利多|利空|利好|多空|后市分析|走势研判|"
    r"会不会涨|会不会跌|能买吗|能卖吗|是涨还是跌|还能涨|还能跌|影响.*大不大|"
    r"有什么.*风险|上涨逻辑|下跌逻辑|长期.*看好|长期.*看空)",
    re.I,
)
# 盘前新闻（中文、英文变体，规则1非快捷按钮的普通 query 触发）
PREMARKET_KEYWORDS = re.compile(r"(盘前新闻|盘前|premarket|pre[_ -]?market|盘前分析)", re.I)


# ---------------------------------------------------------------------------
# 规则优先 Router（同步）
# ---------------------------------------------------------------------------

def _rule_based_decide(query: str, *, has_visual_input: bool = False) -> RouterDecision:
    """Phase 1 纯规则路由；返回 RouterDecision（confidence 规则命中=1.0，仅股票匹配=0.95）。"""
    q = (query or "").strip()
    q_no_space = q.replace(" ", "")
    decision = RouterDecision(
        branch=RouteBranch.FALLBACK,
        decided_by="rule_based",
        reason="规则路由初始化",
        has_visual_input=bool(has_visual_input),
    )
    if not q:
        decision.branch = RouteBranch.GENERAL_QUERY
        decision.reason = "空 query，走通用查询"
        decision.confidence = 1.0
        return decision

    # --- 1) 快捷按钮精确匹配 ---
    for needle in PRESET_SHORTCUT_MAP["PREMARKET_NEWS_HINT"]:
        if q_no_space == needle or q.startswith(needle) or needle in q:
            decision.branch = RouteBranch.PRE_MARKET_NEWS
            decision.reason = "命中快捷按钮/显式关键词：盘前新闻"
            decision.from_shortcut_button = True
            decision.shortcut_type = "premarket_news"
            decision.confidence = 1.0
            return decision
    for needle in PRESET_OTHER_KEYWORDS:
        if q_no_space == needle or needle in q:
            decision.branch = RouteBranch.PRESET_SHORTCUT_OTHER
            decision.reason = f"命中快捷按钮：{needle!r}（复用原逻辑，无需改）"
            decision.from_shortcut_button = True
            decision.shortcut_type = "essay_heat" if needle in PRESET_SHORTCUT_MAP["ESSAY_HEAT_HINT"] else "review_forecast"
            decision.confidence = 1.0
            return decision

    # --- 2) 图片/图表多模态 ---
    if has_visual_input:
        decision.branch = RouteBranch.VISION
        decision.reason = "含图片/图表输入 → qwen3-vl:8b 视觉 Agent"
        decision.confidence = 1.0
        return decision

    # --- 3) 代码生成关键词（规则 3.4）---
    code_hit = bool(CODE_KEYWORDS.search(q))
    decision.has_code_keywords = code_hit

    # --- 4) 影响/风险/多空 关键词（规则 3.5）---
    analysis_hit = bool(ANALYSIS_KEYWORDS.search(q))
    decision.has_analysis_keywords = analysis_hit

    # --- 5) 股票匹配（规则 3.1/3.2）---
    stock_names: List[str] = []
    stock_codes: List[str] = []
    try:
        # 优先用 stock_matcher（保守：失败不抛）
        from shared.data_sources.stock_matcher import extract_stocks  # type: ignore
        matched = extract_stocks(q)
        if matched:
            for si in matched:
                if getattr(si, "name", None):
                    stock_names.append(str(si.name))
                if getattr(si, "code", None):
                    stock_codes.append(str(si.code))
            decision.has_stock_keywords = True
            decision.extracted_stock_names = list(dict.fromkeys(stock_names))
            decision.extracted_stock_codes = list(dict.fromkeys(stock_codes))
    except Exception:
        # 股票清单没加载也不影响主流程 — 回退正则 6 位数字
        for m in re.findall(r"\b\d{6}\b", q):
            stock_codes.append(m)
            decision.has_stock_keywords = True
        decision.extracted_stock_codes = list(dict.fromkeys(stock_codes))

    # --- 6) 显式"盘前新闻" → PRE_MARKET_NEWS ---
    if PREMARKET_KEYWORDS.search(q):
        decision.branch = RouteBranch.PRE_MARKET_NEWS
        decision.reason = "query 包含盘前新闻关键词 → 查6h缓存→并发4源→deepseek-r1推理→保存"
        decision.confidence = 1.0
        return decision

    # --- 7) 代码生成优先于其他 ---
    if code_hit:
        decision.branch = RouteBranch.CODE_GENERATION
        decision.reason = "命中代码/爬虫关键词 → qwen2.5-coder:7b"
        decision.confidence = 0.9
        return decision

    # --- 8) 含股票 → STOCK_QUERY ---
    if decision.has_stock_keywords:
        if analysis_hit:
            decision.branch = RouteBranch.STOCK_QUERY  # 4源并发后 reasoning 负责分析
            decision.reason = "含股票+分析关键词 → 并发4源(180s) → Aggregator → deepseek-r1 推理最终输出"
            decision.confidence = 0.95
            return decision
        decision.branch = RouteBranch.STOCK_QUERY
        decision.reason = "识别到个股/股票代码 → 并发4源(1周缓存+联网+zsxq2条+IMA，180s超时) → Aggregator → DEEPSEEK_V4_FLASH"
        decision.confidence = 0.95
        return decision

    # --- 9) 无股票 + 分析关键词 → IMPACT_ANALYSIS（规则 3.5，没股票也允许）
    if analysis_hit:
        decision.branch = RouteBranch.IMPACT_ANALYSIS
        decision.reason = "命中分析影响/风险/多空关键词 → deepseek-r1:7b（并发联网+zsxq）"
        decision.confidence = 0.88
        return decision

    # --- 10) 兜底：GENERAL_QUERY ---
    decision.branch = RouteBranch.GENERAL_QUERY
    decision.reason = "未命中任何快捷/代码/股票/分析关键词 → 通用查询：并发联网搜 + zsxq 最新信息"
    decision.confidence = 0.8  # < 0.8 会触发 gemma4 语义级联（如果开启）
    return decision


# ---------------------------------------------------------------------------
# Phase 2：gemma4:e4b 语义级联（仅当用户允许且 confidence<0.8 时调用）
# ---------------------------------------------------------------------------

_GEMMA4_ROUTER_PROMPT_TEMPLATE = """你是一个金融投研多Agent系统的路由器，必须严格按以下JSON格式输出路由决策，**只返回JSON，不能有任何解释文字**。

用户输入：{query}

可选的 route 枚举（只能选一个）：
  PRE_MARKET_NEWS         = 用户问盘前新闻（今天/最近的盘前汇总）
  PRESET_SHORTCUT_OTHER   = 盘前小作文热度 或 复盘预测 快捷按钮
  STOCK_QUERY             = 用户输入包含具体股票/板块，需要4源整合分析
  GENERAL_QUERY           = 普通查询，不涉及个股，只需联网+知识星球
  CODE_GENERATION         = 用户要求写脚本/爬虫/抓数据/代码生成
  IMPACT_ANALYSIS         = 用户想分析影响/评估风险/利多利空/涨跌研判
  VISION                  = 含图片/图表多模态
  FALLBACK                = 完全无法判断

输出 JSON Schema：
{{
  "branch": "<上面枚举字符串之一>",
  "reason": "<中文20字以内简短原因>",
  "has_stock_keywords": true/false,
  "extracted_stock_names": ["名称1", "名称2"],
  "extracted_stock_codes": ["代码1"],
  "has_code_keywords": true/false,
  "has_analysis_keywords": true/false,
  "confidence": 0.0~1.0
}}"""


def _parse_gemma4_json(text: str, fallback: RouterDecision) -> RouterDecision:
    """尽力从 gemma4 输出解析 JSON；失败保留 fallback（绝不抛异常）。"""
    if not text:
        return fallback
    # 1. 剥离 <think>...</think>（gemma4 可能输出推理链）
    clean = re.sub(r"<think>.*?</think>", "", text, flags=re.S | re.I).strip()
    # 2. 取第一个 {...}
    m = re.search(r"\{.*\}", clean, flags=re.S)
    if not m:
        return fallback
    try:
        obj = _json.loads(m.group(0))
    except Exception:
        return fallback
    try:
        branch_str = str(obj.get("branch", fallback.branch.value)).strip().upper()
        try:
            branch = RouteBranch(branch_str.lower())
        except Exception:
            # 大小写不敏感
            for rb in RouteBranch:
                if rb.value.lower() == branch_str.lower():
                    branch = rb
                    break
            else:
                branch = fallback.branch
        merged = fallback.model_copy(update={
            "branch": branch,
            "reason": str(obj.get("reason") or fallback.reason),
            "has_stock_keywords": bool(obj.get("has_stock_keywords", fallback.has_stock_keywords)),
            "extracted_stock_names": list(obj.get("extracted_stock_names") or fallback.extracted_stock_names),
            "extracted_stock_codes": list(obj.get("extracted_stock_codes") or fallback.extracted_stock_codes),
            "has_code_keywords": bool(obj.get("has_code_keywords", fallback.has_code_keywords)),
            "has_analysis_keywords": bool(obj.get("has_analysis_keywords", fallback.has_analysis_keywords)),
            "confidence": float(obj.get("confidence") or fallback.confidence or 0.0),
            "decided_by": "gemma4",
        })
        return merged
    except Exception:
        return fallback


async def cascade_gemma4_decide(query: str, rule_result: RouterDecision, *, model: str = "gemma4:e4b") -> RouterDecision:
    """Phase 2：gemma4 语义兜底。调用失败不抛，返回 rule_result。"""
    if rule_result.confidence >= 0.85:
        # 规则已经很可靠，不浪费本地 GPU
        return rule_result
    prompt = _GEMMA4_ROUTER_PROMPT_TEMPLATE.format(query=query)
    try:
        from shared.llm_client.ollama_client import ollama_chat_stream  # type: ignore
        chunks = []
        async for chunk in ollama_chat_stream(
            messages=[{"role": "user", "content": prompt}],
            model=model,
            temperature=0.0,
        ):
            if hasattr(chunk, "text"):
                chunks.append(str(chunk.text or ""))
            elif isinstance(chunk, str):
                chunks.append(chunk)
        merged = _parse_gemma4_json("".join(chunks), rule_result)
        merged.cascade_upgrade_suggestion = None  # gemma4 已是本地最轻量路由模型
        return merged
    except Exception:
        # gemma4 不可用：静默降级（本地 Ollama 没启/模型没下 都不影响主流程）
        rule_result.decided_by = "rule_based"
        return rule_result


# ---------------------------------------------------------------------------
# 对外函数（同步 + 异步两个版本；异步版本会调用 gemma4 级联，同步版本只走规则）
# ---------------------------------------------------------------------------

def decide(query: str, *, has_visual_input: bool = False) -> RouterDecision:
    """同步：只走 Phase 1 规则路由（用于需要快速判断的 API 入口短路）。"""
    return _rule_based_decide(query, has_visual_input=has_visual_input)


async def decide_cascade(
    query: str,
    *,
    has_visual_input: bool = False,
    enable_gemma4: bool = True,
) -> RouterDecision:
    """异步：Phase 1 规则 → Phase 2 gemma4 级联（默认开启）。"""
    rule_res = _rule_based_decide(query, has_visual_input=has_visual_input)
    if not enable_gemma4:
        return rule_res
    return await cascade_gemma4_decide(query, rule_res)


# ============================================================
# 快速冒烟测试：直接运行 `python -m agents.router.agent`
# ============================================================
if __name__ == "__main__":  # pragma: no cover
    async def _smoke():
        cases = [
            ("盘前新闻",),
            ("今天小作文热度",),
            ("复盘预测",),
            ("写个爬虫脚本抓茅台股价",),
            ("茅台今天的利多利空分析",),
            ("给我分析一下宁德时代的估值和护城河",),
            ("美联储加息对A股的影响分析",),
            ("今天天气怎么样",),
        ]
        for (q,) in cases:
            d = decide(q)
            print(f"[RULE] {q!r:30s} → branch={d.branch.value:22s}  has_stock={d.has_stock_keywords}  has_code={d.has_code_keywords}  reason={d.reason}")
    asyncio.run(_smoke())
