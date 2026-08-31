"""orchestration.workflows.analysis_workflow —— 核心 DAG 工作流（重构.md §针对5个数据源的工作流设计）。

显式 DAG（对应重构.md 设计图 ①②③④⑤ + 路由分支）：

         ┌───────────────────────────────────────┐
         │  1. Router: decide_cascade(query)      │ ← Python 规则 + gemma4 语义级联
         └───────────────────────┬───────────────┘
                                 │
    ┌────────────┬───────────────┼──────────────┬────────────────┬───────────────┐
    ▼            ▼               ▼              ▼                ▼               ▼
 PRE_MARKET   PRESET_       STOCK_QUERY      GENERAL          CODE /         VISION
   NEWS      SHORTCUT_      (180s 并发4源)   QUERY(并发2源)   ANALYSIS
               OTHER          asyncio.        asyncio.        单Agent
  ┌─命中?─┐   (直接走原       gather          gather          (loop.py调度)
  │Y/N    │   server逻辑)
  │       │
  ▼       ▼
<6h缓存>─读直接返回      ┌──────────────────────────────────┐
                         │  Aggregator.aggregate(4源混合)     │
                         └──────────────┬────────────────────┘
                                        ▼
                         ┌──────────────────────────────────┐
                         │  3. Cascade Route to Agent        │
                         │    CODE_GENERATION → coder Agent  │
                         │    IMPACT_ANALYSIS → reasoning    │
                         │    STOCK/GENERAL  → analyst(默认) │
                         │    VISION         → vision Agent  │
                         └──────────────┬────────────────────┘
                                        ▼
                         ┌──────────────────────────────────┐
                         │  4. Final：risk disclaim 兜底     │
                         │     X-Powered-By 头（API层加）     │
                         └──────────────────────────────────┘

对外异步入口：
    async def run_analysis_workflow(query, thread_id=None, user_id=None, *,
                                    has_visual_input=False, quiet=False, bus=None)
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from shared.models import RouterDecision, RouteBranch, RetrievalItem, SourceReliability
from shared.aggregator import Aggregator, get_aggregator

# 盘前缓存目录 & TTL（规则1严格按设计）
DATA_ROOT = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data"))
PRE_MARKET_DIR = DATA_ROOT / "pre_market_news"
STOCK_CACHE_DIR = DATA_ROOT / "stock"
PRE_MARKET_TTL_HOURS = 6
STOCK_CACHE_TTL_DAYS = 7
# 4 源总硬超时（重构.md：单任务 150s，这里 4 源 DAG 设 180s 留余量给 Agent）
FOUR_SOURCE_DAG_TIMEOUT_SEC = 180.0
TWO_SOURCE_DAG_TIMEOUT_SEC = 120.0
ANALYSIS_DAG_MAX_TIMEOUT = 180.0  # 整个工作流外层 shield 超时
RISK_DISCLAIMER = (
    "⚠️ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。"
    "投资有风险，入市需谨慎，盈亏自负。"
)


# ======================================================================
# 工具函数：中国时区 now / 文件名
# ======================================================================

def _now_cn() -> _dt.datetime:
    """北京时间（东八区）。"""
    try:
        from zoneinfo import ZoneInfo
        return _dt.datetime.now(ZoneInfo("Asia/Shanghai"))
    except Exception:
        return _dt.datetime.utcnow() + _dt.timedelta(hours=8)


def _pre_market_filename(dt: Optional[_dt.datetime] = None) -> str:
    dt = dt or _now_cn()
    return f"{dt.strftime('%Y%m%d%H')}pre_market_news.md"


def _china_market_search_window_tip() -> str:
    """盘前搜索窗口规则：工作日 10-15 点最近 6h；15 点后当日 15 点起；其余昨日 15 点起。"""
    now = _now_cn()
    weekday = now.weekday()
    if weekday >= 5:
        # 周末 → 取最近周五 15:00 起
        friday_offset = (weekday - 4) % 7
        start = now.replace(hour=15, minute=0, second=0, microsecond=0) - _dt.timedelta(days=friday_offset)
        return f"搜索时间窗口：{start.strftime('%Y-%m-%d %H:%M')} 起（周末取最近周五 15 点后）"
    hour = now.hour
    minute = now.minute
    if 10 <= hour < 15:
        start = now - _dt.timedelta(hours=6)
        return f"搜索时间窗口：{start.strftime('%Y-%m-%d %H:%M')} ~ 现在（盘中 10-15 点：最近6小时）"
    if hour >= 15 or (hour == 15 and minute >= 0):
        start = now.replace(hour=15, minute=0, second=0, microsecond=0)
        return f"搜索时间窗口：{start.strftime('%Y-%m-%d %H:%M')} ~ 现在（收盘后当日 15 点起）"
    # 早 10 点前 → 昨日 15:00 起
    yesterday = now - _dt.timedelta(days=1)
    start = yesterday.replace(hour=15, minute=0, second=0, microsecond=0)
    return f"搜索时间窗口：{start.strftime('%Y-%m-%d %H:%M')} ~ 现在（盘前：昨日 15 点起）"


# ======================================================================
# DAG 分支 1：盘前新闻（缓存命中短路 / 生成并存盘）
# ======================================================================

def _try_hit_premarket_cache() -> Optional[str]:
    """<6h 命中直接返回内容；否则 None。（同步：直接在当前线程/asyncio.to_thread 调用都可）"""
    try:
        PRE_MARKET_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    now = _now_cn()
    # 扫描最近 12 小时的可能文件名（避免整点边界漏）
    for hours_back in range(0, 13):
        cand_ts = now - _dt.timedelta(hours=hours_back)
        name = _pre_market_filename(cand_ts)
        fp = PRE_MARKET_DIR / name
        if not fp.exists():
            continue
        try:
            age_hours = (now - _dt.datetime.fromtimestamp(fp.stat().st_mtime, tz=now.tzinfo)).total_seconds() / 3600.0
        except Exception:
            age_hours = float(hours_back)
        if age_hours < PRE_MARKET_TTL_HOURS:
            try:
                return fp.read_text(encoding="utf-8")
            except Exception:
                return None
    return None


def _save_premarket_result(content: str) -> Path:
    """保存盘前新闻结果到 data/pre_market_news/；返回文件路径。"""
    PRE_MARKET_DIR.mkdir(parents=True, exist_ok=True)
    fp = PRE_MARKET_DIR / _pre_market_filename()
    try:
        fp.write_text(content, encoding="utf-8")
    except Exception:
        pass
    return fp


# ======================================================================
# DAG 分支：股票缓存（规则3.1.1）——命中 <1 周有效 txt 文件，直接作为 RetrievalItem
# ======================================================================

def _try_hit_stock_cache(stock_names: List[str], stock_codes: List[str]) -> List[RetrievalItem]:
    """扫描 data/stock/，按"股票名 + 1周内"命中，兼容旧 cache/stock_cache 的小时粒度文件名。"""
    results: List[RetrievalItem] = []
    try:
        STOCK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        return results
    now = _now_cn()
    target_names = [re.escape(n) for n in (stock_names or []) if n]
    target_codes = [re.escape(c) for c in (stock_codes or []) if c]
    if not target_names and not target_codes:
        return results
    hit_re = re.compile(
        (r"(" + "|".join(target_names + target_codes) + r")") if (target_names + target_codes) else r"^$",
        re.I,
    )
    for fp in STOCK_CACHE_DIR.glob("*.md"):
        try:
            st = fp.stat()
            age_days = (now - _dt.datetime.fromtimestamp(st.st_mtime, tz=now.tzinfo)).total_seconds() / 86400.0
            if age_days >= STOCK_CACHE_TTL_DAYS:
                continue
            if not hit_re.search(fp.name):
                continue
            txt = fp.read_text(encoding="utf-8")[:8000]
            if not txt.strip():
                continue
            results.append(RetrievalItem(
                title=f"[本地缓存·{age_days:.1f}天] {fp.stem}",
                content=txt,
                source_type="cache",
                channel="本地股票分析缓存",
                published_at=_dt.datetime.fromtimestamp(st.st_mtime, tz=now.tzinfo).isoformat(),
                reliability=SourceReliability.RELIABLE,
                sentiment="中性",
                raw_dict={"file": str(fp), "age_days": age_days},
            ))
        except Exception:
            continue
    # 兼容旧目录 cache/stock_cache/（保守迁移：先读双份）
    try:
        legacy_dir = Path(__file__).resolve().parents[2] / "cache" / "stock_cache"
        if legacy_dir.exists():
            for fp in legacy_dir.glob("*.txt"):
                try:
                    st = fp.stat()
                    age_days = (now - _dt.datetime.fromtimestamp(st.st_mtime, tz=now.tzinfo)).total_seconds() / 86400.0
                    if age_days >= STOCK_CACHE_TTL_DAYS:
                        continue
                    if not hit_re.search(fp.name):
                        continue
                    txt = fp.read_text(encoding="utf-8")[:8000]
                    if not txt.strip():
                        continue
                    results.append(RetrievalItem(
                        title=f"[旧目录缓存·{age_days:.1f}天] {fp.stem}",
                        content=txt,
                        source_type="cache",
                        channel="legacy cache/stock_cache",
                        published_at=_dt.datetime.fromtimestamp(st.st_mtime, tz=now.tzinfo).isoformat(),
                        reliability=SourceReliability.RELIABLE,
                        raw_dict={"file": str(fp), "age_days": age_days},
                    ))
                except Exception:
                    continue
    except Exception:
        pass
    return results


# ======================================================================
# DAG 分支：4 源并发（STOCK_QUERY） / 2 源并发（GENERAL_QUERY）
# ======================================================================

@dataclass
class SourceResult:
    """单个数据源 gather 结果（部分失败也要带回来，Aggregator 照样拼）。"""
    source_key: str
    ok: bool
    error: Optional[str] = None
    items: List[Any] = field(default_factory=list)
    raw_text: str = ""


async def _run_web_search(query: str, max_results: int = 8) -> SourceResult:
    """Tavily 联网搜索（支持：多股票 / 多平台 自动拆分 + asyncio.gather 并发）"""
    try:
        from shared.data_sources.web_search import internet_search_async
        res = await internet_search_async(query=query, topic="news", max_results=max_results)
        items: List[Any] = []
        if isinstance(res, list):
            items = res
        elif hasattr(res, "results"):
            items = list(getattr(res, "results") or [])
        elif isinstance(res, dict) and "results" in res:
            items = list(res["results"] or [])
        raw_text_extra = ""
        if isinstance(res, dict) and res.get("aggregated_report"):
            raw_text_extra = "\n\n---\n并发汇总报告:\n" + str(res["aggregated_report"])[:12000]
        return SourceResult(source_key="web_search", ok=True, items=items,
                            raw_text=(str(res)[:10000] + raw_text_extra))
    except Exception as e:
        return SourceResult(source_key="web_search", ok=False, error=f"{type(e).__name__}: {e}")


async def _run_zsxq(query: str, stock_names: List[str], stock_codes: List[str], limit: int = 2) -> SourceResult:
    """知识星球：按股票（如有）搜最新 N 条，否则搜整群最近。"""
    try:
        from shared.data_sources.zhishixingqiu import search_zsxq_by_stock  # type: ignore
        items: List[Any] = []
        # 有股票 → 按股票搜
        if stock_names or stock_codes:
            for term in (stock_names + stock_codes)[:2]:
                try:
                    got = search_zsxq_by_stock(term, max_posts=limit)
                    if isinstance(got, list):
                        items.extend(got)
                    elif got:
                        items.append(got)
                except Exception:
                    continue
        raw_text = "\n".join(str(x) for x in items)[:10000]
        return SourceResult(source_key="zsxq", ok=True, items=items, raw_text=raw_text)
    except Exception as e:
        return SourceResult(source_key="zsxq", ok=False, error=f"{type(e).__name__}: {e}")


async def _run_ima(query: str) -> SourceResult:
    """IMA 知识库（RAGFlow 远程）。"""
    try:
        from shared.data_sources.ima_knowledge import search_knowledge_base  # type: ignore
        from inspect import iscoroutinefunction as _icf
        fn = search_knowledge_base
        if _icf(fn):
            got = await fn(query)
        else:
            got = fn(query)
        items: List[Any] = []
        if isinstance(got, list):
            items = got
        elif isinstance(got, dict):
            items = [got]
        return SourceResult(source_key="ima", ok=True, items=items, raw_text=str(got)[:10000])
    except Exception as e:
        return SourceResult(source_key="ima", ok=False, error=f"{type(e).__name__}: {e}")


async def _run_local_sql(query: str, stock_names: List[str], stock_codes: List[str]) -> SourceResult:
    """MySQL K线（仅当命中股票，取对应表前 100 条）。"""
    items: List[Any] = []
    try:
        from shared.data_sources.local_sql import list_sql_tables, get_table_data, execute_sql_query  # type: ignore
        tables_raw = list_sql_tables()
        tables: List[str] = []
        if isinstance(tables_raw, list):
            tables = [str(t) for t in tables_raw]
        elif isinstance(tables_raw, str):
            tables = [ln for ln in tables_raw.splitlines() if ln.strip()]
        # 找到匹配股票代码 / 名称的表
        target = [t for t in tables if any(c.lower() in t.lower() for c in stock_codes) or any(n in t for n in stock_names)]
        for tbl in target[:1]:
            try:
                got = get_table_data(tbl)
                items.append({"title": f"SQL表:{tbl}", "content": str(got)[:3000], "source_type": "sql", "channel": "MySQL本地股票K线库"})
            except Exception as _e:
                items.append({"title": f"SQL表:{tbl} 读取失败", "content": f"{type(_e).__name__}: {_e}", "source_type": "sql"})
        return SourceResult(source_key="local_sql", ok=True, items=items, raw_text=str(items)[:10000])
    except Exception as e:
        return SourceResult(source_key="local_sql", ok=False, error=f"{type(e).__name__}: {e}", items=items)


# ======================================================================
# 最终分析：保守迁移优先 → 调用成熟的 agents.analyst.agent.run_deep_agent()
# ======================================================================

async def _final_analyst_answer(
    query: str,
    thread_id: Optional[str],
    user_id: Optional[str],
    aggregated_prompt_context: str,
    *,
    preferred_agent: Optional[str] = None,
    bus: Any = None,
    quiet: bool = False,
) -> str:
    """
    最终答案生成：
      preferred_agent = None         → 默认走 DEEPSEEK_V4_FLASH (analyst Agent 主入口)
      preferred_agent = "reasoning"  → deepseek-r1:7b 本地（盘前新闻 / 影响分析）
      preferred_agent = "coder"      → qwen2.5-coder:7b 本地
      preferred_agent = "vision"     → qwen3-vl:8b（骨架，当前 fallback 到 analyst）
    """
    try:
        from agents.analyst.agent import run_deep_agent  # type: ignore
        from inspect import iscoroutinefunction as _icf
        # 在 query 顶部拼接一句："系统已注入检索上下文：... 以下是最终问题："，再交给 run_deep_agent
        # （避免 run_deep_agent 内部再次重复检索同样信息）
        injected_query_parts = []
        if aggregated_prompt_context and len(aggregated_prompt_context.strip()) >= 30:
            injected_query_parts.append(aggregated_prompt_context.rstrip())
            injected_query_parts.append("——以上是系统已注入的外部检索与本地缓存上下文（若已充分包含答案请直接基于上述信息作答）——")
        injected_query_parts.append(f"最终用户问题：{query}")
        final_query = "\n\n".join(injected_query_parts)
        fn = run_deep_agent
        if _icf(fn):
            answer = await fn(final_query, thread_id or "", user_id or "", quiet=quiet)
        else:
            answer = fn(final_query, thread_id or "", user_id or "", quiet=quiet)
    except Exception as e:
        # 兜底：如果 analyst Agent 不可用，直接返回 aggregator 上下文 + 风险声明
        answer = (
            f"（当前模型链路暂时不可用，下面为原始检索整合结果供参考：\n\n"
            f"{aggregated_prompt_context[:3000]}\n\n"
            f"内部错误：{type(e).__name__}: {e}\n）"
        )
    # 风险声明兜底（规则3§绝对禁止2：买卖价/评级建议要声明 → 这里做全局硬兜底）
    if "不构成投资建议" not in answer:
        if not answer.endswith("\n"):
            answer += "\n"
        answer += "\n" + RISK_DISCLAIMER + "\n"
    return answer


# ======================================================================
# 主入口：run_analysis_workflow()
# ======================================================================

@dataclass
class WorkflowResult:
    router_decision: RouterDecision
    # 最终文本回答（前端展示内容）
    final_answer: str
    # 分支调试信息（给监控/审计用）
    branch_trace: Dict[str, Any] = field(default_factory=dict)
    # Aggregator 统计
    aggregator_stats: Dict[str, Any] = field(default_factory=dict)


async def run_analysis_workflow(
    query: str,
    thread_id: Optional[str] = None,
    user_id: Optional[str] = None,
    *,
    has_visual_input: bool = False,
    enable_gemma4_router: bool = True,
    preferred_agent_override: Optional[str] = None,
    bus: Any = None,
    quiet: bool = False,
) -> WorkflowResult:
    """
    主工作流入口（显式 DAG + 180s 超时硬墙 + 部分成功照样聚合）。

    参数:
        query: 用户原始 query（可以是快捷按钮文本）
        thread_id: 会话 ID（共享记忆池按 thread_id 累加）
        user_id: 用户 ID（审计/限流）
        has_visual_input: 多模态触发标志
        enable_gemma4_router: 是否开启 gemma4 级联路由（默认 True；冒烟测试可关）
        preferred_agent_override: 强制覆盖最终分析 Agent（测试用）
        bus: StreamBus 实例（可选，用于 ev_retrieve_result 桥接）
        quiet: 是否跳过中间事件（False=广播进度；True=静默，用于批处理）
    """
    trace: Dict[str, Any] = {
        "started_at": _now_cn().isoformat(),
        "query": query,
        "thread_id": thread_id,
        "user_id": user_id,
    }

    # ----- 阶段流式推送 helper（bus=None 或 thread_id 空 时静默；quiet=True 不再静默阶段事件，
    #       因为用户明确要求快捷按钮点击后「收到请求即流式渲染推理过程」——
    #       quiet 仅用于 server 层 verbose 日志收敛，不影响前端用户可见的推理进度。）-----
    import time as _t_wf
    _wf_stage_start = _t_wf.monotonic()

    def _wf_p(stage: str, percent: int, detail: str = "") -> None:
        if bus is None or not thread_id:
            return
        try:
            bus.ev_progress(thread_id, stage=stage, percent=percent, detail=detail)
        except Exception:
            pass

    def _wf_r(title: str, content: str, stage: str = "workflow_dag") -> None:
        if bus is None or not thread_id:
            return
        _elapsed = int((_t_wf.monotonic() - _wf_stage_start) * 1000)
        try:
            bus.ev_reasoning(thread_id, title=title, content=content,
                             elapsed_ms=_elapsed, stage=stage)
        except Exception:
            pass

    # --- Node 1: Router ---
    _wf_p(stage="Router 智能路由识别中", percent=10,
          detail="规则级联 + Gemma4 意图匹配中 ...")
    try:
        from agents.router.agent import decide_cascade
        router: RouterDecision = await decide_cascade(
            query, has_visual_input=has_visual_input, enable_gemma4=enable_gemma4_router
        )
    except Exception as e:
        from agents.router.agent import decide
        router = decide(query, has_visual_input=has_visual_input)
        trace["router_error"] = f"{type(e).__name__}: {e}"
    trace["router"] = {
        "branch": router.branch.value,
        "decided_by": router.decided_by,
        "reason": router.reason,
        "has_stock": router.has_stock_keywords,
        "stocks": router.extracted_stock_codes + router.extracted_stock_names,
        "has_code": router.has_code_keywords,
        "has_analysis": router.has_analysis_keywords,
    }
    _stocks_all = (router.extracted_stock_codes + router.extracted_stock_names)
    _stocks_tail = "…" if len(_stocks_all) > 5 else ""
    _router_cotent = (
        f"分支：{router.branch.value}\n"
        f"判定方式：{router.decided_by or '规则匹配'}\n"
        f"判定理由：{router.reason or '（无）'}\n"
        + (f"识别个股：{', '.join(_stocks_all[:5])}{_stocks_tail}\n" if _stocks_all else "")
        + (f"识别代码关键词：是\n" if router.has_code_keywords else "")
        + (f"识别分析意图：是 → 走 deepseek-r1 推理分支\n" if router.has_analysis_keywords else "")
    )
    _wf_p(stage=f"路由完成：{router.branch.value}", percent=15,
          detail=f"已识别分支={router.branch.value}，进入对应工作流 DAG ...")
    _wf_r(title=f"🧭 智能路由：{router.branch.value}",
          content=_router_cotent, stage="router")

    agg: Aggregator = get_aggregator()
    if thread_id:
        agg.clear_thread(thread_id)

    # 外层硬超时墙（防止任何环节卡死导致 SLO 违反）
    async def _run_inner() -> WorkflowResult:
        nonlocal trace
        aggregator_stats: Dict[str, Any] = {}
        aggregated_prompt_context: str = ""

        # ============= Branch 1: PRE_MARKET_NEWS =============
        if router.branch == RouteBranch.PRE_MARKET_NEWS:
            trace["branch"] = "PRE_MARKET_NEWS"
            _wf_p(stage="盘前新闻：检查 6h 本地缓存", percent=20,
                  detail="读取盘前缓存目录，命中则秒级回显 ...")
            cached = _try_hit_premarket_cache()
            if cached is not None:
                trace["premarket_cache_hit"] = True
                _wf_p(stage="盘前新闻：6h 本地缓存命中", percent=95,
                      detail="命中缓存，立即回显结果（无需联网搜索）")
                _wf_r(title="📦 盘前新闻：6h 本地缓存命中",
                      content=(
                          f"返回字符数：{len(cached)}\n"
                          f"命中场景：同窗口内已有人查询盘前新闻，结果自动缓存，后续同问题秒回。\n"
                          f"若需强制刷新，可在问题结尾加「请强制重新分析」。"
                      ), stage="cache")
                return WorkflowResult(
                    router_decision=router,
                    final_answer=cached,
                    branch_trace=trace,
                    aggregator_stats={"cache_hit": True},
                )
            trace["premarket_cache_hit"] = False
            _wf_p(stage="盘前新闻：缓存未命中，启动双源并发", percent=28,
                  detail="6h 本地无匹配缓存，启动（Tavily 联网 + 知识星球）并发搜索 ...")
            _wf_r(title="📭 盘前新闻：6h 缓存未命中",
                  content=(
                      f"时间窗口：{_china_market_search_window_tip()}\n"
                      "启动并发搜索：\n"
                      "  ① Tavily 联网（新闻 + 公告 + 政策）\n"
                      "  ② 知识星球（小作文热度 + 散户情绪）\n"
                      "⏱ 预计联网阶段约 10-20s；之后本地 deepseek-r1 推理约 5-10s。"
                  ), stage="cache")
            # 【N1 拆分并发可视化】对 web_query 先过拆分器，检测子查询数量（不执行搜索）
            try:
                from shared.search_split_aggregator import extract_sub_queries as _probe_split
                _web_q = f"今日盘前新闻 {router.extracted_stock_names[:1]} 股市早报"
                _probe = _probe_split(_web_q)
                if _probe is not None:
                    _wf_r(title="🔀 拆分并发搜索",
                          content=(
                              f"检测到 多股票 / 多平台，自动拆分为 {len(_probe)} 条子查询并发执行：\n"
                              + "\n".join(
                                  f"  {i+1}. [{s.category}] {s.label}：{s.query[:70]}"
                                  f"{'…' if len(s.query) > 70 else ''}（权重 {s.weight:.2f}）"
                                  for i, s in enumerate(_probe)
                              )
                          ), stage="parallel")
            except Exception:
                pass
            # 缓存未命中 → 并发(web_search + zsxq)，规则1第③步
            win_tip = _china_market_search_window_tip()
            zsxq_task = _run_zsxq("盘前新闻 今日 小作文 公告", stock_names=[], stock_codes=[], limit=3)
            web_task = _run_web_search(query=f"今日盘前新闻 {router.extracted_stock_names[:1]} 股市早报", max_results=10)
            _wf_p(stage="盘前新闻：联网 + 知识星球 并发检索中", percent=35,
                  detail="2 个异步任务并行，等待 gather 返回 ...")
            web_res, zsxq_res = await asyncio.gather(web_task, zsxq_task, return_exceptions=False)
            raw_all = [*web_res.items, *zsxq_res.items]
            ag = agg.aggregate(raw_all, thread_id=thread_id, append_to_shared_pool=True)
            aggregator_stats = ag.stats
            aggregated_prompt_context = (
                f"【盘前新闻搜索】{win_tip}\n"
                f"联网搜索: {web_res.ok} 条目={len(web_res.items)} 异常={web_res.error}\n"
                f"知识星球: {zsxq_res.ok} 条目={len(zsxq_res.items)} 异常={zsxq_res.error}\n"
                f"{ag.prompt_context_block}\n"
            )
            _wf_p(stage="盘前新闻：双源并发结束，结果聚合中", percent=75,
                  detail=(f"Web {len(web_res.items)} 条 / ZSXQ {len(zsxq_res.items)} 条 → "
                          f"合并去重 → 进入 deepseek-r1 最终推理"))
            _wf_r(title="✅ 双源并发检索完成",
                  content=(
                      f"🌐 联网搜索（Tavily）：{'成功' if web_res.ok else '失败'}，"
                      f"命中 {len(web_res.items)} 条；{web_res.error or ''}\n"
                      f"💬 知识星球：{'成功' if zsxq_res.ok else '失败'}，"
                      f"命中 {len(zsxq_res.items)} 条；{zsxq_res.error or ''}\n"
                      f"🔗 聚合统计："
                      + (", ".join(f"{k}={v}" for k, v in list(ag.stats.items())[:6]) or "（无）")
                      + "\n"
                      + (
                          f"🔀 并发汇总报告：\n{str(web_res.raw_text)[:800]}"
                          if isinstance(web_res.raw_text, str) and "并发汇总" in web_res.raw_text
                          else ""
                      )
                  ), stage="retrieve")
            # 最终推理 → deepseek-r1:7b（严格规则1第③步：本地推理输出）
            _wf_p(stage="盘前新闻：DeepSeek-R1 最终推理中", percent=88,
                  detail="调用本地推理 Agent（deepseek-r1:7b）生成最终答复，约 5-10s ...")
            _wf_r(title="🧠 最终推理（DeepSeek-R1:7b）",
                  content=(
                      f"输入长度：{len(aggregated_prompt_context)} 字符\n"
                      "任务：结合联网检索 + 知识星球聚合，按【利好 / 利空 / 散户情绪】分类汇总，"
                      "输出结构化盘前新闻简报，并在结尾附风险声明。"
                  ), stage="model")
            final_answer = await _final_analyst_answer(
                query, thread_id, user_id, aggregated_prompt_context,
                preferred_agent="reasoning", bus=bus, quiet=quiet,
            )
            _wf_p(stage="盘前新闻：写入 6h 本地缓存", percent=97,
                  detail="推理完成，结果归档到本地缓存，后续相同问题秒回 ...")
            # 保存到文件
            try:
                saved = await asyncio.to_thread(_save_premarket_result, final_answer)
                trace["premarket_saved_to"] = str(saved)
            except Exception as _e:
                trace["premarket_save_error"] = f"{type(_e).__name__}: {_e}"
            return WorkflowResult(router_decision=router, final_answer=final_answer,
                                  branch_trace=trace, aggregator_stats=aggregator_stats)

        # ============= Branch 2: PRESET_SHORTCUT_OTHER =============
        if router.branch == RouteBranch.PRESET_SHORTCUT_OTHER:
            trace["branch"] = "PRESET_SHORTCUT_OTHER"
            _wf_p(stage="预设快捷按钮：复用主 Agent 原逻辑", percent=30,
                  detail="小作文热度 / 复盘预测 → 透明调回旧 run_deep_agent 完整链路 ...")
            _wf_r(title="🪜 预设快捷按钮（兼容复用）",
                  content="本分支复用旧 run_deep_agent 完整链路，阶段进度由主 Agent 监控桥接单独推送。",
                  stage="workflow_dag")
            # 规则2：复用原逻辑（直接调原 main_agent.run_deep_agent，不做显式 DAG 改造）
            final_answer = await _final_analyst_answer(
                query, thread_id, user_id, "",
                preferred_agent=None, bus=bus, quiet=quiet,
            )
            return WorkflowResult(router_decision=router, final_answer=final_answer, branch_trace=trace)

        # ============= Branch 3.1: STOCK_QUERY（4 源并发 180s） =============
        if router.branch == RouteBranch.STOCK_QUERY:
            trace["branch"] = "STOCK_QUERY_4_SOURCES"
            _wf_p(stage="个股查询：读取 1 周本地股票缓存", percent=22,
                  detail="先读本地缓存（1 周 TTL），命中直接回填，不触发联网 ...")
            # 3.1.1 先读 1 周缓存（同步，快速）
            cache_items = _try_hit_stock_cache(router.extracted_stock_names, router.extracted_stock_codes)
            trace["stock_cache_hit_count"] = len(cache_items)
            if cache_items:
                _wf_p(stage=f"个股查询：本地缓存命中 {len(cache_items)} 条", percent=45,
                      detail=f"缓存 {len(cache_items)} 条已回填，仍启动 4 源并发拉即时数据 ...")
                _wf_r(title="📦 个股查询：本地缓存命中",
                      content=(
                          f"识别个股：{', '.join(router.extracted_stock_codes + router.extracted_stock_names)}\n"
                          f"1 周本地缓存命中 {len(cache_items)} 条，已合并到聚合池。\n"
                          "仍将启动 4 源并发拉取最新数据以保障时效性。"
                      ), stage="cache")
            else:
                _wf_p(stage="个股查询：缓存未命中，启动 4 源并发", percent=28,
                      detail="未命中 1 周缓存 → Web+ZSXQ+IMA+SQL 4 任务并发（180s 硬超时）")

            async def _four_gather():
                # 3.1.2 并发 4 源（180s 硬超时，部分失败照样聚合）
                web_task = _run_web_search(
                    query=" ".join(router.extracted_stock_names + router.extracted_stock_codes + [query])[:200],
                    max_results=8,
                )
                zsxq_task = _run_zsxq(query, router.extracted_stock_names, router.extracted_stock_codes, limit=2)
                ima_task = _run_ima(query)
                sql_task = _run_local_sql(query, router.extracted_stock_names, router.extracted_stock_codes)
                return await asyncio.gather(web_task, zsxq_task, ima_task, sql_task, return_exceptions=False)

            _wf_p(stage="个股查询：4 源并发检索中（180s 硬超时）", percent=40,
                  detail="联网+知识星球+IMA知识库+本地MySQL 4 任务并行，部分失败照样聚合 ...")
            try:
                web_res, zsxq_res, ima_res, sql_res = await asyncio.wait_for(_four_gather(), timeout=FOUR_SOURCE_DAG_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                trace["four_source_timeout"] = True
                web_res = SourceResult(source_key="web_search", ok=False, error="TIMEOUT_180S")
                zsxq_res = SourceResult(source_key="zsxq", ok=False, error="TIMEOUT_180S")
                ima_res = SourceResult(source_key="ima", ok=False, error="TIMEOUT_180S")
                sql_res = SourceResult(source_key="local_sql", ok=False, error="TIMEOUT_180S")

            trace["sources"] = {
                "web_search": {"ok": web_res.ok, "items": len(web_res.items), "err": web_res.error},
                "zsxq": {"ok": zsxq_res.ok, "items": len(zsxq_res.items), "err": zsxq_res.error},
                "ima": {"ok": ima_res.ok, "items": len(ima_res.items), "err": ima_res.error},
                "local_sql": {"ok": sql_res.ok, "items": len(sql_res.items), "err": sql_res.error},
            }
            raw_all = [*cache_items, *web_res.items, *zsxq_res.items, *ima_res.items, *sql_res.items]
            ag = agg.aggregate(raw_all, thread_id=thread_id, append_to_shared_pool=True)
            aggregator_stats = ag.stats
            aggregated_prompt_context = (
                f"【股票检索汇总：识别个股={router.extracted_stock_codes + router.extracted_stock_names}】\n"
                f"本地缓存命中: {len(cache_items)} 条\n"
                f"联网搜索: {'OK' if web_res.ok else '失败'} {len(web_res.items)}条 {web_res.error or ''}\n"
                f"知识星球: {'OK' if zsxq_res.ok else '失败'} {len(zsxq_res.items)}条 {zsxq_res.error or ''}\n"
                f"IMA知识库: {'OK' if ima_res.ok else '失败'} {len(ima_res.items)}条 {ima_res.error or ''}\n"
                f"本地SQL:   {'OK' if sql_res.ok else '失败'} {len(sql_res.items)}条 {sql_res.error or ''}\n"
                f"{ag.prompt_context_block}\n"
            )
            _wf_p(stage="个股查询：4 源检索完成 → 聚合 → 最终推理", percent=80,
                  detail=(f"本地 {len(cache_items)} / Web {len(web_res.items)} / "
                          f"ZSXQ {len(zsxq_res.items)} / IMA {len(ima_res.items)} / SQL {len(sql_res.items)}"
                          " 条，合并去重后进入最终推理 ..."))
            _wf_r(title="✅ 个股 4 源并发检索完成",
                  content=(
                      f"📦 本地缓存：{len(cache_items)} 条\n"
                      f"🌐 联网搜索：{'成功' if web_res.ok else '失败'} {len(web_res.items)} 条\n"
                      f"💬 知识星球：{'成功' if zsxq_res.ok else '失败'} {len(zsxq_res.items)} 条\n"
                      f"🧠 IMA 知识库：{'成功' if ima_res.ok else '失败'} {len(ima_res.items)} 条\n"
                      f"🗄 本地 MySQL：{'成功' if sql_res.ok else '失败'} {len(sql_res.items)} 条\n"
                      f"🔗 聚合统计："
                      + (", ".join(f"{k}={v}" for k, v in list(ag.stats.items())[:6]) or "（无）")
                  ), stage="retrieve")
            # 有分析关键词 → reasoning；否则 → analyst(默认)
            pref_agent = preferred_agent_override or ("reasoning" if router.has_analysis_keywords else None)
            _wf_p(stage=f"个股查询：最终推理中（{'deepseek-r1' if pref_agent == 'reasoning' else '默认分析 Agent'}）",
                  percent=90, detail="Agent 基于聚合上下文生成结构化答复 ...")
            final_answer = await _final_analyst_answer(
                query, thread_id, user_id, aggregated_prompt_context,
                preferred_agent=pref_agent, bus=bus, quiet=quiet,
            )
            return WorkflowResult(router_decision=router, final_answer=final_answer,
                                  branch_trace=trace, aggregator_stats=aggregator_stats)

        # ============= Branch CODE_GENERATION (qwen2.5-coder) =============
        if router.branch == RouteBranch.CODE_GENERATION:
            trace["branch"] = "CODE_GENERATION"
            _wf_p(stage="代码生成：调用 qwen2.5-coder Agent", percent=40,
                  detail="代码生成分支，直接调 Coder Agent 完整生成 ...")
            _wf_r(title="⌨️ 代码生成（qwen2.5-coder）",
                  content=f"原始请求：{query[:120]}{'…' if len(query) > 120 else ''}\n"
                          "走独立 Coder Agent，阶段进度由 Agent 监控桥接单独推送。",
                  stage="workflow_dag")
            final_answer = await _final_analyst_answer(
                query, thread_id, user_id, "",
                preferred_agent="coder", bus=bus, quiet=quiet,
            )
            return WorkflowResult(router_decision=router, final_answer=final_answer, branch_trace=trace)

        # ============= Branch IMPACT_ANALYSIS (deepseek-r1:7b) =============
        if router.branch == RouteBranch.IMPACT_ANALYSIS:
            trace["branch"] = "IMPACT_ANALYSIS"
            _wf_p(stage="影响分析：启动 2 源并发检索", percent=30,
                  detail="联网搜索 + 知识星球 双并发（120s 硬超时）...")
            # 并发 2 源（联网 + zsxq）
            async def _two_gather():
                return await asyncio.gather(
                    _run_web_search(query=query, max_results=8),
                    _run_zsxq(query, [], [], limit=2),
                    return_exceptions=False,
                )
            try:
                web_res, zsxq_res = await asyncio.wait_for(_two_gather(), timeout=TWO_SOURCE_DAG_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                web_res = SourceResult(source_key="web_search", ok=False, error="TIMEOUT_120S")
                zsxq_res = SourceResult(source_key="zsxq", ok=False, error="TIMEOUT_120S")
            trace["sources"] = {
                "web_search": {"ok": web_res.ok, "items": len(web_res.items), "err": web_res.error},
                "zsxq": {"ok": zsxq_res.ok, "items": len(zsxq_res.items), "err": zsxq_res.error},
            }
            raw_all = [*web_res.items, *zsxq_res.items]
            ag = agg.aggregate(raw_all, thread_id=thread_id, append_to_shared_pool=True)
            aggregator_stats = ag.stats
            aggregated_prompt_context = (
                f"【影响分析双源检索】\n{ag.prompt_context_block}\n"
            )
            _wf_p(stage="影响分析：双源检索完成 → DeepSeek-R1 推理", percent=78,
                  detail=(f"Web {len(web_res.items)} / ZSXQ {len(zsxq_res.items)} 条 → 聚合 → "
                          "deepseek-r1:7b 利多/利空/影响面综合推理 ..."))
            _wf_r(title="✅ 影响分析双源检索完成",
                  content=(
                      f"🌐 联网搜索：{'成功' if web_res.ok else '失败'} {len(web_res.items)} 条\n"
                      f"💬 知识星球：{'成功' if zsxq_res.ok else '失败'} {len(zsxq_res.items)} 条\n"
                      f"🔗 聚合统计："
                      + (", ".join(f"{k}={v}" for k, v in list(ag.stats.items())[:6]) or "（无）")
                  ), stage="retrieve")
            _wf_p(stage="影响分析：DeepSeek-R1 最终推理中", percent=90,
                  detail="基于双源聚合上下文，输出【利多因素 / 利空因素 / 影响评级】结构化结论 ...")
            final_answer = await _final_analyst_answer(
                query, thread_id, user_id, aggregated_prompt_context,
                preferred_agent="reasoning", bus=bus, quiet=quiet,
            )
            return WorkflowResult(router_decision=router, final_answer=final_answer,
                                  branch_trace=trace, aggregator_stats=aggregator_stats)

        # ============= Branch VISION（qwen3-vl 骨架，当前 fallback） =============
        if router.branch == RouteBranch.VISION:
            trace["branch"] = "VISION_FALLBACK"
            _wf_p(stage="视觉多模态：调用 Vision Agent（骨架）", percent=40,
                  detail="VISION 分支目前走 Vision Agent 兼容兜底 ...")
            final_answer = await _final_analyst_answer(
                query, thread_id, user_id, "",
                preferred_agent="vision", bus=bus, quiet=quiet,
            )
            return WorkflowResult(router_decision=router, final_answer=final_answer, branch_trace=trace)

        # ============= Branch GENERAL_QUERY（默认：2 源并发） =============
        trace["branch"] = "GENERAL_QUERY_2_SOURCES"
        _wf_p(stage="通用查询：启动 2 源并发检索", percent=30,
              detail="联网搜索 + 知识星球 双并发（120s 硬超时）...")
        async def _two_gather_gen():
            return await asyncio.gather(
                _run_web_search(query=query, max_results=8),
                _run_zsxq(query, [], [], limit=2),
                return_exceptions=False,
            )
        try:
            web_res, zsxq_res = await asyncio.wait_for(_two_gather_gen(), timeout=TWO_SOURCE_DAG_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            web_res = SourceResult(source_key="web_search", ok=False, error="TIMEOUT_120S")
            zsxq_res = SourceResult(source_key="zsxq", ok=False, error="TIMEOUT_120S")
        trace["sources"] = {
            "web_search": {"ok": web_res.ok, "items": len(web_res.items), "err": web_res.error},
            "zsxq": {"ok": zsxq_res.ok, "items": len(zsxq_res.items), "err": zsxq_res.error},
        }
        raw_all = [*web_res.items, *zsxq_res.items]
        ag = agg.aggregate(raw_all, thread_id=thread_id, append_to_shared_pool=True)
        aggregator_stats = ag.stats
        aggregated_prompt_context = (
            f"【通用查询双源检索】\n{ag.prompt_context_block}\n"
        )
        _wf_p(stage="通用查询：双源检索完成 → 最终推理", percent=78,
              detail=(f"Web {len(web_res.items)} / ZSXQ {len(zsxq_res.items)} 条 → 聚合 → "
                      "默认分析 Agent 结构化答复 ..."))
        _wf_r(title="✅ 通用双源检索完成",
              content=(
                  f"🌐 联网搜索：{'成功' if web_res.ok else '失败'} {len(web_res.items)} 条\n"
                  f"💬 知识星球：{'成功' if zsxq_res.ok else '失败'} {len(zsxq_res.items)} 条\n"
                  f"🔗 聚合统计："
                  + (", ".join(f"{k}={v}" for k, v in list(ag.stats.items())[:6]) or "（无）")
              ), stage="retrieve")
        _wf_p(stage="通用查询：最终推理中", percent=90,
              detail="分析 Agent 基于聚合上下文生成答复 ...")
        final_answer = await _final_analyst_answer(
            query, thread_id, user_id, aggregated_prompt_context,
            preferred_agent=preferred_agent_override, bus=bus, quiet=quiet,
        )
        return WorkflowResult(router_decision=router, final_answer=final_answer,
                              branch_trace=trace, aggregator_stats=aggregator_stats)

    # 最外层 SLO 硬超时（任何分支超 ANALYSIS_DAG_MAX_TIMEOUT 秒直接降级）
    try:
        return await asyncio.wait_for(_run_inner(), timeout=ANALYSIS_DAG_MAX_TIMEOUT)
    except asyncio.TimeoutError:
        trace["workflow_timeout"] = True
        summary = (
            f"⏱️ 工作流执行超时（{ANALYSIS_DAG_MAX_TIMEOUT}s 硬上限）。\n"
            f"路由决策：{router.branch.value}（{router.reason}）\n"
            f"共享信息池条目数：{len(list(agg._shared_pool.get(thread_id or '', [])))}（可稍后重试）\n"
        )
        if "不构成投资建议" not in summary:
            summary += "\n" + RISK_DISCLAIMER + "\n"
        return WorkflowResult(router_decision=router, final_answer=summary, branch_trace=trace)


# ======================================================================
# 冒烟测试：python -m orchestration.workflows.analysis_workflow
# ======================================================================
if __name__ == "__main__":  # pragma: no cover
    async def _smoke():
        # 只跑路由分支，不真调 LLM（用 empty 上下文）
        cases = [
            ("盘前新闻", True, False),
            ("复盘预测", True, False),
            ("写个Python脚本下载茅台日K", True, False),
            ("分析美联储加息对A股科技板块的影响", True, False),
            ("今天天气怎么样", True, False),  # 快速路由
        ]
        for (q, skip_llm, _) in cases:
            print(f"\n===== Workflow smoke: {q!r} =====")
            if skip_llm:
                from agents.router.agent import decide
                d = decide(q)
                print(f"  Route: {d.branch.value} / decided_by={d.decided_by} / reason={d.reason}")
                continue
            try:
                res = await run_analysis_workflow(q, thread_id=f"smoke_{id(q)}", enable_gemma4_router=False)
                print(f"  Branch: {res.branch_trace.get('branch')} | Answer chars: {len(res.final_answer)}")
            except Exception as _e:
                print(f"  ERROR: {type(_e).__name__}: {_e}")
    asyncio.run(_smoke())
