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
from typing import Any, Dict, List, Optional

from shared.models import RouterDecision, RouteBranch, RetrievalItem, SourceReliability
from shared.aggregator import Aggregator, get_aggregator

# 盘前缓存目录 & TTL（规则1严格按设计）
# 2026-09-09 用户要求：盘前新闻输出迁移到 output/pre_market_news（输出产物与运行时数据分离）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.environ.get("DATA_DIR", _PROJECT_ROOT / "data"))
PRE_MARKET_DIR = _PROJECT_ROOT / "output" / "pre_market_news"
STOCK_CACHE_DIR = DATA_ROOT / "stock"
PRE_MARKET_TTL_HOURS = 6
STOCK_CACHE_TTL_DAYS = 7
# 4 源总硬超时（重构.md：单任务 150s，这里 4 源 DAG 设 180s 留余量给 Agent）
FOUR_SOURCE_DAG_TIMEOUT_SEC = 180.0
TWO_SOURCE_DAG_TIMEOUT_SEC = 120.0
ANALYSIS_DAG_MAX_TIMEOUT = 180.0  # 整个工作流外层 shield 超时
# 盘前新闻终态综答（直连 DEEPSEEK_V4_FLASH）：搜索 ~7s + 首试 120s + 失败重试 50s ≈ 177s < 180s DAG 外墙。
# （2026-09-08 实测：拥堵 + 7800 字上下文时 120s 会超时；150s 单发改 120s+50s 双发——
#   DeepSeek 拥堵为分钟级波动，超时后立即快速重试一次的总体成功率高于单发 150s。）
PREMARKET_FINAL_MODEL_TIMEOUT_SEC = 120.0
PREMARKET_FINAL_RETRY_TIMEOUT_SEC = 50.0
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

def _try_hit_premarket_cache(force_refresh: bool = False) -> Optional[str]:
    """<6h 命中直接返回内容；force_refresh=True 时跳过缓存（用户问题含「请强制重新分析」）。"""
    if force_refresh:
        return None
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
                _cached = fp.read_text(encoding="utf-8")
            except Exception:
                return None
            # 0 字节/纯空白缓存视为未命中（旧版本失败时曾把空串写缓存，导致 6h 内全员拿空结果）
            if _cached.strip():
                return _cached
            continue
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


# 盘前新闻：6 路平台定向并发搜索（Tavily include_domains 白名单限定站点）
# 历史教训：旧实现把平台名拼进一条自然语言查询，Tavily 不做站点定向、拆分器只按股票拆，
#          导致雪球/股吧/同花顺热榜等平台内容实际一条都搜不到（2026-09-08 线上故障）。
# 每条 = (平台标签, 检索词, 域名白名单)；topic 统一 general——热榜/论坛/股吧不在 Tavily news 索引内。
_PREMARKET_SITE_SEARCHES: tuple = (
    ("雪球", "雪球 7x24快讯 热股榜 今日热门股票 热门话题 个股讨论",
     ["xueqiu.com"]),
    ("东方财富股吧", "东方财富股吧 今日热门个股 热门概念板块 热门话题 热议 股票",
     ["guba.eastmoney.com", "eastmoney.com"]),
    ("同花顺", "同花顺 今日头条 热榜 投资日历 快讯 重要公告 个股 事件",
     ["10jqka.com.cn"]),
    ("财联社", "财联社 今日A股头条 电报 热门文章排行 热门个股 新闻",
     ["cls.cn"]),
    ("百度人气榜", "百度股市通 今日股票人气排行榜 讨论热度最高 热门股票",
     ["baidu.com"]),
    ("韭研公社", "韭研公社 公社热榜 今日热门关键词 研报 个股 排行",
     ["jiuyangongshe.com"]),
)

# 单路站点搜索超时墙：6 路并发取最慢一条，45s 上限保证搜索段 ≤ 180s DAG 总预算
_PREMARKET_SITE_SEARCH_TIMEOUT_SEC: float = 45.0


async def _run_site_search(label: str, query: str, domains: List[str],
                           max_results: int = 8) -> SourceResult:
    """Tavily 站点定向搜索（include_domains 白名单），单查询直连不拆分。

    每条结果打上平台 channel 标签（如「雪球(xueqiu.com)」），Aggregator 渲染
    prompt_context_block 时即带来源，供最终模型填「提及的平台」列。
    """
    src_key = f"web:{label}"
    try:
        from shared.data_sources.web_search import internet_search_async
        res = await asyncio.wait_for(
            internet_search_async(query=query, topic="general",
                                  max_results=max_results, include_domains=domains),
            timeout=_PREMARKET_SITE_SEARCH_TIMEOUT_SEC,
        )
        items: List[Any] = []
        if isinstance(res, list):
            items = list(res)
        elif isinstance(res, dict):
            items = list(res.get("_structured_items") or res.get("results") or [])
        # 平台归属标签：覆盖通用 tavily channel，保证最终 prompt 里来源可追溯。
        # 热榜页（雪球 hots / 韭研 study_hot 等）本身无发布日期——它们是「当下抓取的今日榜单」，
        # 补当天日期，避免在 Aggregator 按日期排序时沉底被上下文截断裁掉。
        _today = _now_cn().strftime("%Y-%m-%d")
        for it in items:
            if isinstance(it, dict):
                it["channel"] = f"{label}({domains[0]})"
                it["source_type"] = "web"
                if not str(it.get("published_at") or "").strip():
                    it["published_at"] = _today
        return SourceResult(source_key=src_key, ok=isinstance(res, dict),
                            items=items, raw_text=str(res)[:8000])
    except asyncio.TimeoutError:
        return SourceResult(source_key=src_key, ok=False,
                            error=f"Timeout({_PREMARKET_SITE_SEARCH_TIMEOUT_SEC:.0f}s)")
    except Exception as e:
        return SourceResult(source_key=src_key, ok=False, error=f"{type(e).__name__}: {e}")


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
        from shared.data_sources.local_sql import list_sql_tables, get_table_data  # type: ignore
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
    _premarket_in_flight: bool = False,
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
        _premarket_in_flight: 内部参数——True 表示本调用已在 single-flight 内执行
            （重入），跳过 PRE_MARKET_NEWS 分支的并发去重拦截。外部调用方勿传。
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
    # 任务级繁忙提示（2026-09-09 方案3）：搜索通道已有排队时提前告知，避免用户误以为卡死
    try:
        from shared.utils.concurrency_gate import tavily_gate as _tavily_gate_entry
        if _tavily_gate_entry.waiting > 0:
            _wf_p(stage="搜索通道繁忙", percent=10,
                  detail=(f"当前有 {_tavily_gate_entry.waiting} 个其他用户的搜索任务在排队，"
                          "本任务将自动错峰执行，预计稍慢，请耐心等待 ..."))
    except Exception:
        pass
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
            # ===== single-flight 并发去重（2026-09-09）=====
            # 背景：多会话/多用户并发触发盘前新闻（A 点"盘前新闻"按钮 + B 点"复盘预测"
            # 阶段2 同样走本分支）会双跑 8 路 Tavily 站点搜索（16 路并发触发限流）+ 双份
            # DeepSeek 综答，互相拖垮导致两路都长时间无输出（实测 5-6 分钟零输出）。
            # 盘前新闻内容 = 当日市场信息，跨会话共享一次计算业务上完全合理；
            # 每个调用方拿到共享 WorkflowResult 后各自按自己的 thread 推送/落库。
            # _premarket_in_flight=True 为 flight 内重入调用，直接执行原分支体。
            if not _premarket_in_flight:
                from shared.utils.single_flight import run_single_flight, today_key
                _sf_key = today_key("premarket_news")

                def _notify_follower() -> None:
                    # 仅 follower 触发（on_follow 回调）：共享身份确定后推送，无竞态
                    _wf_p(stage="盘前新闻：检测到同日计算正在进行",
                          percent=22, detail="♻️ 共享进行中的计算结果，避免并发双跑 ...")
                    _wf_r(title="♻️ 盘前新闻：共享计算",
                          content="检测到同日的盘前新闻正在计算中，本次将直接共享该计算结果"
                                  "（避免并发双跑导致搜索限流与分析超时），完成后自动显示。")

                return await run_single_flight(
                    _sf_key,
                    lambda: run_analysis_workflow(
                        query, thread_id, user_id,
                        has_visual_input=has_visual_input,
                        enable_gemma4_router=enable_gemma4_router,
                        preferred_agent_override=preferred_agent_override,
                        bus=bus, quiet=quiet,
                        _premarket_in_flight=True,
                    ),
                    on_follow=_notify_follower,
                )
            trace["branch"] = "PRE_MARKET_NEWS"
            _wf_p(stage="盘前新闻：检查 6h 本地缓存", percent=20,
                  detail="读取盘前缓存目录，命中则秒级回显 ...")
            cached = _try_hit_premarket_cache(force_refresh=("强制重新分析" in str(query)))
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
            _wf_p(stage="盘前新闻：缓存未命中，启动多源并发", percent=28,
                  detail="6h 本地无匹配缓存，启动（A股多平台 + 美股夜盘 + 知识星球）并发搜索 ...")
            _wf_r(title="📭 盘前新闻：6h 缓存未命中",
                  content=(
                      f"时间窗口：{_china_market_search_window_tip()}\n"
                      "启动 8 路并发搜索（Tavily 站点定向 + 专项）：\n"
                      "  ① 雪球（xueqiu.com）：7x24 快讯 / 热股榜 / 热门话题\n"
                      "  ② 东方财富股吧（guba.eastmoney.com）：热门个股 / 热门概念 / 热门话题\n"
                      "  ③ 同花顺（10jqka.com.cn）：头条 / 热榜 / 投资日历 / 快讯 / 公告\n"
                      "  ④ 财联社（cls.cn）：A股头条 / 热门文章排行 / 热门个股\n"
                      "  ⑤ 百度人气榜（baidu.com）：今日股票人气排行榜\n"
                      "  ⑥ 韭研公社（jiuyangongshe.com）：公社热榜关键词前 10 股票\n"
                      "  ⑦ 美股涨跌（可自定义查询：https://finance.sina.com.cn/stock/usstock/sector.shtml）：美光科技公司(MU)/SK海力士(000660.KS)/谷歌(GOOGL)/应用光电(AAOI)/康宁(GLW)/英伟达(NVDA) 盘前盘中涨跌\n"
                      "  ⑧ 知识星球（盘前研报热度）\n"
                      "⏱ 预计联网阶段约 20-40s；之后云端 DeepSeek-V4-Flash 综合作答约 30-60s。"
                  ), stage="cache")
            # 缓存未命中 → 8 路并发（6 平台站点定向 + 美股夜盘 + zsxq）
            # 站点定向用 Tavily include_domains 白名单：旧实现把平台名拼进一条自然语言查询，
            # Tavily 不做站点定向、拆分器只按股票拆，导致各平台热榜内容一条都搜不到。
            # 注意：不把用户 fullQuery 原文喂给美股搜索——StockMatcher 会把「海力士/应用光电」
            # 误匹配成 A 股（海力风电/光电股份）；美股词用「SK海力士/AAOI」写法避开误匹配。
            _us_q = "美股 盘前（盘中）行情 科技股 MU美光 SK海力士 谷歌 Meta AAOI 康宁 英伟达 的最新涨跌及新闻事件"
            win_tip = _china_market_search_window_tip()
            site_tasks = [
                _run_site_search(label=_lbl, query=_q, domains=_dom, max_results=8)
                for _lbl, _q, _dom in _PREMARKET_SITE_SEARCHES
            ]
            us_task = _run_web_search(query=_us_q, max_results=8)
            zsxq_task = _run_zsxq("盘前新闻 今日 小作文 公告", stock_names=[], stock_codes=[], limit=3)
            _wf_p(stage="盘前新闻：6 平台定向 + 美股夜盘 + 知识星球 并发检索中", percent=35,
                  detail="8 个异步任务并行（单路 45s 超时墙），等待 gather 返回 ...")
            _gathered = await asyncio.gather(*site_tasks, us_task, zsxq_task, return_exceptions=False)
            site_res = list(_gathered[:len(_PREMARKET_SITE_SEARCHES)])
            us_res = _gathered[len(_PREMARKET_SITE_SEARCHES)]
            zsxq_res = _gathered[len(_PREMARKET_SITE_SEARCHES) + 1]
            # 美股夜盘条目同样补 channel 标签 + 当天日期：
            # topic=news 命中的富途/tradingkey 快讯含真实涨跌幅但无日期，不补会在聚合排序中沉底被截断。
            _today_us = _now_cn().strftime("%Y-%m-%d")
            for _uit in us_res.items:
                if isinstance(_uit, dict):
                    _uit["channel"] = "美股"
                    if not str(_uit.get("published_at") or "").strip():
                        _uit["published_at"] = _today_us
            raw_all: List[Any] = []
            for _sr in site_res:
                raw_all.extend(_sr.items)
            raw_all.extend(us_res.items)
            raw_all.extend(zsxq_res.items)
            # 盘前分支放大上下文配额：默认块仅 2000 字，6 平台 40+ 条热榜会被截到只剩前几条；
            # 终态 prompt 截取 9000 字（头部状态行 ~300），故块给到 7800，保证平台热榜条目可见。
            ag = agg.aggregate(raw_all, thread_id=thread_id, append_to_shared_pool=True,
                               context_max_chars=7800)
            aggregator_stats = ag.stats
            _site_status = "\n".join(
                f"平台定向·{_sr.source_key.split(':', 1)[-1]}："
                f"{'成功' if _sr.ok else '失败'} 条目={len(_sr.items)} {_sr.error or ''}"
                for _sr in site_res
            )
            aggregated_prompt_context = (
                f"【盘前新闻搜索】{win_tip}\n"
                f"{_site_status}\n"
                f"美股: {us_res.ok} 条目={len(us_res.items)} 异常={us_res.error}\n"
                f"知识星球: {zsxq_res.ok} 条目={len(zsxq_res.items)} 异常={zsxq_res.error}\n"
                "【各平台检索条目（channel 字段即来源平台，填「提及的平台」列时以此为准）】\n"
                f"{ag.prompt_context_block}\n"
            )
            _site_counts = " / ".join(
                f"{_sr.source_key.split(':', 1)[-1]} {len(_sr.items)}条" for _sr in site_res
            )
            _wf_p(stage="盘前新闻：多源并发结束，结果聚合中", percent=75,
                  detail=(f"{_site_counts} | 美股 {len(us_res.items)} 条 / ZSXQ {len(zsxq_res.items)} 条 → "
                          f"合并去重 → 进入 DeepSeek 最终推理"))
            _site_lines = "\n".join(
                f"  {'✅' if _sr.ok else '❌'} {_sr.source_key.split(':', 1)[-1]}："
                f"{'命中 ' + str(len(_sr.items)) + ' 条' if _sr.ok else (_sr.error or '无结果')}"
                for _sr in site_res
            )
            _wf_r(title="✅ 多源并发检索完成",
                  content=(
                      f"🌐 6 路平台定向搜索（Tavily include_domains 白名单）：\n{_site_lines}\n"
                      f"🇺🇸 美股：{'成功' if us_res.ok else '失败'}，"
                      f"命中 {len(us_res.items)} 条；{us_res.error or ''}\n"
                      f"💬 知识星球：{'成功' if zsxq_res.ok else '失败'}，"
                      f"命中 {len(zsxq_res.items)} 条；{zsxq_res.error or ''}\n"
                      f"🔗 聚合统计："
                      + (", ".join(f"{k}={v}" for k, v in list(ag.stats.items())[:6]) or "（无）")
                  ), stage="retrieve")
            # 本地 deepseek-r1:7b 留给单股深度推演类任务，不再用于本链路。
            # 历史教训：不走 run_deep_agent（agent 循环在 DeepSeek 拥堵时无日志返回空串）。
            _wf_p(stage="盘前新闻：DeepSeek 最终推理中", percent=88,
                  detail="结合 A股多平台 + 美股夜盘 + 知识星球聚合素材，生成最终答复（约 30-60s）...")
            _wf_r(title="🧠 最终推理（云端 DeepSeek-V4-Flash）",
                  content=(
                      f"输入长度：{len(aggregated_prompt_context)} 字符\n"
                      "任务：严格遵守用户任务要求（平台热榜/美股夜盘名单/输出结构与字数限制），"
                      "基于聚合素材输出结构化盘前研报热度简报，并在结尾附风险声明；素材未覆盖的项目如实标注。"
                  ), stage="model")
            _fin_prompt = (
                "你是一名金融信息分析师。以下是 6 大财经平台站点定向搜索"
                "（雪球/东方财富股吧/同花顺/财联社/百度人气榜/韭研公社，每条结果的 channel 字段即来源平台）"
                " + 美股 + 知识星球聚合的搜索结果。\n\n"
                f"【搜索结果】\n{str(aggregated_prompt_context)[:9000]}\n\n"
                "【输出结构（必须严格按以下三段 markdown 格式输出，禁止增减段落、禁止用 HTML 标签）】\n"
                "\n"
                "### 1. 平台热点总结（热门个股/事件）\n"
                "以 markdown 表格输出，表头固定为：| 股票/板块 | 事件 | 提及的平台 |\n"
                "（股票/板块列可填个股名或概念板块名；事件列简述热点事件≤30字；提及的平台列填 channel 字段对应的平台名，多平台用顿号分隔）\n"
                "\n"
                "### 2. 美股相关科技股盘前/盘中表现\n"
                "以 markdown 表格输出，表头固定为：| 股票 | 涨跌幅 | 新闻 |\n"
                "（股票固定为：美光、SK海力士、谷歌、Meta、应用光电、康宁、英伟达；涨跌幅有数据填百分比如+5%/-2.3%，无则填「无」；新闻列简述当日要点，无则填「搜索结果未提供相关行情/新闻」）\n"
                "\n"
                "### 3. 推理分析与预测\n"
                "**利好 A股概念/个股：**\n"
                "- 板块/概念：简述利好逻辑，可列具体 A股个股名（需标注「需自选验证」）。\n"
                "**利空 A股概念/个股：**\n"
                "- 板块/概念：简述利空逻辑。\n"
                "\n"
                "（若某方向素材不足，仍保留对应标题并写「暂无明确素材」；禁止编造未出现的个股和数据。）\n"
                "\n"
                "最后单独一行附风险声明：⚠️ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。投资有风险，入市需谨慎，盈亏自负。\n"
            )
            final_answer = ""
            try:
                from shared.llm_client.deepseek_client import _base_model
                from langchain_core.messages import HumanMessage as _HM
                from config.constants import PREMARKET_FINAL_RETRY_TIMEOUT_SEC as _RETRY_TMO
                # 首试 + 快速重试（DeepSeek 拥堵是分钟级波动，超时后立即二发常能命中）
                _fin_err_first: "Exception | None" = None
                for _attempt, _tmo in enumerate(
                    (PREMARKET_FINAL_MODEL_TIMEOUT_SEC, _RETRY_TMO), 1,
                ):
                    try:
                        if _attempt == 2:
                            print(f"[盘前新闻] 综答首试失败({_fin_err_first!r})，{int(_tmo)}s 快速重试 ...")
                            _wf_p(stage="盘前新闻：综答首试超时，自动重试", percent=95,
                                  detail="云端模型繁忙，正在第二次尝试生成简报 ...")
                        _fin_resp = await asyncio.wait_for(
                            _base_model.ainvoke([_HM(content=_fin_prompt)]),
                            timeout=_tmo,
                        )
                        final_answer = (_fin_resp.content or "").strip() if hasattr(_fin_resp, "content") else str(_fin_resp)
                        trace["final_model"] = "cloud:DEEPSEEK_V4_FLASH" + ("(retry)" if _attempt == 2 else "")
                        break
                    except Exception as _fin_err:
                        if _attempt == 1:
                            _fin_err_first = _fin_err
                            continue
                        print(f"[盘前新闻] 直连综合作答失败（含重试）: {_fin_err!r}")
                        trace["final_model_error"] = f"{type(_fin_err).__name__}: {_fin_err}"
                        final_answer = ""
            except Exception as _fin_err:
                print(f"[盘前新闻] 直连综合作答失败: {_fin_err!r}")
                trace["final_model_error"] = f"{type(_fin_err).__name__}: {_fin_err}"
                final_answer = ""
            if final_answer:
                # 顶部注入生成时间戳（在写缓存前注入：缓存命中时展示的是该简报的真实生成时间）
                _ts = _now_cn().strftime("%Y-%m-%d %H:%M")
                final_answer = f"📅 生成时间：{_ts}（北京时间）\n\n{final_answer}"
                _wf_p(stage="盘前新闻：写入 6h 本地缓存", percent=97,
                      detail="推理完成，结果归档到本地缓存，后续相同问题秒回 ...")
                # 保存到文件（空结果禁止写缓存——否则 6h 内所有用户都拿到空串）
                try:
                    saved = await asyncio.to_thread(_save_premarket_result, final_answer)
                    trace["premarket_saved_to"] = str(saved)
                except Exception as _e:
                    trace["premarket_save_error"] = f"{type(_e).__name__}: {_e}"
            else:
                _wf_p(stage="盘前新闻：综答失败，跳过缓存", percent=97,
                      detail="云端模型超时或异常，本次结果不写入缓存，下次请求自动重试 ...")
                final_answer = (
                    "⚠️ 盘前新闻综合作答失败（云端模型超时或繁忙），本次搜索已完成但未能生成简报，请稍后重试。\n\n"
                    "⚠️ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。投资有风险，入市需谨慎，盈亏自负。"
                )
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
            # 全局 Ollama 并发闸（2026-09-09 方案3）：单 GPU 长推理只允许 1 路占位，
            # 多用户并发单股推演时排队 + 前端提示（避免无反馈互拖）。
            from shared.utils.concurrency_gate import ollama_gate as _ollama_gate

            def _gpu_wait(n_waiting: int) -> None:
                _wf_p(stage="影响分析：GPU 推理排队中", percent=90,
                      detail=f"本地 GPU 正被其他用户的推理任务占用，当前排队第 {n_waiting} 位，完成后自动继续 ...")

            final_answer = await _ollama_gate.run(
                lambda: _final_analyst_answer(
                    query, thread_id, user_id, aggregated_prompt_context,
                    preferred_agent="reasoning", bus=bus, quiet=quiet,
                ),
                on_wait=_gpu_wait,
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
