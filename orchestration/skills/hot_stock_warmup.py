"""
Hot Stock Warmup —— 每日 08:00 / 20:00 热门股分析提前预热任务。

执行步骤（由 scheduler 每轮在主线程 actor 中串跑，避免并发烧 token）：
  1) DeepSeek 联网（topic=news）搜索热门股票清单，要求返回 TopK=10 的 JSON Lines：
        1. 贵州茅台 600519
        2. 宁德时代 300750
     解析出 name + code 数组。
  2) 对每只股票，构建精简综合分析（不跑完整 LangGraph main_agent，避免 180s×10）：
     a. Tavily 联网搜索 query = "${股票}${代码} 最新新闻 估值 讨论"（max_results=5）
     b. IMA 知识库同 query（limit=2）
     c. ZSXQ 知识星球（仅 search_zsxq_by_stock，不启动本地 Ollama 分析）
  3) 用 build_citation_context 生成 citation 上下文，再调用一次 DeepSeek
     "按个股分析：200 字中文摘要 + 利空/利多 + 3 要点，必须附 [N]；末尾风险声明"
  4) write_stock_cache(stock_name, final_content, source="warmup") 落盘。
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ----------------------------------------------------------------------
# 懒加载常量 / 工具（避免 import 循环）
# ----------------------------------------------------------------------
def _warmup_cfg() -> Dict[str, Any]:
    try:
        from config.constants import (
            STOCK_CACHE_WARMUP_TOPK as _K,
            STOCK_CACHE_WARMUP_SOURCES as _SRC,
            RISK_DISCLAIMER_CACHE_GUARD as _RDS,
            RECENCY_TIMEZONE_OFFSET_HOURS as _TZH,
        )
    except Exception:
        _K, _SRC, _RDS, _TZH = 10, ("韭研社区", "东方财富股吧", "同花顺股吧", "微信公众号"), (
            "⚠️ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。投资有风险，入市需谨慎，盈亏自负。"), 8
    return dict(K=int(_K), SRC=tuple(_SRC), RDS=str(_RDS), TZH=int(_TZH))


def _nowbj(tz: int) -> datetime:
    return datetime.now(timezone(timedelta(hours=tz)))


def _log(msg: str) -> None:
    ts = _nowbj(_warmup_cfg()["TZH"]).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[HotStockWarmup {ts}] {msg}")


# ----------------------------------------------------------------------
# 阶段 1：联网求 TopN 热门股清单（解析为 [{name, code}]）
# ----------------------------------------------------------------------
_TOP10_SYS_PROMPT = """你是一个专门从中文财经社区挖掘热门个股的助手。
请严格只输出 JSON Lines，每行一个 JSON，格式：
  {"rank": 1, "name": "贵州茅台", "code": "600519", "reason": "XXXX"}
输出范围：1 到 {topk}，必须覆盖 {topk} 行，禁止输出除 JSON Lines 之外的解释、Markdown、序号或前置文字。
"""


def _build_top10_query(topk: int, sources: Tuple[str, ...]) -> str:
    src_joined = "、".join(sources)
    return (
        f"从以下社区/渠道：{src_joined} 查询最新最热门的股票（A股为主，含 ETF / 港股也可），"
        f"综合讨论热度、交易量、新闻关注度汇总排名前 {topk} 只股票清单；"
        "每行独立 JSON，字段 rank 从 1 开始连续，name 是中文名（含 ETF 后缀），"
        "code 是 A 股 6 位代码 / 港股 5 位 / 美股代码，reason 一句话说明热门原因（≤30字）。"
    )


def _parse_topn_text(text: str, topk: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            # 容错：不是严格 JSON → 用 regex 提取 name/code
            m = re.search(r"\"name\"\s*:\s*\"([^\"]+)\"", line)
            m2 = re.search(r"\"code\"\s*:\s*\"?([A-Za-z0-9]{3,7})\"?", line)
            if not m:
                # 再兜底："1. 贵州茅台 600519"
                m_txt = re.match(r"^\d+[\.、]\s*([\u4e00-\u9fa5A-Za-z]{1,12}(?:ETF|LOF)?)\s*([A-Za-z]?\d{4,6})?", line)
                if not m_txt:
                    continue
                obj = {"name": m_txt.group(1).strip(), "code": (m_txt.group(2) or "").strip(), "rank": len(out) + 1}
            else:
                obj = {"name": m.group(1).strip(), "code": (m2.group(1) if m2 else "").strip(), "rank": len(out) + 1}
        name = str(obj.get("name") or "").strip()
        code = str(obj.get("code") or "").strip()
        if not name:
            continue
        out.append({"name": name, "code": code, "reason": str(obj.get("reason") or "").strip()[:60]})
    # 去重（按 name 优先，code 其次），保留前 topk
    seen_names, seen_codes, dedup = set(), set(), []
    for it in out:
        if it["name"] in seen_names:
            continue
        if it["code"] and it["code"] in seen_codes:
            continue
        seen_names.add(it["name"])
        if it["code"]:
            seen_codes.add(it["code"])
        dedup.append(it)
        if len(dedup) >= topk:
            break
    return dedup


async def _fetch_topn_hot_stocks(topk: int, sources: Tuple[str, ...]) -> List[Dict[str, Any]]:
    """阶段 1：优先 DeepSeek 联网搜索热门股；失败降级 Tavily+ 正则抽取。"""
    query = _build_top10_query(topk, sources)
    sys_prompt = _TOP10_SYS_PROMPT.format(topk=topk)
    try:
        from agent.llm import get_deepseek_llm  # type: ignore
        llm = get_deepseek_llm()
        messages = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": query},
        ]
        # 优先 streaming 也能拿到完整文本（这里用同步/异步调用简单取 whole content）
        ans = ""
        resp = await llm.ainvoke(messages)
        ans = getattr(resp, "content", resp) if not isinstance(resp, str) else resp
        if ans:
            parsed = _parse_topn_text(ans, topk)
            if parsed:
                return parsed
    except Exception as e:
        _log(f"阶段1 DeepSeek 失败（降级 Tavily）：{e!r}")
    try:
        from tools.tavily_tool import internet_search
        result = internet_search(query, topic="news", max_results=8, include_raw_content=False)
        items = (result or {}).get("_structured_items") or []
        # 把所有搜索结果的 title + content 拼接后按正则抓股票名/代码对
        blob = "\n".join([
            (str(it.get("title") or "") + "\n" + str(it.get("content") or ""))
            for it in items
        ])
        lines = blob.splitlines() + [blob]
        parsed: List[Dict[str, Any]] = []
        for ln in lines:
            hits = re.findall(r"([\u4e00-\u9fa5]{2,12}(?:ETF|LOF)?)\s*(\(?[A-Za-z]?\d{4,6}\)?)?", ln)
            for name, code in hits:
                parsed.append({
                    "name": name.strip(),
                    "code": code.strip().strip("()（）"),
                    "reason": "Tavily 抽取",
                })
        return _parse_topn_text("\n".join(json.dumps({
            "rank": i + 1, "name": p["name"], "code": p["code"], "reason": p["reason"]
        }, ensure_ascii=False) for i, p in enumerate(parsed[: topk * 3])), topk)
    except Exception as e2:
        _log(f"阶段1 Tavily 降级也失败：{e2!r}")
        return []


# ----------------------------------------------------------------------
# 阶段 2：单只股票的三通道精简综合 + 缓存写盘（不跑完整 main_agent）
# ----------------------------------------------------------------------
_STOCK_ANALYSIS_SYS = """你是投研辅助助手。按以下协议输出，严格禁止多余文字：
一、结论：利空/利多/中性（仅三选一，占 1 行）
二、核心摘要（≤200 字中文正文），引用参考文档务必使用 [1][2]... 编号（N 为你看到的上方文档序号 1..N）
三、3 条要点（每条 1 句话，带 [N] 引用）
四、文末只追加一次以下风险声明（原文拷贝，不要改字）：
{rds}
"""


async def _analyze_single_stock(name: str, code: str, *, idx: int, total: int) -> Optional[str]:
    """三通道精简综合：调用 Tavily(internet_search) + IMA(search_knowledge_base) +
    ZSXQ(search_zsxq_by_stock)，各自拿到 _structured_items 后合并喂 build_citation_context，
    再调用 DeepSeek 生成 200 字摘要。失败返回 None。"""
    _log(f"  [{idx}/{total}] 开始分析 {name} ({code or '无代码'})…")
    q_base = f"{name}{(' ' + code) if code else ''} 最新新闻 估值 讨论 今日行情 研报观点 散户数据"
    items: List[Dict[str, Any]] = []
    # 2a. Tavily
    try:
        from tools.tavily_tool import internet_search
        r1 = internet_search(q_base, topic="news", max_results=5, include_raw_content=False) or {}
        items.extend(r1.get("_structured_items") or [])
    except Exception as e:
        _log(f"    Tavily 失败（跳过）：{e!r}")
    # 2b. IMA
    try:
        from tools.ragflow_tools import search_knowledge_base
        r2 = search_knowledge_base(q_base) or ""
        # IMA 返回 markdown 字符串或 dict
        if isinstance(r2, dict):
            items.extend(r2.get("_structured_items") or [])
    except Exception as e:
        _log(f"    IMA 失败（跳过）：{e!r}")
    # 2c. ZSXQ
    try:
        from tools.zsxq_tool import search_zsxq_by_stock
        r3 = search_zsxq_by_stock(stock_name=name, stock_code=code, limit=5) or {}
        if isinstance(r3, dict):
            items.extend(r3.get("_structured_items") or [])
    except Exception as e:
        _log(f"    ZSXQ 失败（跳过）：{e!r}")
    if not items:
        _log(f"    三通道无任何检索结果，跳过写缓存")
        return None
    # 2d. 构建 citation 上下文 + 时效性窗口（自动 30→90）
    from adapter.stream_adapters import build_citation_context
    norm_docs, ctx_block = build_citation_context(items)
    if not norm_docs:
        return None
    # 2e. DeepSeek 汇总
    try:
        from agent.llm import get_deepseek_llm
        llm = get_deepseek_llm()
        from config.constants import RISK_DISCLAIMER_CACHE_GUARD as _rds_c
        messages = [
            {"role": "system", "content": _STOCK_ANALYSIS_SYS.format(rds=_rds_c)},
            {"role": "user", "content": (
                f"股票：{name}{('（代码：'+code+'）') if code else ''}\n\n"
                f"参考文档：\n{ctx_block}\n\n"
                "请按协议输出中文答案。"
            )},
        ]
        resp = await llm.ainvoke(messages)
        ans = getattr(resp, "content", resp) if not isinstance(resp, str) else resp
        if not ans:
            return None
        if _rds_c not in ans:
            ans = str(ans).rstrip() + "\n\n" + _rds_c
        # 正文最小长度保护：防止模型返回空/占位（< 30 字视为失败）
        if len(str(ans).strip()) < 30:
            return None
        return str(ans)
    except Exception as e:
        _log(f"    DeepSeek 汇总失败：{e!r}")
        return None


# ----------------------------------------------------------------------
# 对外统一入口：warmup() 直接被 scheduler 当回调；返回成功写入文件数
# ----------------------------------------------------------------------
async def warmup() -> int:
    from cache.stock_cache import write_stock_cache, _random_sleep_for_avoid_thundering_herd
    _random_sleep_for_avoid_thundering_herd()
    cfg = _warmup_cfg()
    topk, sources = cfg["K"], cfg["SRC"]
    _log(f"===== 预热开始：Top{topk}，来源 {sources} =====")
    # Step 1：热门股清单
    stocks = await _fetch_topn_hot_stocks(topk, sources)
    if not stocks:
        _log("没有拿到任何热门股，结束预热")
        return 0
    _log(f"阶段1完成，拿到 {len(stocks)} 只：{[s['name'] for s in stocks]}")
    written = 0
    for i, s in enumerate(stocks, 1):
        name, code = s["name"], s.get("code") or ""
        try:
            content = await _analyze_single_stock(name, code, idx=i, total=len(stocks))
            if not content:
                continue
            path = write_stock_cache(name, content, source="warmup")
            if path:
                _log(f"    [OK] 写入缓存：{path}")
                written += 1
            else:
                _log(f"    [WARN] write_stock_cache 返回 None，未写入")
        except Exception as e:
            _log(f"    [{name}] 异常：{e!r}")
            continue
    _log(f"===== 预热结束：成功写入 {written}/{len(stocks)} 份缓存 =====")
    return written


def warmup_sync() -> int:
    """同步包装（scheduler callback 支持同步或 awaitable）。"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None
    if loop is not None and loop.is_running():
        # scheduler 已经在 asyncio loop 里（本项目是的），直接返回协程，scheduler 会 await
        return asyncio.ensure_future(warmup())  # type: ignore[return-value]
    return asyncio.run(warmup())
