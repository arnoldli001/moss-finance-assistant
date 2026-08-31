"""多股票 / 多平台 搜索 拆分 → 并发 → 聚合 模块。

**目标（用户要求原文）**：当调用网络搜索 agent 时，为提高搜索效率，对于涉及到多个股票
或多个平台的搜索任务，采用「拆分 + 并发搜索」再「汇总结果」的方法，提高响应效率。

**设计原则**：
- 向后兼容（兼容兜底）：输入不满足"多股票 ≥2 或 多平台 ≥2"时，返回空拆分列表，
  调用方按原样做单请求搜索，不改变原有 API 返回结构。
- 零外部依赖（除了项目内已有的 stock_matcher 股票抽取，标准库 asyncio /
  concurrent.futures / dataclasses / re）。
- 两条入口：
  ① `run_sync_parallel(query, topic, max_results, include_raw_content, sync_search_fn)`
     — 用于 @tool 同步 internet_search（LangChain 直接调用场景）。
     使用 ThreadPoolExecutor(max_workers=6) 并发调 sync_search_fn。
  ② `run_async_parallel(query, topic, max_results, include_raw_content, async_search_fn)`
     — 用于 async 工作流（analysis_workflow）。
     使用 asyncio.gather(return_exceptions=True) 并发调 async_search_fn。
- 聚合输出与单条 Tavily.search 返回结构完全一致（dict: query / answer / results /
  _structured_items），原下游代码不用改一行即可读取。
- 结果分节：为"汇总报告"新增 aggregated_report 字段（markdown：每只股票/每平台
  分节，标题、时间戳、各节统计），便于 LLM 与前端直接消费。
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import dataclasses
import re
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple


# ================================================================
# 平台关键词表（用户原始需求的"多平台"）：
#   - 中文平台名 + 常用英文名 + 官方缩写，大小写不敏感匹配
#   - 含 雪球/东方财富/同花顺/淘股吧/股吧 + 上交所/深交所/北交所/港交所/SEC/Edgar
# ================================================================
PLATFORM_KEYWORDS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("雪球", ("雪球", "xueqiu", "xq")),
    ("东方财富", ("东方财富", "eastmoney", "east_money", "股吧", "guba", "东方财富网")),
    ("同花顺", ("同花顺", "ths", "10jqka", "i问财")),
    ("淘股吧", ("淘股吧", "taoguba", "tgb")),
    ("上交所", ("上交所", "sse", "上海证券交易所", "沪交所")),
    ("深交所", ("深交所", "szse", "深圳证券交易所")),
    ("北交所", ("北交所", "bse", "北京证券交易所")),
    ("港交所", ("港交所", "hkex", "香港交易所", "联交所")),
    ("SEC", ("sec", "edgar", "美国证监会", "us sec")),
    ("富途", ("富途", "futu", "moomoo", "牛牛圈")),
    ("老虎证券", ("老虎证券", "tigers", "tiger trade", "老虎社区")),
    ("微博财经", ("微博财经", "weibo finance", "新浪财经", "sina finance")),
    ("腾讯自选股", ("腾讯自选股", "自选股", "tencent stocks")),
    ("财联社", ("财联社", "cls", "cls.cn")),
    ("界面新闻", ("界面新闻", "界面", "jiemian")),
)

# 每平台最大并发上限（防止 HTTP 429 与 Tavily Rate Limit）
MAX_PARALLEL_WORKERS: int = 6


@dataclasses.dataclass
class SubQuery:
    """拆分后的子查询。"""
    label: str                          # 人类可读标签，如"贵州茅台 600519"或"平台·雪球"
    category: str                      # "stock" | "platform" | "stock_platform" | "fallback"
    query: str                         # 子查询字符串（喂给 Tavily 的 query）
    weight: float = 1.0                # 聚合时用于结果排序（暂保留默认 1.0）


# ================================================================
# 拆分逻辑
# ================================================================
def _lazy_extract_stocks(text: str) -> List[Any]:
    """用 StockMatcher 抽股票，StockMatcher 失败不抛（保护调用方）。

    返回 List[StockInfo]（StockInfo 至少含 name/code 两个属性）；任何异常返回空列表。
    """
    if not text:
        return []
    try:
        from shared.data_sources.stock_matcher import extract_stocks  # type: ignore
        hits = extract_stocks(text)
        if isinstance(hits, list):
            return hits
    except Exception:
        pass
    # 兜底正则：6 位 A 股代码（00/30/60/68/83/87/43 前缀）作为强信号
    codes = sorted(set(re.findall(r"(?<![A-Za-z0-9.])(?:00\d{4}|30\d{4}|60\d{4}|68\d{4}|8[37]\d{4}|43\d{4})(?![A-Za-z0-9.])", text)))
    if codes:
        class _Pseudo:
            def __init__(self, c: str):
                self.code = c
                self.name = c
        return [_Pseudo(c) for c in codes]
    return []


def _detect_platforms(text: str) -> List[str]:
    """返回命中的平台中文名列表（按 PLATFORM_KEYWORDS 顺序，去重保序）。"""
    if not text:
        return []
    lowered = text.lower()
    result: List[str] = []
    seen: set = set()
    for cn_label, aliases in PLATFORM_KEYWORDS:
        for alias in aliases:
            if alias.lower() in lowered:
                if cn_label not in seen:
                    seen.add(cn_label)
                    result.append(cn_label)
                break
    return result


def _extract_common_tail(query: str, names: List[str], codes: List[str], platforms: List[str]) -> str:
    """从原 query 中剥离：所有名称/代码 token，再所有平台别名（大小写不敏感）。

    再做一次"连接词 / 噪词"清洗，得到真正适合拼在每一条子查询**前**的通用语义。
    例："对比 贵州茅台600519 和 宁德时代300750 在雪球和东方财富上的最新新闻"
         → 剥离后剩："对比 和 在 上的最新新闻"
         → 噪词清洗后剩："最新新闻"（语义完整，可拼到每个子查询前）。
    """
    if not query:
        return ""
    tail = query
    # ---- A. 先删除所有股票名/代码（按长度降序，避免短的先匹配破坏长的）----
    tokens_remove: List[str] = []
    tokens_remove.extend(names)
    tokens_remove.extend(codes)
    # ---- B. 再加入所有命中平台的全部别名（如"雪球"+"xueqiu"都要删）----
    hit_platform_aliases: List[str] = []
    for cn_label, aliases in PLATFORM_KEYWORDS:
        if cn_label in platforms:
            hit_platform_aliases.extend(list(aliases))
    tokens_remove.extend(hit_platform_aliases)
    # 去重保序 + 长度降序（先删长 token，避免"茅台"/"茅"短词先吃一半）
    deduped: List[str] = []
    _seen: set = set()
    for t in tokens_remove:
        if not t:
            continue
        key = t.lower()
        if key in _seen:
            continue
        _seen.add(key)
        deduped.append(t)
    deduped.sort(key=len, reverse=True)
    for t in deduped:
        pattern = re.compile(re.escape(t), re.IGNORECASE)
        tail = pattern.sub(" ", tail)

    # ---- C. 噪词清洗：连接词 / 标点 / 常见占位汉语动词 ----
    # 1) 替换中文标点 → 空白
    tail = re.sub(r"[，。、,.；;：:？?！!·—\-…·（）()《》\[\]【】\"'`~@#\$%\^&\*\+=/\\|<>\{\}]+", " ", tail)
    # 2) 明确列出的无意义连接/介词/语气词
    noise = (
        "对比", "分析一下", "分析", "比较一下", "比较", "分别", "同时",
        "关于", "对于", "在", "上", "中", "里", "一下", "看看", "查询", "搜索",
        "查找", "检索", "查看", "帮我", "麻烦", "请", "请问",
        "的", "了", "和", "与", "或", "及", "以及", "还有", "还有就是",
        "上面", "方面", "一下儿", "各自", "分别是", "分别在", "它们", "他们", "她们",
        "最近", "近期", "目前", "现在", "今天", "昨天", "明天",
        "情况", "信息", "内容", "消息", "数据", "结果", "报告", "一些", "多少",
        "怎么样", "如何", "怎么", "哪些", "哪个", "什么",
    )
    noise_pattern = re.compile(
        r"(?<![\u4e00-\u9fffA-Za-z0-9])(" + "|".join(re.escape(w) for w in noise) + r")(?![\u4e00-\u9fffA-Za-z0-9])"
    )
    tail = noise_pattern.sub(" ", tail)
    # 3) 压缩空格
    tail = re.sub(r"\s+", " ", tail).strip()
    # 4) 空安全：若清洗后为空，回退一个简单 query（避免子查询语义模糊）
    if not tail:
        return ""
    return tail


def _contains_platform(query: str, alias: str) -> bool:
    if not alias or not query:
        return False
    return alias.lower() in query.lower()


def extract_sub_queries(query: str) -> Optional[List[SubQuery]]:
    """按"多股票/多平台"规则拆分查询。

    Returns:
        None — 不满足拆分条件，调用方走原单查询分支（向后兼容）。
        List[SubQuery] — 拆分结果（≥2 条才返回非 None；极端 1 条也返回 None）。
    """
    q = (query or "").strip()
    if not q:
        return None

    # 平台先验：如果原 query 中明确 ≥2 个平台关键词命中 → 用户意图是"跨平台比较"，
    # 则把"既是平台名又是上市公司名"（如 东方财富/同花顺/雪球(暂时非上市)/富途/老虎证券/微博(上市)）
    # 的中文平台名从 stock 匹配中屏蔽，避免"跨平台搜美联储讨论"误拆出 1 只股票×2 平台。
    pre_platforms = _detect_platforms(q)
    polysemous_mask: set = set()
    if len(pre_platforms) >= 2:
        for cn_label, _aliases in PLATFORM_KEYWORDS:
            if cn_label in pre_platforms:
                polysemous_mask.add(cn_label)

    raw_stocks = _lazy_extract_stocks(q)
    stock_identities: List[Tuple[str, str]] = []       # [(display_label, core_keyword)]
    seen_combos: set = set()
    for s in raw_stocks:
        name = getattr(s, "name", None) or ""
        code = getattr(s, "code", None) or ""
        if name and name in polysemous_mask:
            # 用户明显说的是"平台"而非"买这只股票" → 当作股票实体从当前拆分支路剔除
            continue
        key = (name, code)
        if key in seen_combos or (not name and not code):
            continue
        seen_combos.add(key)
        display = " ".join(x for x in [name, code] if x)
        core = " ".join(x for x in [name, code] if x)
        stock_identities.append((display, core))

    platforms = pre_platforms
    # 从 stock_identities 中抽 name/code 给 _extract_common_tail
    names: List[str] = []
    codes: List[str] = []
    for _disp, core in stock_identities:
        parts = core.split(" ")
        if len(parts) == 2:
            names.append(parts[0])
            codes.append(parts[1])
        elif parts and re.search(r"^\d{4,6}$", parts[0]):
            codes.append(parts[0])
        elif parts:
            names.append(parts[0])
    common_tail = _extract_common_tail(q, names, codes, platforms)

    def _build(*, stock_label: str = "", stock_core: str = "", platform: str = "") -> SubQuery:
        parts: List[str] = []
        # 通用语义放最前（如"最新研报和估值分析"），之后是平台限定（提升平台搜索精度），
        # 最后是标的本身（股票）。这样搜索引擎更容易理解"意图 + 范围 + 标的"。
        if common_tail:
            parts.append(common_tail)
        if platform:
            parts.append(f"{platform} 平台")
        if stock_core:
            parts.append(stock_core)
        # 至少要有公共尾 / 平台后缀作为搜索语义；如果 parts 仍空，回退原 query
        sub_q = " ".join(p for p in parts if p).strip() or q
        label_parts = [x for x in [stock_label, (f"平台·{platform}" if platform else "")] if x]
        label = " + ".join(label_parts) or ("通用:" + (common_tail or q[:10]))
        # category 标签
        if stock_label and platform:
            cat = "stock_platform"
        elif stock_label:
            cat = "stock"
        elif platform:
            cat = "platform"
        else:
            cat = "fallback"
        return SubQuery(label=label, category=cat, query=sub_q, weight=1.0)

    subs: List[SubQuery] = []
    multi_stock = len(stock_identities) >= 2
    multi_platform = len(platforms) >= 2

    if multi_stock and multi_platform:
        # 笛卡尔：每只股票 × 每平台
        for sl, sc in stock_identities:
            for pf in platforms:
                subs.append(_build(stock_label=sl, stock_core=sc, platform=pf))
    elif multi_stock:
        for sl, sc in stock_identities:
            if platforms:
                # 平台只 1 个：把它放进子查询提高精度
                subs.append(_build(stock_label=sl, stock_core=sc, platform=platforms[0]))
            else:
                subs.append(_build(stock_label=sl, stock_core=sc))
    elif multi_platform:
        for pf in platforms:
            if stock_identities:
                sl, sc = stock_identities[0]
                subs.append(_build(stock_label=sl, stock_core=sc, platform=pf))
            else:
                subs.append(_build(platform=pf))
    else:
        # 不满足多股票 or 多平台 → 不拆分（返回 None → 兼容原单查询）
        return None

    if len(subs) < 2:
        return None
    return subs


# ================================================================
# 聚合逻辑
# ================================================================
def _merge_structured_items(results: List[Any]) -> List[Dict[str, Any]]:
    """把 N 个子查询结果中的 _structured_items / results 去重合并（按 url 去重）。"""
    merged: Dict[str, Dict[str, Any]] = {}
    for sub_result in results or []:
        items: List[Dict[str, Any]] = []
        if isinstance(sub_result, dict):
            items = list(sub_result.get("_structured_items") or [])
            if not items:
                raw_list = sub_result.get("results") or []
                for r in raw_list:
                    if not isinstance(r, dict):
                        continue
                    url = str(r.get("url") or "")
                    items.append({
                        "doc_id": f"tavily-{abs(hash((url, str(r.get('title') or url)))) & 0xffffffff:x}",
                        "title": str(r.get("title") or url or "无标题"),
                        "url": url,
                        "content": str(r.get("content") or r.get("raw_content") or r.get("snippet") or ""),
                        "source_type": "web",
                        "channel": "tavily",
                        "score": float(r.get("score") or 0.0),
                        "published_at": str(r.get("published_date") or ""),
                    })
        for it in items:
            url = str(it.get("url") or "")
            key = url or (it.get("doc_id") or "")
            if not key:
                key = str(hash(it.get("content") or ""))
            if key in merged:
                # 只合并更高 score 的版本，标题更简洁优先
                prev_score = float(merged[key].get("score") or 0.0)
                if float(it.get("score") or 0.0) > prev_score:
                    merged[key] = it
            else:
                merged[key] = it
    # 按 score 降序 + 保持插入稳定
    return sorted(merged.values(), key=lambda it: float(it.get("score") or 0.0), reverse=True)


def aggregate_results(
    query: str,
    sub_queries: List[SubQuery],
    sub_results: List[Any],
    *,
    per_section_max_items: int = 4,
) -> Dict[str, Any]:
    """把并发搜索结果汇总为单条 Tavily-like 结构。

    输出字段（与 Tavily.search 结果完全兼容）：
      - query(str)             — 原始查询
      - answer(str)            — 结构化 markdown 汇总报告（新增：每子查询分节）
      - results(List[dict])    — 平铺的所有 results（按子查询顺序；去重）
      - _structured_items(List) — 平铺结构化项（与单条查询一样，供 citation / SSE 使用）
      - aggregated_report(str) — 与 answer 相同，留显式别名方便 LLM 直接消费
    """
    structured_items = _merge_structured_items(sub_results)

    # 构造 markdown 汇总
    sections: List[str] = []
    total_hits = 0
    ok_sections = 0
    now_ts = time.strftime("%Y-%m-%d %H:%M:%S")
    sections.append(f"# 🔍 多股票 / 多平台 并发搜索汇总（{now_ts}）")
    sections.append(f"> 原始查询：`{query}`")
    sections.append(f"> 拆分子查询数：`{len(sub_queries)}`（并发执行）")
    sections.append("")
    for idx, (sq, res) in enumerate(zip(sub_queries, sub_results), start=1):
        if isinstance(res, BaseException):
            sections.append(f"## ❌ {sq.label} [{sq.category}] — 搜索失败")
            sections.append(f"- 错误：{type(res).__name__}: {res}")
            sections.append("")
            continue
        items: List[Dict[str, Any]] = []
        if isinstance(res, dict):
            items = list(res.get("_structured_items") or [])
            if not items:
                raw = res.get("results") or []
                for r in raw:
                    if isinstance(r, dict):
                        items.append({
                            "title": str(r.get("title") or ""),
                            "url": str(r.get("url") or ""),
                            "content": str(r.get("content") or ""),
                            "score": float(r.get("score") or 0.0),
                            "published_at": str(r.get("published_date") or ""),
                        })
        total_hits += len(items)
        ok_sections += 1
        sections.append(f"## {idx}. ✅ {sq.label}  [{sq.category}]")
        sections.append(f"- 子查询：`{sq.query}`")
        sections.append(f"- 命中数：{len(items)}")
        shown = items[:per_section_max_items] if items else []
        for j, it in enumerate(shown, start=1):
            title = str(it.get("title") or "无标题")
            url = str(it.get("url") or "")
            content = str(it.get("content") or "").strip()
            if len(content) > 120:
                content = content[:117] + "…"
            pub = str(it.get("published_at") or "")
            line = f"  {j}. "
            if url:
                line += f"[{title}]({url})"
            else:
                line += title
            if pub:
                line += f"  _发布 {pub}_"
            sections.append(line)
            if content:
                sections.append(f"     - {content}")
        if len(items) > per_section_max_items:
            sections.append(f"  …（另有 {len(items) - per_section_max_items} 条未展示，见 results 数组）")
        sections.append("")
    sections.append("---")
    sections.append(f"**总计**：成功 {ok_sections}/{len(sub_queries)} 子查询 · 合并命中 {len(structured_items)} 条（去重前 {total_hits} 条）")
    report_md = "\n".join(sections)

    # results 字段（平铺所有子查询 raw results 去重后转换为 dict）
    flat_results: List[Dict[str, Any]] = []
    seen_urls: set = set()
    for it in structured_items:
        url = str(it.get("url") or "")
        if url in seen_urls:
            continue
        seen_urls.add(url)
        flat_results.append({
            "title": it.get("title", ""),
            "url": url,
            "content": it.get("content", ""),
            "score": it.get("score", 0.0),
            "published_date": it.get("published_at", ""),
        })

    return {
        "query": query,
        "answer": report_md,
        "results": flat_results,
        "_structured_items": structured_items,
        "aggregated_report": report_md,
    }


# ================================================================
# 并发入口（同步）
# ================================================================
def run_sync_parallel(
    query: str,
    topic: str,
    max_results: int,
    include_raw_content: bool,
    sync_search_fn: Callable[..., Any],
    *,
    max_workers: int = MAX_PARALLEL_WORKERS,
) -> Optional[Dict[str, Any]]:
    """同步并发拆分执行。不满足拆分条件时返回 None。

    Args:
        sync_search_fn — 同签名 `fn(query, topic, max_results, include_raw_content)`
            即 internet_search @tool 包装后的原始同步调用（用 .invoke 或原函数）。
    """
    sub_queries = extract_sub_queries(query)
    if sub_queries is None:
        return None
    workers = min(max_workers, max(2, len(sub_queries)))
    sub_results: List[Any] = [None] * len(sub_queries)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="TavilyParallel") as pool:
        future_to_idx = {
            pool.submit(sync_search_fn, sq.query, topic, max_results, include_raw_content): i
            for i, sq in enumerate(sub_queries)
        }
        for fut in concurrent.futures.as_completed(future_to_idx):
            i = future_to_idx[fut]
            try:
                sub_results[i] = fut.result()
            except Exception as exc:  # noqa: BLE001 - 子任务失败也要把异常带回来聚合
                sub_results[i] = exc
    return aggregate_results(query, sub_queries, sub_results)


# ================================================================
# 并发入口（异步）
# ================================================================
async def run_async_parallel(
    query: str,
    topic: str,
    max_results: int,
    include_raw_content: bool,
    async_search_fn: Callable[..., Coroutine[Any, Any, Any]],
) -> Optional[Dict[str, Any]]:
    """异步并发拆分执行（asyncio.gather）。不满足拆分条件时返回 None。"""
    sub_queries = extract_sub_queries(query)
    if sub_queries is None:
        return None
    coros = [async_search_fn(sq.query, topic, max_results, include_raw_content) for sq in sub_queries]
    done = await asyncio.gather(*coros, return_exceptions=True)
    sub_results: List[Any] = list(done)
    return aggregate_results(query, sub_queries, sub_results)
