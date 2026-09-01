# 定义一个网络搜索的工具！
# ======================== 导入核心依赖 ========================
# 类型注解：增强代码提示和静态检查能力
from typing import  Literal
# LangChain 工具装饰器：将普通函数转为 Agent 可调用的工具
from langchain_core.tools import tool
# Tavily 官方客户端：实现网络搜索核心功能
from tavily import TavilyClient

# 系统/第三方依赖
import os  # 系统路径/环境变量处理
import sys
import time
import requests.exceptions as rex
from dotenv import load_dotenv, find_dotenv  # 加载 .env 文件中的环境变量
from pathlib import Path

# 自定义模块：工具调用埋点监控（需确保 api 模块可导入）
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from api.monitor import monitor

# bus 桥接：把检索结果作为 retrieve_result 发布，供 SSE 显示和引用元数据使用
def _try_publish_retrieve_result(channel: str, query: str, items_list):
    """尽力发布，跨线程/无事件循环时不阻塞（run_coroutine_threadsafe 安全兜底）。"""
    try:
        import asyncio as _aio
        from api.stream_bus import get_stream_bus_sync
        from api.context import get_thread_context
        tid = get_thread_context()
        if not tid:
            return
        bus = get_stream_bus_sync()
        if bus._loop is None or not bus._loop.is_running():
            return
        def _sync_pub():
            # ev_retrieve_result 虽然没有 async，但它内部 publish 通过 create_task
            #   所以直接在主线程里 call_soon_threadsafe 即可；这里不 await 结果。
            bus.ev_retrieve_result(tid, channel=channel, query=query, items=items_list)
        bus._loop.call_soon_threadsafe(_sync_pub)
    except Exception:
        pass

# ======================== 初始化配置 ========================
# 使用 find_dotenv() 递归查找 .env 文件，确保从项目根目录加载
load_dotenv(find_dotenv())

# 步骤1： 定义一个TavilyClient对象（惰性：无 key 时不实例化，import 期不因缺 key 失败——CI/无 key 环境可安全加载）
_tavily_api_key = os.getenv("TAVILY_API_KEY")
if not _tavily_api_key:
    print("[Tavily] 警告: TAVILY_API_KEY 未加载，网络搜索将不可用！请检查 .env 文件")
tavily_client = TavilyClient(api_key=_tavily_api_key) if _tavily_api_key else None


def _get_tavily_client() -> TavilyClient:
    """惰性获取 Tavily 客户端；首次调用时才强制要求 key（import 期保持零副作用）。"""
    global tavily_client
    if tavily_client is None:
        key = os.getenv("TAVILY_API_KEY")
        if not key:
            raise RuntimeError("TAVILY_API_KEY 未配置：网络搜索不可用，请检查 .env 或环境变量")
        tavily_client = TavilyClient(api_key=key)
    return tavily_client

# 连接重置等网络异常时的可重试异常类型集合
_CONNECTION_ERRORS = (
    rex.ConnectionError, rex.ConnectTimeout, rex.ReadTimeout, rex.ChunkedEncodingError,
    ConnectionResetError, ConnectionAbortedError, BrokenPipeError,
)

# ===== 全局常量集中引用（替代魔鬼数字，统一修改一处即全局生效）=====
from config.constants import (
    TAVILY_DEFAULT_MAX_RESULTS,
    TAVILY_MAX_RETRIES,
    TAVILY_BACKOFF_BASE,
)

_TAVILY_MAX_RETRIES = TAVILY_MAX_RETRIES


# ===== 并发拆分搜索（shared/search_split_aggregator）的懒加载 =====
def _get_split_support():
    try:
        from shared.search_split_aggregator import (
            run_sync_parallel, run_async_parallel, extract_sub_queries,
        )
        return run_sync_parallel, run_async_parallel, extract_sub_queries
    except Exception:
        return None, None, None


def _raw_tavily_search_once(
    query: str,
    topic: Literal["news", "finance", "general"],
    max_results: int,
    include_raw_content: bool,
):
    """不做 retry 的单次 Tavily 调用 + 结构化 + SSE 发布（子查询并发粒度）。"""
    result = _get_tavily_client().search(query=query, topic=topic,
                                         max_results=max_results, include_raw_content=include_raw_content)
    if isinstance(result, dict):
        raw_results = result.get("results") or []
        items_list = []
        for r in raw_results:
            url = str(r.get("url") or "")
            title = str(r.get("title") or url or "无标题")
            content = str(
                r.get("content") or r.get("raw_content") or r.get("snippet") or ""
            )
            score = float(r.get("score") or 0.0)
            pub = str(r.get("published_date") or "")
            items_list.append({
                "doc_id": f"tavily-{abs(hash((url, title, query))) & 0xffffffff:x}",
                "title": title, "url": url, "content": content,
                "source_type": "web", "channel": "tavily",
                "score": score, "published_at": pub,
            })
        try:
            from adapter.stream_adapters import filter_items_by_recency
            items_list, _a, _b = filter_items_by_recency(
                items_list, channel="tavily", auto_fallback=False
            )
        except Exception:
            pass
        _try_publish_retrieve_result("tavily", query, items_list)
        result["_structured_items"] = items_list
    return result


# 步骤2： 定义一个网络搜索工具
@tool
def internet_search(
        query: str,
        topic: Literal[ "news",  "finance",  "general"] = "general",
        max_results: int = TAVILY_DEFAULT_MAX_RESULTS,
        include_raw_content: bool = False
):
    """
    根据用户问题，进行网络信息收！ 
    注意：主要搜索公开的网络信息！如果指定查询数据库或者rag不能使用此工具！
    :param query: 用户的查询信息
    :param topic: 查询的类型
    :param max_results: 返回的最大条数 
    :param include_raw_content: 是否返回原内容 False 精简 True 详细
    :return: 
    """
    # 每次调用工具，都都会向前端推进调用进度！
    monitor.report_tool(tool_name="网络搜索工具",
                        args={"query": query, "topic": topic, "max_results": max_results,
                              "include_raw_content": include_raw_content})

    # —— [NEW] 多股票 / 多平台：拆分 + 线程池并发 + 汇总 ——
    run_sync_p, _run_async_p, _extract_sq = _get_split_support()
    if run_sync_p is not None:
        parallel = run_sync_p(
            query, topic, max_results, include_raw_content,
            sync_search_fn=_raw_tavily_search_once,
        )
        if parallel is not None:
            total_items = parallel.get("_structured_items") or []
            if total_items:
                _try_publish_retrieve_result("tavily", query, total_items)
            monitor.report_tool(
                tool_name="网络搜索工具-多查询并发汇总",
                args={"sub_queries": len(parallel.get("results") or []),
                      "total_items": len(total_items)},
            )
            return parallel

    # 对连接重置/超时类错误做指数退避重试
    last_err = None
    for attempt in range(1, _TAVILY_MAX_RETRIES + 1):
        try:
            t0 = time.time()
            result = _raw_tavily_search_once(query, topic, max_results, include_raw_content)
            elapsed = time.time() - t0
            hits = len(result.get("results", [])) if isinstance(result, dict) else 0
            if attempt > 1:
                print(f"[Tavily] 第{attempt}次重试成功 ({elapsed:.1f}s, {hits}条, query={query})")
            return result
        except _CONNECTION_ERRORS as e:
            last_err = e
            backoff = TAVILY_BACKOFF_BASE ** attempt
            print(f"[Tavily] 连接异常，第{attempt}次重试 (等待{backoff}s): {type(e).__name__}: {e} (query={query})")
            time.sleep(backoff)
        except Exception as e:
            print(f"[Tavily] 搜索失败: {type(e).__name__}: {e} (query={query})")
            return f"网络搜索失败: {type(e).__name__}: {str(e)}"
    print(f"[Tavily] 重试{_TAVILY_MAX_RETRIES}次全部失败: {type(last_err).__name__}: {last_err} (query={query})")
    return f"网络搜索失败（网络连接异常，已重试{_TAVILY_MAX_RETRIES}次）: {type(last_err).__name__}: {str(last_err)}"


async def internet_search_async(
        query: str,
        topic: Literal["news", "finance", "general"] = "general",
        max_results: int = TAVILY_DEFAULT_MAX_RESULTS,
        include_raw_content: bool = False,
):
    """异步版本网络搜索：供 analysis_workflow / SSE 主链路 await。

    - 单查询：asyncio.to_thread + 同 internet_search 相同的 retry / 结构化 / SSE 发布
    - 多股票 / 多平台：拆分 + asyncio.gather 并发 + aggregate_results 汇总
    返回 dict{query, answer, results, _structured_items, aggregated_report}，
    与同步 internet_search @tool 返回字段完全对齐（可互换）。
    """
    monitor.report_tool(
        tool_name="网络搜索工具(异步)",
        args={"query": query, "topic": topic, "max_results": max_results,
              "include_raw_content": include_raw_content},
    )
    import asyncio as _aio

    _run_sync_p, run_async_p, _extract_sq = _get_split_support()
    if run_async_p is not None:
        async def _worker(sq: str, tp: str, mr: int, irc: bool):
            last_err = None
            for attempt in range(1, _TAVILY_MAX_RETRIES + 1):
                try:
                    return await _aio.to_thread(
                        _raw_tavily_search_once, sq, tp, mr, irc
                    )
                except _CONNECTION_ERRORS as _e:
                    last_err = _e
                    backoff = TAVILY_BACKOFF_BASE ** attempt
                    print(f"[Tavily][async] 连接异常，子查询第{attempt}次重试 (等待{backoff}s): {type(_e).__name__}: {_e} (query={sq[:80]})")
                    await _aio.sleep(backoff)
                except Exception as _e:
                    return f"子查询失败: {type(_e).__name__}: {_e}"
            return f"子查询失败（已重试{_TAVILY_MAX_RETRIES}次）: {type(last_err).__name__}: {str(last_err)}"

        parallel = await run_async_p(
            query, topic, max_results, include_raw_content, async_search_fn=_worker,
        )
        if parallel is not None:
            items = parallel.get("_structured_items") or []
            if items:
                _try_publish_retrieve_result("tavily", query, items)
            return parallel

    last_err = None
    for attempt in range(1, _TAVILY_MAX_RETRIES + 1):
        try:
            t0 = time.time()
            result = await _aio.to_thread(
                _raw_tavily_search_once, query, topic, max_results, include_raw_content
            )
            elapsed = time.time() - t0
            hits = len(result.get("results", [])) if isinstance(result, dict) else 0
            if attempt > 1:
                print(f"[Tavily][async] 第{attempt}次重试成功 ({elapsed:.1f}s, {hits}条, query={query})")
            return result
        except _CONNECTION_ERRORS as e:
            last_err = e
            backoff = TAVILY_BACKOFF_BASE ** attempt
            print(f"[Tavily][async] 连接异常，第{attempt}次重试 (等待{backoff}s): {type(e).__name__}: {e} (query={query})")
            await _aio.sleep(backoff)
        except Exception as e:
            print(f"[Tavily][async] 搜索失败: {type(e).__name__}: {e} (query={query})")
            return f"网络搜索失败: {type(e).__name__}: {str(e)}"
    print(f"[Tavily][async] 重试{_TAVILY_MAX_RETRIES}次全部失败: {type(last_err).__name__}: {last_err} (query={query})")
    return f"网络搜索失败（网络连接异常，已重试{_TAVILY_MAX_RETRIES}次）: {type(last_err).__name__}: {str(last_err)}"














