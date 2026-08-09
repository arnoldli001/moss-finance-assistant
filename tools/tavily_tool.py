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
import time
import requests.exceptions as rex
from dotenv import load_dotenv, find_dotenv  # 加载 .env 文件中的环境变量

# 自定义模块：工具调用埋点监控（需确保 api 模块可导入）
from api.monitor import monitor

# ======================== 初始化配置 ========================
# 使用 find_dotenv() 递归查找 .env 文件，确保从项目根目录加载
load_dotenv(find_dotenv())

# 步骤1： 定义一个TavilyClient对象
_tavily_api_key = os.getenv("TAVILY_API_KEY")
if not _tavily_api_key:
    print("[Tavily] 警告: TAVILY_API_KEY 未加载，网络搜索将不可用！请检查 .env 文件")
tavily_client = TavilyClient(api_key=_tavily_api_key)

# 连接重置等网络异常时的可重试异常类型集合
_CONNECTION_ERRORS = (
    rex.ConnectionError, rex.ConnectTimeout, rex.ReadTimeout, rex.ChunkedEncodingError,
    ConnectionResetError, ConnectionAbortedError, BrokenPipeError,
)
_TAVILY_MAX_RETRIES = 3


# 步骤2： 定义一个网络搜索工具
@tool
def internet_search(
        query: str,
        topic: Literal[ "news",  "finance",  "general"] = "general",
        max_results: int = 5,
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
    # 参数1： 工具的名字  参数2： 就是调用工具的参数信息
    monitor.report_tool(tool_name="网络搜索工具",
                        args={"query": query, "topic": topic, "max_results": max_results,
                              "include_raw_content": include_raw_content})

    # 对连接重置/超时类错误做指数退避重试，避免瞬时并发或 keep-alive 过期导致的 10054 错误
    last_err = None
    for attempt in range(1, _TAVILY_MAX_RETRIES + 1):
        try:
            t0 = time.time()
            result = tavily_client.search(query=query, topic=topic,
                                           max_results=max_results, include_raw_content=include_raw_content)
            elapsed = time.time() - t0
            hits = len(result.get("results", [])) if isinstance(result, dict) else 0
            if attempt > 1:
                print(f"[Tavily] 第{attempt}次重试成功 ({elapsed:.1f}s, {hits}条, query={query})")
            return result
        except _CONNECTION_ERRORS as e:
            last_err = e
            backoff = 2 ** attempt  # 2s / 4s / 8s
            print(f"[Tavily] 连接异常，第{attempt}次重试 (等待{backoff}s): {type(e).__name__}: {e} (query={query})")
            time.sleep(backoff)
        except Exception as e:
            print(f"[Tavily] 搜索失败: {type(e).__name__}: {e} (query={query})")
            return f"网络搜索失败: {type(e).__name__}: {str(e)}"
    # 所有重试用尽
    print(f"[Tavily] 重试{_TAVILY_MAX_RETRIES}次全部失败: {type(last_err).__name__}: {last_err} (query={query})")
    return f"网络搜索失败（网络连接异常，已重试{_TAVILY_MAX_RETRIES}次）: {type(last_err).__name__}: {str(last_err)}"














