# shared/data_sources: 4 个数据源统一接口（重构.md 设计）
#  1. web_search.py        <- Tavily 联网搜索 (internet_search)
#  2. zhishixingqiu.py     <- 知识星球 Playwright 抓取 (fetch_zsxq_group_topics, search_zsxq_by_stock, _run_zsxq_analysis)
#  3. ima_knowledge.py     <- IMA(RAGFlow) 远程知识库 (search_knowledge_base)
#  4. local_sql.py         <- MySQL 股票K线数据库 (list_sql_tables/get_table_data/execute_sql_query)
#  5. stock_matcher.py     <- 高性能股票代码/名称匹配工具
#  6. MyRAGFlow.py         <- IMA SDK 封装（ima_knowledge 依赖）
#
# 对外统一再导出，保留与原 tools/* 相同的符号名，避免后续逐层改引用。
from .web_search import internet_search
# search_zsxq_by_stock 的实代码在 tools/zsxq_tool.py（zhishixingqiu.py 是 sys.modules 重导向壳）。
# 不能在本 __init__ 顶层 eager import：tools.zsxq_tool 初始化中途会经 zsxq_history_store
# 触发本包 __init__，此时 search_zsxq_by_stock 尚未定义（定义在 zsxq_tool.py L1697）→ 循环 ImportError。
# PEP 562 懒加载：首次属性访问时 tools.zsxq_tool 必已完整加载。
def __getattr__(name):
    if name == "search_zsxq_by_stock":
        from tools.zsxq_tool import search_zsxq_by_stock as _fn
        return _fn
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
from .ima_knowledge import search_knowledge_base
from .local_sql import list_sql_tables, get_table_data, execute_sql_query
from .stock_matcher import extract_stocks, lookup_stock, is_stock_code


# 包的公共 API 面：顶层 re-export + PEP 562 懒加载（search_zsxq_by_stock）。
__all__ = [
    "internet_search",
    "search_knowledge_base",
    "list_sql_tables",
    "get_table_data",
    "execute_sql_query",
    "extract_stocks",
    "lookup_stock",
    "is_stock_code",
    "search_zsxq_by_stock",
]
