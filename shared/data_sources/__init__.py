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
from .zhishixingqiu import search_zsxq_by_stock  # 其他符号如需要随用随加
from .ima_knowledge import search_knowledge_base
from .local_sql import list_sql_tables, get_table_data, execute_sql_query
from .stock_matcher import extract_stocks, lookup_stock, is_stock_code
