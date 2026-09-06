# coding=utf-8
"""
薄壳：真实实现的唯一真源在 shared/utils/stock_matcher.py。

保留本文件的原因：
  1. 既有直接 import（无需逐个改引用）：
       agents/router/agent.py: from shared.data_sources.stock_matcher import extract_stocks
       shared/search_split_aggregator.py: from shared.data_sources.stock_matcher import extract_stocks
  2. 两条路径（shared.data_sources.stock_matcher 薄壳 / shared.utils.stock_matcher 真源）
     指向同一个真实模块对象，保证 StockMatcher 单例全局唯一（不会重复加载 stock_list.txt）。
"""
from shared.utils.stock_matcher import *  # noqa: F401,F403

from shared.utils.stock_matcher import (  # noqa: F401
    StockInfo,
    StockMatchHit,
    StockMatcher,
    is_stock_code,
    is_stock_name,
    is_stock_entity,
    lookup_stock,
    extract_stocks,
    get_stock_matcher,
)
