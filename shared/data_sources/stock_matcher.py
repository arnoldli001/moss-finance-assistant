# coding=utf-8
"""
兼容薄壳：真实实现已迁移到 shared/utils/stock_matcher.py。

保留本文件的原因：
  1. 历史直接 import：
       agents/router/agent.py L125: from shared.data_sources.stock_matcher import extract_stocks
       shared/search_split_aggregator.py L80: from shared.data_sources.stock_matcher import extract_stocks
     以及 compat_bootstrap 早期对本模块的 sys.modules 别名缓存，不改一个字都能工作。
  2. 三条路径（tools.stock_matcher / shared.data_sources.stock_matcher / shared.utils.stock_matcher）
     现在都指向同一个真实模块对象，保证 StockMatcher 单例全局唯一（不会重复加载 stock_list.txt）。
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
