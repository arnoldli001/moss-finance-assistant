# coding=utf-8
"""
兼容薄壳：真实实现已迁移到 shared/utils/stock_matcher.py。

保留本文件的原因：
  1. 历史 39 条 `from tools.stock_matcher import ...` 引用链（agents/reasoning、
     governance/guardrails、tests/test_stock_matcher、tools/zsxq_tool 等）不需改动；
  2. 与 markdown_tools/pdf_tools/upload_file_read_tool 三兄弟保持完全一致的
     「真实实现 → shared/utils/，tools/ 下留 re-export」迁移模式。
"""
from shared.utils.stock_matcher import *  # noqa: F401,F403

# 显式 re-export 常用符号（方便 IDE 跳转 / 消除 lint 告警）
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
