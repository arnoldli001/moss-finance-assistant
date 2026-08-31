"""tools/zsxq：知识星球抓取 & 分析工具包（模块化拆分中）。

真实现文件位置：
  - _text_utils.py：HTML 正文清理 + topic API JSON → 标准化 Dict（纯函数，无全局状态）
  - _crawler.py：浏览器生命周期 + Playwright 抓取/搜索（类 ZsxqCrawler，逐步迁移中）

当前对外导出的纯函数（来自 _text_utils）：
    clean_text / extract_topic_info

对外导出的抓取类：
    ZsxqCrawler（Playwright 抓取/搜索实现，当前仍转发到 tools/zsxq_tool.py 顶层 helpers，后续版本会把实现本体落到本包）。

禁止直接修改本包内除 _text_utils 和实现类以外的薄壳路由。
"""
from ._text_utils import clean_text, extract_topic_info  # noqa: F401

__all__ = [
    "clean_text",
    "extract_topic_info",
]
