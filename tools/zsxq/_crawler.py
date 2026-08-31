"""Playwright 浏览器抓取 & 搜索（薄壳转发层，实现本体待迁移）。

**当前架构（迁移过渡）**：
    浏览器抓取实现（_launch_browser / _fetch_topics_via_browser / _fetch_topics_by_search 等 1200+ 行）
    现阶段还保留在顶层 tools/zsxq_tool.py（因为和 zsxq_tool.py 的登录态全局锁、
    SSE bus 推送、DB 保存、history 增量判断等耦合太强），后续版本会按以下思路拆开：
        1) 先抽 3 个独立 helper 文件（✅ 已抽 _text_utils.py）；
        2) 再抽 登录 / 生命周期 管理到 _auth.py；
        3) 再抽 Playwright 抓取/搜索 到本文件；
        4) 最后 zsxq_tool.py 只剩 3 个 @tool 门面。

**对外暴露的类（ZsxqCrawler）**：
    现阶段以 `ZsxqCrawler(project_root, state_file, group_id, headless, browser_path, constants_module)`
    构造函数初始化，内部等价转发到 tools/zsxq_tool.py 的顶层同名 helpers，从而保持外部调用方
    在过渡期已通过统一实例调用，便于后续迁移实现时不需要再改任何调用代码。
"""
from __future__ import annotations

import os
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Set


class ZsxqCrawler:
    """过渡期薄壳：所有方法直接转发到 tools/zsxq_tool.py 顶层 helpers。

    使用：
        crawler = ZsxqCrawler(project_root=..., state_file=..., group_id=..., headless=...)
        if crawler.need_login():
            crawler.login_interactive()
        raw_topics = crawler.fetch_topics_via_browser(group_id=..., ...)
        search_hits = crawler.fetch_topics_by_search(stock_name=..., ...)
    """

    def __init__(
        self,
        *,
        project_root: Path,
        state_file: Path,
        group_id: str,
        headless: bool = True,
        browser_path: Optional[str] = None,
        constants: Optional[ModuleType] = None,
    ) -> None:
        self.project_root = Path(project_root)
        self.state_file = Path(state_file)
        self.group_id = group_id or ""
        self.headless = bool(headless)
        self.browser_path = browser_path or os.getenv("ZSXQ_BROWSER_PATH")
        self._constants = constants

        # 延迟加载顶层 helpers（避免与 zsxq_tool import ZsxqCrawler 形成循环依赖）
        self._helpers: Optional[Dict[str, Callable[..., Any]]] = None

    # ------------------------------------------------------------------
    # 懒加载顶层 helpers（实现本体在 tools/zsxq_tool.py，薄壳转发）
    # ------------------------------------------------------------------
    def _get_helper(self, name: str) -> Callable[..., Any]:
        if self._helpers is None:
            import tools.zsxq_tool as _t  # 延迟 import，避免循环

            self._helpers = {
                "launch_browser": _t._launch_browser,  # type: ignore[attr-defined]
                "need_login": _t._need_login,  # type: ignore[attr-defined]
                "login_interactive": _t._login_interactive,  # type: ignore[attr-defined]
                "fetch_topics_via_browser": _t._fetch_topics_via_browser,  # type: ignore[attr-defined]
                "fetch_topics_by_search": _t._fetch_topics_by_search,  # type: ignore[attr-defined]
            }
        return self._helpers[name]

    # ------------------------------------------------------------------
    # 公共 API（与 zsxq_tool.py 顶层 helpers 相同签名，仅薄壳转发）
    # ------------------------------------------------------------------
    def launch_browser(self, playwright: Any, *, headless: Optional[bool] = None):
        return self._get_helper("launch_browser")(playwright, headless=headless if headless is not None else self.headless)

    def need_login(self) -> bool:
        # 顶层 helper _need_login 目前还是闭包 over 全局 ZSXQ_STATE_FILE；这里直接转发即可，
        # 将来迁移到本类后改为 self.state_file。
        return self._get_helper("need_login")()

    def login_interactive(self) -> bool:
        return self._get_helper("login_interactive")()

    def fetch_topics_via_browser(
        self,
        group_id: str,
        *,
        max_scrolls: int,
        save_to_db: bool = False,
        max_topics: int,
        stop_on_duplicate: bool = False,
        known_topic_ids: Optional[Set[str]] = None,
    ) -> List[Dict]:
        return self._get_helper("fetch_topics_via_browser")(
            group_id,
            max_scrolls=max_scrolls,
            save_to_db=save_to_db,
            max_topics=max_topics,
            stop_on_duplicate=stop_on_duplicate,
            known_topic_ids=known_topic_ids,
        )

    def fetch_topics_by_search(self, stock_name: str, *, group_id: str = "", max_topics: int) -> List[Dict]:
        return self._get_helper("fetch_topics_by_search")(stock_name, group_id=group_id, max_topics=max_topics)
