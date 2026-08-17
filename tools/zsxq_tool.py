"""
知识星球 (zsxq) 群组内容抓取工具 - Playwright 浏览器自动化版

核心原理：
    用 Playwright 启动真实 Chromium 浏览器，导航到群组页面，
    拦截 api.zsxq.com 的 API 响应，直接获取 JSON 数据。
    浏览器自动处理签名、Cookie 等认证，不会被检测为"非官方工具"。

登录方式：
    首次运行时启动有头浏览器，用户扫码登录后保存 storage_state。
    后续运行自动加载 storage_state，无需重新登录。

环境变量（写入 .env）：
    ZSXQ_GROUP_ID=48848484411448   # 默认群组ID
    ZSXQ_HEADLESS=true             # 是否无头模式（首次登录需设为 false）
"""
import os
import sys
import json
import time
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime

# 确保项目根目录在 sys.path 中（直接运行本文件时需要）
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv, find_dotenv

try:
    from langchain_core.tools import tool as _lc_tool
    tool = _lc_tool
except (Exception, KeyboardInterrupt):
    # langchain_core 不可用或导入过慢时，使用空装饰器，保证脚本可直接运行
    def tool(func):
        return func

from api.monitor import monitor

# 使用 find_dotenv() 递归查找 .env
load_dotenv(find_dotenv())

# ===== 全局常量集中引用（替代魔鬼数字，统一修改一处即全局生效）=====
from config.constants import (
    ZSXQ_LOGIN_MAX_WAIT_SEC,
    ZSXQ_LOGIN_SUCCESS_WAIT_SEC,
    ZSXQ_LOGIN_PROGRESS_PRINT_INTERVAL_SEC,
    ZSXQ_DEFAULT_MAX_SCROLLS,
    ZSXQ_DEFAULT_FETCH_MAX_TOPICS,
    ZSXQ_PAGE_GOTO_TIMEOUT_MS,
    ZSXQ_INITIAL_SPA_RENDER_WAIT_SEC,
    ZSXQ_ALL_TAB_VISIBLE_TIMEOUT_MS,
    ZSXQ_ALL_TAB_SWITCH_WAIT_SEC,
    ZSXQ_SCROLL_WAIT_AFTER_SEC,
    ZSXQ_EXPAND_CLICK_INTERVAL_SEC,
    ZSXQ_EXPAND_BTN_VISIBLE_TIMEOUT_MS,
    ZSXQ_EXPAND_BTN_CLICK_TIMEOUT_MS,
    ZSXQ_EXPAND_PASS_SCROLL_WAIT_SEC,
    ZSXQ_DOM_SCROLL_TO_TOP_WAIT_SEC,
    ZSXQ_DOM_MERGE_MIN_RATIO,
    ZSXQ_DOM_MERGE_TAIL_CHECK_LEN,
    ZSXQ_SEARCH_DEFAULT_MAX_TOPICS,
    ZSXQ_SEARCH_BOX_VISIBLE_TIMEOUT_MS,
    ZSXQ_SEARCH_URL_GOTO_RENDER_WAIT_SEC,
    ZSXQ_SEARCH_INPUT_AFTER_FILL_WAIT_SEC,
    ZSXQ_SEARCH_AFTER_ENTER_WAIT_SEC,
    ZSXQ_SEARCH_FILTER_BTN_VISIBLE_TIMEOUT_MS,
    ZSXQ_SEARCH_FILTER_BTN_AFTER_CLICK_WAIT_SEC,
    ZSXQ_SEARCH_RESULTS_SELECTOR_TIMEOUT_MS,
    ZSXQ_SEARCH_RESULTS_FALLBACK_TIMEOUT_MS,
    ZSXQ_SEARCH_SCROLL_MORE_WAIT_SEC,
    ZSXQ_SEARCH_CARD_SCROLL_INTO_VIEW_TIMEOUT_MS,
    ZSXQ_SEARCH_CARD_AFTER_SCROLL_WAIT_SEC,
    ZSXQ_SEARCH_CARD_CLICK_TIMEOUT_MS,
    ZSXQ_SEARCH_CARD_INNER_CLICK_TIMEOUT_MS,
    ZSXQ_SEARCH_DETAIL_AFTER_OPEN_WAIT_SEC,
    ZSXQ_SEARCH_DETAIL_TEXT_VISIBLE_TIMEOUT_MS,
    ZSXQ_SEARCH_DETAIL_TEXT_INNER_TIMEOUT_MS,
    ZSXQ_SEARCH_DETAIL_TEXT_MIN_LEN,
    ZSXQ_SEARCH_DETAIL_AUTHOR_VISIBLE_TIMEOUT_MS,
    ZSXQ_SEARCH_DETAIL_AUTHOR_INNER_TIMEOUT_MS,
    ZSXQ_SEARCH_TOPIC_VALID_MIN_LEN,
    ZSXQ_SEARCH_DETAIL_CLOSE_AFTER_WAIT_SEC,
    ZSXQ_SEARCH_DETAIL_CLOSE_FINAL_WAIT_SEC,
    ZSXQ_SEARCH_DETAIL_OVERLAY_CLICK_OFFSET_PX,
    ZSXQ_SEARCH_DETAIL_VIEWPORT_EDGE_CLICK_X_PX,
    ZSXQ_OLLAMA_ENTRY_TRUNCATE_CHARS,
    ZSXQ_OLLAMA_ERROR_FALLBACK_TRUNCATE_CHARS,
    ZSXQ_BROWSER_LOCK_WAIT_TIMEOUT_SEC,
    ZSXQ_PREVIEW_CONTENT_TRUNCATE_CHARS,
    ZSXQ_DB_SEARCH_MAX_LIMIT,
    ZSXQ_DB_SEARCH_PREVIEW_TRUNCATE_CHARS,
    ZSXQ_HISTORY_TITLE_TRUNCATE_CHARS,
    ZSXQ_CONTENT_HASH_HEAD_CHARS,
    ZSXQ_TRUNCATE_MARKER_TAIL_CHECK_LEN,
    ZSXQ_DEBUG_API_URLS_PRINT_HEAD_COUNT,
    ZSXQ_DEBUG_API_URLS_PRINT_TRUNCATE_CHARS,
    ZSXQ_DEBUG_EMPTY_DUMP_MAX_URLS,
    ZSXQ_DEBUG_EMPTY_DUMP_URL_TRUNCATE_CHARS,
    ZSXQ_DOM_TOPIC_ID_PARENT_DEPTH_MAX,
    ZSXQ_DOM_CARD_FALLBACK_TEXT_MIN_LEN,
    ZSXQ_DOM_RESULT_MIN_LEN,
    ZSXQ_EXTRACT_TITLE_TRUNCATE_CHARS,
    ZSXQ_EXTRACT_AUTHOR_NAME_TRUNCATE_CHARS,
    ZSXQ_TOOL_FETCH_MAX_TOPICS_DEFAULT,
    ZSXQ_TOOL_SEARCH_STOCK_MAX_TOPICS,
    ZSXQ_RESULT_RAW_PREVIEW_TRUNCATE_CHARS,
)
from config.constants import (
    OLLAMA_DEFAULT_BASE_URL,
    OLLAMA_CHAT_DEFAULT_TIMEOUT_SEC,
    OLLAMA_DEFAULT_TEMPERATURE,
)

# ======================== 配置 ========================
ZSXQ_GROUP_ID = os.getenv("ZSXQ_GROUP_ID", "48848484411448")
ZSXQ_HEADLESS = os.getenv("ZSXQ_HEADLESS", "true").lower() == "true"
# storage_state 文件路径（保存登录后的 Cookie）
# 放在项目目录下，避免 TRAE 沙箱拦截 home 目录写入
ZSXQ_STATE_FILE = Path(_PROJECT_ROOT) / "zsxq_news" / ".zsxq_state.json"
# 群组页面 URL
ZSXQ_GROUP_URL = f"https://wx.zsxq.com/group/{ZSXQ_GROUP_ID}"

# ======================== 浏览器互斥锁 ========================
# 防止 search_zsxq_by_stock（按股票搜索）和 fetch_zsxq_group_topics（全量抓取）
# 同时运行两个 Playwright 浏览器会话导致冲突和超时
import threading as _threading
_zsxq_browser_lock = _threading.Lock()
_zsxq_active_operation = None  # 记录当前正在运行的操作名（"search" / "fetch_all" / None）
# 浏览器可执行文件路径（支持任意 Chromium 内核浏览器：360极速、Chrome、Edge 等）
# 留空则使用 Playwright 自带的 Chromium
ZSXQ_BROWSER_PATH = os.getenv(
    "ZSXQ_BROWSER_PATH",
    r"C:\Users\Administrator\AppData\Local\360Chrome\Chrome\Application\360chrome.exe",
)


def _launch_browser(playwright, headless: bool):
    """启动 Chromium 内核浏览器。
    优先用 ZSXQ_BROWSER_PATH 指定的浏览器（360极速/Chrome/Edge 等），
    留空则回退到 Playwright 自带的 Chromium。
    """
    kwargs = {"headless": headless}
    if ZSXQ_BROWSER_PATH and ZSXQ_BROWSER_PATH.strip():
        kwargs["executable_path"] = ZSXQ_BROWSER_PATH
    return playwright.chromium.launch(**kwargs)


def _need_login() -> bool:
    """检查是否需要登录（storage_state 文件不存在或为空）"""
    if not ZSXQ_STATE_FILE.exists():
        return True
    try:
        data = json.loads(ZSXQ_STATE_FILE.read_text(encoding="utf-8"))
        cookies = data.get("cookies", [])
        # 检查是否有 zsxq_access_token
        return not any(c.get("name") == "zsxq_access_token" for c in cookies)
    except Exception:
        return True


def _login_interactive() -> bool:
    """启动有头浏览器，让用户扫码登录，保存 storage_state"""
    from playwright.sync_api import sync_playwright

    print("[ZSXQ] 启动浏览器，请在打开的页面中扫码登录知识星球...")
    print(f"[ZSXQ] 登录后页面会自动跳转，storage_state 将保存到 {ZSXQ_STATE_FILE}")

    with sync_playwright() as p:
        browser = _launch_browser(p, headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        # 导航到登录页
        page.goto("https://wx.zsxq.com/")

        print("[ZSXQ] 等待登录完成（检测到 zsxq_access_token Cookie 后自动保存）...")

        # 等待登录完成（检测 zsxq_access_token Cookie）
        max_wait = ZSXQ_LOGIN_MAX_WAIT_SEC  # 最多等 5 分钟
        for i in range(max_wait):
            time.sleep(1)
            cookies = context.cookies()
            if any(c["name"] == "zsxq_access_token" for c in cookies):
                print("[ZSXQ] 检测到登录成功！")
                # 再等 2 秒让页面完全加载
                time.sleep(ZSXQ_LOGIN_SUCCESS_WAIT_SEC)
                break
            if i % ZSXQ_LOGIN_PROGRESS_PRINT_INTERVAL_SEC == 0 and i > 0:
                print(f"[ZSXQ] 已等待 {i} 秒，继续等待登录...")
        else:
            print(f"[ZSXQ] 登录超时（{ZSXQ_LOGIN_MAX_WAIT_SEC // 60}分钟），请重新运行。")
            browser.close()
            return False

        # 保存 storage_state
        context.storage_state(path=str(ZSXQ_STATE_FILE))
        print(f"[ZSXQ] 登录状态已保存到 {ZSXQ_STATE_FILE}")
        browser.close()
        return True


def _fetch_topics_via_browser(
    group_id: str,
    max_scrolls: int = ZSXQ_DEFAULT_MAX_SCROLLS,
    save_to_db: bool = False,
    max_topics: int = ZSXQ_DEFAULT_FETCH_MAX_TOPICS,
    stop_on_duplicate: bool = False,
    known_topic_ids: Optional[set] = None,
) -> List[Dict]:
    """用 Playwright 浏览器抓取群组主题列表。

    Args:
        group_id: 群组ID
        max_scrolls: 最大滚动次数
        save_to_db: 是否写库（保留参数兼容旧调用）
        max_topics: 最多抓取多少条主题
        stop_on_duplicate: 遇到已抓取过的 topic_id 是否立即停止
        known_topic_ids: 已知 topic_id 集合（用于增量判断）
    """
    from playwright.sync_api import sync_playwright

    topics: List[Dict] = []
    seen_ids = set()
    api_urls_seen = []  # 记录所有拦截到的 API URL（调试用）
    stop_fetching = False  # 增量抓取终止标志

    with sync_playwright() as p:
        # 启动浏览器，加载 storage_state
        browser = _launch_browser(p, headless=ZSXQ_HEADLESS)
        context = browser.new_context(
            storage_state=str(ZSXQ_STATE_FILE),
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        )
        page = context.new_page()

        # 拦截 API 响应
        def handle_response(response):
            try:
                url = response.url
                # 记录所有 api.zsxq.com 的请求（调试用）
                if "api.zsxq.com" in url:
                    api_urls_seen.append(url)
                    # 只打印前 10 个，避免刷屏
                    if len(api_urls_seen) <= ZSXQ_DEBUG_API_URLS_PRINT_HEAD_COUNT:
                        print(f"[ZSXQ] API 请求: {url[:ZSXQ_DEBUG_API_URLS_PRINT_TRUNCATE_CHARS]}")

                # 匹配主题列表 API（放宽匹配条件）
                if "api.zsxq.com" in url and "/topics" in url:
                    try:
                        data = response.json()
                        # 知识星球 API 返回格式：{"succeeded": true, "resp_data": {"topics": [...]}}
                        # 或者直接 {"succeeded": true, "topics": [...]}
                        topics_data = data.get("topics") or data.get("resp_data", {}).get("topics", [])
                        if topics_data:
                            new_topics = topics_data
                            dup_count = 0
                            for t in new_topics:
                                tid = t.get("topic_id")
                                if tid and tid not in seen_ids:
                                    seen_ids.add(tid)
                                    topics.append(t)
                                    # 增量判断：仅统计已抓取过的数量，不再跳过或停止
                                    if stop_on_duplicate and known_topic_ids and str(tid) in known_topic_ids:
                                        dup_count += 1
                                    # 达到最大数量也停止
                                    if len(topics) >= max_topics:
                                        stop_fetching = True
                                        break
                            msg = f"[ZSXQ] 拦截到 {len(new_topics)} 条主题，累计 {len(topics)} 条"
                            if dup_count > 0:
                                msg += f"，遇到 {dup_count} 条已抓取过"
                            print(msg)
                    except Exception as e:
                        # 响应可能不是 JSON（如图片、CSS 等）
                        print(f"[ZSXQ] 响应解析失败: {e}")
            except Exception:
                # 浏览器关闭后的响应会抛异常，忽略即可
                pass

        page.on("response", handle_response)

        # 导航到群组页面
        url = f"https://wx.zsxq.com/group/{group_id}"
        print(f"[ZSXQ] 正在打开群组页面: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=ZSXQ_PAGE_GOTO_TIMEOUT_MS)

        # 等待页面完全加载（SPA 需要额外时间渲染）
        print("[ZSXQ] 等待页面加载（SPA 渲染）...")
        time.sleep(ZSXQ_INITIAL_SPA_RENDER_WAIT_SEC)

        # 尝试点击"全部"标签（知识星球默认可能显示"精华"）
        try:
            # 查找并点击"全部"标签
            all_tab = page.locator("text=全部").first
            if all_tab.is_visible(timeout=ZSXQ_ALL_TAB_VISIBLE_TIMEOUT_MS):
                all_tab.click()
                print("[ZSXQ] 已点击'全部'标签")
                time.sleep(ZSXQ_ALL_TAB_SWITCH_WAIT_SEC)
        except Exception:
            print("[ZSXQ] 未找到'全部'标签（可能已是全部视图）")

        # 打印当前拦截到的 API 数量
        print(f"[ZSXQ] 初始加载共拦截到 {len(api_urls_seen)} 个 API 请求")

        # 滚动加载更多主题
        print(f"[ZSXQ] 开始滚动加载，最多滚动 {max_scrolls} 次，目标 {max_topics} 条...")
        for i in range(max_scrolls):
            if stop_fetching:
                print(f"[ZSXQ] 达到最大数量 {max_topics} 条，停止滚动")
                break
            prev_count = len(topics)
            # 滚动到页面底部
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            # 等待新内容加载（SPA 需要更长等待）
            time.sleep(ZSXQ_SCROLL_WAIT_AFTER_SEC)

            curr_count = len(topics)
            if curr_count == prev_count and i > 0:
                print(f"[ZSXQ] 第 {i+1} 次滚动无新内容，停止加载")
                break
            if curr_count > prev_count:
                print(f"[ZSXQ] 第 {i+1} 次滚动：新增 {curr_count - prev_count} 条，累计 {curr_count} 条")

        # 打印所有拦截到的 API URL（调试用）
        if not topics and api_urls_seen:
            print(f"\n[ZSXQ] 调试：共拦截到 {len(api_urls_seen)} 个 API 请求：")
            for u in api_urls_seen[:ZSXQ_DEBUG_EMPTY_DUMP_MAX_URLS]:
                print(f"  - {u[:ZSXQ_DEBUG_EMPTY_DUMP_URL_TRUNCATE_CHARS]}")

        # 完整内容提取：在列表页面点击每张主题卡片的"展开全部"按钮，再从 DOM 提取全文
        # （不跳转详情页，直接原地展开 + 提取，效率更高）
        if topics:
            print(f"\n[ZSXQ] 开始展开列表页所有'展开全部'按钮并提取完整内容（共 {len(topics)} 条）...")

            # 先滚回顶部，再逐批展开（避免元素不在视口内无法点击）
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(ZSXQ_DOM_SCROLL_TO_TOP_WAIT_SEC)

            # 先把所有"展开全部"按钮全部点击一遍（原地展开）
            # 知识星球列表是无限滚动，每次只能看到当前视口附近的卡片
            # 所以分多次滚动 + 点击
            total_expanded = 0
            for pass_idx in range(max_scrolls + 1):
                try:
                    # 查找当前可见的所有"展开全部"按钮
                    expand_btns = page.locator("text=展开全部")
                    count = expand_btns.count()
                    if count == 0:
                        # 没有更多按钮了，跳过
                        pass
                    else:
                        clicked_this_round = 0
                        for eb in range(count):
                            try:
                                btn = expand_btns.nth(eb)
                                if btn.is_visible(timeout=ZSXQ_EXPAND_BTN_VISIBLE_TIMEOUT_MS):
                                    # 滚动到按钮可见
                                    btn.scroll_into_view_if_needed(timeout=ZSXQ_EXPAND_BTN_VISIBLE_TIMEOUT_MS)
                                    btn.click(timeout=ZSXQ_EXPAND_BTN_CLICK_TIMEOUT_MS)
                                    clicked_this_round += 1
                                    total_expanded += 1
                                    time.sleep(ZSXQ_EXPAND_CLICK_INTERVAL_SEC)
                            except Exception as e:
                                print(f"[ZSXQ] 展开按钮点击失败: {e}")
                        if clicked_this_round:
                            print(f"[ZSXQ] 第{pass_idx+1}轮点击了 {clicked_this_round} 个展开全部按钮")
                except Exception as e:
                    print(f"[ZSXQ] 展开轮次异常: {e}")

                # 滚动到下一批
                page.evaluate("window.scrollBy(0, Math.floor(window.innerHeight * 0.7))")
                time.sleep(ZSXQ_EXPAND_PASS_SCROLL_WAIT_SEC)

            print(f"[ZSXQ] 共点击了 {total_expanded} 个展开全部按钮")

            # 滚回顶部，然后逐个匹配 API 拦截的 topic_id，提取 DOM 中展开后的完整文本
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(ZSXQ_DOM_SCROLL_TO_TOP_WAIT_SEC)

            print("[ZSXQ] 从 DOM 提取已展开的内容并与 API 数据合并...")

            # 构建 topic_id 到 topics 列表索引的映射
            tid_to_idx = {str(t.get("topic_id", "")): i for i, t in enumerate(topics)}

            # 在 JS 中遍历所有主题卡片，提取 topic_id 和对应全文
            dom_results = page.evaluate(f"""() => {{
                const results = {{}};
                // 找所有主题卡片（知识星球列表页每张卡片一般带 topic_id data 属性或链接中含 topic_id）
                const selectors = [
                    '[data-topic-id]',
                    '[data-id]',
                    '[class*="topic-card"]',
                    '[class*="TopicItem"]',
                    '[class*="topic-item"]',
                    '[href*="/topic/"]',
                ];
                const cards = new Set();
                selectors.forEach(sel => {{
                    document.querySelectorAll(sel).forEach(el => cards.add(el));
                }});

                cards.forEach(card => {{
                    // 尝试从元素或子元素 a href 中提取 topic_id
                    let tid = card.getAttribute('data-topic-id') || card.getAttribute('data-id') || '';
                    if (!tid) {{
                        const a = card.querySelector('a[href*="/topic/"]');
                        if (a) {{
                            const m = a.getAttribute('href').match(/topic\\/(\\d+)/);
                            if (m) tid = m[1];
                        }}
                    }}
                    if (!tid) {{
                        // 找父节点
                        let p = card.parentElement;
                        for (let k = 0; p && k < {ZSXQ_DOM_TOPIC_ID_PARENT_DEPTH_MAX}; k++) {{
                            const pid = p.getAttribute && (p.getAttribute('data-topic-id') || p.getAttribute('data-id'));
                            if (pid) {{ tid = pid; break; }}
                            p = p.parentElement;
                        }}
                    }}
                    if (!tid) return;

                    // 找文本内容最长的子元素（talk text）
                    let maxText = '';
                    const textSelectors = [
                        '[class*="talk"] [class*="text"]',
                        '[class*="TopicContent"] [class*="content"]',
                        '[class*="content"] [class*="text"]',
                        '[class*="RichText"]',
                        '[class*="rich-text"]',
                    ];
                    const root = card.closest('[class*="topic"]') || card;
                    textSelectors.forEach(sel => {{
                        root.querySelectorAll(sel).forEach(el => {{
                            const t = (el.innerText || el.textContent || '').trim();
                            if (t.length > maxText.length) maxText = t;
                        }});
                    }});
                    // 兜底：卡片自身 innerText
                    if (!maxText || maxText.length < {ZSXQ_DOM_CARD_FALLBACK_TEXT_MIN_LEN}) {{
                        const t = (root.innerText || '').trim();
                        if (t.length > maxText.length) maxText = t;
                    }}
                    if (maxText && maxText.length > {ZSXQ_DOM_RESULT_MIN_LEN}) {{
                        results[tid] = maxText;
                    }}
                }});
                return results;
            }}""")

            # 把 DOM 提取的内容合并到 API 数据
            merged = 0
            for tid_str, dom_text in dom_results.items():
                idx = tid_to_idx.get(tid_str)
                if idx is None:
                    continue
                api_text = topics[idx].get("talk", {}).get("text", "")
                # 只有 DOM 提取更长或没有截断标签时才替换
                dom_text_clean = dom_text.strip()
                if not dom_text_clean:
                    continue
                if len(dom_text_clean) > len(api_text) * ZSXQ_DOM_MERGE_MIN_RATIO:
                    topics[idx].setdefault("talk", {})
                    if len(dom_text_clean) >= len(api_text):
                        topics[idx]["talk"]["text"] = dom_text_clean
                        merged += 1
                    else:
                        # DOM 虽短一些，但 API 版末尾有截断标签也换
                        if "<e " in api_text[-ZSXQ_DOM_MERGE_TAIL_CHECK_LEN:] and "<e " not in dom_text_clean[-ZSXQ_DOM_MERGE_TAIL_CHECK_LEN:]:
                            topics[idx]["talk"]["text"] = dom_text_clean
                            merged += 1

            print(f"[ZSXQ] DOM 内容合并完成：{merged}/{len(topics)} 条替换为 DOM 提取版本")

        browser.close()

    return topics


def _clean_text(text: str) -> str:
    """清理知识星球正文中的 HTML 标签和多余空白"""
    import re
    if not text:
        return ""
    # 移除 <e type="hashtag" ... /> 等自闭合标签
    text = re.sub(r'<e\s+[^>]*/?>', '', text)
    # 移除其他常见 HTML 标签
    text = re.sub(r'<[^>]+>', '', text)
    # 移除末尾不完整的 HTML 标签（API 截断导致缺少闭合 >）
    text = re.sub(r'<[a-zA-Z][^>]*$', '', text)
    # URL 解码（如 %23 → #）
    try:
        from urllib.parse import unquote
        # 只解码看起来像 URL 编码的部分
        text = re.sub(
            r'%[0-9A-Fa-f]{2}',
            lambda m: unquote(m.group(0)),
            text,
        )
    except Exception:
        pass
    # 合并多余空行
    text = re.sub(r'\n{3,}', '\n\n', text)
    # 去除首尾空白
    return text.strip()


def _extract_topic_info(topic: Dict) -> Dict:
    """从 API 返回的 topic JSON 中提取关键字段"""
    talk = topic.get("talk", {})
    owner = topic.get("talk", {}).get("owner", {}) or topic.get("owner", {})

    title = _clean_text(talk.get("title") or "")
    text = _clean_text(talk.get("text") or "")
    # 合并标题和正文
    full_content = f"{title}\n\n{text}" if title else text

    # 处理时间（知识星球返回 ISO 8601 字符串，如 "2026-08-07T19:22:47.900+0800"）
    create_time_raw = topic.get("create_time", "")
    create_time_str = str(create_time_raw)
    create_time_ts = 0
    try:
        if isinstance(create_time_raw, str) and "T" in create_time_raw:
            # ISO 8601 格式：2026-08-07T19:22:47.900+0800
            # Python 3.7+ 的 fromisoformat 不支持毫秒+时区，需处理
            ts_str = create_time_raw.replace("+0800", "+08:00").replace("+0000", "+00:00")
            dt = datetime.fromisoformat(ts_str)
            create_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            create_time_ts = int(dt.timestamp())
        elif create_time_raw:
            # 可能是毫秒时间戳
            create_time_ts = int(create_time_raw)
            dt = datetime.fromtimestamp(create_time_ts / 1000)
            create_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    return {
        "topic_id": str(topic.get("topic_id", "")),
        "title": title[:ZSXQ_EXTRACT_TITLE_TRUNCATE_CHARS] if title else "",
        "content": full_content,
        "author_name": (owner.get("name") or "")[:ZSXQ_EXTRACT_AUTHOR_NAME_TRUNCATE_CHARS],
        "author_id": str(owner.get("user_id", "")),
        "create_time": create_time_str,
        "create_time_ts": create_time_ts,
        "like_count": int(topic.get("likes_count", 0) or 0),
        "comment_count": int(topic.get("comments_count", 0) or 0),
        "digested": bool(topic.get("digested", False)),
        "group_id": ZSXQ_GROUP_ID,
        "raw_json": json.dumps(topic, ensure_ascii=False),
    }


def _print_topic_preview(topic_info: Dict, index: int):
    """打印主题预览到控制台"""
    print(f"\n[ZSXQ] ─── 第 {index} 条 ──────────────────────")
    print(f"[ZSXQ] 作者: {topic_info['author_name']}    时间: {topic_info['create_time']}")
    content = topic_info["content"]
    # Windows 控制台默认 gbk 编码，无法输出 \xa0 等字符，统一替换为普通空格
    content = content.replace("\xa0", " ")
    if len(content) > ZSXQ_PREVIEW_CONTENT_TRUNCATE_CHARS:
        print(f"[ZSXQ] 正文: {content[:ZSXQ_PREVIEW_CONTENT_TRUNCATE_CHARS]}...")
    else:
        print(f"[ZSXQ] 正文: {content}")


def _save_to_db(topics_info: List[Dict]) -> str:
    """保存到 MySQL 数据库"""
    try:
        from mysql.connector import connect, Error
        from tools.db_tools import get_db_config

        cfg = get_db_config()
        conn = connect(**cfg)
        cur = conn.cursor()

        # 建表
        cur.execute("""
            CREATE TABLE IF NOT EXISTS zsxq_posts (
                id VARCHAR(64) PRIMARY KEY,
                group_id VARCHAR(64) NOT NULL,
                title VARCHAR(255),
                content TEXT,
                author_name VARCHAR(128),
                author_id VARCHAR(64),
                create_time VARCHAR(32),
                create_time_ts BIGINT,
                like_count INT DEFAULT 0,
                comment_count INT DEFAULT 0,
                digested TINYINT DEFAULT 0,
                raw_json LONGTEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uk_id (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        # 插入数据
        sql = """
            INSERT INTO zsxq_posts
            (id, group_id, title, content, author_name, author_id,
             create_time, create_time_ts, like_count, comment_count, digested, raw_json)
            VALUES (%(id)s, %(group_id)s, %(title)s, %(content)s, %(author_name)s,
                    %(author_id)s, %(create_time)s, %(create_time_ts)s, %(like_count)s,
                    %(comment_count)s, %(digested)s, %(raw_json)s)
            ON DUPLICATE KEY UPDATE
                title=VALUES(title), content=VALUES(content),
                like_count=VALUES(like_count), comment_count=VALUES(comment_count)
        """
        for info in topics_info:
            db_data = {
                "id": info["topic_id"],
                "group_id": info["group_id"],
                "title": info["title"],
                "content": info["content"],
                "author_name": info["author_name"],
                "author_id": info["author_id"],
                "create_time": info["create_time"],
                "create_time_ts": info["create_time_ts"],
                "like_count": info["like_count"],
                "comment_count": info["comment_count"],
                "digested": 1 if info["digested"] else 0,
                "raw_json": info["raw_json"],
            }
            cur.execute(sql, db_data)

        conn.commit()
        cur.close()
        conn.close()
        return f"已写入 {len(topics_info)} 条到数据库"
    except Exception as e:
        return f"写入数据库失败: {e}"


# ======================== LangChain 工具 ========================

# 已抓取记录文件路径（记录每个 topic_id 的 create_time + content 摘要，用于增量判断）
# 放在项目目录下，避免 TRAE 沙箱拦截 home 目录写入导致进程被杀
ZSXQ_HISTORY_FILE = Path(_PROJECT_ROOT) / "zsxq_news" / ".zsxq_history.json"


def _load_history() -> Dict:
    """加载已抓取历史记录 {topic_id: {ts: int, content_hash: str, create_time: str}}"""
    if not ZSXQ_HISTORY_FILE.exists():
        return {"topics": {}}
    try:
        return json.loads(ZSXQ_HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"topics": {}}


def _save_history(history: Dict):
    """保存已抓取历史记录"""
    try:
        ZSXQ_HISTORY_FILE.write_text(
            json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        print(f"[ZSXQ] 保存历史记录失败: {e}")


def _content_hash(text: str) -> str:
    """计算内容摘要（MD5 前 16 位，足够去重）"""
    import hashlib
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:ZSXQ_CONTENT_HASH_HEAD_CHARS]


@tool
def fetch_zsxq_group_topics(
    max_topics: int = ZSXQ_TOOL_FETCH_MAX_TOPICS_DEFAULT,
    incremental: bool = True,
    save_to_db: bool = False,
    group_id: str = "",
    max_scrolls: int = ZSXQ_DEFAULT_MAX_SCROLLS,
) -> str:
    """抓取知识星球群组的主题内容（支持增量抓取）。

    用 Playwright 浏览器自动化，通过拦截 API 响应获取主题列表。
    首次使用需先用 ZSXQ_HEADLESS=false 运行一次完成登录。

    Args:
        max_topics: 最多抓取多少条主题（默认 100）
        incremental: 是否增量抓取（默认 True，遇到与上次相同时间戳+内容的主题则停止）
        save_to_db: 是否保存到 MySQL 数据库（默认 False，仅打印到控制台）
        group_id: 群组ID（留空则用 .env 中的 ZSXQ_GROUP_ID）
        max_scrolls: 最大滚动次数（默认 10，每次约加载 10 条）

    Returns:
        抓取结果摘要
    """
    gid = group_id or ZSXQ_GROUP_ID

    try:
        monitor.report_tool("知识星球抓取工具", "start")
    except Exception:
        pass

    # 获取浏览器互斥锁：防止 search_zsxq_by_stock 同时运行
    global _zsxq_active_operation
    if not _zsxq_browser_lock.acquire(timeout=ZSXQ_BROWSER_LOCK_WAIT_TIMEOUT_SEC):
        active = _zsxq_active_operation or "未知操作"
        msg = f"知识星球浏览器正忙（{active}进行中），全量抓取已跳过"
        print(f"[ZSXQ] 跳过：{msg}")
        return msg
    _zsxq_active_operation = "fetch_all"

    try:
        # 检查登录状态
        if _need_login():
            if ZSXQ_HEADLESS:
                return (
                    "未检测到登录状态。请先设置 ZSXQ_HEADLESS=false 并运行一次完成扫码登录，"
                    "登录后会自动保存状态，之后可切回 headless 模式。"
                )
            print("[ZSXQ] 首次使用，需要扫码登录...")
            if not _login_interactive():
                return "登录失败，请重试。"

        # 加载已抓取历史（用于增量判断）
        history = _load_history()
        known_topic_ids = set(history.get("topics", {}).keys()) if incremental else None
        if incremental and known_topic_ids:
            print(f"[ZSXQ] 增量模式：已有 {len(known_topic_ids)} 条历史记录，遇到重复将停止")

        # 抓取主题
        print(f"[ZSXQ] 开始抓取群组 {gid} 的主题，目标 {max_topics} 条...")
        topics = _fetch_topics_via_browser(
            gid,
            max_scrolls=max_scrolls,
            save_to_db=save_to_db,
            max_topics=max_topics,
            stop_on_duplicate=incremental,
            known_topic_ids=known_topic_ids,
        )
    finally:
        # 释放浏览器互斥锁（浏览器操作完成后立即释放，后续处理不需要锁）
        _zsxq_active_operation = None
        _zsxq_browser_lock.release()

    if not topics:
        return f"群组 {gid} 未抓取到新主题（可能上次已抓取到最新内容，或登录已过期）"

    # 提取关键信息
    all_topics_info = [_extract_topic_info(t) for t in topics]
    print(f"[ZSXQ] 共抓取到 {len(all_topics_info)} 条主题")

    # 过滤掉内容未完整展开的主题（末尾有截断的 HTML 标签）—— 仅用于预览和数据库
    _TRUNCATE_MARKERS = [
        '<e type="hashtag" hid="',
        '<e type="hashtag" ',
        '<e type="',
    ]
    def _is_truncated(info: Dict) -> bool:
        content = info.get("content", "")
        tail = content[-ZSXQ_TRUNCATE_MARKER_TAIL_CHECK_LEN:]
        for m in _TRUNCATE_MARKERS:
            if m in tail:
                return True
        return False

    removed = [info for info in all_topics_info if _is_truncated(info)]
    topics_info = [info for info in all_topics_info if not _is_truncated(info)]
    if removed:
        print(f"[ZSXQ] 过滤掉 {len(removed)} 条内容未完整展开的主题（含截断的HTML标签），但仍会写入 JSON")

    # 打印预览到控制台
    print(f"\n[ZSXQ] ═══ 抓取完成，共 {len(topics_info)} 条新主题 ════════════════════")
    for i, info in enumerate(topics_info, 1):
        _print_topic_preview(info, i)

    # 输出 JSON 格式结果：时间戳为 key，{标题}:{正文} 为 value
    # 使用所有抓取到的主题（含被截断过滤的），时间戳重复时追加序号后缀
    json_result = {}
    for info in all_topics_info:
        ts = info.get("create_time", "") or f"unknown_{info.get('topic_id', '')}"
        title = info.get("title", "")
        content = info.get("content", "")
        if title:
            # content = "title\n\ntext"，取正文为 title 之后的部分
            body = content[len(title):].lstrip("\n").strip()
        else:
            # 无独立 title，取正文第一行为标题，其余为正文
            parts = content.split("\n", 1)
            title = parts[0].strip()
            body = parts[1].strip() if len(parts) > 1 else ""
        value = f"{title}:{body}" if body else title
        # value 为空或无内容则跳过
        if not value.strip():
            continue
        # 时间戳重复时将多条信息拼接到同一个 value 中
        if ts in json_result:
            json_result[ts] = json_result[ts] + "\n---\n" + value
        else:
            json_result[ts] = value
    print(f"\n[ZSXQ] JSON 结果（{len(json_result)} 条）：")
    print(json.dumps(json_result, ensure_ascii=False, indent=2))

    # 保存 JSON 到项目目录下的 zsxq_news 文件夹，文件名为输出时间戳
    try:
        news_dir = Path(_PROJECT_ROOT) / "zsxq_news"
        news_dir.mkdir(parents=True, exist_ok=True)
        file_name = datetime.now().strftime("%Y%m%d%H%M%S") + ".json"
        news_file = news_dir / file_name
        news_file.write_text(
            json.dumps(json_result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[ZSXQ] JSON 已保存至：{news_file}")
    except Exception as e:
        print(f"[ZSXQ] JSON 文件保存失败：{e}")

    # 更新历史记录
    if incremental:
        new_count = 0
        for info in topics_info:
            tid = info["topic_id"]
            if tid and tid not in history["topics"]:
                history["topics"][tid] = {
                    "ts": info.get("create_time_ts", 0),
                    "create_time": info.get("create_time", ""),
                    "content_hash": _content_hash(info.get("content", "")),
                    "title": (info.get("title") or "")[:ZSXQ_HISTORY_TITLE_TRUNCATE_CHARS],
                }
                new_count += 1
        _save_history(history)
        print(f"[ZSXQ] 历史记录更新：新增 {new_count} 条，总计 {len(history['topics'])} 条")

    # 保存到数据库
    db_msg = ""
    if save_to_db:
        db_msg = "，" + _save_to_db(topics_info)
    else:
        db_msg = "（未写入数据库，已打印到控制台）"

    try:
        monitor.report_tool("知识星球抓取工具", "end")
    except Exception:
        pass

    return f"群组 {gid} 抓取完成：本次新增 {len(topics_info)} 条主题{db_msg}"


@tool
def search_zsxq_topics(query: str, group_id: str = "") -> str:
    """在已抓取的知识星球主题中搜索关键词。

    注意：需要先调用 fetch_zsxq_group_topics 抓取并保存到数据库后才能搜索。

    Args:
        query: 搜索关键词
        group_id: 群组ID（留空则用 .env 中的 ZSXQ_GROUP_ID）

    Returns:
        匹配的主题列表
    """
    gid = group_id or ZSXQ_GROUP_ID

    try:
        from mysql.connector import connect
        from tools.db_tools import get_db_config

        cfg = get_db_config()
        conn = connect(**cfg)
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, title, content, author_name, create_time, like_count, comment_count "
            "FROM zsxq_posts WHERE group_id=%s AND (title LIKE %s OR content LIKE %s) "
            "ORDER BY create_time_ts DESC LIMIT %s",
            (gid, f"%{query}%", f"%{query}%", ZSXQ_DB_SEARCH_MAX_LIMIT),
        )
        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            return f"未找到包含 '{query}' 的主题"

        lines = [f"找到 {len(results)} 条匹配主题：\n"]
        for i, r in enumerate(results, 1):
            title = r.get("title") or ""
            content = (r.get("content") or "")[:ZSXQ_DB_SEARCH_PREVIEW_TRUNCATE_CHARS]
            lines.append(f"{i}. [{r['author_name']}] {title}")
            lines.append(f"   {content}...")
            lines.append(f"   时间: {r['create_time']}  赞: {r['like_count']}  评论: {r['comment_count']}\n")
        return "\n".join(lines)
    except Exception as e:
        return f"搜索失败: {e}"


# ======================== 按股票名搜索知识星球（浏览器搜索框） ========================

def _fetch_topics_by_search(
    stock_name: str,
    group_id: str = "",
    max_topics: int = ZSXQ_SEARCH_DEFAULT_MAX_TOPICS,
) -> List[Dict]:
    """用 Playwright 浏览器在知识星球搜索框中搜索股票名，
    点击"当前星球"搜索，再点"最新"排序，然后逐条点击打开主题详情，
    复制完整内容后退出详情，再取下一条。

    DOM 结构（Angular 组件）：
      app-search-result > app-joined-group-topic > div.topic-container > app-topic-preview
      > div > div.main-content > div.content > div

    Args:
        stock_name: 要搜索的股票名/关键词
        group_id: 群组ID（留空用默认）
        max_topics: 最多返回多少条（默认 5）
    """
    from playwright.sync_api import sync_playwright

    gid = group_id or ZSXQ_GROUP_ID
    topics: List[Dict] = []
    api_topics: Dict[str, Dict] = {}  # API 拦截到的元数据（topic_id → raw dict）

    with sync_playwright() as p:
        browser = _launch_browser(p, headless=ZSXQ_HEADLESS)
        context = browser.new_context(
            storage_state=str(ZSXQ_STATE_FILE),
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        )
        page = context.new_page()

        # 拦截 API 响应（保留用于获取 topic_id / author / create_time 等元数据）
        def handle_response(response):
            try:
                url = response.url
                if "api.zsxq.com" in url and ("search" in url or "topics" in url):
                    try:
                        data = response.json()
                        topics_data = (
                            data.get("topics")
                            or data.get("resp_data", {}).get("topics", [])
                            or []
                        )
                        for t in topics_data:
                            tid = str(t.get("topic_id", ""))
                            if tid:
                                api_topics[tid] = t
                    except Exception:
                        pass
            except Exception:
                pass

        page.on("response", handle_response)

        # ============ Step 1: 导航到群组页面 ============
        url = f"https://wx.zsxq.com/group/{gid}"
        print(f"[ZSXQ-Search] 正在打开群组页面: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=ZSXQ_PAGE_GOTO_TIMEOUT_MS)
        time.sleep(ZSXQ_SEARCH_FILTER_BTN_AFTER_CLICK_WAIT_SEC)

        # ============ Step 2: 找搜索框并输入股票名 ============
        print(f"[ZSXQ-Search] 在搜索框中输入: {stock_name}")
        search_input = None
        # 尝试多种选择器找到搜索框
        for sel in [
            'input[placeholder*="搜索"]',
            'input[placeholder*="search"]',
            'input[type="search"]',
            '[class*="search"] input',
            '[class*="Search"] input',
        ]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=ZSXQ_SEARCH_BOX_VISIBLE_TIMEOUT_MS):
                    search_input = el
                    break
            except Exception:
                continue

        if search_input is None:
            # 尝试点击搜索图标展开搜索框
            for icon_sel in ['text=搜索', '[class*="search-icon"]', '[class*="SearchIcon"]']:
                try:
                    icon = page.locator(icon_sel).first
                    if icon.is_visible(timeout=ZSXQ_SEARCH_BOX_VISIBLE_TIMEOUT_MS):
                        icon.click()
                        time.sleep(ZSXQ_SEARCH_DETAIL_CLOSE_AFTER_WAIT_SEC)
                        break
                except Exception:
                    continue
            # 再次找输入框
            for sel in ['input[placeholder*="搜索"]', 'input[type="search"]', '[class*="search"] input']:
                try:
                    el = page.locator(sel).first
                    if el.is_visible(timeout=ZSXQ_SEARCH_BOX_VISIBLE_TIMEOUT_MS):
                        search_input = el
                        break
                except Exception:
                    continue

        if search_input is None:
            print("[ZSXQ-Search] 未找到搜索框，尝试用 URL 直接搜索")
            search_url = f"https://wx.zsxq.com/group/{gid}?keyword={stock_name}"
            page.goto(search_url, wait_until="domcontentloaded", timeout=ZSXQ_PAGE_GOTO_TIMEOUT_MS)
            time.sleep(ZSXQ_SEARCH_URL_GOTO_RENDER_WAIT_SEC)
        else:
            search_input.click()
            search_input.fill(stock_name)
            time.sleep(ZSXQ_SEARCH_INPUT_AFTER_FILL_WAIT_SEC)
            page.keyboard.press("Enter")
            print("[ZSXQ-Search] 已输入并按回车搜索")
            time.sleep(ZSXQ_SEARCH_AFTER_ENTER_WAIT_SEC)

        # ============ Step 3: 点击"当前星球"按钮 ============
        print('[ZSXQ-Search] 尝试点击"当前星球"按钮...')
        try:
            for sel in ['text=当前星球', 'button:has-text("当前星球")', 'span:has-text("当前星球")']:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible(timeout=ZSXQ_SEARCH_FILTER_BTN_VISIBLE_TIMEOUT_MS):
                        btn.click()
                        print('[ZSXQ-Search] 已点击"当前星球"')
                        time.sleep(ZSXQ_SEARCH_FILTER_BTN_AFTER_CLICK_WAIT_SEC)
                        break
                except Exception:
                    continue
        except Exception as e:
            print(f'[ZSXQ-Search] 点击"当前星球"失败: {e}')

        # ============ Step 4: 点击"最新"排序 ============
        print('[ZSXQ-Search] 尝试点击"最新"按钮...')
        try:
            for sel in ['text=最新', 'button:has-text("最新")', 'span:has-text("最新")']:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible(timeout=ZSXQ_SEARCH_FILTER_BTN_VISIBLE_TIMEOUT_MS):
                        btn.click()
                        print('[ZSXQ-Search] 已点击"最新"排序')
                        time.sleep(ZSXQ_SEARCH_FILTER_BTN_AFTER_CLICK_WAIT_SEC)
                        break
                except Exception:
                    continue
        except Exception as e:
            print(f'[ZSXQ-Search] 点击"最新"失败: {e}')

        # ============ Step 5: 等待搜索结果渲染完成 ============
        print("[ZSXQ-Search] 等待搜索结果渲染...")
        time.sleep(ZSXQ_SEARCH_FILTER_BTN_AFTER_CLICK_WAIT_SEC)

        # 确认搜索结果列表已出现（Angular 组件 app-search-result）
        try:
            page.wait_for_selector("app-search-result app-topic-preview", timeout=ZSXQ_SEARCH_RESULTS_SELECTOR_TIMEOUT_MS)
        except Exception:
            # 尝试备选选择器
            try:
                page.wait_for_selector("app-topic-preview, .topic-container, [class*='topic-preview']", timeout=ZSXQ_SEARCH_RESULTS_FALLBACK_TIMEOUT_MS)
            except Exception:
                print("[ZSXQ-Search] 未检测到搜索结果组件，尝试从 DOM 强制提取...")

        # ============ Step 6: 逐条点击打开主题详情，复制完整内容 ============
        print(f"[ZSXQ-Search] 开始逐条打开主题详情，目标 {max_topics} 条...")

        for idx in range(max_topics):
            try:
                # 重新定位每一条 app-topic-preview（因为退出详情后 DOM 可能重新渲染）
                # 用户提供的 DOM 路径：app-search-result > ... > app-topic-preview:nth-child(N)
                topic_cards = page.locator("app-topic-preview")
                total_cards = topic_cards.count()
                print(f"[ZSXQ-Search] 当前搜索结果共 {total_cards} 条，准备打开第 {idx+1} 条")

                if idx >= total_cards:
                    # 尝试滚动加载更多
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(ZSXQ_SEARCH_SCROLL_MORE_WAIT_SEC)
                    topic_cards = page.locator("app-topic-preview")
                    total_cards = topic_cards.count()
                    if idx >= total_cards:
                        print(f"[ZSXQ-Search] 只有 {total_cards} 条搜索结果，已全部抓取")
                        break

                card = topic_cards.nth(idx)

                # 确保卡片在视口内
                try:
                    card.scroll_into_view_if_needed(timeout=ZSXQ_SEARCH_CARD_SCROLL_INTO_VIEW_TIMEOUT_MS)
                    time.sleep(ZSXQ_SEARCH_CARD_AFTER_SCROLL_WAIT_SEC)
                except Exception:
                    pass

                # 尝试从卡片上获取 topic_id（用于与 API 数据关联）
                card_topic_id = ""
                try:
                    # 从卡片的子链接中提取 topic_id
                    href = card.locator("a[href*='/topic/']").first.get_attribute("href")
                    if href:
                        import re as _re
                        m = _re.search(r"topic/(\d+)", href)
                        if m:
                            card_topic_id = m.group(1)
                except Exception:
                    pass

                # 点击卡片打开详情视图
                print(f"[ZSXQ-Search] 点击第 {idx+1} 条主题卡片...")
                try:
                    card.click(timeout=ZSXQ_SEARCH_CARD_CLICK_TIMEOUT_MS)
                except Exception:
                    # 尝试点击内部元素
                    try:
                        card.locator("div.main-content, div.content, .topic-container").first.click(timeout=ZSXQ_SEARCH_CARD_INNER_CLICK_TIMEOUT_MS)
                    except Exception as e:
                        print(f"[ZSXQ-Search] 点击卡片失败: {e}")
                        continue

                # 等待详情视图加载
                time.sleep(ZSXQ_SEARCH_DETAIL_AFTER_OPEN_WAIT_SEC)

                # 从详情视图中提取完整内容
                detail_text = ""
                author_name = ""
                create_time_str = ""
                topic_title = ""

                try:
                    # 详情页可能用多种选择器，逐个尝试
                    # 用户提供的路径：... > div.main-content > div.content > div
                    detail_selectors = [
                        # 知识星球详情页正文区域
                        "div.main-content div.content",
                        "div.main-content > div.content > div",
                        "[class*='topic-detail'] [class*='content']",
                        "[class*='topic-content']",
                        "[class*='TopicContent']",
                        "[class*='talk-content']",
                        "[class*='talk'] [class*='text']",
                        # 搜索结果详情
                        "app-topic-detail div.content",
                        "app-topic-detail [class*='content']",
                        # 兜底：弹窗中的文本区
                        "div[class*='dialog'] div[class*='content']",
                        "div[class*='modal'] div[class*='content']",
                    ]
                    for ds in detail_selectors:
                        try:
                            el = page.locator(ds).first
                            if el.is_visible(timeout=ZSXQ_SEARCH_DETAIL_TEXT_VISIBLE_TIMEOUT_MS):
                                detail_text = el.inner_text(timeout=ZSXQ_SEARCH_DETAIL_TEXT_INNER_TIMEOUT_MS)
                                if detail_text and len(detail_text) > ZSXQ_SEARCH_DETAIL_TEXT_MIN_LEN:
                                    print(f"[ZSXQ-Search] 提取到正文 ({len(detail_text)} 字符)，选择器: {ds}")
                                    break
                        except Exception:
                            continue

                    # 如果上面没取到，兜底取整个详情页 body 文本
                    if not detail_text or len(detail_text) < ZSXQ_SEARCH_DETAIL_TEXT_MIN_LEN:
                        try:
                            # 详情通常是弹窗/遮罩层
                            overlay = page.locator(
                                "[class*='overlay'], [class*='dialog'], [class*='modal'], "
                                "app-topic-detail, [class*='topic-detail']"
                            ).first
                            if overlay.is_visible(timeout=ZSXQ_SEARCH_DETAIL_TEXT_VISIBLE_TIMEOUT_MS):
                                detail_text = overlay.inner_text(timeout=ZSXQ_SEARCH_DETAIL_TEXT_INNER_TIMEOUT_MS)
                                print(f"[ZSXQ-Search] 兜底提取详情区域 ({len(detail_text)} 字符)")
                        except Exception:
                            pass

                    # 提取作者名
                    for author_sel in [
                        "[class*='author'] [class*='name']",
                        "[class*='user-name']",
                        "[class*='nickname']",
                        "app-topic-detail [class*='author']",
                    ]:
                        try:
                            el = page.locator(author_sel).first
                            if el.is_visible(timeout=ZSXQ_SEARCH_DETAIL_AUTHOR_VISIBLE_TIMEOUT_MS):
                                author_name = el.inner_text(timeout=ZSXQ_SEARCH_DETAIL_AUTHOR_INNER_TIMEOUT_MS).strip()
                                if author_name:
                                    break
                        except Exception:
                            continue

                    # 提取创建时间
                    for time_sel in [
                        "[class*='create-time']",
                        "[class*='time']",
                        "[class*='date']",
                        "app-topic-detail [class*='time']",
                    ]:
                        try:
                            el = page.locator(time_sel).first
                            if el.is_visible(timeout=ZSXQ_SEARCH_DETAIL_AUTHOR_VISIBLE_TIMEOUT_MS):
                                create_time_str = el.inner_text(timeout=ZSXQ_SEARCH_DETAIL_AUTHOR_INNER_TIMEOUT_MS).strip()
                                if create_time_str:
                                    break
                        except Exception:
                            continue

                except Exception as e:
                    print(f"[ZSXQ-Search] 提取详情内容失败: {e}")

                # 如果取到了内容，组装 topic dict
                if detail_text and len(detail_text) > ZSXQ_SEARCH_TOPIC_VALID_MIN_LEN:
                    # 清理文本
                    detail_text = _clean_text(detail_text)

                    # 尝试从 API 元数据中获取 topic_id
                    tid = card_topic_id or ""

                    # 构造 topic dict（兼容 _extract_topic_info 的格式）
                    topic_dict = {
                        "topic_id": tid or f"search_{idx}_{int(time.time())}",
                        "talk": {
                            "text": detail_text,
                            "title": topic_title or "",
                            "owner": {
                                "name": author_name,
                                "user_id": "",
                            },
                        },
                        "owner": {
                            "name": author_name,
                            "user_id": "",
                        },
                        "create_time": create_time_str,
                        "likes_count": 0,
                        "comments_count": 0,
                        "digested": False,
                        "group_id": gid,
                    }

                    # 如果 API 拦截到了相同 topic_id 的元数据，合并
                    if tid and tid in api_topics:
                        api_t = api_topics[tid]
                        topic_dict["create_time"] = api_t.get("create_time", create_time_str)
                        topic_dict["likes_count"] = api_t.get("likes_count", 0)
                        topic_dict["comments_count"] = api_t.get("comments_count", 0)
                        talk = api_t.get("talk", {})
                        if talk.get("owner", {}).get("name"):
                            topic_dict["talk"]["owner"]["name"] = talk["owner"]["name"]
                            topic_dict["owner"]["name"] = talk["owner"]["name"]

                    topics.append(topic_dict)
                    print(f"[ZSXQ-Search] 第 {idx+1} 条已提取: 作者={author_name or '未知'}, "
                          f"内容长度={len(detail_text)}, topic_id={tid or 'N/A'}")

                # ============ 退出详情视图（点击浏览器两侧 / Escape）============
                print("[ZSXQ-Search] 退出详情视图...")
                exited = False

                # 方式1: 按 Escape 键
                try:
                    page.keyboard.press("Escape")
                    time.sleep(ZSXQ_SEARCH_DETAIL_CLOSE_AFTER_WAIT_SEC)
                    # 检查详情是否已关闭
                    if not page.locator(
                        "app-topic-detail, [class*='topic-detail'], [class*='dialog'], [class*='modal']"
                    ).first.is_visible(timeout=ZSXQ_SEARCH_DETAIL_AUTHOR_VISIBLE_TIMEOUT_MS):
                        exited = True
                except Exception:
                    pass

                # 方式2: 点击遮罩层两侧空白区域
                if not exited:
                    try:
                        # 找遮罩/弹窗容器，点击其左右两侧空白区域
                        for overlay_sel in [
                            "[class*='overlay']",
                            "[class*='mask']",
                            "[class*='dialog-wrapper']",
                            "[class*='modal-wrapper']",
                            "[class*='backdrop']",
                        ]:
                            try:
                                overlay = page.locator(overlay_sel).first
                                if overlay.is_visible(timeout=ZSXQ_SEARCH_DETAIL_AUTHOR_VISIBLE_TIMEOUT_MS):
                                    # 点击左侧空白区域
                                    box = overlay.bounding_box()
                                    if box:
                                        # 点击左上角偏移位置（遮罩层边缘）
                                        page.mouse.click(box["x"] + ZSXQ_SEARCH_DETAIL_OVERLAY_CLICK_OFFSET_PX, box["y"] + ZSXQ_SEARCH_DETAIL_OVERLAY_CLICK_OFFSET_PX)
                                        time.sleep(ZSXQ_SEARCH_DETAIL_CLOSE_AFTER_WAIT_SEC)
                                        exited = True
                                        break
                            except Exception:
                                continue
                    except Exception:
                        pass

                # 方式3: 点击浏览器视口边缘
                if not exited:
                    try:
                        viewport = page.viewport_size
                        if viewport:
                            # 点击视口左边缘
                            page.mouse.click(ZSXQ_SEARCH_DETAIL_VIEWPORT_EDGE_CLICK_X_PX, viewport["height"] // 2)
                            time.sleep(ZSXQ_SEARCH_DETAIL_CLOSE_AFTER_WAIT_SEC)
                            exited = True
                    except Exception:
                        pass

                # 方式4: 再次按 Escape
                if not exited:
                    try:
                        page.keyboard.press("Escape")
                        time.sleep(ZSXQ_SEARCH_DETAIL_CLOSE_AFTER_WAIT_SEC)
                    except Exception:
                        pass

                time.sleep(ZSXQ_SEARCH_DETAIL_CLOSE_FINAL_WAIT_SEC)  # 等待列表恢复

            except Exception as e:
                print(f"[ZSXQ-Search] 第 {idx+1} 条抓取异常: {e}")
                # 尝试退出可能的详情视图
                try:
                    page.keyboard.press("Escape")
                    time.sleep(ZSXQ_SEARCH_DETAIL_CLOSE_AFTER_WAIT_SEC)
                except Exception:
                    pass
                continue

        browser.close()

    print(f"[ZSXQ-Search] 共抓取到 {len(topics)} 条关于「{stock_name}」的内容")
    return topics[:max_topics]


def _analyze_with_qwen8b(stock_name: str, topics_info: List[Dict]) -> str:
    """调用本地 Ollama Qwen3-8B 模型对知识星球搜索结果进行分析汇总。

    Args:
        stock_name: 用户查询的股票名
        topics_info: 从知识星球搜索到的主题信息列表

    Returns:
        分析汇总文本
    """
    from urllib import request as _url_req, error as _url_err

    # 拼接搜索结果内容
    entries = []
    for i, info in enumerate(topics_info, 1):
        author = info.get("author_name", "未知")
        t = info.get("create_time", "")
        content = info.get("content", "")[:ZSXQ_OLLAMA_ENTRY_TRUNCATE_CHARS]
        entries.append(f"{i}. [{author} {t}] {content}")
    content_text = "\n\n".join(entries)

    if not content_text.strip():
        return f"知识星球中未搜索到与「{stock_name}」相关的内容"

    system_prompt = (
        "你是A股金融分析师。分析知识星球社区中关于特定股票的讨论，"
        "提取关键观点、市场情绪（利好/利空），并汇总核心信息。"
        "注意区分可靠信息（来自研报/公告）和不可靠信息（来自股吧/论坛/个人观点）。"
    )
    user_prompt = f"""请分析以下知识星球中关于「{stock_name}」的搜索结果，给出：
1. 市场情绪判断（利好/利空/中性）
2. 核心观点汇总（3-5条要点）
3. 关键数据或事件（如有）
4. 信息来源可靠性评估

搜索结果（共{len(topics_info)}条）：
{content_text}"""

    payload = json.dumps({
        "model": "qwen3:8b",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "options": {"temperature": OLLAMA_DEFAULT_TEMPERATURE},
        "stream": False,
    }).encode("utf-8")

    req = _url_req.Request(
        OLLAMA_DEFAULT_BASE_URL.rstrip("/") + "/api/chat",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with _url_req.urlopen(req, timeout=OLLAMA_CHAT_DEFAULT_TIMEOUT_SEC) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body["message"]["content"]
    except _url_err.URLError as e:
        return f"[Qwen8B分析失败] Ollama连接失败: {e}，原始搜索结果如下：\n\n{content_text[:ZSXQ_OLLAMA_ERROR_FALLBACK_TRUNCATE_CHARS]}"
    except Exception as e:
        return f"[Qwen8B分析失败] {e}，原始搜索结果如下：\n\n{content_text[:ZSXQ_OLLAMA_ERROR_FALLBACK_TRUNCATE_CHARS]}"


@tool
def search_zsxq_by_stock(stock_name: str) -> str:
    """在知识星球中搜索指定股票的研报/小作文/新闻内容。

    仅当用户【明确、单只】地询问某只股票在"知识星球/小作文/圈子/社群"中的讨论时才调用此工具。
    严禁在以下场景调用本工具（违反将造成级联超时）：
      - 盘前新闻汇总 / 盘前策略 / 复盘预测 等批量多股分析任务
      - 用户没有明确提到"知识星球/小作文/圈子"却只是泛泛问"新闻/研报"的场景
      - 对一批结果股票（如"美光、海力士、谷歌、Meta..."）逐只调用本工具的级联动作
    这些场景应改用 task/网络搜索助手 完成新闻/研报获取。

    Args:
        stock_name: 要搜索的股票名称（如"贵州茅台"、"宁德时代"）

    Returns:
        知识星球搜索结果 + Qwen8B 分析汇总
    """
    try:
        monitor.report_tool("知识星球股票搜索", "start")
    except Exception:
        pass

    # 获取浏览器互斥锁：防止 fetch_zsxq_group_topics 同时运行
    global _zsxq_active_operation
    if not _zsxq_browser_lock.acquire(timeout=ZSXQ_BROWSER_LOCK_WAIT_TIMEOUT_SEC):
        # 锁被占用，说明另一个 zsxq 浏览器操作正在运行
        active = _zsxq_active_operation or "未知操作"
        msg = f"知识星球浏览器正忙（{active}进行中），请稍后"
        print(f"[ZSXQ-Search] 跳过：{msg}")
        return msg
    _zsxq_active_operation = "search"

    try:
        # 检查登录状态
        if _need_login():
            if ZSXQ_HEADLESS:
                return (
                    "未检测到知识星球登录状态。请先设置 ZSXQ_HEADLESS=false 并运行一次完成扫码登录。"
                )
            print("[ZSXQ-Search] 首次使用，需要扫码登录...")
            if not _login_interactive():
                return "知识星球登录失败，请重试。"

        print(f"[ZSXQ-Search] 开始搜索股票: {stock_name}")
        topics = _fetch_topics_by_search(stock_name, max_topics=ZSXQ_TOOL_SEARCH_STOCK_MAX_TOPICS)

        if not topics:
            return f"知识星球中未搜索到与「{stock_name}」相关的内容"

        # 提取关键信息
        topics_info = [_extract_topic_info(t) for t in topics]
        print(f"[ZSXQ-Search] 搜索到 {len(topics_info)} 条关于「{stock_name}」的内容")

        # 保存搜索结果到 JSON 文件（供后续连续问答使用）
        try:
            news_dir = Path(_PROJECT_ROOT) / "zsxq_news"
            news_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            search_result = {
                "stock_name": stock_name,
                "search_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total": len(topics_info),
                "topics": [
                    {
                        "author": info.get("author_name", ""),
                        "time": info.get("create_time", ""),
                        "title": info.get("title", ""),
                        "content": info.get("content", ""),
                        "topic_id": info.get("topic_id", ""),
                    }
                    for info in topics_info
                ],
            }
            result_file = news_dir / f"search_{stock_name}_{ts}.json"
            result_file.write_text(
                json.dumps(search_result, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"[ZSXQ-Search] 搜索结果已保存至: {result_file}")
        except Exception as e:
            print(f"[ZSXQ-Search] 保存搜索结果失败: {e}")

        # 打印预览
        for i, info in enumerate(topics_info, 1):
            _print_topic_preview(info, i)

        # 调用 Qwen3-8B 进行分析汇总
        print(f"\n[ZSXQ-Search] 调用 Qwen3-8B 分析汇总...")
        analysis = _analyze_with_qwen8b(stock_name, topics_info)

        # 组装最终返回内容（包含原始内容 + 分析汇总）
        raw_summary = "\n\n".join([
            f"--- 第{i}条 [{info.get('author_name', '')} {info.get('create_time', '')}] ---\n"
            f"{info.get('content', '')[:ZSXQ_RESULT_RAW_PREVIEW_TRUNCATE_CHARS]}"
            for i, info in enumerate(topics_info, 1)
        ])

        result = (
            f"【知识星球搜索结果 - {stock_name}】\n"
            f"搜索到 {len(topics_info)} 条相关内容\n\n"
            f"=== Qwen8B 分析汇总 ===\n{analysis}\n\n"
            f"=== 原始搜索内容 ===\n{raw_summary}"
        )

        try:
            monitor.report_tool("知识星球股票搜索", "end")
        except Exception:
            pass

        return result

    finally:
        # 释放浏览器互斥锁
        _zsxq_active_operation = None
        _zsxq_browser_lock.release()


# ======================== 主入口（调试用） ========================

if __name__ == "__main__":
    print("=" * 60)
    print("知识星球抓取工具（Playwright 浏览器自动化版）")
    print("=" * 60)
    print(f"群组ID: {ZSXQ_GROUP_ID}")
    print(f"Headless: {ZSXQ_HEADLESS}")
    print(f"State文件: {ZSXQ_STATE_FILE}")
    print(f"需要登录: {_need_login()}")
    print()

    # 直接调用工具函数（增量抓取最新 200 条）
    # 兼容 langchain StructuredTool（.invoke）和普通函数（直接调用）
    if hasattr(fetch_zsxq_group_topics, "invoke"):
        result = fetch_zsxq_group_topics.invoke({
            "max_topics": 100,
            "incremental": True,
            "save_to_db": False,
            "max_scrolls": 10,
        })
    else:
        result = fetch_zsxq_group_topics(
            max_topics=100,
            incremental=True,
            save_to_db=False,
            max_scrolls=10,
        )
    print(f"\n最终返回: {result}")
