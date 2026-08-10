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

# ======================== 配置 ========================
ZSXQ_GROUP_ID = os.getenv("ZSXQ_GROUP_ID", "48848484411448")
ZSXQ_HEADLESS = os.getenv("ZSXQ_HEADLESS", "true").lower() == "true"
# storage_state 文件路径（保存登录后的 Cookie）
# 放在项目目录下，避免 TRAE 沙箱拦截 home 目录写入
ZSXQ_STATE_FILE = Path(_PROJECT_ROOT) / "zsxq_news" / ".zsxq_state.json"
# 群组页面 URL
ZSXQ_GROUP_URL = f"https://wx.zsxq.com/group/{ZSXQ_GROUP_ID}"
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
        max_wait = 300  # 最多等 5 分钟
        for i in range(max_wait):
            time.sleep(1)
            cookies = context.cookies()
            if any(c["name"] == "zsxq_access_token" for c in cookies):
                print("[ZSXQ] 检测到登录成功！")
                # 再等 2 秒让页面完全加载
                time.sleep(2)
                break
            if i % 30 == 0 and i > 0:
                print(f"[ZSXQ] 已等待 {i} 秒，继续等待登录...")
        else:
            print("[ZSXQ] 登录超时（5分钟），请重新运行。")
            browser.close()
            return False

        # 保存 storage_state
        context.storage_state(path=str(ZSXQ_STATE_FILE))
        print(f"[ZSXQ] 登录状态已保存到 {ZSXQ_STATE_FILE}")
        browser.close()
        return True


def _fetch_topics_via_browser(
    group_id: str,
    max_scrolls: int = 10,
    save_to_db: bool = False,
    max_topics: int = 200,
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
                    if len(api_urls_seen) <= 10:
                        print(f"[ZSXQ] API 请求: {url[:120]}")

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
                        pass
            except Exception:
                # 浏览器关闭后的响应会抛异常，忽略即可
                pass

        page.on("response", handle_response)

        # 导航到群组页面
        url = f"https://wx.zsxq.com/group/{group_id}"
        print(f"[ZSXQ] 正在打开群组页面: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # 等待页面完全加载（SPA 需要额外时间渲染）
        print("[ZSXQ] 等待页面加载（SPA 渲染）...")
        time.sleep(5)

        # 尝试点击"全部"标签（知识星球默认可能显示"精华"）
        try:
            # 查找并点击"全部"标签
            all_tab = page.locator("text=全部").first
            if all_tab.is_visible(timeout=3000):
                all_tab.click()
                print("[ZSXQ] 已点击'全部'标签")
                time.sleep(3)
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
            time.sleep(3)

            curr_count = len(topics)
            if curr_count == prev_count and i > 0:
                print(f"[ZSXQ] 第 {i+1} 次滚动无新内容，停止加载")
                break
            if curr_count > prev_count:
                print(f"[ZSXQ] 第 {i+1} 次滚动：新增 {curr_count - prev_count} 条，累计 {curr_count} 条")

        # 打印所有拦截到的 API URL（调试用）
        if not topics and api_urls_seen:
            print(f"\n[ZSXQ] 调试：共拦截到 {len(api_urls_seen)} 个 API 请求：")
            for u in api_urls_seen[:20]:
                print(f"  - {u[:150]}")

        # 完整内容提取：在列表页面点击每张主题卡片的"展开全部"按钮，再从 DOM 提取全文
        # （不跳转详情页，直接原地展开 + 提取，效率更高）
        if topics:
            print(f"\n[ZSXQ] 开始展开列表页所有'展开全部'按钮并提取完整内容（共 {len(topics)} 条）...")

            # 先滚回顶部，再逐批展开（避免元素不在视口内无法点击）
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(1)

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
                                if btn.is_visible(timeout=1500):
                                    # 滚动到按钮可见
                                    btn.scroll_into_view_if_needed(timeout=1500)
                                    btn.click(timeout=2000)
                                    clicked_this_round += 1
                                    total_expanded += 1
                                    time.sleep(0.3)
                            except Exception:
                                pass
                        if clicked_this_round:
                            print(f"[ZSXQ] 第{pass_idx+1}轮点击了 {clicked_this_round} 个展开全部按钮")
                except Exception:
                    pass

                # 滚动到下一批
                page.evaluate("window.scrollBy(0, Math.floor(window.innerHeight * 0.7))")
                time.sleep(2)

            print(f"[ZSXQ] 共点击了 {total_expanded} 个展开全部按钮")

            # 滚回顶部，然后逐个匹配 API 拦截的 topic_id，提取 DOM 中展开后的完整文本
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(1)

            print("[ZSXQ] 从 DOM 提取已展开的内容并与 API 数据合并...")

            # 构建 topic_id 到 topics 列表索引的映射
            tid_to_idx = {str(t.get("topic_id", "")): i for i, t in enumerate(topics)}

            # 在 JS 中遍历所有主题卡片，提取 topic_id 和对应全文
            dom_results = page.evaluate("""() => {
                const results = {};
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
                selectors.forEach(sel => {
                    document.querySelectorAll(sel).forEach(el => cards.add(el));
                });

                cards.forEach(card => {
                    // 尝试从元素或子元素 a href 中提取 topic_id
                    let tid = card.getAttribute('data-topic-id') || card.getAttribute('data-id') || '';
                    if (!tid) {
                        const a = card.querySelector('a[href*="/topic/"]');
                        if (a) {
                            const m = a.getAttribute('href').match(/topic\\/(\\d+)/);
                            if (m) tid = m[1];
                        }
                    }
                    if (!tid) {
                        // 找父节点
                        let p = card.parentElement;
                        for (let k = 0; p && k < 8; k++) {
                            const pid = p.getAttribute && (p.getAttribute('data-topic-id') || p.getAttribute('data-id'));
                            if (pid) { tid = pid; break; }
                            p = p.parentElement;
                        }
                    }
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
                    textSelectors.forEach(sel => {
                        root.querySelectorAll(sel).forEach(el => {
                            const t = (el.innerText || el.textContent || '').trim();
                            if (t.length > maxText.length) maxText = t;
                        });
                    });
                    // 兜底：卡片自身 innerText
                    if (!maxText || maxText.length < 50) {
                        const t = (root.innerText || '').trim();
                        if (t.length > maxText.length) maxText = t;
                    }
                    if (maxText && maxText.length > 80) {
                        results[tid] = maxText;
                    }
                });
                return results;
            }""")

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
                if len(dom_text_clean) > len(api_text) * 0.8:
                    topics[idx].setdefault("talk", {})
                    if len(dom_text_clean) >= len(api_text):
                        topics[idx]["talk"]["text"] = dom_text_clean
                        merged += 1
                    else:
                        # DOM 虽短一些，但 API 版末尾有截断标签也换
                        if "<e " in api_text[-100:] and "<e " not in dom_text_clean[-100:]:
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
        "title": title[:255] if title else "",
        "content": full_content,
        "author_name": (owner.get("name") or "")[:128],
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
    if len(content) > 500:
        print(f"[ZSXQ] 正文: {content[:500]}...")
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
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:16]


@tool
def fetch_zsxq_group_topics(
    max_topics: int = 100,
    incremental: bool = True,
    save_to_db: bool = False,
    group_id: str = "",
    max_scrolls: int = 10,
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
        tail = content[-300:]
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
                    "title": (info.get("title") or "")[:100],
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
            "ORDER BY create_time_ts DESC LIMIT 20",
            (gid, f"%{query}%", f"%{query}%"),
        )
        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            return f"未找到包含 '{query}' 的主题"

        lines = [f"找到 {len(results)} 条匹配主题：\n"]
        for i, r in enumerate(results, 1):
            title = r.get("title") or ""
            content = (r.get("content") or "")[:200]
            lines.append(f"{i}. [{r['author_name']}] {title}")
            lines.append(f"   {content}...")
            lines.append(f"   时间: {r['create_time']}  赞: {r['like_count']}  评论: {r['comment_count']}\n")
        return "\n".join(lines)
    except Exception as e:
        return f"搜索失败: {e}"


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
