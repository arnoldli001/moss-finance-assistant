# coding = utf-8
"""
全局常量集中管理文件。

原问题：代码中散落大量硬编码的"魔鬼数字"（如 180、300、3、5、2000 等），
修改一处需全局 grep 多处，容易遗漏导致行为不一致。

解决方案：本文件按"功能模块"分组集中定义所有常量，每个常量附中文注释说明含义。
所有使用方统一 from config.constants import XXX 导入引用。

分组说明：
    1. TIMEOUTS         — 超时类（Agent 主流程、后台分析、HTTP 请求、WS 心跳等秒数）
    2. FETCH_LIMITS     — 查询条数/页数/主题数量（搜索结果数、抓取主题数、滚动次数等）
    3. MEMORY           — 记忆/滑窗/摘要压缩（最近 N 轮、压缩段数、单段上限字符）
    4. CONTEXT_ENGINEER — 上下文工程（上下文上限字符、去重条数、相似度阈值）
    5. CIRCUIT_BREAKER  — 熔断器（失败阈值、时间窗口、冷却时间、半开探测次数）
    6. SLO_TARGETS      — SLO 目标（可用性、P95、幻觉率、任务硬上限）
    7. DEGRADATION      — 降级链（每层超时、Token 硬上限）
    8. SCHEDULER        — 定时调度（轮询间隔、时区偏移、预设任务触发时间、查找天数上限）
    9. ZSXQ_BROWSER     — 知识星球 Playwright 抓取（等待秒数、登录超时、滚动等待等）
    10. TAVILY_SEARCH   — Tavily 搜索（最大重试次数、指数退避基底秒）
    11. OLLAMA          — Ollama 模型（单次请求超时、预检超时、温度参数）
    12. RATE_LIMIT      — 请求速率限制（每分钟最多请求数、窗口秒数）
    13. SECURITY        — 安全相关（默认值、文本长度阈值）
    14. OUTPUT_FORMAT   — 输出格式化（最大字符、小数位数）
    15. TEXT_SANITIZE   — 文本净化（最小长度、空行合并阈值）
"""
from __future__ import annotations

import os


# ======================================================================
# 1. TIMEOUTS — 超时类（单位：秒）
# ======================================================================

# Agent 主流程默认超时：用户聊天请求的单轮最长执行时间
DEFAULT_AGENT_TIMEOUT_SEC: float = 180.0

# 后台任务默认超时（知识星球抓取+分析、盘前自动化等更耗时的任务）
DEFAULT_BACKGROUND_TIMEOUT_SEC: float = 300.0

# HTTP 请求类短超时（Ollama 预检、ngrok 隧道 API 读取、探针等）
SHORT_HTTP_TIMEOUT_SEC: float = 2.0

# 中等长度 HTTP 请求超时（Ollama 模型列表探测等）
MEDIUM_HTTP_TIMEOUT_SEC: float = 180.0

# 子进程/外部命令等待超时（taskkill、ngrok 关闭等）
SUBPROCESS_WAIT_TIMEOUT_SEC: float = 180.0

# 调度器关闭阶段等待 scheduler 后台协程结束的超时
SCHEDULER_CANCEL_WAIT_SEC: float = 1.5

# 启动调度器后等待初始化日志打完的短等待
SCHEDULER_STARTUP_WAIT_SEC: float = 0.3

# ngrok 旧进程清理后等待
NGROK_CLEANUP_WAIT_SEC: float = 2.0

# ngrok 隧道启动时每轮等待本地 API 就绪的间隔
NGROK_TUNNEL_POLL_INTERVAL_SEC: float = 1.0

# ngrok 隧道启动最多尝试的轮数（每轮 1 秒 => 最多等待 10 秒）
NGROK_TUNNEL_MAX_POLL_ROUNDS: int = 10


# ======================================================================
# 2. FETCH_LIMITS — 查询条数/页数/主题数量
# ======================================================================

# Tavily 单次搜索默认返回的最大结果条数
TAVILY_DEFAULT_MAX_RESULTS: int = 5

# 知识星球群组抓取：默认最大滚动次数（触发更多主题加载）
ZSXQ_DEFAULT_MAX_SCROLLS: int = 10

# 知识星球群组抓取：单轮最多抓取的主题条数
ZSXQ_DEFAULT_MAX_TOPICS: int = 200

# 知识星球 API 请求调试时最多打印到控制台的 URL 数量
ZSXQ_DEBUG_API_URL_MAX_PRINT: int = 10

# 知识星球 DOM 提取：只保留长度超过该字符数的正文（避免空片段/短句混入）
ZSXQ_DOM_MIN_TEXT_LEN: int = 50

# 知识星球 DOM 提取：父节点向上最多查找多少层（防止找不到 topic_id 陷入死循环）
ZSXQ_DOM_PARENT_SEARCH_MAX_DEPTH: int = 8

# 知识星球正文预览：控制台打印的正文上限字符（超过则截断加省略号）
ZSXQ_PREVIEW_MAX_CONTENT_CHARS: int = 500

# 知识星球主题标题：入库前截断上限字符
ZSXQ_TITLE_MAX_CHARS: int = 255

# 知识星球作者名字段：入库前截断上限字符
ZSXQ_AUTHOR_NAME_MAX_CHARS: int = 128

# 幻觉防护报告：未验证数字/股票代码对外展示的最多条目数
HALLUCINATION_REPORT_MAX_UNVERIFIED_ITEMS: int = 10

# 幻觉防护报告：Schema 错误对外展示的最多条目数
HALLUCINATION_REPORT_MAX_SCHEMA_ERRORS: int = 5

# 幻觉防护报告：引用缺口对外展示的最多条目数
HALLUCINATION_REPORT_MAX_CITATION_GAPS: int = 3

# 幻觉防护报告：引用缺口 snippet 前后文截取字符数
HALLUCINATION_REPORT_SNIPPET_CHARS: int = 120

# PTD 路由器：tokenizer 无法估计时的保守估计 token 数（每 tool 估算值）
PTD_TOKEN_FALLBACK_PER_TOOL_ESTIMATE: int = 300


# ======================================================================
# 3. MEMORY — 记忆/滑窗/摘要压缩（MemoryManager 模块）
# ======================================================================

# 滑窗大小：最近 N 轮完整保留原始对话内容
MEMORY_WINDOW_KEEP_LAST_N: int = int(os.getenv("MEM_WINDOW_KEEP_LAST_N", "10"))

# 超过多少轮触发摘要压缩
MEMORY_SUMMARY_TRIGGER_TURNS: int = int(os.getenv("MEM_SUMMARY_TRIGGER_TURNS", "20"))

# 压缩后的摘要段数（早期 / 中期 / 近期）
MEMORY_SUMMARY_SEGMENTS: int = int(os.getenv("MEM_SUMMARY_SEGMENTS", "3"))

# 单段摘要最大字符数
MEMORY_SUMMARY_MAX_CHARS_PER_SEG: int = int(os.getenv("MEM_SUMMARY_MAX_CHARS_PER_SEG", "800"))

# 关键决策最大保留条数（防止无限膨胀）
MEMORY_KEY_DECISION_MAX_KEEP: int = int(os.getenv("MEM_KEY_DECISION_MAX_KEEP", "50"))

# 相关性阈值：低于此值的历史对话不放入上下文
MEMORY_RELEVANCE_THRESHOLD: float = float(os.getenv("MEM_RELEVANCE_THRESHOLD", "0.15"))

# 最多保留几条相关历史对话（相关度过滤后最多几条）
MEMORY_MAX_RELEVANT_TURNS: int = int(os.getenv("MEM_MAX_RELEVANT_TURNS", "6"))

# 整体记忆上下文输出的总字符硬上限（超过截断，防止 token 爆炸）
MEMORY_CONTEXT_TOTAL_MAX_CHARS: int = 12000

# 关键决策摘要：关键决策抽取的句子最多保留几条
MEMORY_KEY_DECISION_LINES_TOPK: int = 5

# 关键决策文本：抽取后单条决策最大字符（超过截断加省略号）
MEMORY_KEY_DECISION_TEXT_MAX_CHARS: int = 500

# 摘要构建：每段 Top 句子拼接最多保留几条
MEMORY_SUMMARY_SENTENCE_LIMIT_PER_SEG: int = 12

# 摘要构建句子过滤：最短字符（太短的句子视为无信息量跳过）
MEMORY_SUMMARY_MIN_SENTENCE_CHARS: int = 6

# 优先级评分：基础优先级分
MEMORY_PRIORITY_BASE: int = 50

# 优先级评分：每命中一次关键词的加分
MEMORY_PRIORITY_KEYWORD_HIT_BONUS_EACH: int = 8

# 优先级评分：关键词命中的最高额外加分上限（封顶）
MEMORY_PRIORITY_KEYWORD_HIT_BONUS_CAP: int = 40

# 优先级评分：命中闲聊正则的扣减
MEMORY_PRIORITY_SMALLTALK_PENALTY: int = 30

# 优先级评分：非常短的纯响应再扣分
MEMORY_PRIORITY_SHORT_TEXT_PENALTY: int = 10

# 优先级评分：超短纯响应判定阈值（字符数）
MEMORY_PRIORITY_SHORT_TEXT_THRESHOLD_CHARS: int = 8

# 关键决策额外加权：摘要排序时关键决策再额外加的分
MEMORY_SUMMARY_KEY_DECISION_SCORE_BONUS: int = 50

# 关键决策正则命中额外加分：摘要排序时单条句子额外加分
MEMORY_SUMMARY_KEYWORD_REGEX_HIT_BONUS: int = 20

# 相关度：股票代码精确匹配加权（如果双方有共同股票代码，相关度加多少）
MEMORY_RELEVANCE_CODE_MATCH_BONUS: float = 0.3

# 粗略 Token 估算：中文字符 × 系数
TOKEN_ESTIMATE_CHINESE_COEF: float = 1.5

# 粗略 Token 估算：其他字符 × 系数
TOKEN_ESTIMATE_OTHER_COEF: float = 0.25


# ======================================================================
# 4. CONTEXT_ENGINEER — 上下文工程（检索结果处理）
# ======================================================================

# 上下文工程输出：最多字符数阈值（超过按相关度裁剪低相关内容）
CONTEXT_MAX_CHARS: int = int(os.getenv("CONTEXT_MAX_CHARS", "2000"))

# 相似资讯去重：每组相似资讯保留最近的条目数（用于对比）
CONTEXT_DEDUP_KEEP_RECENT: int = 2

# 相似资讯去重：Jaccard 相似度阈值，超过则视为相似归为同组
CONTEXT_DEDUP_SIMILARITY_THRESHOLD: float = 0.4

# Context Engineer 专用常量（与 AGENTS.md 规范对齐）
# 上下文总字符硬上限（2000字精简裁剪阈值）
CE_TOTAL_MAX_CHARS: int = 2000

# 低相关内容丢弃阈值（字符数）
CE_IRRELEVANT_DROP_THRESHOLD_CHARS: int = 500

# 单条新闻片段截取字符上限
CE_NEWS_SNIPPET_MAX_CHARS: int = 500

# 单条条目内容字符上限（超过截断）
CE_ITEM_MAX_CHARS: int = 800

# 尾部保留的最近字符数（避免尾部关键信息被截断）
CE_TAIL_KEEP_LAST_CHARS: int = 400

# 时间衰减半衰期（天）：30天内线性衰减
CE_TIME_DECAY_HALF_LIFE_DAYS: float = 30.0

# ======================================================================
# 4.5 PROGRESSIVE TOOL DISCLOSURE — 渐进式工具披露路由器
# ======================================================================

# 阶段零工具选择菜单：每轮最多允许选择几个工具（防止模型全选回来）
PTD_MAX_TOOLS_PER_ROUND: int = 4


# ======================================================================
# 5. CIRCUIT_BREAKER — 熔断器（时间窗口三态）
# ======================================================================

# 熔断器失败阈值默认值：N 次失败即触发熔断
CB_DEFAULT_FAILURE_THRESHOLD: int = 3

# 熔断器默认失败统计时间窗口（秒）：在此窗口内统计失败次数
CB_DEFAULT_FAILURE_WINDOW_SEC: float = 60.0

# 熔断器默认冷却时间（秒）：OPEN 状态保持多久后进入 HALF_OPEN 探测
CB_DEFAULT_RECOVERY_COOLDOWN_SEC: float = 30.0

# 熔断器默认半开探测成功次数：HALF_OPEN 下连续 N 次成功才回到 CLOSED
CB_DEFAULT_HALF_OPEN_SUCCESS_NEEDED: int = 2

# ------------------ 具体被保护对象的默认值 ------------------

# DeepSeek 主模型：60 秒 3 次熔断，冷却 30 秒
CB_DEEPSEEK_FAILURE_THRESHOLD: int = 3
CB_DEEPSEEK_FAILURE_WINDOW_SEC: float = 60.0
CB_DEEPSEEK_RECOVERY_COOLDOWN_SEC: float = 30.0

# IMA 知识库：同 DeepSeek
CB_IMA_FAILURE_THRESHOLD: int = 3
CB_IMA_FAILURE_WINDOW_SEC: float = 60.0
CB_IMA_RECOVERY_COOLDOWN_SEC: float = 30.0

# Qwen8B 本地模型：错误阈值稍宽（本地资源不足更常见），冷却更短便于恢复
CB_QWEN8B_FAILURE_THRESHOLD: int = 5
CB_QWEN8B_FAILURE_WINDOW_SEC: float = 60.0
CB_QWEN8B_RECOVERY_COOLDOWN_SEC: float = 15.0

# 知识星球抓取：失败窗口更长（调用少，失败集中），冷却更长以避免反复封禁
CB_ZXSQ_FAILURE_THRESHOLD: int = 3
CB_ZXSQ_FAILURE_WINDOW_SEC: float = 120.0
CB_ZXSQ_RECOVERY_COOLDOWN_SEC: float = 60.0

# 主 Agent 整体：主流程失败窗口设为更长的 300 秒
CB_MAIN_AGENT_FAILURE_THRESHOLD: int = 3
CB_MAIN_AGENT_FAILURE_WINDOW_SEC: float = 300.0
CB_MAIN_AGENT_RECOVERY_COOLDOWN_SEC: float = 60.0


# ======================================================================
# 6. SLO_TARGETS — SLO 可靠性目标定义
# ======================================================================

# 可用性目标：成功请求 / 总请求 ≥ 该值
SLO_AVAILABILITY_TARGET: float = 0.99

# P95 延迟目标（秒）：95% 请求应在该时间内完成
SLO_LATENCY_P95_SEC: float = 30.0

# 幻觉防护通过率目标（幻觉率 ≤ 5% 等价）
SLO_HALLUCINATION_PASS_RATE: float = 0.95

# 单任务硬上限：执行时间不得超过该秒数（与 AGENTS.md 规范一致）
SLO_MAX_TASK_SEC: float = 150.0

# 单任务 Token 硬上限：单任务消费不得超过该 Tokens
SLO_MAX_TOKENS: int = 1_000_000

# 错误预算滚动窗口（秒）：默认 30 天
SLO_ERROR_BUDGET_WINDOW_SEC: int = 30 * 24 * 3600

# SLO Monitor 内存滑动窗口最多保留事件数（超过丢弃最旧）
SLO_MONITOR_MEMORY_WINDOW_SIZE: int = 10000


# ======================================================================
# 7. DEGRADATION — 四级降级链硬上限
# ======================================================================

# 单任务执行时间上限（秒）：降级链中每层的默认超时
DEGRADE_MAX_TASK_SECONDS: float = 240.0

# 单任务 Token 上限：超过立即终止并降级
DEGRADE_MAX_TASK_TOKENS: int = 1_000_000


# ======================================================================
# 8. SCHEDULER — 定时任务调度
# ======================================================================

# 是否启用调度器（可通过 .env SCHEDULER_ENABLED=0 关闭）
SCHEDULER_ENABLED: bool = os.getenv("SCHEDULER_ENABLED", "1").strip() not in ("0", "false")

# 中国时区 UTC+8 偏移小时（可通过 .env SCHEDULER_TZ_OFFSET 调整）
SCHEDULER_TIMEZONE_OFFSET_HOURS: int = int(os.getenv("SCHEDULER_TZ_OFFSET", "8"))

# 调度器轮询间隔（秒）：每 N 秒检查一次当前时间是否命中预设任务
SCHEDULER_POLL_INTERVAL_SEC: int = 30

# 计算下次触发时间时最多向前找多少天（防止 weekday_only 配置错误导致死循环）
SCHEDULER_NEXT_RUN_MAX_LOOKAHEAD_DAYS: int = 14

# ------------------ 预设任务：盘前自动化触发时间 ------------------

# 盘前小作文热度触发：小时 & 分钟（工作日 9:13）
SCHEDULER_PRESET_HEAT_HOUR: int = 9
SCHEDULER_PRESET_HEAT_MINUTE: int = 13

# 盘前新闻触发：小时 & 分钟（工作日 9:15）
SCHEDULER_PRESET_NEWS_HOUR: int = 9
SCHEDULER_PRESET_NEWS_MINUTE: int = 15


# ======================================================================
# 9. ZSXQ_BROWSER — 知识星球 Playwright 抓取相关时间/间隔
# ======================================================================

# 扫码登录等待的最大秒数（5 分钟）
ZSXQ_LOGIN_MAX_WAIT_SEC: int = 300

# 登录成功后额外等待多少秒让页面完全加载
ZSXQ_LOGIN_SUCCESS_WAIT_SEC: int = 2

# 扫码登录进度：每多少秒打印一次"已等待 N 秒"提示
ZSXQ_LOGIN_PROGRESS_PRINT_INTERVAL_SEC: int = 30

# 打开群组页面的加载超时（毫秒，传 Playwright goto）
ZSXQ_PAGE_GOTO_TIMEOUT_MS: float = 60000

# 打开群组页面后等待 SPA 渲染的 sleep 秒数
ZSXQ_INITIAL_SPA_RENDER_WAIT_SEC: int = 5

# 尝试点击"全部"标签的等待可见超时（毫秒）
ZSXQ_ALL_TAB_VISIBLE_TIMEOUT_MS: float = 3000

# 点击"全部"标签后额外等待内容切换加载
ZSXQ_ALL_TAB_SWITCH_WAIT_SEC: int = 3

# 每次滚动加载后的等待秒数（SPA 需时间从 API 拉新主题）
ZSXQ_SCROLL_WAIT_AFTER_SEC: int = 3

# 每轮点击"展开全部"之间的间隔秒（反检测 + 等待动画结束）
ZSXQ_EXPAND_CLICK_INTERVAL_SEC: float = 0.3

# 单个"展开全部"按钮可见等待超时（毫秒）
ZSXQ_EXPAND_BTN_VISIBLE_TIMEOUT_MS: float = 1500

# 单个"展开全部"按钮点击超时（毫秒）
ZSXQ_EXPAND_BTN_CLICK_TIMEOUT_MS: float = 2000

# 所有"展开全部"按钮点完后，滚到下一批的等待秒数
ZSXQ_EXPAND_PASS_SCROLL_WAIT_SEC: int = 2

# DOM 提取前，滚回顶部后的等待秒数
ZSXQ_DOM_SCROLL_TO_TOP_WAIT_SEC: int = 1

# DOM 合并：DOM 版长度至少达到 API 版长度的多少比例才替换（避免用被截断的更短 DOM）
ZSXQ_DOM_MERGE_MIN_RATIO: float = 0.8

# DOM 合并：从末尾截取多少字符来判断是否有截断标签
ZSXQ_DOM_MERGE_TAIL_CHECK_LEN: int = 100

# —— 按股票名搜索（search_zsxq_by_stock / _fetch_topics_by_search）专用常量 ——

# 按股票名搜索默认最多取多少条主题结果
ZSXQ_SEARCH_DEFAULT_MAX_TOPICS: int = 2

# 搜索框定位：每个候选选择器的元素可见等待超时（毫秒）
ZSXQ_SEARCH_BOX_VISIBLE_TIMEOUT_MS: float = 2000

# 搜索 URL 直达跳转后 SPA 渲染等待秒数
ZSXQ_SEARCH_URL_GOTO_RENDER_WAIT_SEC: int = 5

# 搜索框输入后按 Enter 前的等待秒数（让输入法把字真正写进输入框）
ZSXQ_SEARCH_INPUT_AFTER_FILL_WAIT_SEC: float = 0.5

# 搜索按 Enter 后的加载等待秒数
ZSXQ_SEARCH_AFTER_ENTER_WAIT_SEC: int = 3

# "当前星球"/"最新" 排序按钮可见等待超时（毫秒）
ZSXQ_SEARCH_FILTER_BTN_VISIBLE_TIMEOUT_MS: float = 3000

# 点击"当前星球"/"最新"后的等待秒数
ZSXQ_SEARCH_FILTER_BTN_AFTER_CLICK_WAIT_SEC: int = 3

# 等待搜索结果组件出现的超时（毫秒）
ZSXQ_SEARCH_RESULTS_SELECTOR_TIMEOUT_MS: float = 10000

# 搜索结果备选选择器等待超时（毫秒）
ZSXQ_SEARCH_RESULTS_FALLBACK_TIMEOUT_MS: float = 5000

# 滚动加载更多主题后的等待秒数
ZSXQ_SEARCH_SCROLL_MORE_WAIT_SEC: int = 2

# 搜索结果主题卡片 scroll_into_view 超时（毫秒）
ZSXQ_SEARCH_CARD_SCROLL_INTO_VIEW_TIMEOUT_MS: float = 3000

# 搜索结果卡片滚动进入视口后的额外等待秒数
ZSXQ_SEARCH_CARD_AFTER_SCROLL_WAIT_SEC: float = 0.3

# 点击搜索结果卡片的超时（毫秒）
ZSXQ_SEARCH_CARD_CLICK_TIMEOUT_MS: float = 5000

# 搜索结果卡片内部 fallback 点击超时（毫秒）
ZSXQ_SEARCH_CARD_INNER_CLICK_TIMEOUT_MS: float = 3000

# 进入主题详情视图后的等待渲染秒数
ZSXQ_SEARCH_DETAIL_AFTER_OPEN_WAIT_SEC: int = 2

# 详情正文选择器：元素可见等待超时（毫秒）
ZSXQ_SEARCH_DETAIL_TEXT_VISIBLE_TIMEOUT_MS: float = 2000

# 详情正文选择器：inner_text 提取超时（毫秒）
ZSXQ_SEARCH_DETAIL_TEXT_INNER_TIMEOUT_MS: float = 5000

# 判定正文"有效"的最少字符数（避免拿标题占位当正文）
ZSXQ_SEARCH_DETAIL_TEXT_MIN_LEN: int = 30

# 详情作者/时间选择器：元素可见等待超时（毫秒）
ZSXQ_SEARCH_DETAIL_AUTHOR_VISIBLE_TIMEOUT_MS: float = 1000

# 详情作者/时间：inner_text 提取超时（毫秒）
ZSXQ_SEARCH_DETAIL_AUTHOR_INNER_TIMEOUT_MS: float = 2000

# 判定组装后的详情结果"有效"的最少字符数（过滤空壳主题）
ZSXQ_SEARCH_TOPIC_VALID_MIN_LEN: int = 10

# 退出详情视图：Escape 后等待详情关闭 / 遮罩点击后等待 / 视口点击后等待
ZSXQ_SEARCH_DETAIL_CLOSE_AFTER_WAIT_SEC: int = 1

# 退出详情最终兜底：Escape 后等待列表恢复
ZSXQ_SEARCH_DETAIL_CLOSE_FINAL_WAIT_SEC: int = 1

# 遮罩点击左上角偏移像素（避免点到弹窗内容区）
ZSXQ_SEARCH_DETAIL_OVERLAY_CLICK_OFFSET_PX: int = 5

# 视口边缘点击 X 坐标（左边缘）
ZSXQ_SEARCH_DETAIL_VIEWPORT_EDGE_CLICK_X_PX: int = 2

# —— Ollama 调用 Qwen3-8B 分析汇总专用 ——

# 单条知识星球结果截断字符数（避免拼出超长 prompt 把模型卡死）
ZSXQ_OLLAMA_ENTRY_TRUNCATE_CHARS: int = 500

# 知识星球搜索结果前置内容兜底截断长度（失败时仍能给用户看原始结果）
ZSXQ_OLLAMA_ERROR_FALLBACK_TRUNCATE_CHARS: int = 2000

# 浏览器互斥锁 acquire 最大等待秒数（超时则返回"浏览器正忙"，避免永久阻塞）
ZSXQ_BROWSER_LOCK_WAIT_TIMEOUT_SEC: int = 5

# _print_topic_preview 中正文预览截断长度（Windows gbk 控制台避免刷爆）
ZSXQ_PREVIEW_CONTENT_TRUNCATE_CHARS: int = 500

# 搜索结果数据库查询最大返回条数
ZSXQ_DB_SEARCH_MAX_LIMIT: int = 20

# 数据库搜索结果中每条内容预览截断字符数
ZSXQ_DB_SEARCH_PREVIEW_TRUNCATE_CHARS: int = 200

# 历史记录中标题存储截断字符数
ZSXQ_HISTORY_TITLE_TRUNCATE_CHARS: int = 100

# 内容 hash 截取前 N 位（MD5 足够去重）
ZSXQ_CONTENT_HASH_HEAD_CHARS: int = 16

# 内容合并截断尾段检查长度（判断未完整展开）
ZSXQ_TRUNCATE_MARKER_TAIL_CHECK_LEN: int = 300

# API URL 调试打印：最多打印多少条防止刷屏
ZSXQ_DEBUG_API_URLS_PRINT_HEAD_COUNT: int = 10

# API URL 调试打印：URL 前多少字符截断
ZSXQ_DEBUG_API_URLS_PRINT_TRUNCATE_CHARS: int = 120

# 未抓到主题时打印的调试 URL：最多多少条
ZSXQ_DEBUG_EMPTY_DUMP_MAX_URLS: int = 20

# 未抓到主题时打印的调试 URL：单条最多多少字符
ZSXQ_DEBUG_EMPTY_DUMP_URL_TRUNCATE_CHARS: int = 150

# JS DOM 提取时 topic_id 父节点最多向上查找多少层（防无限）
ZSXQ_DOM_TOPIC_ID_PARENT_DEPTH_MAX: int = 8

# JS DOM 提取兜底：卡片自身文本最小长度阈值（低于则丢弃）
ZSXQ_DOM_CARD_FALLBACK_TEXT_MIN_LEN: int = 50

# JS DOM 提取最终保存：正文至少多少字符才作为候选写入结果
ZSXQ_DOM_RESULT_MIN_LEN: int = 80

# _extract_topic_info 中标题字段数据库截断长度（VARCHAR 255）
ZSXQ_EXTRACT_TITLE_TRUNCATE_CHARS: int = 255

# _extract_topic_info 中作者名字段数据库截断长度（VARCHAR 128）
ZSXQ_EXTRACT_AUTHOR_NAME_TRUNCATE_CHARS: int = 128

# fetch_zsxq_group_topics 工具默认 max_topics 值
ZSXQ_TOOL_FETCH_MAX_TOPICS_DEFAULT: int = 100

# search_zsxq_by_stock 工具默认 max_topics 值
ZSXQ_TOOL_SEARCH_STOCK_MAX_TOPICS: int = 5

# search_zsxq_by_stock 最终拼接内容预览截断字符数
ZSXQ_RESULT_RAW_PREVIEW_TRUNCATE_CHARS: int = 300

# _fetch_topics_via_browser 默认抓取主题数上限（与 ZSXQ_DEFAULT_MAX_TOPICS 保持一致，用于明确语义）
ZSXQ_DEFAULT_FETCH_MAX_TOPICS: int = 200


# ======================================================================
# 10. TAVILY_SEARCH — Tavily 搜索
# ======================================================================

# Tavily 最大重试次数（连接重置等瞬时错误的重试）
TAVILY_MAX_RETRIES: int = 3

# 指数退避基础秒数：第 N 次重试等待 = TAVILY_BACKOFF_BASE ^ N
TAVILY_BACKOFF_BASE: int = 2


# ======================================================================
# 11. OLLAMA — Ollama 本地模型调用
# ======================================================================

# Ollama 默认服务 Base URL
OLLAMA_DEFAULT_BASE_URL: str = "http://localhost:11434"

# Ollama 单次请求超时（秒）：分析类请求可能较长
OLLAMA_CHAT_DEFAULT_TIMEOUT_SEC: int = 300

# Ollama 默认 temperature（越低越确定，0.2 为金融分析常用取值）
OLLAMA_DEFAULT_TEMPERATURE: float = 0.2

# Ollama 预检（/api/tags 探活）的超时秒数
OLLAMA_PROBE_TIMEOUT_SEC: int = 5

# Ollama 启动等待探针：每轮重试间隔（秒）
OLLAMA_LAUNCH_POLL_INTERVAL_SEC: float = 1.0

# Ollama 启动等待探针：最多轮数（默认最多 20 轮 = 最多 20 秒）
OLLAMA_LAUNCH_POLL_MAX_ROUNDS: int = 20

# Ollama 模型拉取子进程最长等待秒数
OLLAMA_PULL_TIMEOUT_SEC: int = 600

# Ollama `ollama list` CLI 调用超时（秒）
OLLAMA_MODELS_LIST_TIMEOUT_SEC: float = 15.0

# Ollama pull 日志尾部保留行数（用于进度心跳推送 + 失败诊断）
OLLAMA_PULL_LOG_TAIL_KEEP: int = 10

# Ollama pull 前端心跳进度推送间隔（秒）
OLLAMA_PULL_PROGRESS_INTERVAL_SEC: float = 20.0

# Ollama pull 单行日志截断上限字符（超过加省略号避免前端气泡过宽）
OLLAMA_PULL_PROGRESS_LINE_MAX_CHARS: int = 120

# Ollama pull 硬上限超时（秒）：超过即 kill 子进程，避免永久卡死
OLLAMA_PULL_HARD_TIMEOUT_SEC: int = 3600


# ======================================================================
# 12. RATE_LIMIT — 请求速率限制
# ======================================================================

# 每个 IP 每分钟最多请求数（WebSocket 和静态资源不计入）
RATE_LIMIT_PER_MINUTE: int = 60

# 速率窗口秒数：当前计录窗口长度
RATE_LIMIT_WINDOW_SEC: int = 60


# ======================================================================
# 13. SECURITY — 安全/通用默认值
# ======================================================================

# 默认服务端口（ngrok / 日志提示对齐）
DEFAULT_SERVER_PORT: int = 8000

# 默认 ngrok 本地 API 端口
NGROK_LOCAL_API_PORT: int = 4040

# 单条未验证数字展示：最多几条（幻觉防护警示对外展示）
UNVERIFIED_NUMBER_DISPLAY_MAX: int = 5

# 单条未验证股票代码展示：最多几条
UNVERIFIED_STOCK_CODE_DISPLAY_MAX: int = 5

# LLM-as-Judge 对外问题：最多几条
LLM_JUDGE_ISSUE_DISPLAY_MAX: int = 2

# Schema 错误对外展示：最多几条
SCHEMA_ERROR_DISPLAY_MAX: int = 2


# ======================================================================
# 14. OUTPUT_FORMAT — 输出格式化
# ======================================================================

# 非静默模式下控制台日志截断长度（避免 5000 字刷终端）
CONSOLE_RESULT_TRUNCATE_MAX_LEN: int = 500

# 报告/告警中百分比显示保留位数（小数点后）
METRIC_PERCENTAGE_DECIMALS: int = 2

# 报告/告警中小数（延迟等）显示保留位数
METRIC_FLOAT_DECIMALS: int = 3

# 可用性/通过率 显示保留位数（小数点后 4 位）
METRIC_AVAILABILITY_DECIMALS: int = 4

# 文件大小格式化：KB/MB 保留的小数位数
FILE_SIZE_DISPLAY_DECIMALS: int = 1


# ======================================================================
# 15. TEXT_SANITIZE — 前端输出文本净化
# ======================================================================

# 判定"极短文本可能是实质空"的长度阈值（小于该长度再用白名单过滤掉）
SANITIZE_SHORT_TEXT_THRESHOLD_LEN: int = 10

# 文本净化：短文本阈值（monitor.py / server.py 共用，与 SANITIZE_SHORT_TEXT_THRESHOLD_LEN 语义相同）
TEXT_SANITIZE_SHORT_TEXT_THRESHOLD_LEN: int = 10

# 连续空行合并阈值：N 个以上连续空行压缩为 2 个（即两段间距为一个空行）
SANITIZE_BLANK_LINE_MERGE_THRESHOLD: int = 3


# ======================================================================
# 16. MONITOR — 监控/心跳相关
# ======================================================================

# WebSocket 心跳间隔（毫秒）：前端定时 ping 后端保活
WS_HEARTBEAT_INTERVAL_MS: int = 30000

# WS 断线重连基础延迟（毫秒），后续每次翻倍直到上限
WS_RECONNECT_BASE_DELAY_MS: int = 1000

# WS 断线重连最大延迟（毫秒）
WS_RECONNECT_MAX_DELAY_MS: int = 10000

# WS 重连恢复成功后，"连接已恢复"提示展示多少毫秒后消失
WS_RECONNECT_RECOVERY_HINT_MS: int = 2000

# 思考气泡计时：每多少毫秒更新一次"思考中 N 秒"
THINKING_TIMER_TICK_MS: int = 1000

# 监控进度条：每多少毫秒更新一次显示
PROGRESS_UPDATE_INTERVAL_MS: int = 5000

# 删除操作二次确认窗口期（毫秒）：3 秒内点第二次才真正删除
DELETE_CONFIRM_WINDOW_MS: int = 3000

# Toast 通知：默认展示时长（毫秒）
TOAST_DEFAULT_DURATION_MS: int = 3000

# 复制按钮高亮展示"已复制"态多少毫秒后恢复
COPY_BTN_HIGHLIGHT_MS: int = 1500

# 长按触发菜单延迟（毫秒）
LONG_PRESS_TRIGGER_MS: int = 500

# 长按按压反馈：按压效果多少毫秒后撤消
LONG_PRESS_PRESSING_RELEASE_MS: int = 200

# 点击快捷按钮后等待多少毫秒再刷新（给后端启动任务留时间）
SHORTCUT_BUTTON_POST_CLICK_WAIT_MS: int = 200

# 消息空行合并阈值（与后端 SANITIZE 对应）
MSG_BLANK_LINE_MERGE_THRESHOLD: int = 3

# 设备像素比上限（防止超高分屏下 canvas 过大）
DEVICE_PIXEL_RATIO_UPPER_LIMIT: int = 2

# 对话气泡最大宽度占聊天区宽度的比例（0.8 即 80%）
CHAT_BUBBLE_MAX_WIDTH_RATIO: float = 0.8

# 消息之间的纵向间距像素（canvas 绘制用）
CHAT_MESSAGE_GAP_PX: int = 16

# 头像尺寸 + 气泡额外纵向空间（canvas 绘制行距）
CHAT_AVATAR_ROW_EXTRA_H_PX: int = 8

# 追踪查询接口：默认 limit 条数
TRACE_RECENT_DEFAULT_LIMIT: int = 10


# ======================================================================
# 17. ACTOR / SCHEDULER / LLM — Actor 模型/调度器/LLM 调用 默认值
# ======================================================================

# Actor 邮箱默认最大容量：超过后新消息直接拒绝，防止 Actor 积压导致 OOM
ACTOR_MAILBOX_LIMIT_DEFAULT: int = 10000

# Actor 调用 ask() 默认请求响应超时（秒）：普通请求 30s 内完成，更长用后台任务
ACTOR_ASK_DEFAULT_TIMEOUT_SEC: float = 30.0

# RequestContext 示例 timeout（docstring 示例）
REQUEST_CONTEXT_DEFAULT_TIMEOUT_SEC: float = 120.0

# LLM 调用默认超时（秒）：普通单次对话不超过 1 分钟
LLM_CHAT_DEFAULT_TIMEOUT_SEC: int = 60

# LangGraph 主智能体递归深度上限：防止循环 PTD/降级链无限递归
MAIN_AGENT_RECURSION_LIMIT: int = 50

# 主 Agent verbose 日志：content 超长截断上限（字符），用于避免刷爆终端
MAIN_AGENT_VERBOSE_MAX_LEN: int = 500

# 主 Agent 工具结果 verbose 日志：单条工具结果截断上限
MAIN_AGENT_VERBOSE_TOOL_RESULT_MAX_LEN: int = 200

# 主 Agent 会话历史查询：默认 limit 条数
MAIN_AGENT_SESSION_HISTORY_LIMIT_DEFAULT: int = 40

# 主 Agent 记忆 context 超长告警阈值（字符）：超过时打印 warning 裁剪日志
MAIN_AGENT_MEMORY_CONTEXT_WARN_LEN: int = 2000

# Context Engineer 打包时 overhead 预留字符（防止加 headers 等就超过上限）
CE_OVERHEAD_RESERVE_CHARS: int = 100

# HTTP 状态码：未授权（用于 error_classifier 示例/分支）
HTTP_CODE_UNAUTHORIZED: int = 401

# HTTP 状态码：请求过多（速率限制 429）
HTTP_CODE_TOO_MANY_REQUESTS: int = 429

# HTTP 状态码：资源未找到（404）
HTTP_CODE_NOT_FOUND: int = 404

# Server 输出过滤：最终返回独立行最小字符长度（低于则视为内部调试噪声跳过）
SERVER_FINAL_RETURN_LINE_MIN_LEN: int = 200

# Server 输出过滤：JSON 调试行最小字符长度（超过该长度且以 {/[ 开头视为大段 JSON 转储跳过）
SERVER_JSON_DEBUG_LINE_MIN_LEN: int = 300

# Server 进度推送：safe_text 安全截断长度（字符），超过加省略号
SERVER_PROGRESS_SAFE_TRUNCATE_LEN: int = 150

# Server 失败诊断：stdout 尾部保留最多行数（用于错误上下文打印）
SERVER_OUTPUT_MAX_STDOUT_TAIL_LINES: int = 20

# 调度器：启动后查近期任务时，最多向后推多少天（找最近 2 周）
SCHEDULER_LOOK_AHEAD_DAYS_MAX: int = 14

# 调度器：盘前任务默认触发"分"（盘前新闻/盘前策略默认早上 8:13，避开整点拥堵）
SCHEDULER_PRE_MARKET_DEFAULT_MINUTE: int = 13

# 调度器：盘后复盘预测任务默认触发"分"（下午 15:15，收盘后 15 分钟）
SCHEDULER_AFTER_MARKET_DEFAULT_MINUTE: int = 15


# ======================================================================
# 18. TEST_ZSXQ — 知识星球分析 Runner 专用常量（tools/zsxq_analysis_runner.py 调用）
# ======================================================================

# zsxq_analysis_runner.py 调用 Ollama 分析：单条内容截断字符（防 prompt 过长）
TEST_ZXSQ_OLLAMA_ENTRY_TRUNCATE_CHARS: int = 300

# zsxq_analysis_runner.py 调用 Ollama 分析：请求超时（秒）
TEST_ZXSQ_OLLAMA_TIMEOUT_SEC: int = 300

# zsxq_analysis_runner.py 调用 Ollama 分析：temperature
TEST_ZXSQ_OLLAMA_TEMPERATURE: float = 0.2

# zsxq_analysis_runner.py 调用 zsxq-cli 子进程：超时（秒）
TEST_ZXSQ_CLI_TIMEOUT_SEC: int = 600

# 控制台预览：单条 value 预览截断字符（避免刷终端）
TEST_ZXSQ_PREVIEW_VALUE_TRUNCATE_CHARS: int = 300

# Ollama 分析前：把拼好的 content_text 长度超过该值时压缩（防止超 GPU 显存）
TEST_ZXSQ_OLLAMA_CONTENT_COMPRESS_THRESHOLD: int = 15000

# 调试行判定阈值：含 JSON dump 的单行长度超过该字符且以 { / [ 开头视为调试行
TEST_ZXSQ_DEBUG_LINE_JSON_LEN: int = 200

# 调试行判定阈值：普通长调试行超过该字符也过滤
TEST_ZXSQ_DEBUG_LINE_LONG_LEN: int = 300

# 最终摘要结果预览：超过该字符截断展示给进度条提示
TEST_ZXSQ_FINAL_SUMMARY_PREVIEW_TRUNCATE: int = 200

# 幻觉防护：未验证数字最多展示几条
TEST_ZXSQ_UNVERIFIED_NUMS_MAX_DISPLAY: int = 5


# ======================================================================
# 19. OBSERVABILITY — OpenTelemetry 分布式追踪接入参数
# ======================================================================

# OTel 服务名（出现在 Jaeger / Tempo 的 service 维度）
OTEL_SERVICE_NAME: str = "moss-finance-agent"
# OTel 服务版本（从环境变量 MOSS_VERSION 读，默认 1.0.0）
OTEL_SERVICE_VERSION: str = "1.0.0"
# OTel 导出协议：console / otlp / none（none = 注册但不上报，仅本地 span）
OTEL_EXPORTER_TYPE: str = "console"
# OTLP 导出端点（如 http://localhost:4317）
OTEL_OTLP_ENDPOINT: str = "http://localhost:4317"
# 采样率：1.0 = 全采样，0.1 = 10% 采样（生产环境建议 0.05~0.1）
OTEL_TRACE_SAMPLE_RATIO: float = 1.0
# LLM 调用 span 默认属性前缀
OTEL_LLM_SPAN_NAME_PREFIX: str = "llm.chat"
# 工具调用 span 默认属性前缀
OTEL_TOOL_SPAN_NAME_PREFIX: str = "tool.call"
# Agent 主流程 span 名
OTEL_AGENT_SPAN_NAME: str = "agent.run"


# ======================================================================
# 20. EVAL — LLM 输出质量评估（结构化回归测试）
# ======================================================================

# 评估集默认路径
EVAL_GOLDEN_SET_PATH: str = "tests/eval/golden_set.json"
# 评估结果输出目录
EVAL_RESULTS_DIR: str = "output/eval"
# LLM-as-judge 评估模型（用 DeepSeek 主模型当裁判）
EVAL_JUDGE_MODEL: str = "deepseek-chat"
# 评估 judge 调用超时（秒）
EVAL_JUDGE_TIMEOUT_SEC: int = 60
# 评估 judge temperature（裁判应低温稳定）
EVAL_JUDGE_TEMPERATURE: float = 0.0
# 评估阈值：低于此分判为不通过
EVAL_PASS_SCORE_THRESHOLD: float = 0.7
# 评估阈值：幻觉率超过此值 CI 阻断
EVAL_HALLUCINATION_RATE_BLOCK_THRESHOLD: float = 0.05
# 评估并发数（同时跑几个样本）
EVAL_CONCURRENCY: int = 4
# 单样本最大重试次数（被评估模型生成失败时）
EVAL_SAMPLE_MAX_RETRIES: int = 2
# http 模式：POST /api/task 启动任务后的整体等待超时（秒），与 Agent 默认超时一致
EVAL_HTTP_TASK_TIMEOUT_SEC: int = 180
# http 模式：WebSocket 建连超时（秒）
EVAL_WS_CONNECT_TIMEOUT_SEC: int = 10
# http 模式：POST 启动任务的 API 路径
EVAL_AGENT_TASK_PATH: str = "/api/task"
# http 模式：监听 Agent 输出推送的 WS 路径前缀（最终拼接为 /ws/{thread_id}）
EVAL_AGENT_WS_PATH_PREFIX: str = "/ws/"


# ======================================================================
# 21. ACTOR_PERSISTENCE — Actor 状态持久化快照
# ======================================================================

# 快照存储后端：file / redis / memory
ACTOR_SNAPSHOT_BACKEND: str = "file"
# 文件后端存储目录
ACTOR_SNAPSHOT_FILE_DIR: str = "data/actor_snapshots"
# 快照触发：每处理多少条消息做一次增量快照
ACTOR_SNAPSHOT_INTERVAL_MSGS: int = 100
# 强制全量快照间隔（防止增量日志无限增长）
ACTOR_SNAPSHOT_FULL_INTERVAL_MSGS: int = 1000
# 快照保留多少份历史版本（FIFO 淘汰）
ACTOR_SNAPSHOT_KEEP_VERSIONS: int = 5
# 启动时是否自动恢复最近快照
ACTOR_SNAPSHOT_AUTO_RESTORE: bool = True
# Redis 后端连接 URL
ACTOR_SNAPSHOT_REDIS_URL: str = "redis://localhost:6379/0"
# Redis key 前缀
ACTOR_SNAPSHOT_REDIS_PREFIX: str = "actor:snap:"


# ======================================================================
# 22. SECURITY_MW — RBAC 权限中间件 + prompt 注入防护
# ======================================================================

# RBAC 角色 → 权限映射文件路径
RBAC_POLICY_FILE: str = "config/rbac_policy.json"
# 默认角色（未认证用户）
RBAC_DEFAULT_ROLE: str = "guest"
# 数据行级权限：每用户可见的最大行数（散户隔离）
RBAC_ROW_LEVEL_MAX_ROWS: int = 100
# prompt 注入防护：危险关键词列表（用户输入包含则告警）
PROMPT_INJECTION_DANGEROUS_KEYWORDS: tuple = (
    "ignore previous instructions",
    "忽略上述指令",
    "忽略以上指令",
    "system:",
    "<|im_start|>",
    "<|endoftext|>",
    "your new role is",
    "你现在的角色是",
)
# prompt 注入防护：单次输入最大长度（防止 prompt bomb）
PROMPT_INJECTION_MAX_LEN: int = 8000
# prompt 注入防护：是否拒绝（True）还是仅告警（False）
PROMPT_INJECTION_REJECT: bool = False
# prompt 注入防护：LLM 分类器慢路开关（双层防护第二层；0=仅正则快路）
PROMPT_INJECTION_LLM_ENABLED: bool = os.getenv("PROMPT_INJECTION_LLM_ENABLED", "1").strip() not in ("0", "false")
# prompt 注入防护：LLM 分类器使用的本地模型（走 Ollama）
PROMPT_INJECTION_LLM_MODEL: str = os.getenv("PROMPT_INJECTION_LLM_MODEL", "qwen3:8b")
# prompt 注入防护：LLM 分类器单次判定超时秒数
PROMPT_INJECTION_LLM_TIMEOUT_SEC: float = float(os.getenv("PROMPT_INJECTION_LLM_TIMEOUT_SEC", "8"))
# prompt 注入防护：LLM 判定为注入的最低置信度（>= 该值视为确认注入并拒绝）
PROMPT_INJECTION_LLM_CONFIDENCE_THRESHOLD: float = float(os.getenv("PROMPT_INJECTION_LLM_CONFIDENCE_THRESHOLD", "0.7"))
# prompt 注入防护：LLM 分类器调用失败时是否拒绝（False=放行并告警，可用性优先）
PROMPT_INJECTION_LLM_FAIL_CLOSED: bool = os.getenv("PROMPT_INJECTION_LLM_FAIL_CLOSED", "0").strip() in ("1", "true")
# 审计日志路径
SECURITY_AUDIT_LOG_PATH: str = "logs/security_audit.jsonl"


# ======================================================================
# 23. SEMANTIC_CACHE — 语义缓存层（降级链缓存兜底）
# ======================================================================

# 语义缓存后端：memory / redis
SEMANTIC_CACHE_BACKEND: str = "memory"
# 缓存默认 TTL（秒）：5 分钟，适合热点查询
SEMANTIC_CACHE_DEFAULT_TTL_SEC: int = 300
# 缓存最大条数（memory 后端 LRU 淘汰）
SEMANTIC_CACHE_MAX_ENTRIES: int = 1000
# 语义相似度阈值：cosine ≥ 此值才命中缓存
SEMANTIC_CACHE_SIMILARITY_THRESHOLD: float = 0.92
# 嵌入模型：本地 sentence-transformers 或 OpenAI
SEMANTIC_CACHE_EMBED_MODEL: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
# 嵌入向量维度
SEMANTIC_CACHE_EMBED_DIM: int = 384
# 是否对带股票代码的查询启用缓存（带代码通常需要实时数据，默认关闭）
SEMANTIC_CACHE_ENABLE_FOR_STOCK_QUERY: bool = False
# Redis 后端连接 URL（与 ACTOR 共用）
SEMANTIC_CACHE_REDIS_URL: str = "redis://localhost:6379/1"
# Redis key 前缀
SEMANTIC_CACHE_REDIS_PREFIX: str = "semcache:"


# ======================================================================
# 24. MODEL_ROUTER — 多模型动态路由（成本/SLA）
# ======================================================================

# 路由策略：simple / cost_aware / sla_aware
MODEL_ROUTER_STRATEGY: str = "cost_aware"
# 简单问题路由到便宜模型（DeepSeek 标准版）
MODEL_ROUTER_CHEAP_MODEL: str = "deepseek-chat"
# 复杂问题路由到强模型（DeepSeek 推理版）
MODEL_ROUTER_STRONG_MODEL: str = "deepseek-reasoner"
# 本地模型兜底（Ollama qwen3:8b）
MODEL_ROUTER_LOCAL_MODEL: str = "qwen3:8b"
# 简单问题判定：输入 ≤ 此字符数
MODEL_ROUTER_SIMPLE_MAX_CHARS: int = 100
# 简单问题判定：是否包含代码片段
MODEL_ROUTER_SIMPLE_NO_CODE: bool = True
# 复杂问题判定：关键词（如"对比"、"分析"、"评估"）
MODEL_ROUTER_COMPLEX_KEYWORDS: tuple = (
    "对比", "分析", "评估", "护城河", "估值", "策略", "复盘", "深度"
)
# 每模型成本上限（USD/天，超限降级到下一档）
MODEL_ROUTER_DAILY_BUDGET_USD: float = 5.0
# 每模型日调用次数上限（防刷爆）
MODEL_ROUTER_DAILY_CALL_LIMIT: int = 500
# 模型调用失败时降级链顺序
MODEL_ROUTER_FALLBACK_CHAIN: tuple = (
    "deepseek-reasoner",   # 主：强推理
    "deepseek-chat",       # 备：标准
    "qwen3:8b",            # 末：本地兜底
)
# SLA 路由：P95 延迟超过此值秒数，下一轮自动切到更快的模型
MODEL_ROUTER_SLA_LATENCY_THRESHOLD_SEC: float = 15.0


# ======================================================================
# 25. STREAM_RESUME — 流式输出断点续传
# ======================================================================

# 续传存储后端：memory / redis / file
STREAM_RESUME_BACKEND: str = "memory"
# memory 后端最大保留会话数（LRU 淘汰）
STREAM_RESUME_MEMORY_MAX_SESSIONS: int = 100
# 单会话 partial output 最大保留字符（防止内存爆）
STREAM_RESUME_PARTIAL_MAX_CHARS: int = 50000
# 单会话 token 缓冲区（用于断点续推给 LLM）
STREAM_RESUME_TOKEN_BUFFER_MAX: int = 4096
# 续传 token 默认 TTL（秒）：30 分钟内可续传
STREAM_RESUME_TOKEN_TTL_SEC: int = 1800
# 续传后 LLM 续推的最大 token 数
STREAM_RESUME_CONTINUE_MAX_TOKENS: int = 1024
# Redis 后端连接 URL
STREAM_RESUME_REDIS_URL: str = "redis://localhost:6379/2"
# Redis key 前缀
STREAM_RESUME_REDIS_PREFIX: str = "stream:"


# ======================================================================
# 26. OUTPUT_VALIDATOR — 输出 schema 校验 + 自动拦截
# ======================================================================

# 校验失败后是否自动重试（True=重试 False=直接拒绝）
OUTPUT_VALIDATOR_AUTO_RETRY: bool = True
# 校验失败最大重试次数
OUTPUT_VALIDATOR_MAX_RETRIES: int = 2
# 校验失败重试时的 prompt 提示模板
OUTPUT_VALIDATOR_RETRY_HINT: str = (
    "上一次输出违反了以下规则，请修正后重新回答：\n{violations}\n"
    "请确保本次输出符合规范。"
)
# 风险声明：涉及买卖建议的关键词
OUTPUT_VALIDATOR_RISK_KEYWORDS: tuple = (
    "买入", "卖出", "建议买入", "建议卖出", "目标价", "止损",
    "建仓", "加仓", "减仓", "清仓", "推荐", "应该买", "应该卖"
)
# 必须附带的风险声明文本
OUTPUT_VALIDATOR_RISK_DISCLAIMER: str = (
    "⚠️ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。"
    "投资有风险，入市需谨慎，盈亏自负。"
)
# 校验失败的严重程度：block=拦截 warn=告警放行
OUTPUT_VALIDATOR_VIOLATION_SEVERITY: str = "block"
# 校验日志路径
OUTPUT_VALIDATOR_LOG_PATH: str = "logs/output_violations.jsonl"


# ======================================================================
# 27. STREAM_BUS — 流式事件总线（SSE 多播）
# ======================================================================

# 订阅者队列上限：超过后丢弃最旧帧，防止消费端太慢撑爆内存（经验 4096 ≈ 4MB）
STREAM_BUS_QUEUE_MAXSIZE: int = 4096
# 最大订阅数（= 最多同时活跃的 SSE 连接数）：单进程默认 256，够 MVP
STREAM_BUS_MAX_SUBS: int = 256
# 心跳间隔（秒）：防止 Nginx / Cloudflare / 浏览器 60s 空闲断连（RFC 默认 60s）
STREAM_BUS_HEARTBEAT_INTERVAL_SEC: int = 15
# SSE 端点首包（OPEN 帧）超时：超过 500ms 视为网关/应用异常
STREAM_SSE_OPEN_FRAME_TIMEOUT_MS: int = 500
# SSE 客户端轮询 request.is_disconnected() 的间隔（秒）
STREAM_DISCONNECT_POLL_INTERVAL_SEC: float = 0.5


# ======================================================================
# 28. STREAM_RESUME 事件级断点续传（SSE Last-Event-ID）
# ======================================================================
# 每个 thread_id 保留的事件环形缓冲大小（单位：SSE 帧条数）
#   经验：一条事件平均 ~400 字节，2000 ≈ 0.8MB / 会话，100 并发约 80MB，可接受
STREAM_RESUME_EVENT_RING_MAX: int = 2000
# 重连请求中 body 也允许传 last_event_id（除了 header 之外的兜底路径）
STREAM_RESUME_BODY_LAST_EVENT_ID_ALLOW: bool = True
# 服务端重启后 没有事件缓冲 时建议客户端："resync"=如果有 final_text 就一次性同步给它；
#   否则就是 "restart"=返回 full replay_start 让客户端提示"请重新提问"
STREAM_RESUME_COLD_RESTART_SUGGESTION: str = "resync"
# 已"done"的会话缓冲保留时长（秒）：TTL 到期后重连会走 gap
STREAM_RESUME_DONE_SESSION_TTL_SEC: int = 1800
# 最多同时缓存"事件级"的线程数（与 bus max_subs 对齐）
STREAM_RESUME_MAX_THREAD_STATES: int = 512


# ======================================================================
# 29. 检索来源展示：精简 Token + 减少视觉噪音
# ======================================================================
# 前端 [N] 悬停卡片 / 侧边栏「🔎 实时检索来源」/ 来源池快照 的 snippet 硬上限（中文字）
# 用户规则：只显示最相关片段，最多 100 字；超过按"答案命中句中心窗口 ± 50"抽取（不是头部硬截）
CITATION_SNIPPET_MAX_CHARS: int = 100
# 中心窗口半径：命中关键词位置前后各取多少字（合计 ≈ 2*SNIPPET_HALO + max(命中词长)）
CITATION_SNIPPET_HALO_CHARS: int = 50
# 标题/URL 最大长度（避免极少数巨长 URL 吃掉一整个卡片的 token）
CITATION_TITLE_MAX_CHARS: int = 80
CITATION_URL_MAX_CHARS: int = 256
# Prompt 中注入的单文档"元数据（可靠性/通道/时间）"是否仅在"与默认值不同"时才写入
#  论坛=待验证 & web=默认渠道 → 不写 → 省 ~40 token/文档
CITATION_PROMPT_OMIT_DEFAULT_META: bool = True
# Prompt 中单文档正文上限（不是硬缩 snippet，而是给模型看的上下文正文；与 snippet 展示独立）
CITATION_PROMPT_DOC_CONTENT_MAX: int = 800


# ======================================================================
# 30. RECENCY — 检索结果时效性窗口过滤
# ======================================================================
# 用户规则：只检索最近 1 个月的新闻；若近 1 个月无结果则自动扩大到近 3 个月
# 优先级窗口（天数）：默认只保留 ≤ RECENCY_PREFER_DAYS 天内发布的条目
RECENCY_PREFER_DAYS: int = 30
# 降级窗口（天数）：只有当 prefer 过滤后全通道合计 0 条命中，才放宽到该值
RECENCY_FALLBACK_DAYS: int = 90
# published_at 字符串的 strptime 兼容格式列表（RFC3339/T+Z 会优先用 fromisoformat）
RECENCY_PARSE_FORMATS: tuple = (
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y年%m月%d日",
)
# 在这些通道中，如果 published_at 无法解析 / 超期 也不删除（= 静态知识库 PDF
# 的 created_at/updated_at 只是入库时间，不代表新闻时效性）。
# 其它通道（tavily / zsxq）无法解析 则按"过期处理"删除，防止陈旧内容混入。
RECENCY_KEEP_ON_PARSE_FAIL_CHANNELS: tuple = ("ima",)
# 时间窗口基准时间计算时区（北京时间 UTC+8），与盘前复盘预测 SCHEDULER_TZ_OFFSET_HOURS 对齐
RECENCY_TIMEZONE_OFFSET_HOURS: int = int(os.getenv("RECENCY_TZ_OFFSET", "8"))


# ======================================================================
# 31. STOCK_CACHE — 本地股票分析 txt 缓存（按小时粒度，加速查询响应）
# ======================================================================
# 用户规则：当日 08:00 / 20:00 提前预热热门股分析；用户问到时优先读缓存
# 缓存根目录：<project>/cache/stock_cache （gitignore 已忽略 cache/）
STOCK_CACHE_DIR: str = os.getenv("STOCK_CACHE_DIR",
                                 os.path.abspath(os.path.join(
                                     os.path.dirname(os.path.abspath(__file__)),
                                     "..", "cache", "stock_cache"
                                 )))
# 单文件名格式：YYYYMMDDHH_<sanitized_stock_name>.txt
STOCK_CACHE_FILE_FMT: str = "%Y%m%d%H"
# 预热定时：工作日 早 08:00 / 晚 20:00（可通过 .env 覆盖，便于调试）
STOCK_CACHE_WARMUP_HOURS: tuple = tuple(int(h.strip()) for h in os.getenv(
    "STOCK_CACHE_WARMUP_HOURS", "8,20").split(",") if h.strip())
STOCK_CACHE_WARMUP_MINUTE: int = int(os.getenv("STOCK_CACHE_WARMUP_MINUTE", "0"))
STOCK_CACHE_WARMUP_WEEKDAY_ONLY: bool = os.getenv(
    "STOCK_CACHE_WARMUP_WEEKDAY_ONLY", "1").strip() not in ("0", "false")
# 预热时请求 DeepSeek 获取的热门股数量上限
STOCK_CACHE_WARMUP_TOPK: int = int(os.getenv("STOCK_CACHE_WARMUP_TOPK", "10"))
# 热门股候选来源清单（DeepSeek 会让联网搜索这些社区的最新热门股）
STOCK_CACHE_WARMUP_SOURCES: tuple = (
    "韭研社区", "东方财富股吧", "同花顺股吧", "雪球", "微信公众号",
)
# 缓存文件 TTL（秒）：用于『当小时未结束，但用户换了更准确的股票名后仍能刷新』——
# 实际判断按『当日已存在的"时"粒度文件』优先级；当日 08 时缓存 → 20 时自动降级为旧数据，不覆盖新
STOCK_CACHE_DEFAULT_TTL_SEC: int = int(os.getenv("STOCK_CACHE_DEFAULT_TTL_SEC",
                                                  str(6 * 3600)))  # 默认 6 小时
# 文件最大字节（保护磁盘 + 防止 warmup 产出 10M+ 垃圾）
STOCK_CACHE_MAX_BYTES: int = int(os.getenv("STOCK_CACHE_MAX_BYTES", str(512 * 1024)))  # 512KB
# 总缓存文件上限：超过后按 mtime 删除最旧文件
STOCK_CACHE_TOTAL_FILES_LIMIT: int = int(os.getenv("STOCK_CACHE_TOTAL_FILES_LIMIT", str(500)))
# 请求侧缓存是否启用：可通过 .env STOCK_CACHE_ENABLED=0 关闭（便于对比新老结果）
STOCK_CACHE_ENABLED: bool = os.getenv("STOCK_CACHE_ENABLED", "1").strip() not in ("0", "false")
# 风险声明强制：缓存文件末尾必须包含这条；命中时若缺失则在返回给前端时补回
RISK_DISCLAIMER_CACHE_GUARD: str = (
    "⚠️ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。投资有风险，入市需谨慎，盈亏自负。"
)


