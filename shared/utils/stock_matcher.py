# coding=utf-8
"""
高性能股票代码/名称匹配工具。

设计目标：
  1. 启动时一次性加载 stock_list.txt，构建多维度索引（O(1) 字典查找）。
  2. 支持按代码精确查名称、按名称精确查代码、按名称模糊匹配、从一段文本中抽取所有股票实体。
  3. 防止短词简称误匹配（如"大金"≠"大金融"中的"大金"）：对 2~3 字短匹配加词边界约束。
  4. 进程内单例 + 懒加载，首次调用才读文件；后续调用零 IO。

数据源：data/stock_list.txt（每行 6位代码+名称，GBK 编码，无分隔符）。
"""
from __future__ import annotations

import os
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# ======================================================================
# 数据结构
# ======================================================================

@dataclass(frozen=True)
class StockInfo:
    """一条股票基础信息。"""
    code: str               # 6 位数字代码，如 "600519"
    name: str               # 全称，如 "贵州茅台"
    market: str             # 市场简称：SH沪主板/SZ深主板/CY创业板/KC科创板/BJ北交所/OTH其他

    @property
    def display(self) -> str:
        return f"{self.name}({self.code})"


# ----------------------------------------------------------------------
# 兼容补充：StockMatchHit 类型别名（部分老代码 import 它做类型标注，
# 即使定义方未显式使用也保留，避免薄壳 re-export 时 NameError）
# ----------------------------------------------------------------------
try:  # pragma: no cover - 仅做类型提示时会被 IDE 看到
    from typing import NamedTuple
    class StockMatchHit(NamedTuple):
        """文本抽取时的单次命中（目前 extract_from_text 直接返回 StockInfo，
        本别名仅用于类型兼容，未来若需要 span 信息可扩展）。"""
        info: StockInfo
        start: int = -1
        end: int = -1
except Exception:  # pragma: no cover - 极端退化
    class StockMatchHit:  # type: ignore[no-redef]
        def __init__(self, info: StockInfo, start: int = -1, end: int = -1):
            self.info = info
            self.start = start
            self.end = end


# ======================================================================
# 内部常量
# ======================================================================

# A 股代码前缀 → 市场映射
_CODE_PREFIX_MARKET: Tuple[Tuple[str, str], ...] = (
    ("60", "SH"),       # 上交所主板
    ("688", "KC"),      # 科创板
    ("00", "SZ"),       # 深交所主板
    ("30", "CY"),       # 创业板
    ("8", "BJ"),        # 北交所
    ("4", "BJ"),        # 北交所老股
)

# 常见股票后缀（用于生成"短别名"时剥离，比如中国平安→平安）
_COMMON_SUFFIXES: Tuple[str, ...] = (
    "股份", "集团", "科技", "电子", "电气", "信息", "智能", "能源",
    "材料", "医药", "医疗", "生物", "环保", "工程", "机械", "制造",
    "建设", "发展", "投资", "控股", "实业", "贸易", "物流", "文化",
    "传媒", "通信", "网络", "软件", "数据", "金融", "证券", "银行",
    "保险", "地产", "置业", "旅游", "餐饮", "食品", "饮料", "化工",
    "新材", "光电", "动力", "汽车", "重工", "航空", "航天", "防务",
    "装备", "资源", "矿业", "水泥", "玻璃", "钢铁", "有色", "金属",
    "农业", "牧业", "渔业", "环保", "水务", "燃气", "电力", "热力",
    "服务", "股份有限公司", "有限公司", "集团有限公司",
)

# 高歧义短别名黑名单（匹配这些词时必须触发"强语境校验"，否则拒绝）
#   经验：来自 2276024，短词纯子串匹配极易命中行业词/口语词
#   分层规则：
#     (a) 行业/领域通用词：金融、银行、科技...
#     (b) 日常高频时间/代词：今天、明天、现在、大家...
#     (c) 方位/形容词前缀：中国、东方、未来...
_AMBIGUOUS_SHORT_ALIAS_BLACKLIST: Set[str] = {
    # —— 行业/领域通用词 ——
    "兄弟", "大金", "金融", "银行", "证券", "保险", "地产", "科技",
    "能源", "医药", "电子", "信息", "智能", "材料", "环保", "机械",
    "建设", "发展", "投资", "控股", "实业", "贸易", "物流", "文化",
    "传媒", "通信", "网络", "软件", "数据", "汽车", "重工", "航空",
    "资源", "矿业", "水泥", "玻璃", "钢铁", "有色", "农业", "电力",
    "服务", "装备", "制造", "工程", "光电", "新材", "生物", "医疗",
    "水务", "燃气", "热力", "地产", "置业", "旅游", "餐饮", "食品",
    "饮料", "化工", "动力", "航天", "防务", "牧业", "渔业",
    # —— 公司治理通用词 ——
    "股份", "集团", "有限", "实业", "投资", "发展", "建设",
    # —— 地理/方位/形容词前缀 ——
    "中国", "东方", "西部", "南方", "北方", "华夏", "中华", "神州",
    "国际", "国家", "全球", "世界", "未来", "创新", "创业", "成长",
    "价值", "核心", "先进", "高端", "绿色", "智慧", "数字", "融合",
    "改革", "开放", "共享", "协同", "生态", "平台", "中央", "中国",
    # —— 极高频日常时间词（防止"今天国际"→"今天"误判）——
    "今天", "昨日", "明天", "后天", "前日", "上周", "本周", "下周",
    "上月", "本月", "下月", "去年", "今年", "明年", "周末", "周一",
    "周二", "周三", "周四", "周五", "周六", "周日", "现在", "目前",
    "当前", "近期", "今日",
    # —— 社交/口语高频词（防止"兄弟科技"→"兄弟们"误判的兄弟也在这组）
    "大家", "我们", "你们", "他们", "她们", "自己", "朋友", "老师",
    "老板", "师傅", "先生", "女士",
}

# 股票强语境关键词（文本中出现至少一个，才接受高歧义短别名命中）
#   注：这里词不宜过泛，否则"走强/拉升/板块"这类纯行情评论也会触发误识别。
#   原则：必须是"出现在讨论某只具体股票的场景下概率极高"的词。
_STRONG_STOCK_CONTEXT_KEYWORDS: Tuple[str, ...] = (
    # —— 强股票专属词（出现任意一个即 90%+ 概率是在聊个股）——
    "股份", "股票代码", "证券代码", "代号", "涨停", "跌停", "涨停板", "跌停板", "连板",
    "市盈率", "市净率", "ROE", "每股收益", "EPS", "分红", "派息", "配股",
    "增发", "回购", "解禁", "IPO", "上市", "退市", "停牌", "复牌", "ST",
    "年报", "半年报", "季报", "中报", "财报", "研报", "公告", "龙虎榜",
    "买入", "卖出", "加仓", "减仓", "建仓", "清仓", "抄底", "逃顶", "止盈", "止损",
    "目标价", "支撑位", "压力位", "仓位", "持仓", "个股", "概念股", "白马股", "蓝筹股",
    "龙头股", "题材股", "换手率", "成交量", "成交额", "量比", "股价",
    "领涨", "领跌", "利好", "利空",
    # —— 带代码/市场标识的强信号 ——
    ".SH", ".SZ", ".HK", ".US", "沪市", "深市", "科创板", "创业板", "北交所",
    # —— 股票名显式包裹的上下文 ——
    "这只", "该股", "这支股", "此股", "请问",
    "推荐", "看好", "不看好", "点评", "分析", "估值", "怎么样",
)

# 注：中文之间没有空格分隔，因此用"左右两侧字符类型"做纯正则词边界对中文是错的。
# 本文件采用以下替代策略：
#   (a) 全称正则：直接用 (长词1|长词2|...) 不加边界，正则引擎 alternation
#       天然"左最长匹配"——只要我们按长度倒序拼接 alternation，长词总是优先。
#   (b) 短别名文本扫描：先扫全称和代码，记录它们覆盖的字符区间；再扫短别名，
#       任何与已覆盖区间重叠的短别名命中都直接丢弃。这样"贵州茅台"命中后
#       就不会再额外产出"贵州""茅台"等重复短别名。
#   (c) 独立 token 查询（lookup 单个词）：已由 caller 切分好，天然是"词边界"情形，
#       但 2~3 字短别名仍需上下文校验（防止"大金融/兄弟们"被误识别）。
# 保留以下常量仅用于对字母数字代码/英文片段的边界检查。
_CODE_BOUNDARY_LEFT = r"(?<![A-Za-z0-9.])"
_CODE_BOUNDARY_RIGHT = r"(?![A-Za-z0-9.])"


# ======================================================================
# StockMatcher 主类（进程内单例）
# ======================================================================

class StockMatcher:
    """
    高性能股票匹配器。

    用法：
        matcher = StockMatcher.get_instance()  # 单例，懒加载
        matcher.is_valid_code("600519")        # True
        matcher.is_valid_name("贵州茅台")      # True
        matcher.lookup_by_code("600519")       # StockInfo
        matcher.lookup_by_name("贵州茅台")     # StockInfo
        matcher.lookup("茅台")                 # StockInfo（短别名匹配，带约束）
        matcher.extract_from_text("贵州茅台和工商银行今日上涨")
            # [StockInfo(600519,贵州茅台), StockInfo(601398,工商银行)]
    """

    _instance: Optional["StockMatcher"] = None
    _instance_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 单例入口
    # ------------------------------------------------------------------
    @classmethod
    def get_instance(cls) -> "StockMatcher":
        """获取全局单例（线程安全，懒加载）。"""
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    # 【路径兼容修正】本文件现位于 shared/utils/，比原 tools/ 深一级，
                    # 原 .parent.parent（tools → 项目根）现需要 .parent.parent.parent
                    # （shared/utils → shared → 项目根）。
                    default_path = (
                        Path(__file__).resolve().parent.parent.parent
                        / "data" / "stock_list.txt"
                    )
                    cls._instance = cls(str(default_path))
        return cls._instance

    # ------------------------------------------------------------------
    # 构造：读取文件 + 建索引
    # ------------------------------------------------------------------
    def __init__(self, stock_file_path: str):
        self._stock_file: str = stock_file_path
        self._file_mtime: float = 0.0
        self._lock = threading.RLock()

        # 索引容器
        self._all: List[StockInfo] = []
        self._by_code: Dict[str, StockInfo] = {}
        self._by_name: Dict[str, StockInfo] = {}
        # 短别名 → [StockInfo,...]（一对多需要冲突消解）
        self._by_alias: Dict[str, List[StockInfo]] = {}
        # 所有 2~3 字高歧义短别名集合（快速判定是否需要上下文校验）
        self._ambiguous_shorts: Set[str] = set()
        # 编译：全称正则（大模式，用于文本抽取）
        self._full_name_regex: Optional[re.Pattern] = None
        self._code_regex: re.Pattern = re.compile(
            _CODE_BOUNDARY_LEFT + r"(\d{6})" + _CODE_BOUNDARY_RIGHT
        )

        self._load_and_build()

    # ------------------------------------------------------------------
    # 加载 + 索引构建
    # ------------------------------------------------------------------
    def _load_and_build(self) -> None:
        path = Path(self._stock_file)
        if not path.exists():
            # 允许文件缺失（降级为纯格式校验，不抛异常阻断启动）
            import logging
            logging.getLogger(__name__).warning(
                "[StockMatcher] stock_list.txt 未找到: %s，所有匹配将返回空/False", path
            )
            self._file_mtime = 0.0
            return

        self._file_mtime = path.stat().st_mtime

        # 1. 读取并解析（GBK，兼容 GB18030 兜底）
        raw_text: str = ""
        for enc in ("gbk", "gb18030", "utf-8"):
            try:
                raw_text = path.read_text(encoding=enc)
                break
            except (UnicodeDecodeError, LookupError):
                continue
        if not raw_text:
            return

        line_re = re.compile(r"^([0-9A-Z]{2,6})(.+)$")
        stocks: List[StockInfo] = []
        for line_no, raw_line in enumerate(raw_text.splitlines(), 1):
            line = raw_line.strip().lstrip("\ufeff")
            if not line:
                continue
            m = line_re.match(line)
            if not m:
                continue
            code_raw, name = m.group(1), m.group(2).strip()
            # 只保留 6 位数字 A 股代码（跳过 BZ 开头的北交所预览等）
            if len(code_raw) != 6 or not code_raw.isdigit():
                continue
            name = name.strip()
            if len(name) < 2:
                continue
            market = self._detect_market(code_raw)
            info = StockInfo(code=code_raw, name=name, market=market)
            stocks.append(info)

        self._all = stocks

        # 2. 代码→信息（O(1)）
        self._by_code = {s.code: s for s in stocks}

        # 3. 全称→信息（O(1)，同名若冲突取后覆盖）
        #    注意：理论上同名不同代码极少，但保留覆盖行为以便查找
        self._by_name = {}
        for s in stocks:
            # 若重名，优先保留非 ST/非退市的简单策略：这里先到先得，再用 code 索引兜底
            self._by_name.setdefault(s.name, s)

        # 4. 短别名索引（去掉常见后缀后的 2~4 字片段）
        alias_map: Dict[str, List[StockInfo]] = {}
        for s in stocks:
            aliases = self._generate_aliases(s.name)
            for alias in aliases:
                alias_map.setdefault(alias, []).append(s)
        # 5. 标记高歧义短别名（多个股票共享同一个短别名，或落在黑名单中）
        ambiguous: Set[str] = set()
        for alias, slist in alias_map.items():
            if len(alias) <= 3:
                if alias in _AMBIGUOUS_SHORT_ALIAS_BLACKLIST or len(slist) > 1:
                    ambiguous.add(alias)
        self._by_alias = alias_map
        self._ambiguous_shorts = ambiguous

        # 6. 编译全称正则（长词1|长词2|...，不加边界，按长度倒序→长词优先）
        sorted_names = sorted({s.name for s in stocks}, key=len, reverse=True)
        if sorted_names:
            escaped = [re.escape(n) for n in sorted_names if n]
            # 4600 多条 alternation 对 Python re 仍在可接受范围（<1s）。
            # 如果后续性能不足，可切换为 Aho-Corasick 自动机。
            self._full_name_regex = re.compile("(" + "|".join(escaped) + ")")
        else:
            self._full_name_regex = None

        # 7. 短别名正则（同样按长度倒序，不加边界；运行时再做区间冲突去重）
        alias_keys = sorted(
            (a for a in alias_map.keys() if 2 <= len(a) <= 4),
            key=len, reverse=True,
        )
        if alias_keys:
            escaped_a = [re.escape(a) for a in alias_keys]
            self._alias_regex: Optional[re.Pattern] = re.compile(
                "(" + "|".join(escaped_a) + ")"
            )
        else:
            self._alias_regex = None

    @staticmethod
    def _detect_market(code: str) -> str:
        for prefix, market in _CODE_PREFIX_MARKET:
            if code.startswith(prefix):
                return market
        return "OTH"

    @staticmethod
    def _generate_aliases(name: str) -> Set[str]:
        """
        从全称生成短别名集合（2~4 字，过度歧义的不入库）。

        策略（按"常用度"优先级）：
          1. 去掉通用行业后缀（中国平安→平安，三花智控→三花）。
          2. 尾部 2/3/4 字切片（贵州茅台→茅台、宁德时代→时代）。
          3. 首部 2/3/4 字前缀（避免过度拆分，且对落入黑名单的高歧义前缀跳过）。
        """
        aliases: Set[str] = set()
        n = len(name)
        if n < 2:
            return aliases

        # ---- 1) 去通用后缀 ----
        stripped = name
        changed = True
        while changed:
            changed = False
            for suf in _COMMON_SUFFIXES:
                if stripped.endswith(suf) and len(stripped) - len(suf) >= 2:
                    stripped = stripped[: -len(suf)]
                    changed = True
        if 2 <= len(stripped) <= 4 and stripped != name:
            aliases.add(stripped)

        # ---- 2) 尾部 2/3/4 字切片（民间俗称通常是尾字，如茅台/平安/白药）----
        for k in (2, 3, 4):
            if n > k:
                tail = name[-k:]
                if tail not in _AMBIGUOUS_SHORT_ALIAS_BLACKLIST:
                    aliases.add(tail)
        # 如果整条名称正好 4 字，也把它作为一个 4 字别名（便于"宁德时代"做子串匹配）
        if n == 4:
            aliases.add(name)

        # ---- 3) 首部 2/3/4 字前缀（黑名单中的高歧义前缀跳过）----
        for k in (2, 3, 4):
            if n > k:
                prefix = name[:k]
                if prefix not in _AMBIGUOUS_SHORT_ALIAS_BLACKLIST:
                    aliases.add(prefix)

        # 防御：去掉等于自身、空、单字、5 字以上的畸形项
        aliases.discard(name)
        aliases = {a for a in aliases if 2 <= len(a) <= 4}
        return aliases

    # ------------------------------------------------------------------
    # 热更新：文件变动后重建索引（可由定时任务调用）
    # ------------------------------------------------------------------
    def reload_if_changed(self) -> bool:
        """若 stock_list.txt mtime 变化，重建索引。返回是否发生了重建。"""
        path = Path(self._stock_file)
        if not path.exists():
            return False
        mtime = path.stat().st_mtime
        if mtime != self._file_mtime:
            with self._lock:
                # double check
                if path.stat().st_mtime != self._file_mtime:
                    self._load_and_build()
                    return True
        return False

    # ------------------------------------------------------------------
    # 查询 API
    # ------------------------------------------------------------------
    @property
    def total_count(self) -> int:
        return len(self._all)

    def is_valid_code(self, code: str) -> bool:
        """给定 6 位代码，判断是否是有效股票代码（来自清单）。"""
        if not code or len(code) != 6:
            return False
        return code in self._by_code

    def is_valid_name(self, name: str) -> bool:
        """给定股票全称，判断是否在清单中。"""
        if not name:
            return False
        return name in self._by_name

    def lookup_by_code(self, code: str) -> Optional[StockInfo]:
        """按 6 位代码精确查找。"""
        if not code:
            return None
        return self._by_code.get(code)

    def lookup_by_name(self, name: str) -> Optional[StockInfo]:
        """按股票全称精确查找。"""
        if not name:
            return None
        return self._by_name.get(name)

    def lookup(self, token: str, context_text: str = "") -> Optional[StockInfo]:
        """
        智能查找：自动判断 token 是代码还是名称（含短别名）。

        Args:
            token: 用户输入的词，可能是代码、全称、或简称（如"茅台"）。
            context_text: 可选，包含该 token 的完整上下文，用于歧义消解。
        Returns:
            StockInfo 或 None。
        """
        if not token:
            return None
        token = token.strip()
        if len(token) == 6 and token.isdigit():
            return self._by_code.get(token)

        # 全称优先
        if token in self._by_name:
            return self._by_name[token]

        # 短别名
        candidates = self._by_alias.get(token)
        if not candidates:
            return None

        # 单候选直接返回（若落在高歧义集合，则仍需上下文校验）
        if len(candidates) == 1:
            only = candidates[0]
            if self._need_context_check(token):
                ctx = context_text if context_text else token
                # 强信任规则：上下文中直接出现该候选的"全称"或"代码"，
                # 说明用户确实在讨论这支股票（即使没有强关键词也放行）。
                if only.name in ctx or only.code in ctx:
                    return only
                if not self._has_strong_stock_context(ctx):
                    return None
            return only

        # 多候选：用上下文选择最可能的一个
        return self._disambiguate(candidates, context_text if context_text else token)

    def is_stock(self, token: str, context_text: str = "") -> bool:
        """
        对外统一入口：判断 token 是否是一个"股票相关实体"。
        相比 is_valid_code / is_valid_name 更宽容，支持短别名 + 上下文消解。
        """
        return self.lookup(token, context_text) is not None

    def extract_from_text(self, text: str) -> List[StockInfo]:
        """
        从一段自然语言文本中抽取所有出现的股票实体（去重并保持顺序）。

        策略（分层 + 区间冲突消解，避免重复/误匹配）：
          1. 先扫 6 位数字代码（最高置信度），记录命中区间。
          2. 再用"长词优先"的全称正则扫名称，与步骤 1 区间冲突的命中直接丢弃。
          3. 最后扫短别名，与前两步已覆盖区间重叠者丢弃，并做"高歧义短别名强语境"校验。
        """
        if not text:
            return []

        n = len(text)
        # 已被高优先级命中覆盖的字符区间（用 0/1 字节数组标记）
        covered = bytearray(n)

        def _mark(s: int, e: int) -> None:
            # 区间 [s, e) 标为已覆盖
            if s < 0:
                s = 0
            if e > n:
                e = n
            for i in range(s, e):
                covered[i] = 1

        def _has_overlap(s: int, e: int) -> bool:
            if s < 0:
                s = 0
            if e > n:
                e = n
            for i in range(s, e):
                if covered[i]:
                    return True
            return False

        seen: Set[str] = set()
        result: List[StockInfo] = []

        def _push(info: Optional[StockInfo], span: Tuple[int, int]) -> None:
            if not info or info.code in seen:
                return
            seen.add(info.code)
            result.append(info)
            _mark(*span)

        # Step 1: 代码（最高优先级）
        for m in self._code_regex.finditer(text):
            info = self._by_code.get(m.group(1))
            if info:
                _push(info, m.span())

        # Step 2: 全称正则（长词优先，区间冲突即丢弃）
        if self._full_name_regex is not None:
            for m in self._full_name_regex.finditer(text):
                if _has_overlap(*m.span()):
                    continue
                info = self._by_name.get(m.group(1))
                if info:
                    _push(info, m.span())

        # Step 3: 短别名（区间冲突 + 中文伪边界 + 强语境 三重过滤）
        if self._alias_regex is not None:
            def _is_cjk(ch: str) -> bool:
                return "\u4e00" <= ch <= "\u9fff" if ch else False

            for m in self._alias_regex.finditer(text):
                start, end = m.span()
                if _has_overlap(start, end):
                    continue
                alias = m.group(1)
                alias_len = len(alias)
                left_cjk = _is_cjk(text[start - 1]) if start > 0 else False
                right_cjk = _is_cjk(text[end]) if end < n else False

                # --- 中文伪边界（硬过滤，先于 lookup 执行，优先级最高）---
                # 规则 1：≤3 字 alias 若"双侧同时紧贴中文汉字" → 一定嵌入在更长词中间。
                #   比如"日上"⊂"今日上涨"、"茅台"理论不会出现这种形态。
                #   此规则是绝对性过滤，即使 lookup 通过、有强语境，也必须遵守。
                if alias_len <= 3 and alias not in self._by_name and left_cjk and right_cjk:
                    continue

                # --- lookup + 强语境校验（对 len<=3 别名强制要求语境）---
                window = text[max(0, start - 20): min(n, end + 20)]
                ctx = text if len(text) < len(window) + 100 else text + "\n" + window
                info = self.lookup(alias, ctx)
                if info is None:
                    continue

                # --- 黑名单 alias 的额外"单侧中文邻接"限制（仅当局部窗口完全无强语境时生效）---
                #   比如"今天盘前策略"中"今天"右侧紧贴"盘"中文 → 黑名单+单侧中文+小窗口无强语境→丢弃
                #   但"兄弟科技涨停了"中"兄弟"右侧紧贴"科"中文 → 小窗口有"涨停"强语境 → 保留
                if (alias_len <= 3 and alias not in self._by_name
                        and alias in _AMBIGUOUS_SHORT_ALIAS_BLACKLIST
                        and (left_cjk or right_cjk)):
                    if not self._has_strong_stock_context(window):
                        continue

                if info.code not in seen:
                    seen.add(info.code)
                    result.append(info)
                    _mark(start, end)

        return result

    # ------------------------------------------------------------------
    # 歧义消解辅助
    # ------------------------------------------------------------------
    def _need_context_check(self, alias: str) -> bool:
        """
        哪些短别名需要"强语境校验"才能判定为股票？
        策略（宁可漏判不可误判，避免张冠李戴）：
          * 所有 ≤3 字的短别名，一律校验（"日上/今天/大金/兄弟"这类极易嵌入日常用语）。
          * 4 字别名仅当在"一对多冲突表"中或落在黑名单上才校验。
        """
        L = len(alias)
        if L <= 3:
            return True
        # 4 字
        return alias in self._ambiguous_shorts

    @staticmethod
    def _has_strong_stock_context(text: str) -> bool:
        if not text:
            return False
        return any(kw in text for kw in _STRONG_STOCK_CONTEXT_KEYWORDS)

    def _disambiguate(self, candidates: List[StockInfo], context: str) -> Optional[StockInfo]:
        """
        多候选冲突消解：
          1. 上下文出现哪个全称，优先；
          2. 上下文出现哪个代码，优先；
          3. 否则返回 None（拒绝猜测，避免张冠李戴）。
        """
        if not context:
            return None
        # 1) 全称命中
        for c in candidates:
            if c.name in context:
                return c
        # 2) 代码命中
        for c in candidates:
            if c.code in context:
                return c
        # 3) 均不明确 → 返回 None，调用方视为"未识别"
        return None


# ======================================================================
# 便捷函数（面向外部模块的扁平接口，不用每次 get_instance）
# ======================================================================

def is_stock_code(code: str) -> bool:
    """是否是清单中的有效股票代码。"""
    return StockMatcher.get_instance().is_valid_code(code)


def is_stock_name(name: str) -> bool:
    """是否是清单中的有效股票全称。"""
    return StockMatcher.get_instance().is_valid_name(name)


def is_stock_entity(token: str, context_text: str = "") -> bool:
    """是否是股票（代码/全称/短别名，带上下文消解）。"""
    return StockMatcher.get_instance().is_stock(token, context_text)


def lookup_stock(token: str, context_text: str = "") -> Optional[StockInfo]:
    """查找股票信息，找不到返回 None。"""
    return StockMatcher.get_instance().lookup(token, context_text)


def extract_stocks(text: str) -> List[StockInfo]:
    """从文本中抽取所有股票实体。"""
    return StockMatcher.get_instance().extract_from_text(text)


def get_stock_matcher() -> StockMatcher:
    """获取单例实例（用于热更新 reload_if_changed 等高级操作）。"""
    return StockMatcher.get_instance()
