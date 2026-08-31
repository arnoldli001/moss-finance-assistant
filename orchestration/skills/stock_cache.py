"""
Stock Cache — 本地 txt 按小时粒度缓存股票分析结果。

架构：
  1) Warmup（预热）：工作日 08:00 / 20:00，DeepSeek 联网搜热门股 Top10，
     然后对每只股票运行「Tavily + IMA + ZSXQ 三通道综合分析」，结果以
     『YYYYMMDDHH_<股票名>.txt』形式落盘 STOCK_CACHE_DIR。
  2) Query（请求侧命中）：用户问某只股票时，先按『当日 + 股票名归一』
     在目录中查找：优先"本小时的缓存"；无则取"当日更早小时的缓存"；
     均无则视为 MISS，跑完整 agent 链路。命中时通过 SSE 打字机直接回灌。
  3) Writeback（未命中回填）：完整链路跑完后，在 main_agent.report_task_result
     之后立即把 final_content 追加写入一份当日当小时 + 股票名的 txt，下次
     同一日内的同类提问即可秒回。

文件结构约定：
  STOCK_CACHE_DIR/
    2026082908_贵州茅台.txt     ← 2026-08-29 08 时（早预热）的茅台分析
    2026082920_贵州茅台.txt     ← 2026-08-29 20 时（晚预热）的茅台分析
    2026082915_宁德时代.txt     ← 用户 15:32 首次提问"宁德时代是否持有"的回填缓存

TXT 头元数据（以 '#META#' 开头，一行一个 k=v）：
  第 1 行固定：#META# stock_name=贵州茅台
  第 2 行固定：#META# generated_at=2026-08-29 08:02:13+08:00
  第 3 行固定：#META# source=warmup | source=query_writeback  (来源标识)
  第 4 行固定：#META# risk_disclaimer_appended=1（末尾已附风险声明：是/否）
  其余行是正文（按 AGENTS 规则的中文 Markdown），最后一行必须是风险声明。
"""
from __future__ import annotations

import os
import re
import time
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ----------------------------------------------------------------------
# 常量层：延迟加载（独立测试仍有默认值兜底）
# ----------------------------------------------------------------------
def _cfg() -> Dict[str, Any]:
    try:
        from config.constants import (
            STOCK_CACHE_DIR as _D,
            STOCK_CACHE_FILE_FMT as _F,
            STOCK_CACHE_DEFAULT_TTL_SEC as _TTL,
            STOCK_CACHE_MAX_BYTES as _MAXB,
            STOCK_CACHE_TOTAL_FILES_LIMIT as _FLIM,
            STOCK_CACHE_ENABLED as _ENA,
            RISK_DISCLAIMER_CACHE_GUARD as _RDS,
            RECENCY_TIMEZONE_OFFSET_HOURS as _TZH,
        )
    except Exception:
        base = Path(__file__).resolve().parent.parent / "cache" / "stock_cache"
        _D, _F, _TTL, _MAXB, _FLIM, _ENA, _RDS, _TZH = (
            str(base), "%Y%m%d%H", 6 * 3600, 512 * 1024, 500, True,
            "⚠️ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。投资有风险，入市需谨慎，盈亏自负。", 8,
        )
    return dict(D=_D, F=_F, TTL=int(_TTL), MAXB=int(_MAXB), FLIM=int(_FLIM),
                ENA=bool(_ENA), RDS=str(_RDS), TZH=int(_TZH))


def _nowbj(tz_hours: int) -> datetime:
    return datetime.now(timezone(timedelta(hours=tz_hours)))


# ----------------------------------------------------------------------
# 工具：股票名提取（从用户问句里剥出"目标股票"实体，空字符串表示非股票问题）
# ----------------------------------------------------------------------
_STOPWORDS_AFTER = {
    "现在", "今天", "今日", "当前", "目前", "现在看", "值得", "可以", "是否", "会不",
    "买入", "持有", "卖出", "建仓", "止盈", "止损", "走势", "行情", "分析", "评估",
    "股价", "价格", "估值", "财报", "半年报", "年报", "季报", "最新", "新闻", "研报",
    "未来", "中线", "短线", "长期", "短期", "建议", "怎么样", "如何", "可以买",
    "能买", "该买", "该不该", "要不要", "多少", "多少钱", "目标价", "目标",
}

_RE_STOCK_RAW = re.compile(
    r"(?P<name>"
    # A股常见后缀 + 括号带代码
    r"[\u4e00-\u9fa5A-Za-z]{1,12}(?:\([A-Za-z]?\d{4,6}\))?"
    # ETF / LOF 后缀
    r"|[\u4e00-\u9fa5A-Za-z]{1,10}\s*(?:ETF|LOF|QDII|B股|H股|A股)"
    r"|(?:上证|沪深|创业板|科创|中证|恒生|纳斯达克|标普|道琼斯)\w{0,8}"
    # 纯 6 位 A 股代码（SH/SZ 前缀可选）
    r"|(?:SH|SZ|sh|sz)?\d{6}"
    # 美股 1-5 字母代码
    r"|(?:NASDAQ|NYSE|nasdaq|nyse)?:\s*[A-Z]{1,5}"
    r"|^[A-Z]{1,5}$"
    r")"
)

# A股代码 → 股票名 的静态映射兜底（从 astock_list.json 懒加载，由经验 2064478 提供）
_CODE_MAP: Optional[Dict[str, str]] = None
_CODE_MAP_MTIME: float = 0.0


def _load_code_map(root: Path) -> Dict[str, str]:
    """经验 2064478：全量 astock_list.json 落地后，读 {code: name} 表。"""
    global _CODE_MAP, _CODE_MAP_MTIME
    jf = root / "astock_list.json"
    if not jf.exists():
        return {}
    try:
        mtime = jf.stat().st_mtime
        if _CODE_MAP is not None and abs(mtime - _CODE_MAP_MTIME) < 1e-6:
            return _CODE_MAP
        import json
        with open(jf, "r", encoding="utf-8") as fp:
            raw = json.load(fp)
        mapping: Dict[str, str] = {}
        # 兼容两种格式：list[{code,name}] 或 {code: name}
        if isinstance(raw, dict):
            for k, v in raw.items():
                if isinstance(v, dict):
                    mapping[str(k)] = str(v.get("name") or "")
                else:
                    mapping[str(k)] = str(v)
        elif isinstance(raw, list):
            for it in raw:
                if isinstance(it, dict):
                    code = str(it.get("code") or it.get("symbol") or "").strip()
                    name = str(it.get("name") or "").strip()
                    if code and name:
                        mapping[code] = name
        _CODE_MAP = mapping
        _CODE_MAP_MTIME = mtime
        return _CODE_MAP
    except Exception:
        return {}


def extract_stock_name(query: str, *, project_root: Optional[Path] = None) -> str:
    """从用户问句提取"股票实体"（优先股票名，其次代码）。
    非股票问句返回空字符串（这样请求侧缓存自动跳过，不误命中）。"""
    if not query:
        return ""
    q = (str(query)
         .replace("，", " ").replace("。", " ").replace("？", " ").replace("！", " ")
         .replace("、", " ").replace("：", " ").replace(":", " ")
         .replace("“", " ").replace("”", " ").replace("\"", " ")
         .replace("'", " ").replace("《", " ").replace("》", " "))
    q = re.sub(r"\s+", " ", q).strip()

    # 1) 尝试匹配显式关键词模式：『关于 / 分析 / 请问 / 看看 XXX』『XXX 现在/今天/值得』
    #    额外支持模式 A：目标 + 代码 + 现在/今日（ETF 常放在 6 位代码之前）
    #         "请问 华安黄金ETF 518880 现在能抄底吗？" → 华安黄金ETF
    patterns = [
        # A. <请问|分析|看看…> <实体> <6位代码可选> <现在|今天|值得|…>
        re.compile(r"(?:请问|分析下?|看看|说下?|谈谈|点评|对于|关于|查一下?|查询下?|评估下?|我想知道|帮我看看)\s*"
                   r"(?P<a>[\u4e00-\u9fa5A-Za-z0-9()（）]{1,14}?(?:ETF|LOF|QDII|[股债])?)\s*"
                   r"(?P<c>[A-Za-z]?\d{4,6})?\s*"
                   r"(?=现在|今天|今日|当前|目前|值得|可以|是否|买入|持有|卖出|建仓|走势|行情|分析|评估|股价|价格|估值|最新|新闻|研报|财报|半年报|年报|季报|目标价|未来|中线|短线|长期|建议|怎么样|如何|能买|该买|该不该|要不要|买吗|卖吗|$)"),
        re.compile(r"(?P<b>[\u4e00-\u9fa5A-Za-z0-9()（）]{1,14}?(?:ETF|LOF|QDII)?)[ \t]*"
                   r"(?P<cc>[A-Za-z]?\d{4,6})?\s*"
                   r"(现在|今天|今日|当前|目前|值得|可以买|是否|买入|持有|卖出|建仓|走势|行情|股价|估值|财报|半年报|年报|最新|新闻|研报|未来|中线|短线|长期|建议|怎么样|如何|目标价|该买|能不能|还能|会不会)"),
    ]
    for pat in patterns:
        m = pat.search(q)
        if m:
            gd = m.groupdict()
            # 优先级（避免把「华安黄金ETF 518880」误降级成纯 518880 代码存）：
            #   (0) 若 a + c（或 b + cc）拼起来刚好是 6 位 A 股代码 → 直接返回（修复
            #       非贪婪把 6 位拆成 a="6" c="00519" 的 bug）
            #   (1) a/b 中含 ETF/LOF/QDII 的中文名实体
            #   (2) 显式 6 位代码（c/cc）（对应"分析下 600519 最新…"这类只有代码的问句）
            #   (3) a/b 经 _looks_like_stock_entity 通过
            #   (4) 兜底 _clean_candidate(a/b)
            for ka, kc in (("a", "c"), ("b", "cc")):
                va_raw = str(gd.get(ka) or "").strip()
                vc_raw = str(gd.get(kc) or "").strip()
                # 只在 ka 本身完全不含中文/字母时（即它是被非贪婪切散的代码前缀，
                # 例如 600519 被切成 a="6" + c="00519"），才按 RAW 直接拼接，
                # 避免 _clean_candidate 对 len=1 的纯数字做清洗被丢弃。
                if (va_raw and vc_raw
                        and not re.search(r"[\u4e00-\u9fa5A-Za-z]", va_raw)
                        and re.fullmatch(r"\d{1,2}", va_raw)
                        and re.fullmatch(r"[A-Za-z]?\d{4,6}", vc_raw)):
                    merged = va_raw + vc_raw
                    if _looks_like_stock_entity(merged):
                        return merged
            name_cand = ""
            for k in ("a", "b"):
                v = gd.get(k)
                if v and re.search(r"ETF|LOF|QDII|[股债]", str(v)):
                    name_cand = _clean_candidate(str(v))
                    if name_cand:
                        return name_cand
            for k_code in ("c", "cc"):
                v = gd.get(k_code)
                if v and _looks_like_stock_entity(str(v)):
                    cleaned = _clean_candidate(str(v))
                    if cleaned:
                        return cleaned
            cand = ""
            for k in ("a", "b"):
                v = gd.get(k)
                if v and _looks_like_stock_entity(v):
                    cand = v
                    break
            if not cand:
                for k in ("a", "b"):
                    v = gd.get(k)
                    vv = _clean_candidate(v or "")
                    if vv:
                        cand = vv
                        break
            cand = _clean_candidate(cand)
            if cand:
                return cand

    # 2) 回退：按 _RE_STOCK_RAW 扫描取第一个"看起来像股票实体"的
    best = ""
    for m in _RE_STOCK_RAW.finditer(q):
        cand_raw = m.group("name")
        if not _looks_like_stock_entity(cand_raw):
            continue
        cand = _clean_candidate(cand_raw)
        if not cand:
            continue
        # 偏好中文（≥2字且含真正股票信号：ETF/LOF/股份/科技/茅台/指数名关键字 或 尾部跟了6位代码）
        if (re.search(r"(ETF|LOF|QDII|[\u4e00-\u9fa5]{2,})", cand)
                and len(cand) >= 2):
            return cand
        if not best and len(cand) >= 3 and _looks_like_stock_entity(cand):
            best = cand
    if best:
        return best

    # 3) 兜底：用 code_map 翻译纯数字（可能用户只打了代码，希望按名存缓存）
    if project_root is None:
        try:
            from utils.path_utils import get_project_root_path
            project_root = Path(get_project_root_path())
        except Exception:
            project_root = Path(__file__).resolve().parent.parent
    mp = _load_code_map(project_root)
    digits = re.findall(r"\d{5,6}", q)
    for d in digits:
        if d in mp:
            return mp[d]
    return ""


# 真正"像股票实体"的白名单判定：防止「今天晚上吃什么」被错识别成股票。
_ETF_LOF = ("ETF", "LOF", "QDII", "REITs", "ETF联接")
_INDEX_PREFIX = ("上证", "沪深", "创业板", "科创", "中证", "恒生", "纳斯达克", "标普", "道琼斯",
                 "日经", "富时", "DAX", "CAC", "FTSE")
_SUFFIX_HINT = ("股份", "集团", "科技", "电子", "证券", "银行", "保险", "医药", "能源", "汽车",
                "地产", "消费", "制造", "新材", "信息", "智能", "光伏", "锂电", "芯片", "白酒",
                "啤酒", "航空", "物流", "电力", "环保")
_INVALID_CAND_KEYWORDS = ("什么", "怎么", "为什么", "如何", "哪里", "哪只", "哪个", "何时",
                          "多少", "今天", "晚上", "早上", "下午", "中午", "明日", "昨日", "当前",
                          "今晚", "昨夜", "大盘", "个股", "股票", "股市", "A股", "港股", "美股")


def _looks_like_stock_entity(cand: str) -> bool:
    if not cand:
        return False
    s = cand.strip()
    if not s:
        return False
    # 纯 6 位 A 股代码 或 5 位数字 或 SH/SZ+6 位 → 视为代码
    if re.fullmatch(r"(?:SH|SZ)?\d{5,6}", s, flags=re.IGNORECASE):
        return True
    # 纯 1~5 大写英文字母 → 视为美股代码（AAPL / TSLA…）
    if re.fullmatch(r"[A-Z]{1,5}", s):
        return True
    # 排除完全由无效关键词组成的
    if s in _INVALID_CAND_KEYWORDS:
        return False
    if any(s == kw for kw in _INVALID_CAND_KEYWORDS):
        return False
    # ETF/LOF 后缀直接通过
    if any(tag in s for tag in _ETF_LOF):
        return True
    # 指数名前缀直接通过
    if any(s.startswith(pre) for pre in _INDEX_PREFIX):
        return True
    # 含有 A 股常见行业/公司后缀提示词 且长度 ≥ 2 中文 或 ≥ 4 中英
    if (any(seg in s for seg in _SUFFIX_HINT)
            and len(re.findall(r"[\u4e00-\u9fa5A-Za-z]", s)) >= 2):
        return True
    # 含中文 2+ 字，且紧邻数字代码（正则里后面有 6 位数字）→ 认为是股票实体
    if re.search(r"[\u4e00-\u9fa5]{2,}", s):
        # 再去掉"无效关键词"占主体的情况：如 今天晚上 = 2+中文但不是股票
        cjk_only = "".join(re.findall(r"[\u4e00-\u9fa5]+", s))
        non_invalid = cjk_only
        for kw in _INVALID_CAND_KEYWORDS:
            non_invalid = non_invalid.replace(kw, "")
        if len(non_invalid) >= 2:
            return True
    return False


def _clean_candidate(cand: str) -> str:
    c = cand.strip(" 　-_")
    # 剥掉"股票""股份""集团"后缀：避免"贵州茅台股票"匹配成"贵州茅台股票"（应为"贵州茅台"）
    for suf in ("股票", "走势", "行情", "分析", "评估", "研报", "新闻", "的"):
        if c.endswith(suf) and len(c) - len(suf) >= 2:
            c = c[:-len(suf)]
    # 大写 SH600519 → 规范化 600519 ；若 code_map 能解出名字，返回名字（否则代码）
    if re.fullmatch(r"(?:SH|SZ)?\d{6}", c, flags=re.IGNORECASE):
        code = c.upper().replace("SH", "").replace("SZ", "")
        return code
    c = c.strip(" 　-_()（）")
    if len(c) >= 2 and not re.fullmatch(r"[的了呢啊嘛吗吧你我他她们它这那和与或及在到从把给被让向对关于]\s*", c):
        return c
    return ""


# ----------------------------------------------------------------------
# 工具：安全文件名（sanitize + 中文名保留）；同一股票的多种表达归一到同一文件名种子
# ----------------------------------------------------------------------
_INVALID_FN_CHARS = re.compile(r"[\\/:*?\"<>|\r\n\t]+")
_NORMALIZE_SPACE = re.compile(r"\s+")


def sanitize_stock_filename(stock_name: str) -> str:
    """返回用于文件名的股票段（中文字符保留，非法字符替换成_，前后去空白）。
    空字符串返回 'UNKNOWN_STOCK'（用于防御：虽然调用方应先判 extract 非空）。"""
    if not stock_name:
        return "UNKNOWN_STOCK"
    s = _INVALID_FN_CHARS.sub("_", stock_name)
    s = _NORMALIZE_SPACE.sub("_", s).strip(" ._")
    return s or "UNKNOWN_STOCK"


# ----------------------------------------------------------------------
# 核心：stock_cache_dir / 读 / 写 / 命中查询
# ----------------------------------------------------------------------
def _ensure_dir() -> Path:
    c = _cfg()
    p = Path(c["D"])
    p.mkdir(parents=True, exist_ok=True)
    return p


def _day_key_prefix(dt: datetime, fmt: str) -> str:
    """从 fmt 抽出"只到日"的子格式，返回 dt.strftime(DAY_FMT) 字符串。
    不直接用 fmt[:8] 的原因：'%Y'/'%m'等 strftime 转义符是 2 字符 1 字段，
    fmt="%Y%m%d%H" 只有 8 字符，fmt[:8] 就是整串（含小时 %H）—— 会导致"跨小时"误判为非当日。
    因此直接硬编码 DAY_FMT = "%Y%m%d"（与 fmt 的前缀对齐，本项目只使用这一种小时粒度）。"""
    return dt.strftime("%Y%m%d")


def _match_same_day(ts: datetime, prefix: str) -> bool:
    return ts.strftime("%Y%m%d") == prefix


def build_cache_file_path(stock_name: str, *, at_time: Optional[datetime] = None) -> Tuple[Path, datetime]:
    c = _cfg()
    tz = timezone(timedelta(hours=c["TZH"]))
    if at_time is None:
        at_time = datetime.now(tz)
    if at_time.tzinfo is None:
        at_time = at_time.replace(tzinfo=tz)
    else:
        at_time = at_time.astimezone(tz)
    d = _ensure_dir()
    name_seg = sanitize_stock_filename(stock_name)
    fname = at_time.strftime(c["F"]) + "_" + name_seg + ".txt"
    return d / fname, at_time


def query_cache_by_stock_name(
    stock_name: str,
    *,
    project_root: Optional[Path] = None,
    prefer_same_hour: bool = True,
    at_time: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """请求侧缓存命中查询：返回 {path, content, stock_name, generated_at, source,
    same_hour: bool, age_hours: float}；无命中返回 None。

    命中优先级：
      1) 当日"同小时"且文件名匹配 → same_hour=True（最新鲜）；
      2) 否则遍历当日所有小时的缓存，取 mtime 最新的 1 个 → same_hour=False；
      3) 否则 MISS。
    文件内容若缺失风险声明，自动在返回结果末尾补上 RISK_DISCLAIMER_CACHE_GUARD。
    """
    c = _cfg()
    if not c["ENA"] or not stock_name:
        return None
    if at_time is None:
        at_time = _nowbj(c["TZH"])
    # 归一：支持别名/代码但最终都按股票名匹配；调用端传的"贵州茅台/茅台"可能产生差异命中，
    # 这里先拿提取后的正式名再 sanitize；对于 6 位代码同样按代码名匹配（两种都尝试）
    candidates = [sanitize_stock_filename(stock_name)]
    if project_root is None:
        try:
            from utils.path_utils import get_project_root_path
            project_root = Path(get_project_root_path())
        except Exception:
            project_root = Path(__file__).resolve().parent.parent
    # 若传入是代码，尝试转名字作为另一路候选
    if re.fullmatch(r"\d{5,6}", stock_name):
        mp = _load_code_map(project_root)
        name = mp.get(stock_name)
        if name:
            candidates.append(sanitize_stock_filename(name))

    cache_dir = _ensure_dir()
    day_prefix = _day_key_prefix(at_time, c["F"])
    same_hour_prefix = at_time.strftime(c["F"])

    # 所有当日文件：按 (是否 same_hour, mtime) 排序取最优
    ranked: List[Tuple[int, float, Path]] = []
    for fp in cache_dir.glob("*.txt"):
        stem = fp.stem  # YYYYMMDDHH_茅台
        if "_" not in stem:
            continue
        ts_str, name_part = stem.split("_", 1)
        if not ts_str.startswith(day_prefix) or len(ts_str) != len(same_hour_prefix):
            continue
        if name_part not in candidates:
            continue
        try:
            mt = fp.stat().st_mtime
        except OSError:
            continue
        rank_same = 1 if ts_str == same_hour_prefix else 0
        ranked.append((rank_same, mt, fp))
    if not ranked:
        return None
    ranked.sort(key=lambda t: (t[0], t[1]), reverse=True)
    best_same_hour_rank, best_mt, best_path = ranked[0]
    # 保护：读取 + 解析头元数据
    try:
        raw_bytes = best_path.read_bytes()[: max(c["MAXB"], 1)]
    except OSError:
        return None
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = raw_bytes.decode("utf-8", errors="replace")
    meta, body = _parse_meta_and_body(text)
    # TTL 兜底：若 mtime 距今 > TTL，按"过期失效"视为 MISS（防止缓存用了 2 天以上没人清理）
    age_sec = time.time() - best_mt
    if age_sec > max(c["TTL"], 12 * 3600):
        return None
    # 风险声明强制：缺失则补
    if c["RDS"] not in body and (not body or RISK_DISCLAIMER_PLACEHOLDER_CHECK(body) is False):
        body = (body.rstrip() + "\n\n" + c["RDS"]).lstrip()
    gen_at = meta.get("generated_at") or datetime.fromtimestamp(best_mt, tz=timezone(timedelta(hours=c["TZH"])))
    if isinstance(gen_at, str):
        try:
            gen_at_parsed = datetime.fromisoformat(gen_at.replace("Z", "+00:00"))
            if gen_at_parsed.tzinfo is None:
                gen_at_parsed = gen_at_parsed.replace(tzinfo=timezone(timedelta(hours=c["TZH"])))
            gen_at = gen_at_parsed
        except Exception:
            gen_at = datetime.fromtimestamp(best_mt, tz=timezone(timedelta(hours=c["TZH"])))
    return {
        "path": str(best_path),
        "content": body,
        "stock_name": meta.get("stock_name") or stock_name,
        "generated_at": gen_at,
        "source": meta.get("source") or "unknown",
        "same_hour": bool(best_same_hour_rank),
        "age_hours": round(age_sec / 3600.0, 2),
    }


def RISK_DISCLAIMER_PLACEHOLDER_CHECK(body: str) -> bool:
    """仅作为 query_cache 里 body 结尾风险声明判定的工具函数。"""
    return bool(body and ("以上信息来自互联网公开资料" in body
                          or "不构成投资建议" in body))


def _parse_meta_and_body(text: str) -> Tuple[Dict[str, Any], str]:
    meta: Dict[str, Any] = {}
    lines = text.splitlines(keepends=True)
    cut = 0
    for line in lines:
        if line.startswith("#META#"):
            kv = line[len("#META#"):].strip()
            if "=" in kv:
                k, v = kv.split("=", 1)
                meta[k.strip()] = v.strip()
            cut += 1
        else:
            break
    body = "".join(lines[cut:])
    return meta, body


def write_stock_cache(
    stock_name: str,
    content: str,
    *,
    source: str = "query_writeback",
    at_time: Optional[datetime] = None,
    project_root: Optional[Path] = None,
) -> Optional[str]:
    """写一条缓存到『当日当小时 + 股票名』文件。返回写入文件路径；失败返回 None。
    source ∈ {warmup, query_writeback}。
    """
    c = _cfg()
    if not c["ENA"] or not stock_name or not content:
        return None
    safe_name = sanitize_stock_filename(stock_name)
    if safe_name == "UNKNOWN_STOCK":
        return None
    path, ts = build_cache_file_path(stock_name, at_time=at_time)
    tz = timezone(timedelta(hours=c["TZH"]))
    gen_at_str = ts.astimezone(tz).isoformat(timespec="seconds")
    body = content.rstrip() + "\n\n"
    if c["RDS"] not in body:
        body += c["RDS"] + "\n"
    # 组头元数据
    header_lines = [
        f"#META# stock_name={stock_name}",
        f"#META# generated_at={gen_at_str}",
        f"#META# source={('warmup' if str(source) == 'warmup' else 'query_writeback')}",
        f"#META# risk_disclaimer_appended=1",
    ]
    final = "\n".join(header_lines) + "\n" + body
    if len(final.encode("utf-8")) > c["MAXB"]:
        # 超长保护：按字节截到 MAXB（保留 UTF-8 末尾完整性）
        final_b = final.encode("utf-8")[:c["MAXB"]]
        # 尝试去掉尾部损坏的 utf-8 字节
        while final_b and (final_b[-1] & 0xC0) == 0x80:
            final_b = final_b[:-1]
        final = final_b.decode("utf-8", errors="ignore")
    try:
        tmp_path = path.with_suffix(".txt.tmp")
        with open(tmp_path, "w", encoding="utf-8") as fp:
            fp.write(final)
        os.replace(tmp_path, path)
    except OSError:
        return None
    _evict_old_cache_files_if_needed()
    return str(path)


def _evict_old_cache_files_if_needed() -> None:
    """总文件数超限时，按 mtime 从最旧开始删除。"""
    c = _cfg()
    limit = c["FLIM"]
    if limit <= 0:
        return
    cd = _ensure_dir()
    try:
        files = [(f.stat().st_mtime, f) for f in cd.glob("*.txt")]
    except OSError:
        return
    if len(files) <= limit:
        return
    files.sort(key=lambda t: t[0])
    n_remove = len(files) - limit
    for _, fp in files[:n_remove]:
        try:
            fp.unlink()
        except OSError:
            pass


# ----------------------------------------------------------------------
# 命中返回内容生成器：用于 SSE 打字机"分段回灌"
# ----------------------------------------------------------------------
_DISCLAIMER_SEG_LEN = 24  # 每段推送字符长度（小=更顺滑，大=更省包）


def iter_cache_hit_chunks(content: str, segment_len: int = _DISCLAIMER_SEG_LEN) -> List[str]:
    """把缓存文本切成一段段小增量，模拟流式打字机（逐段 yield delta）。
    这里返回的是 list[str]，方便单测与 SSE 端点复用。"""
    if not content:
        return []
    segs: List[str] = []
    i = 0
    # 按 CJK 语义分块：遇到换行时作为一段结束，避免把 \n 拆成半段
    lines = content.splitlines(keepends=True)
    for ln in lines:
        start = 0
        while start < len(ln):
            end = min(len(ln), start + segment_len)
            segs.append(ln[start:end])
            start = end
    return segs


# ----------------------------------------------------------------------
# 调试辅助：随机清理 & 大小统计（脚本调用）
# ----------------------------------------------------------------------
def _random_sleep_for_avoid_thundering_herd(min_sec: float = 0.5, max_sec: float = 2.5) -> None:
    """预热任务是所有服务器节点在 08:00/20:00 同时触发，小睡一段时间错开。"""
    time.sleep(random.uniform(min_sec, max_sec))
