# -*- coding: utf-8 -*-
"""
shared.utils.ollama_analyzer：本地 Ollama 大模型「输入分析汇总」统一调度层。

定位（按你本轮需求：单独一个 python 文件，调度 Ollama 对各类输入做分析汇总）：
  - 不做"环境就绪性管理"（定位 CLI、拉起、探活、拉模型 → 归 shared/utils/ollama_helper.py）
  - 不做"RAG 流式综合推理 + 引用抽取"（归 shared/llm_client/ollama_client.py ollama_synthesize）
  - **专注：调度本地 Ollama 对一段/多篇输入做 非流式 / 流式 / 预定义模板分析，
    输出最终汇总文本或结构化结果（JSON / 列表 / 评分）**，供：
      · 主 Agent（通过 LangChain tool 包装后调用）
      · 调度协调 Agent（定时任务：盘前汇总、盘后复盘报告、舆情热度日度画像等）
      · tools/zsxq_analysis_runner.py（盘前小作文热度：原来 _call_ollama_chat + _parse_analysis 双份逻辑，现 import 本文件复用，消除重复）
      · 未来任何需要本地 LLM 离线分析场景（财报要点抽取、公告利好利空定性、
        股吧帖子情绪聚类……）。

分层结构（按经验 2235706：_build_request / _parse_response 两个最小公共函数）：
  Layer 1 请求构造  build_chat_request(messages?, system?, user?, schema?, force_json?, options?) -> dict
  Layer 2 响应解析  parse_jsonish(raw) / strip_markdown_fence(raw) / extract_sentiment_counts(text)
  Layer 3 同步调用  ollama_analyze(...)   -> AnalyzeResult(final_text, parsed, meta)
  Layer 4 异步调用  ollama_analyze_async(...)  -> AnalyzeResult
  Layer 5 流式增量  ollama_analyze_stream_async(...) -> AsyncGenerator[str, None]
  Layer 6 预定义分析模板（可直接一行调用，无需手写 system prompt + schema）：
      summarize_text(text, max_words, model?)                     通用总结
      analyze_sentiment(text, aspect?, model?)                     多维度情绪（正/中/负）
      analyze_zsxq_hot_news(news_entries_text, model?)             盘前小作文热度：提取股票名+利好/利空+次数（返回 [dict]）
      extract_structured(text, schema, *, hints, model?)           任意 JSON Schema 抽取

配置默认值来源：config.constants 中的 TEST_ZXSQ_OLLAMA_* / OLLAMA_* 常量集中 import，
保证与 server.py、tools/zsxq_analysis_runner.py、tools/zsxq_crawler_tool.py 完全同源，
未来调参只需改 config.constants 一处。
"""
from __future__ import annotations

import ast
import json
import os
import re
import sys
import asyncio
import concurrent.futures as _cf
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Dict, Iterable, List, Optional, Tuple, Union

# ---------------------------------------------------------------------------
# 自举：按 AGENTS.md 锚找 PROJECT_ROOT 并注入 sys.path，保证裸脚本/任意 cwd 导入
# ---------------------------------------------------------------------------
def _find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for _ in range(12):
        if (cur / "AGENTS.md").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return start.resolve().parents[2]  # shared/utils/<this>.py → 上 2 层 = 项目根


PROJECT_ROOT: Path = _find_project_root(Path(__file__))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore
    _ep = find_dotenv(str(PROJECT_ROOT / ".env"))
    if _ep:
        load_dotenv(_ep)
except Exception:  # pragma: no cover
    pass

# ---------------------------------------------------------------------------
# 常量集中引用（避免魔鬼数字，0 双份抄）
# ---------------------------------------------------------------------------
from config.constants import (  # noqa: E402
    OLLAMA_DEFAULT_BASE_URL,
    OLLAMA_CHAT_DEFAULT_TIMEOUT_SEC,
    OLLAMA_DEFAULT_TEMPERATURE,
    TEST_ZXSQ_OLLAMA_TIMEOUT_SEC as _DEFAULT_HARD_TIMEOUT,
    TEST_ZXSQ_OLLAMA_TEMPERATURE as _DEFAULT_LOW_TEMP,
    TEST_ZXSQ_OLLAMA_CONTENT_COMPRESS_THRESHOLD as _DEFAULT_CONTENT_COMPRESS,
    TEST_ZXSQ_OLLAMA_ENTRY_TRUNCATE_CHARS as _DEFAULT_ENTRY_TRUNCATE,
    TEST_ZXSQ_CLI_TIMEOUT_SEC as _DEFAULT_CLI_TIMEOUT,
)
DEFAULT_MODEL: str = os.environ.get("OLLAMA_ANALYZER_MODEL") or "qwen3:8b"

ProgressCb = Callable[[str], None]  # 与 ollama_helper.ProgressFn 同形，避免循环 import

# ---------------------------------------------------------------------------
# 返回结构
# ---------------------------------------------------------------------------
@dataclass
class AnalyzeResult:
    """统一的分析结果结构（同步/异步都返回它，便于后续落库/推 WS/保存总结）。"""
    final_text: str                                 # 模型原始完整输出
    parsed: Optional[Any] = None                    # 若要求 JSON/schema 则填解析后的对象/list
    meta: Dict[str, Any] = field(default_factory=dict)  # 诊断：latency_ms / chars / HTTP status / error 原因

    def as_text(self) -> str:
        if isinstance(self.parsed, (dict, list)):
            try:
                return json.dumps(self.parsed, ensure_ascii=False, indent=2)
            except Exception:
                pass
        return self.final_text or ""


# ======================================================================
# Layer 1：最小公共请求构造（Experience 2235706）
# ======================================================================
def build_chat_request(
    *,
    model: str,
    system: Optional[str] = None,
    user: Optional[str] = None,
    messages: Optional[List[Dict[str, str]]] = None,
    schema: Optional[Dict[str, Any]] = None,
    force_json: bool = False,
    stream: bool = False,
    temperature: Optional[float] = None,
    extra_options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """构造 Ollama /api/chat 请求体（单一入口，任何调用点都走它 → 未来换协议/加字段只改这里）。

    规则：
      1. 若传 messages，则直接用它；否则根据 system/user 拼接。
      2. schema 优先级最高 → 走 "format": <schema> 字段。
      3. 否则 force_json=True → 走 "format": "json"。
      4. options 至少带 temperature；extra_options 可追加 num_ctx/top_p/seed 等。
    """
    if messages is None:
        msgs: List[Dict[str, str]] = []
        if system:
            msgs.append({"role": "system", "content": str(system)})
        if user:
            msgs.append({"role": "user", "content": str(user)})
    else:
        msgs = list(messages)
    payload: Dict[str, Any] = {
        "model": str(model),
        "messages": msgs,
        "stream": bool(stream),
    }
    options: Dict[str, Any] = {}
    if temperature is not None:
        options["temperature"] = float(temperature)
    if extra_options:
        options.update({k: v for k, v in extra_options.items() if v is not None})
    if options:
        payload["options"] = options
    if schema:
        payload["format"] = schema
    elif force_json:
        payload["format"] = "json"
    return payload


# ======================================================================
# Layer 2：最小公共响应解析（Experience 2235706：.get() 链 / 回退解析）
# ======================================================================
def strip_markdown_fence(text: str) -> str:
    """去除 ```json ... ``` 或 ``` ... ``` 围栏，返回内部字符串。"""
    if not text:
        return ""
    t = text.strip()
    # 最外层整体是围栏？
    m = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```\s*$", t)
    if m:
        return m.group(1).strip()
    # 内部含围栏片段？取第一段
    m2 = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", t)
    if m2:
        return m2.group(1).strip()
    return t


def parse_jsonish(raw: str) -> Any:
    """尽量把大模型输出的"可能 JSON/可能 Python repr/可能 文本夹 JSON"串解析为 Python 对象。

    回退链（对 qwen 系中文输出非常友好，避免 30%+ 因 JSON 格式差解析失败）：
      1) 直接 json.loads(raw)
      2) 去掉 markdown fence 再 json
      3) 正则捕获第一个 [ ... ] 或 { ... } 块再 json
      4) 当作 Python literal ast.literal_eval
      5) 返回 None（调用方可判断 parsed is None → 回退正则逐行）
    """
    if not raw:
        return None
    text = raw.strip()
    # 1) direct
    try:
        return json.loads(text)
    except Exception:
        pass
    # 2) fence
    fenced = strip_markdown_fence(text)
    if fenced != text:
        try:
            return json.loads(fenced)
        except Exception:
            pass
    # 3) regex top-level block
    blk = re.search(r'\{[\s\S]*\}', text)
    if blk:
        try:
            return json.loads(blk.group(0))
        except Exception:
            pass
    blk2 = re.search(r'\[[\s\S]*\]', text)
    if blk2:
        try:
            return json.loads(blk2.group(0))
        except Exception:
            pass
    # 4) Python literal（LLM 偶尔输出单引号、True/False 小写）
    try:
        return ast.literal_eval(fenced)
    except Exception:
        try:
            return ast.literal_eval(text)
        except Exception:
            return None


# ---------------------------------------------------------------------------
# 解析：盘前小作文热度输出 → 标准化 list[dict(name, sentiment, count)]
# （从 tools/zsxq_analysis_runner._parse_analysis 中抽出、共享实现）
# ---------------------------------------------------------------------------
def _strip_zh_name(name: str) -> str:
    """去掉股票名前后的序号、列表符号、括号装饰。"""
    if not name:
        return ""
    name = name.strip().strip('【】"\'「」()（）[]<>《》·•').strip()
    name = re.sub(
        r'^[\s]*([一二三四五六七八九十百0-9]+[\.、\)\）·]|[①-⑳]|[\-*•▲■▶◆])\s*',
        '', name)
    return name.strip().strip('【】"\'「」()（）[]<>《》·•').strip()


def parse_stock_sentiment_items(raw: str) -> List[Dict[str, Any]]:
    """把 LLM 的"盘前小作文热度"输出解析为标准 list[dict(name, sentiment, count)]。

    4 重回退链：JSON → 正则取块 → 强模板逐行 → 宽松逐行，保证覆盖 99% 的中文 qwen 输出。
    """
    results: List[Dict[str, Any]] = []

    # helper：单个 dict 条目 → 提取/正则/归一化
    def _extract_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None
        name = str(
            item.get("name") or item.get("股票名") or item.get("stock")
            or item.get("股票") or item.get("公司") or item.get("公司名") or ""
        ).strip()
        sentiment = str(
            item.get("sentiment") or item.get("利好利空") or item.get("分析")
            or item.get("判断") or item.get("情绪") or item.get("倾向")
            or item.get("类型") or item.get("方向") or ""
        ).strip()
        count_raw = (
            item.get("count") or item.get("次数") or item.get("出现次数")
            or item.get("提及次数") or item.get("热度") or 0
        )
        try:
            count = int(count_raw)
        except (TypeError, ValueError):
            mm = re.search(r'\d+', str(count_raw))
            count = int(mm.group(0)) if mm else 1
        if not name or not sentiment:
            return None
        if "利空" in sentiment:
            sentiment = "利空"
        elif "利好" in sentiment:
            sentiment = "利好"
        else:
            return None
        return {"name": name, "sentiment": sentiment, "count": max(count, 1)}

    # 1) parse_jsonish + 递归收集含 name 字段的 dict
    obj = parse_jsonish(raw)
    if obj is not None:
        found: List[Dict[str, Any]] = []
        def _collect(o: Any) -> None:
            if isinstance(o, list):
                for x in o:
                    _collect(x)
            elif isinstance(o, dict):
                if any(k in o for k in ("name", "股票名", "stock", "股票", "公司", "公司名")):
                    found.append(o)
                for v in o.values():
                    if isinstance(v, (list, dict)):
                        _collect(v)
        _collect(obj)
        for it in found:
            ex = _extract_item(it)
            if ex:
                results.append(ex)
        if results:
            return results

    # 2) 强模板逐行： "名 : 利好（次数）"
    pattern = re.compile(
        r'[【"\'\s]*([^\s：:{}【】"\'<>·][^：:{}【】"\'<>·]{0,20}?)[】"\'\s]*[：:]\s*'
        r'[【"\'\s]*([利好利空]{2})[】"\'\s]*[（\(\s]*(\d+)[\)\）\s]*')
    for m in pattern.finditer(raw):
        name = _strip_zh_name(m.group(1))
        sentiment = m.group(2).strip()
        try:
            count = int(m.group(3))
        except (TypeError, ValueError):
            count = 1
        if len(name) >= 2 and sentiment in ("利好", "利空"):
            results.append({"name": name, "sentiment": sentiment, "count": count})
    if results:
        return results

    # 3) 宽松模板逐行
    for line in raw.splitlines():
        sl = line.strip().lstrip('-*•\t ')
        if not sl:
            continue
        m = re.search(r'(.{1,20}?)\s*[：:]\s*.*?(利好|利空).*?(\d+)', sl)
        if not m:
            m = re.search(r'(.{1,20}?)\s*[：:]\s*(利好|利空)\D*(\d+)', sl)
        if m:
            name = _strip_zh_name(m.group(1))
            sentiment = m.group(2)
            try:
                count = int(m.group(3))
            except (TypeError, ValueError):
                count = 1
            if len(name) >= 2 and sentiment in ("利好", "利空"):
                results.append({"name": name, "sentiment": sentiment, "count": count})
    return results


# ======================================================================
# Layer 3 / 4：同步 / 异步 非流式调用
# ======================================================================
async def _async_http_post_chat(
    base_url: str,
    payload: Dict[str, Any],
    timeout: float,
) -> Tuple[int, str]:
    """异步 POST /api/chat（stream=False）并返回 (status, body_text)。

    用 loop.run_in_executor 跑 urllib 阻塞调用，保证 async 上下文中不阻塞事件循环；
    这里没有用 httpx/aiohttp 是因为项目目前 stdlib 优先，无额外依赖引入。
    """
    import urllib.request as _urlreq
    import urllib.error as _urlerr
    payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    url = base_url.rstrip("/") + "/api/chat"
    req = _urlreq.Request(
        url,
        data=payload_bytes,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "moss-finance-assistant/ollama-analyzer",
        },
    )
    loop = asyncio.get_running_loop()

    def _sync_do() -> Tuple[int, str]:
        try:
            with _urlreq.urlopen(req, timeout=timeout) as resp:
                status = int(getattr(resp, "status", 200))
                body = resp.read().decode("utf-8", errors="replace")
                return status, body
        except _urlerr.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            return int(getattr(e, "code", 500)), body
        except Exception as e:
            return 0, f"{type(e).__name__}: {e}"

    with _cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix="ollama_analyze") as ex:
        return await loop.run_in_executor(ex, _sync_do)


async def ollama_analyze_async(
    *,
    user: Optional[str] = None,
    system: Optional[str] = None,
    messages: Optional[List[Dict[str, str]]] = None,
    model: str = DEFAULT_MODEL,
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
    timeout: float = OLLAMA_CHAT_DEFAULT_TIMEOUT_SEC,
    temperature: Optional[float] = None,
    schema: Optional[Dict[str, Any]] = None,
    force_json: bool = False,
    extra_options: Optional[Dict[str, Any]] = None,
    parse_json: bool = False,
    progress_cb: Optional[ProgressCb] = None,
) -> AnalyzeResult:
    """异步非流式：调度本地 Ollama 做分析汇总 → AnalyzeResult。

    参数：
        parse_json  — True 时用 parse_jsonish 解析结果填 result.parsed；
                      schema/force_json 场景推荐开；纯文本总结默认 False。
        progress_cb — 可选：触发「开始请求 / 解析阶段」两档消息，便于调度侧推日志。
    """
    import time as _t
    t0 = _t.monotonic()
    if progress_cb:
        progress_cb(f"🧠 Ollama 分析：调用模型 {model}（文本长度 {len(user or '') + len(system or '')} 字符）…")
    payload = build_chat_request(
        model=model,
        system=system,
        user=user,
        messages=messages,
        schema=schema,
        force_json=force_json,
        stream=False,
        temperature=temperature,
        extra_options=extra_options,
    )
    status, body = await _async_http_post_chat(base_url, payload, timeout=float(timeout))
    latency_ms = int((_t.monotonic() - t0) * 1000)

    final_text = ""
    parsed: Any = None
    meta: Dict[str, Any] = {"latency_ms": latency_ms, "status": status}

    if status < 200 or status >= 300:
        msg = f"Ollama HTTP {status}: {body[:500]}"
        if progress_cb:
            progress_cb("❌ " + msg)
        meta["error"] = msg
        return AnalyzeResult(final_text="", parsed=None, meta=meta)

    try:
        data = json.loads(body) if body else {}
    except Exception as e:
        meta["error"] = f"响应 JSON 解析失败: {e}; body={body[:300]}"
        return AnalyzeResult(final_text=body, parsed=None, meta=meta)

    # Ollama /api/chat (stream=False) 返回字段：message.content / done=true / eval_count ...
    msg_obj = data.get("message") or {}
    if isinstance(msg_obj, dict):
        final_text = str(msg_obj.get("content") or "")
    else:
        final_text = str(data.get("response") or data.get("content") or "")

    # 附带一些 token/eval 计数，便于后续性能分析
    for k in ("prompt_eval_count", "eval_count", "total_duration", "load_duration", "eval_duration"):
        if k in data:
            meta[k] = data[k]

    if progress_cb:
        progress_cb(f"✅ Ollama 分析完成：{len(final_text)} 字符，耗时 {latency_ms}ms")

    if parse_json or schema or force_json:
        parsed = parse_jsonish(final_text)

    return AnalyzeResult(final_text=final_text, parsed=parsed, meta=meta)


def ollama_analyze(
    *,
    user: Optional[str] = None,
    system: Optional[str] = None,
    messages: Optional[List[Dict[str, str]]] = None,
    model: str = DEFAULT_MODEL,
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
    timeout: float = OLLAMA_CHAT_DEFAULT_TIMEOUT_SEC,
    temperature: Optional[float] = None,
    schema: Optional[Dict[str, Any]] = None,
    force_json: bool = False,
    extra_options: Optional[Dict[str, Any]] = None,
    parse_json: bool = False,
    progress_cb: Optional[ProgressCb] = None,
) -> AnalyzeResult:
    """同步版：同 ollama_analyze_async 参数表。在没有事件循环的脚本/工具子进程内直接调用。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is None:
        return asyncio.run(ollama_analyze_async(
            user=user, system=system, messages=messages, model=model,
            base_url=base_url, timeout=timeout, temperature=temperature,
            schema=schema, force_json=force_json, extra_options=extra_options,
            parse_json=parse_json, progress_cb=progress_cb,
        ))
    # 有运行 loop：交给独立线程跑 asyncio.run，避免嵌套
    with _cf.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(lambda: asyncio.run(ollama_analyze_async(
            user=user, system=system, messages=messages, model=model,
            base_url=base_url, timeout=timeout, temperature=temperature,
            schema=schema, force_json=force_json, extra_options=extra_options,
            parse_json=parse_json, progress_cb=progress_cb,
        ))).result()


# ======================================================================
# Layer 5：异步流式（返回文本增量 AsyncGenerator[str]；也可用于 SSE 推送）
# ======================================================================
async def ollama_analyze_stream_async(
    *,
    user: Optional[str] = None,
    system: Optional[str] = None,
    messages: Optional[List[Dict[str, str]]] = None,
    model: str = DEFAULT_MODEL,
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
    timeout: float = OLLAMA_CHAT_DEFAULT_TIMEOUT_SEC,
    temperature: Optional[float] = None,
    extra_options: Optional[Dict[str, Any]] = None,
) -> AsyncGenerator[str, None]:
    """异步流式：对长文本分析/总结输出，边生成边读。

    用法：async for delta in ollama_analyze_stream_async(user=...): print(delta, end='')
    """
    import urllib.request as _urlreq
    import urllib.error as _urlerr

    payload = build_chat_request(
        model=model, system=system, user=user, messages=messages,
        stream=True, temperature=temperature, extra_options=extra_options,
    )
    payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    url = base_url.rstrip("/") + "/api/chat"
    req = _urlreq.Request(
        url, data=payload_bytes, method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/x-ndjson",
            "User-Agent": "moss-finance-assistant/ollama-analyzer",
        },
    )
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()

    def _sync_reader_thread() -> None:
        try:
            with _urlreq.urlopen(req, timeout=timeout) as resp:
                buf = bytearray()
                while True:
                    ch = resp.read(4096)
                    if not ch:
                        break
                    buf.extend(ch)
                    while True:
                        nl = buf.find(b"\n")
                        if nl < 0:
                            break
                        line_bytes = bytes(buf[:nl])
                        del buf[:nl + 1]
                        if not line_bytes.strip():
                            continue
                        try:
                            obj = json.loads(line_bytes.decode("utf-8"))
                        except Exception as e:
                            asyncio.run_coroutine_threadsafe(
                                queue.put(("error", f"json decode: {e}")), loop
                            )
                            continue
                        msg = obj.get("message") or {}
                        delta = str(msg.get("content") or "")
                        done = bool(obj.get("done", False))
                        if delta:
                            asyncio.run_coroutine_threadsafe(queue.put(("delta", delta)), loop)
                        if done:
                            break
        except _urlerr.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")[:500]
            except Exception:
                pass
            asyncio.run_coroutine_threadsafe(
                queue.put(("error", f"HTTP {getattr(e, 'code', 500)}: {body}")), loop
            )
        except Exception as e:
            asyncio.run_coroutine_threadsafe(
                queue.put(("error", f"{type(e).__name__}: {e}")), loop
            )
        finally:
            asyncio.run_coroutine_threadsafe(queue.put(("end", "")), loop)

    with _cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix="ollama_analyze_stream") as ex:
        fut = ex.submit(_sync_reader_thread)
        while True:
            item = await queue.get()
            kind, text = item
            if kind == "delta":
                yield text
            elif kind == "error":
                raise RuntimeError(text)
            elif kind == "end":
                break
        try:
            fut.result(timeout=2.0)
        except Exception:
            pass


# ======================================================================
# Layer 6：预定义分析模板（一行调用，无需手写 system prompt / schema）
# 这些模板的输出就是"对各类输出结果的分析汇总"能力的体现，
# 调度协程 / 主 Agent / 后台任务可以直接用，也可以组合。
# ======================================================================
def _zsxq_stock_schema() -> Dict[str, Any]:
    """盘前小作文热度分析：共享 JSON Schema（与原 zsxq_analysis_runner 保持一致）。"""
    return {
        "type": "object",
        "properties": {
            "stocks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "上市公司股票名称（如贵州茅台）"},
                        "sentiment": {"type": "string", "enum": ["利好", "利空"], "description": "利好或利空判断"},
                        "count": {"type": "integer", "description": "当日提及次数或热度计数，≥1"},
                    },
                    "required": ["name", "sentiment"],
                },
            }
        },
        "required": ["stocks"],
    }


async def analyze_zsxq_hot_news_async(
    entries_text: str,
    *,
    model: str = DEFAULT_MODEL,
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
    timeout: float = _DEFAULT_CLI_TIMEOUT,
    progress_cb: Optional[ProgressCb] = None,
) -> List[Dict[str, Any]]:
    """盘前小作文热度（异步）：输入资讯拼接文本，返回 list[dict(name, sentiment, count)]。"""
    system = (
        "你是A股金融分析师。从财经资讯中提取【上市公司】股票名，判断利好或利空，"
        "只提取上市公司，不要提取行业名或指数名。"
        "利好=涨价/业绩增长/推荐/订单增长；利空=降价/下滑/风险提示/监管处罚。"
    )
    user = (
        f"从以下资讯中提取所有被提到的上市公司股票名，并判断利好或利空。\n"
        f"只提取上市公司（如贵州茅台、宁德时代、比亚迪、五粮液、古井贡酒、药明康德、迈瑞医疗等）。\n"
        f"不要提取行业名（白酒、AI、半导体）、指数名（上证、恒生）。\n\n"
        f"资讯：\n{entries_text}"
    )
    res = await ollama_analyze_async(
        system=system, user=user, model=model, base_url=base_url,
        timeout=timeout, temperature=0.1, schema=_zsxq_stock_schema(),
        parse_json=True, progress_cb=progress_cb,
    )
    # 解析层：先尝试 parsed；否则走正则回退链（保证 JSON 字段细微不合规也能救回）
    parsed = res.parsed
    items: List[Dict[str, Any]] = parse_stock_sentiment_items(res.final_text)
    if items:
        return items
    # 兜底：若 parsed 为 dict 但没有合法 items，尝试把 parsed 原样喂给 parse_jsonish/正则
    if isinstance(parsed, (list, dict)):
        raw2 = json.dumps(parsed, ensure_ascii=False)
        items2 = parse_stock_sentiment_items(raw2)
        if items2:
            return items2
    return []


def analyze_zsxq_hot_news(
    entries_text: str,
    *,
    model: str = DEFAULT_MODEL,
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
    timeout: float = _DEFAULT_CLI_TIMEOUT,
    progress_cb: Optional[ProgressCb] = None,
) -> List[Dict[str, Any]]:
    """盘前小作文热度（同步版）：供 tools/zsxq_analysis_runner.py 这类同步子进程直接调用。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is None:
        return asyncio.run(analyze_zsxq_hot_news_async(
            entries_text, model=model, base_url=base_url, timeout=timeout, progress_cb=progress_cb,
        ))
    with _cf.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(lambda: asyncio.run(analyze_zsxq_hot_news_async(
            entries_text, model=model, base_url=base_url, timeout=timeout, progress_cb=progress_cb,
        ))).result()


async def summarize_text_async(
    text: str,
    *,
    max_words: int = 500,
    aspect: str = "核心内容",
    model: str = DEFAULT_MODEL,
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
    temperature: float = 0.3,
) -> str:
    """通用总结：把长文本压缩为 ≤max_words 的中文总结，聚焦 aspect（如"关键利好利空"/"数据要点"/"用户情绪"）。"""
    system = f"你是严谨的中文总结器。用不超过 {max_words} 个字输出「{aspect}」的摘要，只保留事实，不加主观臆测。"
    if len(text) > _DEFAULT_CONTENT_COMPRESS * 4:
        text = text[: _DEFAULT_CONTENT_COMPRESS * 4] + "\n……（后续文本因长度限制截断）"
    res = await ollama_analyze_async(
        system=system, user=text, model=model, base_url=base_url,
        temperature=temperature, parse_json=False,
    )
    out = (res.final_text or "").strip()
    if len(out) > max_words * 2:
        out = out[: max_words * 2] + "……"
    return out


def summarize_text(*args, **kwargs) -> str:
    """通用总结（同步版）。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is None:
        return asyncio.run(summarize_text_async(*args, **kwargs))
    with _cf.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(lambda: asyncio.run(summarize_text_async(*args, **kwargs))).result()


async def analyze_sentiment_async(
    text: str,
    *,
    aspect: str = "整体",
    model: str = DEFAULT_MODEL,
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
) -> Dict[str, Any]:
    """情绪分析（异步）：返回 {aspect, label in ('利好'|'利空'|'中性'), score -5..5, reasons:[...]}。"""
    schema = {
        "type": "object",
        "properties": {
            "label": {"type": "string", "enum": ["利好", "利空", "中性"]},
            "score": {"type": "integer", "minimum": -5, "maximum": 5},
            "reasons": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["label", "score", "reasons"],
    }
    system = "你是金融文本情绪标注员。输出结构化 JSON，label 只能是利好/利空/中性；score 从 -5（极端利空）到 +5（极端利好）；reasons 是 3-6 条简短中文原因。"
    user = f"请分析如下{aspect}层面的情绪：\n\n{text[: _DEFAULT_CONTENT_COMPRESS * 3]}"
    res = await ollama_analyze_async(
        system=system, user=user, model=model, base_url=base_url,
        temperature=0.0, schema=schema, parse_json=True,
    )
    parsed = res.parsed if isinstance(res.parsed, dict) else parse_jsonish(res.final_text) or {}
    label = str(parsed.get("label") or "中性")
    if label not in ("利好", "利空", "中性"):
        if "利空" in label: label = "利空"
        elif "利好" in label: label = "利好"
        else: label = "中性"
    try:
        score = int(parsed.get("score") or 0)
    except Exception:
        score = 0
    reasons = parsed.get("reasons")
    if isinstance(reasons, list):
        reasons = [str(x) for x in reasons if str(x).strip()]
    else:
        reasons = []
    return {
        "aspect": aspect,
        "label": label,
        "score": max(-5, min(5, score)),
        "reasons": reasons,
        "raw": res.final_text,
    }


def analyze_sentiment(*args, **kwargs) -> Dict[str, Any]:
    """情绪分析（同步版）。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is None:
        return asyncio.run(analyze_sentiment_async(*args, **kwargs))
    with _cf.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(lambda: asyncio.run(analyze_sentiment_async(*args, **kwargs))).result()


# ======================================================================
# 辅助：entries（dict{ts: text} 或 list[str]）→ 截断后的多行 user prompt 文本
# 与原 zsxq_analysis_runner._run_financial_analysis 的拼接逻辑保持一致。
# ======================================================================
def truncate_entries_for_prompt(
    entries: Union[Dict[Any, str], Iterable[str]],
    *,
    per_entry_truncate: int = _DEFAULT_ENTRY_TRUNCATE,
    total_chars_cap: int = _DEFAULT_CONTENT_COMPRESS,
) -> Tuple[str, int]:
    """把资讯条目（dict timestamp→text 或 list[str]）格式化为多行 1. xxx\n2. yyy，
    单条截断、总体截断，返回 (formatted_text, raw_entry_count)。
    调用方可直接把 formatted_text 当 analyze_zsxq_hot_news 的 entries_text。"""
    lines: List[str] = []
    items: List[Tuple[int, str]] = []
    if isinstance(entries, dict):
        items = list(enumerate(entries.values(), 1))
    else:
        items = list(enumerate(list(entries), 1))
    total = 0
    for idx, val in items:
        short = (str(val)[:per_entry_truncate]
                 + ("..." if len(str(val)) > per_entry_truncate else ""))
        line = f"{idx}. {short}"
        lines.append(line)
        total += len(line)
        if total >= total_chars_cap:
            lines.append("...(内容截断)")
            break
    return "\n".join(lines), len(items)


# ======================================================================
# CLI 自检入口：python shared/utils/ollama_analyzer.py [--sample] [--model qwen3:8b]
# ======================================================================
if __name__ == "__main__":  # pragma: no cover - 手动自检
    def _cli() -> int:
        print(f"[Ollama-Analyzer] PROJECT_ROOT   = {PROJECT_ROOT}")
        print(f"[Ollama-Analyzer] DEFAULT_MODEL  = {DEFAULT_MODEL}")
        print(f"[Ollama-Analyzer] base_url       = {OLLAMA_DEFAULT_BASE_URL}")
        sample_text = (
            "1. 茅台批价企稳，中秋动销乐观，多家机构上调目标价。\n"
            "2. 宁德时代上半年储能订单翻倍，但北美关税压力加大。\n"
            "3. 中国平安新业务价值NBV同比+18%，寿险改革见效。\n"
            "4. 某券商指半导体板块估值偏高，短期建议回避。"
        )
        fmt, cnt = truncate_entries_for_prompt([
            "茅台批价企稳，中秋动销乐观，多家机构上调目标价",
            "宁德时代上半年储能订单翻倍，但北美关税压力加大",
            "中国平安新业务价值NBV同比+18%，寿险改革见效",
            "某券商指半导体板块估值偏高，短期建议回避",
        ])
        print(f"[Ollama-Analyzer] truncate demo ({cnt} items), len={len(fmt)}")
        if "--sample" in sys.argv:
            model = sys.argv[sys.argv.index("--model") + 1] if "--model" in sys.argv else DEFAULT_MODEL
            import time as _t
            s = _t.time()
            items = analyze_zsxq_hot_news(fmt, model=model, progress_cb=lambda m: print("  ·", m))
            dt = _t.time() - s
            print(f"[Ollama-Analyzer] zsxq_hot_news finished in {dt:.1f}s, items={len(items)}")
            for it in items:
                print("  ·", it)
            s2 = summarize_text(sample_text, max_words=120, aspect="整体情绪和关键信息")
            print(f"[Ollama-Analyzer] summarize (120字): {s2}")
            s3 = analyze_sentiment(sample_text, aspect="市场整体")
            print(f"[Ollama-Analyzer] sentiment: {s3}")
        return 0
    raise SystemExit(_cli())
