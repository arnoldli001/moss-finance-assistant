# -*- coding: utf-8 -*-
"""
adapter.ollama_client：本地 Ollama qwen-8b 流式客户端（§3 协议归一化 + §4 编排整合推理）。

对外两个 API：
  - async ollama_chat_stream(messages, ...) -> AsyncIterator[NormalizedChunk]
    直接把 Ollama /api/chat stream 用 ThinkTagSplitter 归一化，产出 reasoning/delta 块。
  - async ollama_synthesize(user_query: str,
                            retrieved_context_block: str,
                            *, reasoning_cb, delta_cb, citations_cb,
                            thread_id, bus) -> (final_text, used_citation_indices)
    编排层 §4 的"最终整合推理"：把 3 路检索 [citation:N] 上下文拼进 system prompt，
    让 qwen-8b 做"先思考后引用 [N]"最终答案，回调 bus.ev_reasoning / ev_delta，
    并且在每段 delta 中提取引用编号，供 bus 下发 citation_meta 增量映射。

说明：Ollama /api/chat stream 协议
  POST {model:str, messages:[{role,content}], stream:true}
  每一行 {"message":{"role":"assistant","content": "增量"}, done:bool, ...}
  对于 qwen3:8b 开启 <think>，content 内部会包含 <think> 标签文本；
  客户端用 ThinkTagSplitter 统一剥离，不依赖 Ollama 端未来是否暴露 reasoning_content 字段。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

import urllib.request as _urlreq

from .stream_adapters import (
    NormalizedChunk,
    ThinkTagSplitter,
    extract_citations_from_delta,
    patch_system_prompt_require_reasoning_and_citations,
)

logger = logging.getLogger(__name__)


# ---- 配置集中引用（避免循环，直接 import constants）-------------------------
def _cfg(name: str, default: Any) -> Any:
    try:
        from config import constants as C
        return getattr(C, name, default)
    except Exception:
        return default


_DEFAULT_BASE_URL = "http://localhost:11434"
_DEFAULT_MODEL = "qwen3:8b"
_DEFAULT_TIMEOUT = 300
_DEFAULT_TEMP = 0.2


# ======================================================================
# §3.8 归一化流式客户端：把 Ollama /api/chat stream → NormalizedChunk 序列
# ======================================================================

async def ollama_chat_stream(
    messages: List[Dict[str, str]],
    *,
    model: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout: Optional[float] = None,
    temperature: Optional[float] = None,
) -> AsyncIterator[NormalizedChunk]:
    """
    以 AsyncGenerator 形式返回 NormalizedChunk，自动跨 chunk 剥离 <think>。

    调用方注意：使用 async for chunk in ollama_chat_stream(...)。
    """
    base_url = (base_url or _cfg("OLLAMA_DEFAULT_BASE_URL", _DEFAULT_BASE_URL)).rstrip("/")
    model = model or _cfg("MODEL_ROUTER_LOCAL_MODEL", _DEFAULT_MODEL)
    timeout = timeout if timeout is not None else float(_cfg("OLLAMA_CHAT_DEFAULT_TIMEOUT_SEC", _DEFAULT_TIMEOUT))
    temperature = (
        temperature if temperature is not None
        else float(_cfg("OLLAMA_DEFAULT_TEMPERATURE", _DEFAULT_TEMP))
    )
    url = base_url + "/api/chat"
    payload = json.dumps({
        "model": model,
        "messages": list(messages),
        "stream": True,
        "options": {
            "temperature": temperature,
        },
    }).encode("utf-8")
    req = _urlreq.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/x-ndjson",
            "User-Agent": "moss-finance-assistant/1.0",
        },
    )

    splitter = ThinkTagSplitter()

    # 用 loop.run_in_executor 跑阻塞 urllib，避免阻塞事件循环（Ollama 本地 CPU-bound）
    loop = asyncio.get_running_loop()

    def _sync_reader(queue: asyncio.Queue) -> None:
        """运行在执行器线程：流式读 response line → 塞进 queue。"""
        try:
            with _urlreq.urlopen(req, timeout=timeout) as resp:
                # 流式逐行（Ollama 用 newline 分隔 JSON）
                buf = bytearray()
                while True:
                    chunk = resp.read(4096)
                    if not chunk:
                        break
                    buf.extend(chunk)
                    # 按 \n 切开
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
                            logger.warning("ollama stream decode error: %s line=%s", e, line_bytes[:120])
                            continue
                        # 提取 message.content 增量
                        msg = obj.get("message") or {}
                        content = msg.get("content") or ""
                        done = bool(obj.get("done", False))
                        asyncio.run_coroutine_threadsafe(
                            queue.put(("delta", content, done)), loop
                        )
                        if done:
                            break
                # 尾部 if 还有残留字节（极少），decode 一下
                if buf:
                    try:
                        tail = buf.decode("utf-8")
                    except Exception:
                        tail = ""
                    if tail.strip():
                        asyncio.run_coroutine_threadsafe(
                            queue.put(("delta", tail, True)), loop
                        )
        except Exception as e:
            asyncio.run_coroutine_threadsafe(
                queue.put(("error", f"{type(e).__name__}: {e}", True)), loop
            )
        finally:
            asyncio.run_coroutine_threadsafe(
                queue.put(("end", "", True)), loop
            )

    queue: asyncio.Queue = asyncio.Queue()
    import concurrent.futures as cfut
    with cfut.ThreadPoolExecutor(max_workers=1, thread_name_prefix="ollama_reader") as ex:
        fut = ex.submit(_sync_reader, queue)
        reasoning_buf: List[str] = []
        while True:
            item = await queue.get()
            (kind, text, done_flag) = item
            if kind == "error":
                raise RuntimeError(text)
            if kind == "end":
                # 强制 splitter flush
                for nc in splitter.ingest("", is_final=True):
                    if nc.type == "reasoning":
                        reasoning_buf.append(nc.text)
                        yield nc
                    elif nc.type == "reasoning_end":
                        seg = "".join(reasoning_buf)
                        reasoning_buf.clear()
                        yield NormalizedChunk(type="reasoning_end", text=seg)
                    else:
                        yield nc
                break
            # kind == "delta"
            for nc in splitter.ingest(text, is_final=bool(done_flag)):
                if nc.type == "reasoning":
                    reasoning_buf.append(nc.text)
                    yield nc
                elif nc.type == "reasoning_end":
                    seg = "".join(reasoning_buf)
                    reasoning_buf.clear()
                    yield NormalizedChunk(type="reasoning_end", text=seg)
                elif nc.type == "delta":
                    norm_text, cits = extract_citations_from_delta(nc.text)
                    if cits:
                        # 保留 cits
                        nc2 = NormalizedChunk(type="delta", text=norm_text, citations=list(cits))
                        yield nc2
                    else:
                        if norm_text:
                            nc2 = NormalizedChunk(type="delta", text=norm_text)
                            yield nc2
                else:
                    yield nc
            if done_flag:
                break
        # 等 executor 收尾（防止线程泄漏）
        try:
            fut.result(timeout=2.0)
        except Exception as ex_err:
            logger.warning("ollama reader thread cleanup: %s", ex_err)


# ======================================================================
# §4.5 qwen-8b 最终整合：把 3 路检索 + Prompt 规范 合成最终答案（回调 bus）
# ======================================================================

@dataclass
class SynthesisResult:
    final_text: str
    used_citation_indices: List[int]   # 所有在最终正文中出现过的 [N]
    reasoning_segment: str             # 完整 <think> 思考（用户可查看）
    latency_ms: int
    usage: Dict[str, int]


async def ollama_synthesize(
    user_query: str,
    retrieved_context_block: str,
    *,
    system_prompt_tail: str = "",
    model: Optional[str] = None,
    base_url: Optional[str] = None,
    # 以下是 SSE 总线回调：传 None 就静音（测试场景）
    reasoning_segment_cb: Optional[Callable[[str, str], Any]] = None,
    delta_cb: Optional[Callable[[str], Any]] = None,
    citation_hit_cb: Optional[Callable[[List[int], str], Any]] = None,
) -> SynthesisResult:
    """qwen-8b 综合推理门面。

    参数:
      reasoning_segment_cb(stage, content)  每段 reasoning_end 触发一次
      delta_cb(text)                        每段正文增量触发一次（已做 [N] 归一化）
      citation_hit_cb([idxs], snippet)      每次遇到 [N] 角标时触发一次，片段可用于卡片 hover
    """
    t0 = time.monotonic()
    # System Prompt：要求"先 <think> 四段思考，再正文 [N] 引用"
    system_prompt = (
        "你是「MOSS 金融助理解答综合器」（本地离线推理，qwen-8b）。\n"
        "你的职责：基于用户问题和「参考文档」（已经带 [citation:N] 编号），"
        "先输出思考，再输出最终投资分析正文。\n"
        + system_prompt_tail
        + ("\n\n" + retrieved_context_block if retrieved_context_block else "")
    )
    system_prompt = patch_system_prompt_require_reasoning_and_citations(system_prompt)

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_query},
    ]
    final_buf: List[str] = []
    reasoning_buf: List[str] = []
    citations_seen: List[int] = []
    # 记录最近一段 delta 片段（用于 citation snippet）
    last_sentence_parts: List[str] = []

    async for nc in ollama_chat_stream(messages, model=model, base_url=base_url):
        if nc.type == "reasoning":
            reasoning_buf.append(nc.text)
        elif nc.type == "reasoning_end":
            seg = "".join(reasoning_buf)
            reasoning_buf.clear()
            if reasoning_segment_cb is not None:
                try:
                    reasoning_segment_cb("model_coT", seg)
                except Exception:
                    pass
        elif nc.type == "delta":
            if nc.text:
                final_buf.append(nc.text)
                last_sentence_parts.append(nc.text)
                if delta_cb is not None:
                    try:
                        delta_cb(nc.text)
                    except Exception:
                        pass
                if nc.citations:
                    # 用最近 200 字作为 hover snippet
                    seg_text = ("".join(last_sentence_parts))[-200:]
                    for idx in nc.citations:
                        if idx not in citations_seen:
                            citations_seen.append(idx)
                    if citation_hit_cb is not None:
                        try:
                            citation_hit_cb(list(nc.citations), seg_text)
                        except Exception:
                            pass
                    # 截断累积句尾：避免片段无限长
                    if len("".join(last_sentence_parts)) > 400:
                        last_sentence_parts = [("".join(last_sentence_parts))[-200:]]
    ms = int((time.monotonic() - t0) * 1000)
    final_text = "".join(final_buf)
    return SynthesisResult(
        final_text=final_text,
        used_citation_indices=citations_seen,
        reasoning_segment="".join(reasoning_buf),
        latency_ms=ms,
        usage={"completion_chars": len(final_text), "reasoning_chars": 0},
    )


async def ollama_probe(base_url: Optional[str] = None, timeout: float = 3.0) -> bool:
    """健康检查：返回本地 Ollama /api/tags 是否可达。"""
    base_url = (base_url or _cfg("OLLAMA_DEFAULT_BASE_URL", _DEFAULT_BASE_URL)).rstrip("/")
    try:
        req = _urlreq.Request(base_url + "/api/tags", method="GET")
        with _urlreq.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 500
    except Exception:
        return False
