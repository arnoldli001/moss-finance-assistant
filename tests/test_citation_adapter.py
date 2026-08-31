# -*- coding: utf-8 -*-
"""协议适配 + 引用 4 核心逻辑的轻量单元 smoke（不依赖外部 API）。

场景：
  1) ThinkTagSplitter 跨 chunk 剥离 <think>（模拟 Experience 401403 中"标签切分"）
  2) normalize_citation_markers：三写法 + markdown 链接不误匹配
  3) build_citation_context → 正文中注入 [citation:N]（模型 Prompt 用）
  4) assign_citations_by_overlap：模型完全未输出 [N] 时按 overlap 动态挂引用
  5) bus ev_retrieve_result / ev_citation_meta / ev_reasoning(stage) 三事件
       发布后 subscriber 收到且字段正确

运行：python tests/test_citation_adapter.py（纯同步 + 临时 asyncio.run，3 秒内）
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import List, Dict

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

PASS = 0
FAIL = 0


def expect(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name}  {detail}")


# =================================================================
# 场景 1: ThinkTagSplitter 跨 chunk 剥离 <think>
# =================================================================
def test_think_tag_splitter():
    print("\n== 场景 1: ThinkTagSplitter 跨 chunk ==")
    from adapter.stream_adapters import ThinkTagSplitter, NormalizedChunk
    sp = ThinkTagSplitter()
    # 模拟 chunk 切分：'<th' + 'ink>思考1思考2</th' + 'ink>正文片段'
    chunks_in = ['<th', 'ink>第一步：识别意图\n第二步：选 Tavily\n</th', 'ink>正文根据[1]显示利多。']
    out: List[NormalizedChunk] = []
    for c in chunks_in:
        out.extend(list(sp.ingest(c, is_final=False)))
    out.extend(list(sp.ingest("", is_final=True)))
    types = [x.type for x in out]
    reasoning_text = "".join([x.text for x in out if x.type == "reasoning"])
    delta_text = "".join([x.text for x in out if x.type == "delta"])
    expect("含 reasoning 事件", "reasoning" in types)
    expect("含 delta 事件",   "delta" in types)
    expect("reasoning 文本正确", "第一步：识别意图" in reasoning_text and "第二步：选 Tavily" in reasoning_text,
           f"got={reasoning_text[:80]!r}")
    expect("正文 delta 不含 <think> 标签", "<think" not in delta_text and "ink>" not in delta_text,
           f"got={delta_text!r}")
    expect("正文 delta 保留角标 [1]", "[1]" in delta_text, f"got={delta_text!r}")
    # 另一个形态：<think/> ... </think/>
    sp2 = ThinkTagSplitter()
    out2 = list(sp2.ingest("<think/>模型 CoT 段 A</think/>答 B", is_final=True))
    t2 = "".join(x.text for x in out2 if x.type == "reasoning")
    d2 = "".join(x.text for x in out2 if x.type == "delta")
    expect("<think/>形态 reasoning 提取", "模型 CoT 段 A" in t2, f"got={t2!r}")
    expect("<think/>形态正文干净", "答 B" in d2 and "<" not in d2, f"got={d2!r}")


# =================================================================
# 场景 2: 引用角标归一 + Markdown 链接规避
# =================================================================
def test_normalize_citations():
    print("\n== 场景 2: 引用归一 [N] / [citation:N] / [[N]] / (N) + MD 链接规避 ==")
    from adapter.stream_adapters import normalize_citation_markers
    # 三种写法 + 规避 Markdown 链接
    raw = (
        "这是正文引用[citation:3]，还有一些[[5]]以及(2)写法。"
        "注意 [茅台酒](https://example.com/moutai) 和 [研报](https://pdf) 不是角标！"
        "最后再引用一次 [1] 结尾。"
    )
    new_text, cites_ordered = normalize_citation_markers(raw)
    expect("输出包含所有标准 [N]", all(f"[{i}]" in new_text for i in [1, 2, 3, 5]),
           f"new_text={new_text!r}")
    expect("旧写法被移除", "[citation:" not in new_text and "[[" not in new_text, f"new={new_text!r}")
    expect("Markdown 链接保留且不被误替换",
           "[茅台酒](https://example.com/moutai)" in new_text and "[研报](https://pdf)" in new_text,
           f"new={new_text!r}")
    expect("cited 顺序正确（按出现顺序）", cites_ordered == [3, 5, 2, 1], f"got={cites_ordered}")


# =================================================================
# 场景 3: build_citation_context 注入 [citation:N] 上下文
# =================================================================
def test_build_citation_context():
    print("\n== 场景 3: build_citation_context [citation:N] Prompt ==")
    from adapter.stream_adapters import build_citation_context
    docs: List[Dict] = [
        {"doc_id": "t1", "title": "茅台上半年净利同比增20%", "url": "https://news.cn/1",
         "content": "贵州茅台发布半年报，净利同比增20%超预期。", "channel": "tavily",
         "reliability": "可靠", "published_at": "2026-08-28"},
        {"doc_id": "i1", "title": "IMA 白酒行业报告", "content": "白酒估值PE 25倍，处于近5年均值之上。",
         "channel": "ima", "reliability": "可靠", "knowledge_base": "白酒行业"},
        {"doc_id": "z1", "title": "散户讨论茅台出货", "content": "吧里有大户在出茅台，大家小心。",
         "channel": "zsxq", "source_type": "forum"},
    ]
    norm_docs, block = build_citation_context(docs)
    expect("3 条被规范化", len(norm_docs) == 3)
    expect("索引 1..3 连续", [d.index for d in norm_docs] == [1, 2, 3])
    expect("Prompt 含 3 个 [citation:N]", all(f"[citation:{i}]" in block for i in [1, 2, 3]),
           f"block_head={block[:200]!r}")
    expect("Prompt 末尾要求'请用 [N] 角标引用'", "正文时请用 [N]" in block or "[N] 角标引用上述文档" in block)
    expect("ZSXQ channel=forum 的可靠性=待验证",
           any(d.channel == "zsxq" and "待验证" in d.reliability for d in norm_docs))


# =================================================================
# 场景 4: assign_citations_by_overlap 动态挂引用
# =================================================================
def test_assign_citations_by_overlap():
    print("\n== 场景 4: assign_citations_by_overlap fallback 动态挂 [N] ==")
    from adapter.stream_adapters import (
        build_citation_context, assign_citations_by_overlap, CitationDocument
    )
    # 构造 docs：关键词 茅台/净利/超预期 对应 doc1；白酒/PE/估值 对应 doc2；出货/大户 对应 doc3
    docs_dict = [
        {"title": "茅台上半年净利增20%超预期", "content": "贵州茅台发布半年报，净利同比增20%超预期",
         "channel": "tavily", "reliability": "可靠"},
        {"title": "白酒行业估值PE 25倍", "content": "白酒行业估值 PE 25 倍，高于近5年均值",
         "channel": "ima", "reliability": "可靠"},
        {"title": "散户讨论茅台大户出货", "content": "吧里有大户在出茅台，建议短线规避",
         "channel": "zsxq", "source_type": "forum", "reliability": "待验证"},
    ]
    norm_docs, _ = build_citation_context(docs_dict)
    # 测试句 1：茅台半年报业绩超预期 → 应 top=1
    r1 = assign_citations_by_overlap("茅台半年报业绩超预期，净利同比增20%。", norm_docs, top_k=1, min_score=0.02)
    expect("句子 1 命中 doc1(索引1)", r1 and r1[0] == 1, f"got={r1}")
    # 测试句 2：白酒行业估值 PE 偏高 → 应 doc2(索引2)
    r2 = assign_citations_by_overlap("白酒行业估值 PE 水平不低。", norm_docs, top_k=1, min_score=0.02)
    expect("句子 2 命中 doc2(索引2)", r2 and r2[0] == 2, f"got={r2}")
    # 测试句 3：大户出货 → 应 doc3(索引3)
    r3 = assign_citations_by_overlap("吧里大户正在出货。", norm_docs, top_k=1, min_score=0.02)
    expect("句子 3 命中 doc3(索引3)", r3 and r3[0] == 3, f"got={r3}")
    # 测试句 4：完全无关的句子 → 返回空 or score<min_score
    r4 = assign_citations_by_overlap("今天天气不错适合跑步。", norm_docs, top_k=1, min_score=0.2)
    expect("无关句 4 挂不到引用", r4 is None or len(r4) == 0, f"got={r4}")


# =================================================================
# 场景 5: bus 三事件 publish → subscriber 收到（内存总线 smoke）
# =================================================================
async def _async_bus_smoke():
    from api.stream_bus import StreamEventBus, new_event_id
    from api.stream_protocol import (
        StreamEventType, ReasoningPayload, RetrieveResultPayload,
        RetrieveResultItem, CitationMetaPayload, CitationMetaItem,
    )
    from dataclasses import asdict

    bus = StreamEventBus()
    thread_id = "t-smoke-001"
    bus.bind_loop(asyncio.get_running_loop())
    state = bus.get_thread_state(thread_id)

    received = {k: [] for k in ["retrieve_result", "citation_meta", "reasoning"]}

    async def _collect_frames(sub):
        # sub 是 async iterator：收集所有 SSE 文本帧，直到 publish 结束后取消
        try:
            async for frame in sub:
                # 解析 event: + data:
                lines = frame.split("\n")
                et = None
                data_json = ""
                for ln in lines:
                    if ln.startswith("event:"):
                        et = ln[len("event:"):].strip()
                    elif ln.startswith("data:"):
                        data_json = ln[len("data:"):].strip()
                if et and data_json:
                    import json as _json
                    try:
                        payload = _json.loads(data_json)
                    except Exception:
                        payload = None
                    if et in received:
                        received[et].append(payload)
        except Exception:
            # 取消或关闭正常退出
            return

    sub = bus.subscribe(thread_id)
    collect_task = asyncio.create_task(_collect_frames(sub))
    try:
        # 等 _register() 完成（subscribe 是异步注册，create_task 需要调度一次）
        await asyncio.sleep(0.05)
        # 5a. ev_retrieve_result（注意：items 是 Dict 列表，不是 RetrieveResultItem dataclass）
        items_dicts = [
            {"doc_id": "t1", "title": "A 新闻", "url": "https://a",
             "content": "内容1", "channel": "tavily", "reliability": "可靠", "score": 0.9},
            {"doc_id": "t2", "title": "B 新闻", "url": "https://b",
             "content": "内容2", "channel": "tavily", "score": 0.8},
        ]
        bus.ev_retrieve_result(thread_id, channel="tavily", query="贵州茅台新闻", items=items_dicts)
        # 5b. ev_citation_meta（引用映射 1→A 新闻、2→B 新闻）—— dataclass 列表
        cit_items = [
            CitationMetaItem(index=1, title="A 新闻", url="https://a",
                             channel="tavily", reliability="可靠", snippet="片段A"),
            CitationMetaItem(index=2, title="B 新闻", url="https://b",
                             channel="tavily", snippet="片段B"),
        ]
        bus.ev_citation_meta(thread_id, items=cit_items)
        # 5c. ev_reasoning 三阶段
        bus.ev_reasoning(thread_id, title="用户问的是个股新闻",
                         content="分析：用户明确提到'茅台新闻'→ 应走 Tavily + IMA 双通道",
                         stage="intent_classify")
        bus.ev_reasoning(thread_id, title="Tavily：搜贵州茅台 新闻",
                         content="query='贵州茅台 新闻 近一周' max_results=5；IMA 知识库搜白酒半年报",
                         stage="retrieval_plan")
        bus.ev_reasoning(thread_id, title="模型内部思考",
                         content="<think> 这是剥离后残留的检查（测试标题实际会替换） </think>",
                         stage="model_coT")
        # 让 subscriber 有时间收到 publish（内部 call_soon create_task）
        await asyncio.sleep(0.15)
    finally:
        collect_task.cancel()
        try:
            await collect_task
        except (asyncio.CancelledError, Exception):
            pass
        bus.unsubscribe(thread_id, sub)

    # Assert
    expect("retrieve_result 事件被收到", len(received["retrieve_result"]) == 1)
    if received["retrieve_result"]:
        p = received["retrieve_result"][0]
        expect("retrieve_result channel=tavily", p.get("channel") == "tavily")
        expect("retrieve_result items=2 条", isinstance(p.get("items"), list) and len(p["items"]) == 2)
        expect("检索池 state.retrieved_docs 已注册",
               len(state.iterate_all_retrieved_items()) >= 2)

    # 注意：ev_retrieve_result 末尾会自动 push 一批 pending citation_meta（来源池编号对齐），
    #       加上手动的 ev_citation_meta，所以 total ≥ 1 条。
    expect("citation_meta 事件被收到(≥1)", len(received["citation_meta"]) >= 1)
    if received["citation_meta"]:
        all_cit_items = []
        for p in received["citation_meta"]:
            if isinstance(p.get("items"), list):
                all_cit_items.extend(p["items"])
        expect("citation_meta 总条目 ≥ 2", len(all_cit_items) >= 2)
        indices = sorted(set(int(x.get("index")) for x in all_cit_items if x.get("index")))
        expect("索引 1,2 都出现", 1 in indices and 2 in indices, f"indices={indices}")
        # ThreadState citation_meta 注册状态
        meta_state = state.snapshot_all_citation_meta_items()
        expect("state._citation_meta 也已注册 ≥ 2 条", len(meta_state) >= 2)

    # reasoning：retrieve_result 内部也可能发 source_ref / citation_meta，但不会发出额外 reasoning，
    #            所以应该是严格 3 条（或 +1 条 tool_result 里可能的 reasoning？严格：事件名是 reasoning 才记）
    reasonings = received["reasoning"]
    expect("reasoning 事件收到 3 条", len(reasonings) == 3, f"got count={len(reasonings)} list={[(r.get('stage'), (r.get('title') or '')[:20]) for r in reasonings]}")
    if len(reasonings) == 3:
        stages = [r.get("stage") for r in reasonings]
        expect("stages 三段正确", stages == ["intent_classify", "retrieval_plan", "model_coT"],
               f"stages={stages}")
        # 每段 title 非空
        expect("每段 reasoning 都带 title", all(isinstance(r.get("title"), str) and r["title"] for r in reasonings))


def test_bus_events():
    print("\n== 场景 5: bus 三新增事件 publish/subscribe smoke ==")
    asyncio.run(_async_bus_smoke())


# =================================================================
# 主入口
# =================================================================
if __name__ == "__main__":
    t0 = time.time()
    test_think_tag_splitter()
    test_normalize_citations()
    test_build_citation_context()
    test_assign_citations_by_overlap()
    test_bus_events()
    dt = time.time() - t0
    print(f"\n======================================")
    print(f"结果: PASS={PASS}  FAIL={FAIL}  耗时={dt:.2f}s")
    sys.exit(0 if FAIL == 0 else 1)
