# -*- coding: utf-8 -*-
"""bench_ptd_tokens.py — PTD（渐进式工具披露）token 节省实测基准。

用项目真实工具 Schema 离线跑出可复现的量化数据，替代"估计约节省 X%"口径；
不调 LLM / 外部 API，CI 可复跑。

方法：对照组=全量 Schema 每轮注入；PTD 组=路由菜单(阶段零,固定)+选中子集 Schema(执行阶段)。
两条路径：a) 协议路由（模拟 __TOOL_ROUTE__ 输出解析）；b) 启发式兜底（query 关键词，离线确定性）。
token 计数优先 tiktoken(cl100k_base)，未安装退化为 chars/4（与 tool_router 估算口径一致）。

输出：控制台表格 + benchmarks/results/ptd_tokens_latest.json
运行：python benchmarks/bench_ptd_tokens.py
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# ---- 真源导入（shared/llm_client/tool_router.py 为 PTD 真实现）----
from shared.llm_client.tool_router import (  # noqa: E402
    build_route_menu,
    parse_route_output,
    heuristic_select_from_query,
    filter_tools_by_selected,
    _tool_name,
    ProgressiveToolDisclosureModel,
)

# ---- 项目真实工具（与主 Agent 实际注册一致）----
from tools.markdown_tools import generate_markdown  # noqa: E402
from tools.pdf_tools import convert_md_to_pdf  # noqa: E402
from tools.upload_file_read_tool import read_file_content  # noqa: E402
from tools.ragflow_tools import search_knowledge_base  # noqa: E402
from tools.tavily_tool import internet_search  # noqa: E402
from tools.db_tools import list_sql_tables, get_table_data, execute_sql_query  # noqa: E402

RESULTS_DIR = _HERE / "results"

# Windows GBK 控制台兜底：报告含 ✅ 等 emoji，避免 UnicodeEncodeError
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ======================================================================
# task 子调度工具 mock（与 tests/test_ptd.py 同款，deepagents 打包件离线替身）
# ======================================================================

class _MockTaskTool:
    name = "task"
    description = (
        "调用某个子智能体执行任务。参数 subagent_type 可选：网络搜索助手、数据库查询助手、RAGFlow助手。"
        "description 填任务的具体描述。"
    )

    def get_input_schema(self):
        return {
            "type": "object",
            "properties": {
                "subagent_type": {
                    "type": "string",
                    "enum": ["网络搜索助手", "数据库查询助手", "RAGFlow助手"],
                    "description": "要调用的子智能体名称",
                },
                "description": {"type": "string", "description": "子智能体要执行的任务描述"},
            },
            "required": ["subagent_type", "description"],
        }


# ======================================================================
# token 计数
# ======================================================================

def _make_token_counter():
    """优先 tiktoken 真实计数，退化为 chars/4（与 tool_router 估算口径一致）。"""
    try:
        import tiktoken  # type: ignore
        enc = tiktoken.get_encoding("cl100k_base")

        def _count(text: str) -> int:
            return len(enc.encode(text or ""))
        return _count, "tiktoken:cl100k_base"
    except Exception:
        return (lambda text: max(1, len(text or "") // 4)), "heuristic:chars_div_4"


TOKEN_COUNT, TOKEN_COUNTER_NAME = _make_token_counter()

# Schema estimator（真源实现，静态方法）
SCHEMA_ESTIMATOR = ProgressiveToolDisclosureModel.__dict__["_estimate_total_tool_schema_chars"]


def _schema_obj(t: Any) -> Dict[str, Any]:
    """取工具的 input schema 并保证 JSON 可序列化（兼容 pydantic 模型类 / dict / 异常）。"""
    try:
        schema = t.get_input_schema() if hasattr(t, "get_input_schema") else {}
        # pydantic 模型类 → 转成 dict（v2 用 model_json_schema，v1 用 schema()）
        if isinstance(schema, type):
            if hasattr(schema, "model_json_schema"):
                schema = schema.model_json_schema()
            else:
                schema = schema.schema()
        json.dumps(schema, ensure_ascii=False)  # 可序列化校验
        return schema
    except Exception:
        return {}


def _schema_json_chars(tools: List[Any]) -> int:
    """执行阶段实际注入的 function-calling JSON 字符数（与 estimator 同源逻辑）。"""
    return sum(len(json.dumps(_schema_obj(t), ensure_ascii=False)) for t in tools)


# ======================================================================
# 场景集：真实金融投研 query 分布
# ======================================================================

# (场景标签, 用户query, 模拟的协议路由输出 / None=走启发式)
SCENARIOS: List[Tuple[str, str]] = [
    ("新闻速览（联网搜索）",       "帮我搜一下最近贵州茅台的新闻，做个利空利多速览"),
    ("研报生成（搜索+MD）",        "搜索宁德时代最新公告和研报观点，整理成一份markdown速览报告"),
    ("PDF 导出",                  "把刚才那份估值分析导出成 PDF"),
    ("数据库查询",                "查一下内部数据库本月感冒药销售明细 top10"),
    ("知识库检索",                "在IMA知识库里搜国产替代相关的内部研报"),
    ("文件解析",                  "读取我上传的这个 excel 文件，分析里面的库存表"),
    ("混合（搜索+知识库+MD+PDF）", "联网搜最近半导体政策新闻，结合知识库研报，生成md并转成pdf给我"),
    ("闲聊（无工具需求）",         "你好，你能做什么？"),
    ("估值分析（多源）",           "搜索腾讯控股最新股价和新闻，查下数据库里的相关销售数据，做个估值对比"),
    ("纯知识星球",                "知识星球里关于比亚迪的小作文怎么看？"),
]


def _simulate_protocol_selection(query: str) -> List[str]:
    """模拟阶段零模型按协议选工具：对场景做确定性映射（模拟"理想路由"）。"""
    # 用启发式结果模拟一个理性模型的路由输出（模型比关键词更准，这里取保守下界）
    q = query.lower()
    ids: List[str] = []
    def has(*kws): return any(k in q for k in kws)
    if has("搜", "新闻", "公告", "股价", "联网"): ids.append("task/网络搜索助手")
    if has("数据库", "销售明细", "销售数据"): ids.append("task/数据库查询助手")
    if has("知识库", "ima", "内部研报"): ids.append("task/RAGFlow助手")
    if has("知识星球", "小作文"): ids.append("search_zsxq_by_stock")
    if has("excel", "上传", "读取"): ids.append("read_file_content")
    if has("markdown", "md", "报告", "速览"): ids.append("generate_markdown")
    if has("pdf"): ids.append("convert_md_to_pdf")
    return ids


# ======================================================================
# 基准执行
# ======================================================================

def build_tool_pools() -> Dict[str, List[Any]]:
    return {
        "主Agent真实工具池(4)": [
            generate_markdown, convert_md_to_pdf, read_file_content, _MockTaskTool(),
        ],
        "扩展工具池(9，含子Agent工具)": [
            generate_markdown, convert_md_to_pdf, read_file_content, _MockTaskTool(),
            internet_search, search_knowledge_base,
            list_sql_tables, get_table_data, execute_sql_query,
        ],
    }


def bench_pool(pool_name: str, all_tools: List[Any], route_menu: str) -> Dict[str, Any]:
    """对一个工具池跑全部场景，返回明细 + 汇总。"""
    full_schema_chars = SCHEMA_ESTIMATOR(all_tools)
    full_schema_json = _schema_json_chars(all_tools)
    menu_chars = len(route_menu)
    menu_tokens = TOKEN_COUNT(route_menu)

    details = []
    for label, query in SCENARIOS:
        # 路径A：协议路由（模拟模型输出 → parse → 虚拟→真实映射）
        proto_ids, _ = parse_route_output(f"__TOOL_ROUTE__: {json.dumps(_simulate_protocol_selection(query), ensure_ascii=False)}")
        proto_real = set()
        for i in proto_ids:
            from shared.llm_client.tool_router import VIRTUAL_TO_REAL
            proto_real.add(VIRTUAL_TO_REAL.get(i, i))
        if "convert_md_to_pdf" in proto_real:
            proto_real.add("generate_markdown")

        # 路径B：启发式兜底（离线确定性）
        heur_real = heuristic_select_from_query(query)

        row = {"scenario": label, "query": query}
        for path_name, selected in (("protocol", proto_real), ("heuristic", heur_real)):
            subset = filter_tools_by_selected(all_tools, selected)
            subset_chars = SCHEMA_ESTIMATOR(subset)
            subset_tokens = TOKEN_COUNT(json.dumps(
                [_schema_obj(t) for t in subset], ensure_ascii=False)) if subset else 0
            # PTD 每轮总开销 = 路由菜单(固定) + 子集 Schema(执行阶段)
            ptd_chars = menu_chars + subset_chars
            ptd_tokens = menu_tokens + subset_tokens
            full_tokens = TOKEN_COUNT(json.dumps(
                [_schema_obj(t) for t in all_tools], ensure_ascii=False))
            saving_chars = (1 - ptd_chars / full_schema_chars) * 100 if full_schema_chars else 0
            saving_tokens = (1 - ptd_tokens / full_tokens) * 100 if full_tokens else 0
            row[path_name] = {
                "selected_tools": sorted(selected),
                "n_selected": len(selected),
                "subset_schema_chars": subset_chars,
                "ptd_total_chars": ptd_chars,
                "ptd_total_tokens": ptd_tokens,
                "saving_pct_chars": round(saving_chars, 1),
                "saving_pct_tokens": round(saving_tokens, 1),
            }
        details.append(row)

    # 自适应门控结论：full_tokens <= menu_tokens 时 PTD 净收益为负（见 tool_router 自适应旁路）
    full_tokens = TOKEN_COUNT(json.dumps(
        [_schema_obj(t) for t in all_tools], ensure_ascii=False))
    adaptive_on = full_tokens <= menu_tokens

    def _stats(path: str, key: str) -> Dict[str, float]:
        vals = [d[path][key] for d in details]
        return {"min": round(min(vals), 1), "avg": round(statistics.mean(vals), 1),
                "max": round(max(vals), 1)}

    summary = {
        "full_schema_chars": full_schema_chars,
        "full_schema_json_chars": full_schema_json,
        "full_schema_tokens": full_tokens,
        "route_menu_chars": menu_chars,
        "route_menu_tokens": menu_tokens,
        "adaptive_gating_triggered": adaptive_on,
        "protocol_saving_pct_tokens": _stats("protocol", "saving_pct_tokens"),
        "heuristic_saving_pct_tokens": _stats("heuristic", "saving_pct_tokens"),
        "zero_hit_saving_pct_tokens": next(
            (d["heuristic"]["saving_pct_tokens"] for d in details if d["scenario"].startswith("闲聊")), None),
    }
    return {"pool": pool_name, "n_tools": len(all_tools), "summary": summary, "details": details}


def print_report(pool_name: str, result: Dict[str, Any]) -> None:
    s = result["summary"]
    print(f"\n{'=' * 72}")
    print(f"【{pool_name}】工具数={result['n_tools']}")
    print(f"{'=' * 72}")
    print(f"  全量披露 Schema: {s['full_schema_chars']} chars / "
          f"{s['full_schema_tokens']} tokens (tiktoken={TOKEN_COUNTER_NAME.startswith('tiktoken')})")
    print(f"  PTD 路由菜单(固定): {s['route_menu_chars']} chars / {s['route_menu_tokens']} tokens")
    print(f"\n  {'场景':<24}{'协议路由节省%':>14}{'启发式节省%':>14}   选中工具")
    for d in result["details"]:
        p = d["protocol"]["saving_pct_tokens"]
        h = d["heuristic"]["saving_pct_tokens"]
        tools = ",".join(d["heuristic"]["selected_tools"]) or "(无)"
        print(f"  {d['scenario']:<26}{p:>12}{h:>14}   {tools[:40]}")
    ps, hs = s["protocol_saving_pct_tokens"], s["heuristic_saving_pct_tokens"]
    print(f"\n  汇总(token口径): 协议路由 min/avg/max = {ps['min']}/{ps['avg']}/{ps['max']}%"
          f" | 启发式 = {hs['min']}/{hs['avg']}/{hs['max']}%")
    if s["zero_hit_saving_pct_tokens"] is not None:
        print(f"  零命中场景（闲聊）: 节省 {s['zero_hit_saving_pct_tokens']}%")
    if s["adaptive_gating_triggered"]:
        print("  【注意】自适应门控触发: 全量Schema tokens <= 路由菜单 tokens → PTD 负优化，"
              "运行时自动旁路（tool_router 自适应门控），全量披露才是最优解")
    else:
        print("  ✅ PTD 净收益为正，自适应门控不触发")


def main() -> int:
    t0 = time.monotonic()
    print("PTD token 节省实测（离线，不调 LLM）")
    print(f"token 计数器: {TOKEN_COUNTER_NAME}")

    route_menu = build_route_menu()
    pools = build_tool_pools()

    results = {}
    for name, tools in pools.items():
        results[name] = bench_pool(name, tools, route_menu)
        print_report(name, results[name])

    # 每工具 Schema 开销明细（写报告用）
    per_tool = {}
    for name, tools in pools.items():
        if name.startswith("扩展"):
            per_tool = {_tool_name(t): SCHEMA_ESTIMATOR([t]) for t in tools}

    out = {
        "benchmark": "ptd_token_savings",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "token_counter": TOKEN_COUNTER_NAME,
        "method": {
            "对照组": "全量工具 Schema 每轮注入",
            "PTD组": "路由菜单(阶段零,固定) + 选中子集 Schema(执行阶段)",
            "路径": ["protocol(模拟模型按__TOOL_ROUTE__协议路由)",
                     "heuristic(关键词启发式兜底,离线确定性)"],
            "说明": "扩展工具池仅用于规模化趋势演示；主Agent真实运行时工具池为4个",
        },
        "per_tool_schema_chars": per_tool,
        "results": results,
        "elapsed_sec": round(time.monotonic() - t0, 2),
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULTS_DIR / "ptd_tokens_latest.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n结果已写入: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
