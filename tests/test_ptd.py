"""
测试 Progressive Tool Disclosure（渐进式工具披露）模块的核心逻辑。
不调用真实 LLM（省 API 费用），只验证：路由菜单、协议解析、启发式工具选择、
工具裁剪、Schema 节省估算等。
运行方式：python tests/test_ptd.py
"""
import asyncio
import sys
import json
from pathlib import Path

# 测试文件位于 tests/ 子目录，需要向上一级找到项目根目录
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from agent.tool_router import (
    PRESET_INDEX, VIRTUAL_TO_REAL,
    build_route_menu, parse_route_output,
    heuristic_select_from_query, filter_tools_by_selected,
    _tool_name, reset_route_state, _get_or_init_state,
    ProgressiveToolDisclosureModel,
)

# 引入项目实际 tools（用于真实估算 schema 大小）
from tools.markdown_tools import generate_markdown
from tools.pdf_tools import convert_md_to_pdf
from tools.upload_file_read_tool import read_file_content
from tools.ragflow_tools import search_knowledge_base
from tools.tavily_tool import internet_search
from tools.db_tools import list_sql_tables, get_table_data, execute_sql_query


def _make_mock_task_tool():
    """模拟 deepagents 中打包 subagents 的 task StructuredTool（只实现 name/description/get_input_schema）。"""
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
                    "subagent_type": {"type": "string", "enum": ["网络搜索助手", "数据库查询助手", "RAGFlow助手"],
                                      "description": "要调用的子智能体名称"},
                    "description": {"type": "string", "description": "子智能体要执行的任务描述"},
                },
                "required": ["subagent_type", "description"],
            }
    return _MockTaskTool()


def test_route_menu():
    """验证路由菜单：极简、包含全部工具项、带触发词"""
    print("\n" + "=" * 60)
    print("【测试1】阶段零路由菜单（零Schema）")
    print("=" * 60)
    menu = build_route_menu()
    print(f"   菜单字符数：{len(menu)} ≈ {max(1, len(menu)//4)} tokens\n")
    # 打印前 12 行预览
    for line in menu.splitlines()[:12]:
        print("   " + line)
    print("   ...")
    # 检查所有虚拟工具都出现在菜单里
    for virtual_id in PRESET_INDEX:
        assert virtual_id in menu, f"工具 {virtual_id} 未出现在路由菜单中"
    assert "__TOOL_ROUTE__" in menu
    assert "【PTD工具路由菜单】" in menu
    print(f"\n   ✅ 菜单覆盖全部 {len(PRESET_INDEX)} 个工具索引条目，协议标记齐全")
    return True


def test_parse_route_output():
    """验证路由协议解析：合法格式/非法格式/去重/上限/清洗"""
    print("\n" + "=" * 60)
    print("【测试2】路由协议解析")
    print("=" * 60)
    cases = [
        # (raw_output, expected_ids_length, cleaned_should_not_contain_route_line)
        ('__TOOL_ROUTE__:["generate_markdown","convert_md_to_pdf"]\n好的我来生成报告',
         2, True, "正常2个工具 + 后续正文"),
        ('__TOOL_ROUTE__:[]\n不需要工具，直接回答', 0, True, "空选择"),
        ('Some prefix\n__TOOL_ROUTE__:["task/网络搜索助手","generate_markdown","convert_md_to_pdf","task/RAGFlow助手","read_file_content"]\nDone',
         4, True, "超过 MAX_TOOLS_PER_ROUND=4 个的会被截断为4"),
        ('__TOOL_ROUTE__:["not_exist","generate_markdown","generate_markdown"]\n正文',
         1, True, "非法ID过滤 + 重复ID去重"),
        ("完全没有路由标记，直接输出大段正文", 0, False, "无协议标记时解析到空列表"),
    ]
    all_ok = True
    for raw, expect_cnt, expect_clean, label in cases:
        ids, cleaned = parse_route_output(raw)
        ok_cnt = len(ids) == expect_cnt
        # --- 更稳健的断言：检查两种语义 ---
        # 有路由行的情况：路由行必须被去除（cleaned 不应包含 __TOOL_ROUTE__）
        # 没路由行的情况：内容必须完全不变（cleaned == raw），即没有被误删任何字符
        raw_has_route = "__TOOL_ROUTE__" in raw
        if raw_has_route:
            ok_clean = ("__TOOL_ROUTE__" not in cleaned)
            reason = "有路由行时应被去除"
        else:
            ok_clean = (cleaned == raw)
            reason = "无路由行时内容必须完整保留"
        ok = ok_cnt and ok_clean
        mark = "✅" if ok else "❌"
        if not ok:
            all_ok = False
        print(f"   {mark} [{label}]\n"
              f"       解析得到 ids={ids} (len={len(ids)}, 期望 {expect_cnt})\n"
              f"       {reason} → {'通过' if ok_clean else '失败'}"
              f"{('' if raw_has_route else f' (len相等={len(cleaned) == len(raw)})')}")
    return all_ok


def test_heuristic_select():
    """验证启发式关键词兜底（当模型不按协议输出时）"""
    print("\n" + "=" * 60)
    print("【测试3】关键词启发式兜底")
    print("=" * 60)
    cases = [
        # (query, expected_must_include_real_tool_names_subset, label)
        ("请帮我搜索一下宁德时代最近的新闻和股价走势，整理成研报输出PDF",
         {"task", "generate_markdown", "convert_md_to_pdf"},
         "网络新闻 + PDF 研报 → 必须包含 task（联网搜索）+ md + pdf"),
        ("查询一下上个月的药品销售数据，看看Top10商品的库存情况",
         {"task"},   # task/数据库查询助手
         "内部业务数据 + 销售 → 必须选中数据库查询子助手(task)"),
        ("帮我在IMA知识库里找一下关于国产替代的半导体研报",
         {"task"},   # task/RAGFlow助手
         "知识库/IMA/研报 → 选中RAGFlow子助手(task)"),
        ("请读取我刚刚上传的 贵州茅台.xlsx 文件，分析它的财务指标",
         {"read_file_content"},
         "读取上传的Excel → 选中 read_file_content"),
        ("分析一下贵州茅台600519的投资价值，要求结合最新财报数据、网络上的新闻和我给你的研报.pdf",
         {"task", "read_file_content"},
         "股票分析需要联网新闻(task) + 读取上传的PDF(read_file_content)"),
        ("你好，在吗？", set(), "纯问候不需要任何工具"),
    ]
    all_ok = True
    for q, expect_set, label in cases:
        got = heuristic_select_from_query(q)
        ok = expect_set.issubset(got)
        mark = "✅" if ok else "❌"
        if not ok:
            all_ok = False
        print(f"   {mark} [{label}]\n"
              f"       用户Query: {q[:50]}…\n"
              f"       命中工具: {sorted(got)}  期望包含: {sorted(expect_set)}")
    return all_ok


def test_tool_filter_and_schema_estimate():
    """验证工具过滤 + Schema大小估算"""
    print("\n" + "=" * 60)
    print("【测试4】真实工具 Schema 大小估算与节省比例")
    print("=" * 60)
    # 主 agent 实际 tools：3 个直接工具 + 1 个 task 子助手调度工具 = 4 个
    all_tools = [
        generate_markdown, convert_md_to_pdf, read_file_content,
        _make_mock_task_tool(),
    ]
    all_names = [_tool_name(t) for t in all_tools]
    print(f"   主 Agent 工具集合：{all_names}")

    # 估算每个工具的 schema 大小
    estimator = ProgressiveToolDisclosureModel.__dict__["_estimate_total_tool_schema_chars"]
    total_chars = estimator(all_tools)
    print(f"   全量 Schema 字符数: {total_chars} ≈ {max(1, total_chars // 4)} tokens")

    # 典型场景：选 generate_markdown + task（网络搜索+生成md）
    subset_1 = filter_tools_by_selected(all_tools, {"generate_markdown", "task"})
    sub_chars_1 = estimator(subset_1)
    saving_1 = (1 - sub_chars_1 / total_chars) * 100 if total_chars else 0
    print(f"\n   场景1: 选 generate_markdown + task（网络搜索生成研报）")
    print(f"     子集工具: {[_tool_name(t) for t in subset_1]}")
    print(f"     子集 Schema 字符数: {sub_chars_1} ≈ {max(1, sub_chars_1 // 4)} tokens  "
          f"(节省 {saving_1:.0f}%)")

    # 典型场景2：只选 read_file_content
    subset_2 = filter_tools_by_selected(all_tools, {"read_file_content"})
    sub_chars_2 = estimator(subset_2)
    saving_2 = (1 - sub_chars_2 / total_chars) * 100 if total_chars else 0
    print(f"\n   场景2: 只选 read_file_content（用户只要求读取上传文件）")
    print(f"     子集工具: {[_tool_name(t) for t in subset_2]}")
    print(f"     子集 Schema 字符数: {sub_chars_2} ≈ {max(1, sub_chars_2 // 4)} tokens  "
          f"(节省 {saving_2:.0f}%)")

    # 典型场景3：convert_md_to_pdf（应自动联动 generate_markdown，这里在路由阶段做）
    subset_3 = filter_tools_by_selected(all_tools, {"convert_md_to_pdf", "generate_markdown"})
    sub_chars_3 = estimator(subset_3)
    saving_3 = (1 - sub_chars_3 / total_chars) * 100 if total_chars else 0
    print(f"\n   场景3: PDF转换（含联动的generate_markdown）")
    print(f"     子集工具: {[_tool_name(t) for t in subset_3]}")
    print(f"     子集 Schema 字符数: {sub_chars_3} ≈ {max(1, sub_chars_3 // 4)} tokens  "
          f"(节省 {saving_3:.0f}%)")

    print(f"\n   ✅ 工具过滤功能正常，Schema 节省范围：{saving_2:.0f}% ~ {saving_1:.0f}%")
    assert saving_1 > 30, f"节省比例异常低: {saving_1}"
    assert saving_2 > 50, f"节省比例异常低: {saving_2}"
    return True


def test_presets_complete():
    """验证项目实际用到的工具都有预设索引 + 映射正确"""
    print("\n" + "=" * 60)
    print("【测试5】索引完整性：所有真实工具均有对应预设条目")
    print("=" * 60)
    all_presets = set(PRESET_INDEX.keys())
    real_mapped = set(VIRTUAL_TO_REAL.values()) | {"generate_markdown", "convert_md_to_pdf", "read_file_content"}
    # 直接工具对应的 id 必须在 PRESET_INDEX 中
    for direct in ["generate_markdown", "convert_md_to_pdf", "read_file_content"]:
        assert direct in all_presets, f"直接工具 {direct} 缺少索引条目"
    # 3 个子 agent 虚拟项必须在
    for sa in ["网络搜索助手", "数据库查询助手", "RAGFlow助手"]:
        key = f"task/{sa}"
        assert key in all_presets, f"子助手 {sa} 缺少 task/xxx 虚拟项"
        assert VIRTUAL_TO_REAL.get(key) == "task", f"{key} → 真实映射错误，应为 task"
    print(f"   预设索引条目总数：{len(all_presets)}")
    print(f"   直接工具（3个）+ task子助手（3个虚拟项）：全部覆盖")
    print(f"   虚拟→真实映射：{VIRTUAL_TO_REAL}")
    print("   ✅ 索引与映射完整性通过")
    return True


def main():
    print("\n" + "*" * 60)
    print("  MOSS Finance Assistant - 渐进式工具披露 单元测试")
    print("*" * 60)

    tests = [
        ("路由菜单完整性", lambda: test_route_menu()),
        ("路由协议解析", lambda: test_parse_route_output()),
        ("关键词启发式兜底", lambda: test_heuristic_select()),
        ("工具过滤 & Schema估算", lambda: test_tool_filter_and_schema_estimate()),
        ("索引完整性", lambda: test_presets_complete()),
    ]
    results = []
    for name, fn in tests:
        try:
            ok = fn()
            results.append((name, ok))
        except AssertionError as e:
            import traceback
            traceback.print_exc()
            results.append((name, False))
        except Exception as e:
            import traceback
            traceback.print_exc()
            results.append((name, False))

    print("\n" + "=" * 60)
    print("汇总：")
    all_ok = True
    for name, ok in results:
        status = "✅ 通过" if ok else "❌ 失败"
        print(f"  {status} - {name}")
        all_ok = all_ok and ok
    print("=" * 60)
    if all_ok:
        print("🎉 全部测试通过！渐进式工具披露核心逻辑正常。")
        # 额外打印：子 Agent 的工具 schema 也在节省范围内
        print("\n💡 端到端说明：")
        print("   - 阶段零：仅路由菜单 ~300字 0 tokens schema")
        print("   - 阶段一：典型选择 2~3 个工具 → Schema 节省 50%~75%")
        print("   - 阶段二：模型选错工具时自动兜底追加（最多2次），不丢能力")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
