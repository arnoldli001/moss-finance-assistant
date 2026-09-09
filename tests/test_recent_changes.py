#coding = utf-8
"""最近改动（2026-09-09）的回归护栏：目录迁移 / 存档机制 / include_answer。

不 import server.py（拉起全套 Actor 依赖太重），对其做源码标记断言。
运行：python tests/test_recent_changes.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

failures = []


def check(name, cond):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    if not cond:
        failures.append(name)


def main():
    # 1) 盘前新闻输出目录已迁移到 output/pre_market_news
    from orchestration.workflows.analysis_workflow import PRE_MARKET_DIR, PRE_MARKET_TTL_HOURS
    check("PRE_MARKET_DIR 位于 output/pre_market_news",
          PRE_MARKET_DIR.parts[-3:] == ("output", "pre_market_news") or
          str(PRE_MARKET_DIR).replace("\\", "/").endswith("output/pre_market_news"))
    check("盘前缓存 TTL 仍为 6 小时", PRE_MARKET_TTL_HOURS == 6)

    # 2) 复盘预测存档机制（server.py 源码标记断言）
    server_src = (Path(__file__).parent.parent / "interfaces" / "api" / "server.py").read_text(
        encoding="utf-8")
    check("RECAP_ARCHIVE_DIR 指向 output/Market_Recap_Outlook",
          'RECAP_ARCHIVE_DIR = Path(project_root) / "output" / "Market_Recap_Outlook"' in server_src)
    check("存档新鲜窗口 = 3 小时", "RECAP_ARCHIVE_FRESH_HOURS = 3.0" in server_src)
    check("存档文件名按创建时间（YYYYMMDDHHMMSS.md）",
          "%Y%m%d%H%M%S" in server_src and '_archive_fp.write_text' in server_src)
    check("仅成功综答才写档（_ds_ok 门控）",
          "if _ds_ok and analysis_result.strip():" in server_src)
    check("dedup 入口先查 3h 存档（命中即 report_task_result）",
          "monitor.report_task_result(_cached)" in server_src)
    check("存档命中后写会话历史",
          "await _save_zsxq_to_history(thread_id, _cached)" in server_src)

    # 3) Tavily include_answer（两份实现同步）
    for rel in ("shared/data_sources/web_search.py", "tools/tavily_tool.py"):
        src = (Path(__file__).parent.parent / rel).read_text(encoding="utf-8")
        check(f"{rel} 已启用 include_answer=True", "include_answer=True" in src)

    # 4) 综答流式 + 流停滞检测 + N发重试 + 300s 专用外墙（2026-09-10：暗推理根因定位后的最终方案）
    from config.constants import (
        PREMARKET_TASK_TIMEOUT_SEC, PREMARKET_FINAL_LLM_CLIENT_TIMEOUT_SEC,
        PREMARKET_FINAL_ATTEMPT_TOTAL_SEC, PREMARKET_FINAL_ATTEMPTS,
        PREMARKET_FINAL_STALL_SEC, PREMARKET_FINAL_RETRY_BACKOFF_SEC,
    )
    check("盘前专用外墙 300s + 专用客户端 read 超时 75s（>暗推理窗口，破 60s 硬顶）",
          PREMARKET_TASK_TIMEOUT_SEC == 300.0 and PREMARKET_FINAL_LLM_CLIENT_TIMEOUT_SEC == 75)
    _all_fail = (PREMARKET_FINAL_ATTEMPTS * PREMARKET_FINAL_ATTEMPT_TOTAL_SEC
                 + (PREMARKET_FINAL_ATTEMPTS - 1) * PREMARKET_FINAL_RETRY_BACKOFF_SEC)
    check(f"综答预算 {PREMARKET_FINAL_ATTEMPTS:.0f}发×{PREMARKET_FINAL_ATTEMPT_TOTAL_SEC:.0f}s"
          f"+退避{PREMARKET_FINAL_RETRY_BACKOFF_SEC:.0f}s（全灭{_all_fail:.0f}s +17s 开销 ≤ 300s 外墙）",
          PREMARKET_FINAL_ATTEMPTS == 2 and PREMARKET_FINAL_ATTEMPT_TOTAL_SEC == 120.0
          and PREMARKET_FINAL_STALL_SEC == 30.0 and PREMARKET_FINAL_RETRY_BACKOFF_SEC == 20.0
          and _all_fail + 17.0 <= PREMARKET_TASK_TIMEOUT_SEC)
    _wf_src = (Path(__file__).parent.parent /
               "orchestration/workflows/analysis_workflow.py").read_text(encoding="utf-8")
    check("综答使用专用模型实例 + 流停滞检测（_fin_model/_afin_invoke/_last_chunk）",
          "_premarket_final_model as _fin_model" in _wf_src
          and "_afin_invoke" in _wf_src and "_last_chunk" in _wf_src
          and "流停滞" in _wf_src)
    check("旧 TTFT 哨兵已彻底移除（防 IDE 缓冲回刷复发）",
          "TTFT超" not in _wf_src and "PREMARKET_FINAL_TTFT_GUARD_SEC" not in _wf_src)
    check("deepseek_client 已有盘前综答专用实例",
          "_premarket_final_model = init_chat_model" in
          (Path(__file__).parent.parent / "shared/llm_client/deepseek_client.py").read_text(encoding="utf-8"))
    _ds_src = (Path(__file__).parent.parent /
               "shared/llm_client/deepseek_client.py").read_text(encoding="utf-8")
    check("盘前综答实例已关闭思考模式（thinking disabled，暗推理根治）",
          '_premarket_final_model' in _ds_src
          and '"thinking": {"type": "disabled"}' in _ds_src)
    check("server.py 按钮/定时任务接入 300s 外墙",
          "_PREMARKET_TASK_TIMEOUT if _is_news_btn else _DEFAULT_AGENT_TIMEOUT" in server_src
          and '"scheduler_news_auto", "system", None, _PREMARKET_TASK_TIMEOUT' in server_src)
    # 5) DAG 内墙参数化（2026-09-10 事故：workflow 内部 180s shield 把三发综答整体击杀，
    #    server 端 300s 形同虚设——内墙必须能被盘前分支放宽）
    check("workflow DAG 内墙已参数化（dag_timeout_sec + _dag_wall 兜底 180s 常量）",
          "dag_timeout_sec: Optional[float] = None" in _wf_src
          and "_dag_wall = dag_timeout_sec or ANALYSIS_DAG_MAX_TIMEOUT_SEC" in _wf_src
          and "dag_timeout_sec=dag_timeout_sec" in _wf_src)
    check("server 盘前分支传 dag_timeout_sec=300（非盘前走默认 180s）",
          "dag_timeout_sec=(PREMARKET_TASK_TIMEOUT_SEC" in server_src
          and "if _is_premarket_news_query(query) else None" in server_src)

    print()
    if failures:
        print(f"结果：{len(failures)} 项失败 -> {failures}")
        sys.exit(1)
    print("结果：全部通过")


if __name__ == "__main__":
    main()
