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

    # 4) 综答流式 + TTFT 哨兵常量（2026-09-10：替代原 ainvoke 120s+50s 整墙傻等）
    from orchestration.workflows.analysis_workflow import (
        PREMARKET_FINAL_TTFT_GUARD_SEC, PREMARKET_FINAL_GEN_SEC,
        PREMARKET_FINAL_RETRY_BACKOFF_SEC, PREMARKET_FINAL_RETRY_TTFT_SEC,
        PREMARKET_FINAL_RETRY_GEN_SEC,
    )
    _worst = (PREMARKET_FINAL_TTFT_GUARD_SEC + PREMARKET_FINAL_RETRY_BACKOFF_SEC
              + PREMARKET_FINAL_RETRY_TTFT_SEC + PREMARKET_FINAL_RETRY_GEN_SEC)
    check("综答 TTFT 哨兵+退避预算 40+60+30+40=170s（含搜索 ~7s 守 180s 外墙）",
          PREMARKET_FINAL_TTFT_GUARD_SEC == 40.0 and PREMARKET_FINAL_GEN_SEC == 60.0
          and PREMARKET_FINAL_RETRY_BACKOFF_SEC == 60.0
          and PREMARKET_FINAL_RETRY_TTFT_SEC == 30.0 and PREMARKET_FINAL_RETRY_GEN_SEC == 40.0
          and _worst + 7.0 <= 180.0)
    check("综答已改流式 asteam 哨兵（_afin_invoke 定义）",
          "_afin_invoke" in (Path(__file__).parent.parent /
                             "orchestration/workflows/analysis_workflow.py").read_text(encoding="utf-8"))

    print()
    if failures:
        print(f"结果：{len(failures)} 项失败 -> {failures}")
        sys.exit(1)
    print("结果：全部通过")


if __name__ == "__main__":
    main()
