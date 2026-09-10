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
    check("盘前缓存 TTL 为 2 小时（2026-09-10 由 6h 缩短）", PRE_MARKET_TTL_HOURS == 2)

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
          "await _save_zsxq_to_history(thread_id, _cached, user_label=\"复盘预测\")" in server_src)

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
    check("美股表格固定 7 行骨架 + 时效铁律（根治表头挤行/当日数据被拒填）",
          "| 美光 |  |  |" in _wf_src and "| 英伟达 |  |  |" in _wf_src
          and "时效铁律" in _wf_src and "隔夜收盘/盘前行情" in _wf_src)
    # markdown 标题规范化后处理（low-effort 模型吞换行的确定性兜底）
    from orchestration.workflows.analysis_workflow import _normalize_premarket_markdown
    _fixed = _normalize_premarket_markdown(
        "###2.美股表现|股票 |涨跌幅 |新闻 |\n\n###3.推理与预测**利好：**\n")
    check("标题规范化兜底（拼行拆分 + # 后补空格）",
          "### 2.美股表现\n\n|股票 |涨跌幅 |新闻 |" in _fixed
          and "### 3.推理与预测\n\n**利好：**" in _fixed)
    check("美股隔夜实时行情注入（新浪 gb_ 接口，权威涨跌幅替代新闻搜索）",
          "_fetch_us_quotes_block_sync" in _wf_src
          and "hq.sinajs.cn/list=" in _wf_src
          and "美股隔夜收盘实时行情" in _wf_src
          and "_quotes_task" in _wf_src)
    check("deepseek_client 已有盘前综答专用实例",
          "_premarket_final_model = init_chat_model" in
          (Path(__file__).parent.parent / "shared/llm_client/deepseek_client.py").read_text(encoding="utf-8"))
    _ds_src = (Path(__file__).parent.parent /
               "shared/llm_client/deepseek_client.py").read_text(encoding="utf-8")
    check("盘前综答实例思考强度=low（防回退 high 暗推理超时 / disabled 整合力丧失）",
          '_premarket_final_model' in _ds_src
          and 'reasoning_effort="low"' in _ds_src
          and '"thinking": {"type": "disabled"}' not in _ds_src)
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

    # 5) 个股检索上下文落库修复（2026-09-10：注入长文曾作为 HumanMessage 落 checkpointer，
    #    历史恢复时撑爆用户气泡、无排版，且事后补落的原话气泡顺序颠倒）
    import inspect as _inspect
    from agents.analyst.agent import run_deep_agent as _rda
    _agent_src = (Path(__file__).parent.parent / "agents" / "analyst" / "agent.py"
                  ).read_text(encoding="utf-8")
    check("run_deep_agent 支持 injected_context（检索上下文走 SystemMessage）",
          "injected_context" in _inspect.signature(_rda).parameters
          and '"role": "system"' in _agent_src
          and "_astream_msgs" in _agent_src)
    check("workflow 不再把检索上下文拼进 user query（最终用户问题拼接已移除）",
          "最终用户问题" not in _wf_src
          and "已注入的外部检索与本地缓存上下文（若已充分" not in _wf_src
          and "injected_context=_injected" in _wf_src)
    _zsxq_route_src = (Path(__file__).parent.parent / "interfaces" / "api" / "routes" / "zsxq.py"
                       ).read_text(encoding="utf-8")
    _appjs = (Path(__file__).parent.parent / "static" / "js" / "app.js"
              ).read_text(encoding="utf-8")
    check("_save_zsxq_to_history 落库幂等（agent 已落库时不重复追加气泡）",
          "_already_saved" in _zsxq_route_src and "aget_state" in _zsxq_route_src)
    check("StructuredTool 数据源经 _ainvoke_toolish 调用（修 'StructuredTool' is not callable）",
          "_ainvoke_toolish(search_knowledge_base" in _wf_src
          and "_ainvoke_toolish(list_sql_tables)" in _wf_src
          and "_ainvoke_toolish(get_table_data, tbl)" in _wf_src)

    # 6) 后台任务脱离 WS 生命周期（2026-09-10：切会话关闭 WS 曾双杀复盘预测等
    #    fire-and-forget 后台任务——token 取消 + task.cancel——任务没落库就死，
    #    前端切回后永久"AI 正在思考中"）
    _ctx_src = (Path(__file__).parent.parent / "agent" / "request_context.py"
                ).read_text(encoding="utf-8")
    _reg_src = (Path(__file__).parent.parent / "shared" / "actors" / "session_registry_actor.py"
                ).read_text(encoding="utf-8")
    check("CancellationToken 支持 detached（后台任务 WS 断开不级联取消）",
          "detached" in _ctx_src
          and 'tok.detached and "disconnect" in reason' in _ctx_src
          and "skipped" in _ctx_src)
    check("STOP_AND_REMOVE_TASK 支持 keep_bg（WS 断开只清理聊天任务）",
          'keep_bg = bool(p.get("keep_bg", False))' in _reg_src
          and "t2 is not None and not keep_bg" in _reg_src)
    check("复盘预测/盘前研报热度后台任务以 detached=True 启动，WS 断开传 keep_bg",
          "detached=True" in server_src
          and '"keep_bg": True' in server_src
          and "user_label=\"复盘预测\"" in server_src)
    check("GET /api/task/status 暴露任务存活权威信号（结果落库后才注销）",
          '@app.get("/api/task/status")' in server_src
          and "SRMsg.GET_TASK_INFO" in server_src)
    check("前端 TaskManager 以任务存活状态为准（复盘阶段1 中间结果先落库，"
          "历史增长不可靠）",
          "_fetchRunning" in _appjs and "/api/task/status" in _appjs
          and "resumeIfRunning" in _appjs
          and "任务已在后台完成" in _appjs)

    print()
    if failures:
        print(f"结果：{len(failures)} 项失败 -> {failures}")
        sys.exit(1)
    print("结果：全部通过")


if __name__ == "__main__":
    main()
