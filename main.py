"""main.py —— MOSS Finance Assistant 统一入口（重构.md 决策⑨：新建根目录 main.py）。

封装两个用途：
  1. HTTP 服务启动（uvicorn FastAPI）
     $ python main.py server                 # 默认 127.0.0.1:8000
     $ python main.py server --host 0.0.0.0  # 对外暴露
     $ python main.py server --port 9000 --reload

  2. CLI 子命令（方便冒烟 / 批处理 / 运维 / 复盘预测脚本 / 管理员管理）
     $ python main.py router "今天茅台怎么样"        # 测试 Router 规则
     $ python main.py task   "贵州茅台 新闻速览"       # 跑一次完整 workflow（非流式）
     $ python main.py task-stream "盘前新闻"           # 跑 workflow 实时流式打印进度与回答
     $ python main.py task-coder "写脚本抓茅台股价"    # 强制走 coder Agent
     $ python main.py task-reasoning "分析加息影响"    # 强制走 reasoning Agent
     $ python main.py cache-premarket                 # 预热生成盘前新闻缓存（手工预热）
     $ python main.py skills-scan                     # 扫描 orchestration/skills/ 输出技能清单
     $ python main.py test-imports                    # 最小冒烟：所有架构层 import + 旧别名别名
     $ python main.py scheduler-next                  # 显示 APScheduler 下一次触发时间

实现策略：
  - 最开头先 `import shared.compat_bootstrap` 打旧路径别名补丁，保证后续 `from agent.xxx import` 兼容
  - CLI 用 argparse（标准库，不引新依赖）
  - server 启动使用 uvicorn.run，等价于 `uvicorn interfaces.api.server:app`
"""
from __future__ import annotations

# ============================================================
# 0. 兼容别名 Bootstrap（必须第一个 import；整个项目任何地方 import 都安全）
# ============================================================
# pyright: reportUnusedImport=false
import shared.compat_bootstrap  # noqa: F401 — 副作用：sys.modules 旧别名注入，test-imports 依赖它
# pyright: reportUnusedImport=information

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple


# ============================================================
# CLI: python main.py test-imports
# ============================================================
def cmd_test_imports(_args) -> int:
    """架构层最小冒烟：逐个 import 新路径 + 旧路径别名，打印 PASS/FAIL，任何失败退出码=1。"""
    failed: List[Tuple[str, str]] = []
    passed: List[str] = []
    cases: List[tuple] = [
        # (name, import_stmt_lambda)
        ("shared.config.constants",   lambda: __import__("shared.config.constants", fromlist=["*"])),
        ("shared.models",             lambda: __import__("shared.models", fromlist=["*"])),
        ("shared.aggregator",         lambda: __import__("shared.aggregator", fromlist=["*"])),
        ("shared.actors",             lambda: __import__("shared.actors", fromlist=["*"])),
        ("shared.llm_client.model_router", lambda: __import__("shared.llm_client.model_router", fromlist=["*"])),
        ("shared.data_sources",       lambda: __import__("shared.data_sources", fromlist=["*"])),
        ("agents.router.agent",       lambda: __import__("agents.router.agent", fromlist=["*"])),
        ("orchestration.workflows.analysis_workflow", lambda: __import__("orchestration.workflows.analysis_workflow", fromlist=["*"])),
        ("orchestration.loop",        lambda: __import__("orchestration.loop", fromlist=["*"])),
        ("governance.guardrails",     lambda: __import__("governance.guardrails", fromlist=["*"])),
        ("governance.monitor",        lambda: __import__("governance.monitor", fromlist=["*"])),
        ("governance.logger",         lambda: __import__("governance.logger", fromlist=["*"])),
        ("governance.feedback",       lambda: __import__("governance.feedback", fromlist=["*"])),
        # 以下是旧路径别名（由 compat_bootstrap 注入）——若这些能 import，说明 server.py/main_agent.py 原代码就能跑
        ("compat: config.constants",  lambda: __import__("config.constants", fromlist=["*"])),
        ("compat: tools.stock_matcher", lambda: __import__("tools.stock_matcher", fromlist=["*"])),
        ("compat: tools.tavily_tool", lambda: __import__("tools.tavily_tool", fromlist=["*"])),
        ("compat: tools.zsxq_tool",   lambda: __import__("tools.zsxq_tool", fromlist=["*"])),
        ("compat: tools.ragflow_tools", lambda: __import__("tools.ragflow_tools", fromlist=["*"])),
        ("compat: tools.db_tools",    lambda: __import__("tools.db_tools", fromlist=["*"])),
        ("compat: adapter.ollama_client", lambda: __import__("adapter.ollama_client", fromlist=["*"])),
        ("compat: agent.actor_base",  lambda: __import__("agent.actor_base", fromlist=["*"])),
        ("compat: agent.scheduler",   lambda: __import__("agent.scheduler", fromlist=["*"])),
        ("compat: agent.circuit_breaker", lambda: __import__("agent.circuit_breaker", fromlist=["*"])),
        ("compat: agent.model_router", lambda: __import__("agent.model_router", fromlist=["*"])),
        ("compat: agent.main_agent",  lambda: __import__("agent.main_agent", fromlist=["*"])),
        ("compat: cache.stock_cache", lambda: __import__("cache.stock_cache", fromlist=["*"])),
        ("compat: api.middleware.audit_logger", lambda: __import__("api.middleware.audit_logger", fromlist=["*"])),
        ("compat: api.middleware.rbac", lambda: __import__("api.middleware.rbac", fromlist=["*"])),
        ("compat: api.middleware.prompt_sanitizer", lambda: __import__("api.middleware.prompt_sanitizer", fromlist=["*"])),
    ]
    for name, loader in cases:
        try:
            loader()
            passed.append(name)
            print(f"  [PASS] {name}")
        except Exception as e:
            failed.append((name, f"{type(e).__name__}: {e}"))
            print(f"  [FAIL] {name}  ->  {type(e).__name__}: {e}")

    print(f"\nImport test: {len(passed)} passed / {len(failed)} failed / total {len(cases)}")
    if failed:
        print("\nFailed cases:")
        for (n, e) in failed:
            print(f"  - {n}: {e}")
        return 1
    return 0


# ============================================================
# CLI: python main.py router "<query>"
# ============================================================
def cmd_router(args) -> int:
    """规则路由冒烟测试（异步级联版本）。"""
    from agents.router.agent import decide_cascade, decide

    async def _run():
        if getattr(args, "no_gemma4", False):
            return decide(args.query)
        return await decide_cascade(args.query, enable_gemma4=not args.no_gemma4)

    d = asyncio.run(_run())
    print(json.dumps({
        "branch": d.branch.value,
        "decided_by": d.decided_by,
        "reason": d.reason,
        "confidence": d.confidence,
        "has_stock_keywords": d.has_stock_keywords,
        "extracted_stock_codes": d.extracted_stock_codes,
        "extracted_stock_names": d.extracted_stock_names,
        "has_code_keywords": d.has_code_keywords,
        "has_analysis_keywords": d.has_analysis_keywords,
        "has_visual_input": d.has_visual_input,
        "from_shortcut_button": d.from_shortcut_button,
        "shortcut_type": d.shortcut_type,
        "cascade_upgrade_suggestion": d.cascade_upgrade_suggestion,
    }, ensure_ascii=False, indent=2))
    return 0


# ============================================================
# CLI: python main.py task / task-stream / task-coder / task-reasoning
# ============================================================
_OVERRIDE_AGENT_MAP = {
    "task": None,
    "task-stream": None,
    "task-coder": "coder",
    "task-reasoning": "reasoning",
}


def cmd_task(args) -> int:
    """完整 workflow 冒烟：非流式输出。"""
    from orchestration.workflows.analysis_workflow import run_analysis_workflow
    override = _OVERRIDE_AGENT_MAP.get(getattr(args, "_subcommand", "task"))

    async def _run():
        res = await run_analysis_workflow(
            args.query,
            thread_id=args.thread_id or f"cli_{os.getpid()}",
            user_id=args.user_id or "cli_user",
            enable_gemma4_router=not args.no_gemma4,
            preferred_agent_override=override,
            quiet=not args.verbose,
        )
        return res

    stream_mode = getattr(args, "_subcommand", "task") == "task-stream"
    if stream_mode:
        # 流式模式：简单版——先跑完整 workflow 再分段打印（真正的 SSE 走 HTTP 接口）
        print("[Stream mode: CLI 简化版——执行完毕后按段打印]")
    res = asyncio.run(_run())
    print("\n========== ROUTER DECISION ==========")
    print(f"branch={res.router_decision.branch.value}  decided_by={res.router_decision.decided_by}")
    print(f"reason={res.router_decision.reason}")
    if res.aggregator_stats:
        print(f"aggregator_stats={json.dumps(res.aggregator_stats, ensure_ascii=False)}")
    print(f"branch_trace keys={list(res.branch_trace.keys())}")
    print("\n========== FINAL ANSWER ==========")
    print(res.final_answer)
    return 0


# ============================================================
# CLI: python main.py cache-premarket
# ============================================================
def cmd_cache_premarket(_args) -> int:
    """强制刷新盘前新闻缓存（不管6小时TTL），用于管理员预热。"""
    from orchestration.workflows.analysis_workflow import (
        run_analysis_workflow, _save_premarket_result, PRE_MARKET_DIR,
    )
    PRE_MARKET_DIR.mkdir(parents=True, exist_ok=True)
    async def _run():
        return await run_analysis_workflow(
            "盘前新闻", thread_id="admin_premarket_warmup", user_id="admin",
            enable_gemma4_router=False, preferred_agent_override="reasoning", quiet=False,
        )
    res = asyncio.run(_run())
    saved = _save_premarket_result(res.final_answer)
    print(f"盘前新闻缓存写入: {saved} ({len(res.final_answer)} chars)")
    return 0


# ============================================================
# CLI: python main.py skills-scan
# ============================================================
def cmd_skills_scan(_args) -> int:
    """扫描 orchestration/skills 和根目录 skills 输出技能清单。"""
    roots: List[Path] = [
        Path(__file__).resolve().parent / "orchestration" / "skills",
        Path(__file__).resolve().parent / "skills",
    ]
    for root in roots:
        if not root.exists():
            print(f"[SKIP] 目录不存在: {root}")
            continue
        print(f"\n=== Skills @ {root} ===")
        for p in sorted(root.iterdir()):
            if p.is_dir():
                skill_md = p / "SKILL.md"
                line1 = ""
                if skill_md.exists():
                    try:
                        line1 = skill_md.read_text(encoding="utf-8").splitlines()[0][:120]
                    except Exception:
                        pass
                scripts = [x.name for x in list(p.glob("scripts/*.py"))][:5]
                print(f"  - {p.name:40s} scripts={scripts}  head={line1!r}")
    return 0


# ============================================================
# CLI: python main.py scheduler-next
# ============================================================
def cmd_scheduler_next(_args) -> int:
    """打印 scheduler 下一次触发时间（依赖 orchestration.scheduler.scheduler 已实现的 TaskScheduler.get_next_run_times 实例方法）。"""
    try:
        from orchestration.scheduler.scheduler import (
            get_scheduler, setup_preset_tasks,
        )
    except Exception as e:
        print(f"无法加载 scheduler: {type(e).__name__}: {e}")
        return 1
    scheduler = get_scheduler()
    # 首次执行前注册预设任务，保证 next_run_times 返回盘前任务的触发时间
    async def _zsxq_noop():
        return None
    async def _news_noop():
        return None
    setup_preset_tasks(scheduler, _zsxq_noop, _news_noop)
    print("下一次触发时间:")
    try:
        for name, ts in scheduler.get_next_run_times().items():
            print(f"  - {name:40s} -> {ts}")
    except Exception as _e:
        print(f"  ERROR: {type(_e).__name__}: {_e}")
    return 0


# ============================================================
# CLI: python main.py server（启动 uvicorn）
# ============================================================
def _port_is_listening(host: str, port: int) -> Optional[dict]:
    """检查 (host, port) 是否处于 LISTENING 状态。
    返回 None = 空闲；否则返回占用信息 {pid, process_name, cmdline, is_our_project, status_url_body}。
    说明：只看 LISTENING（有真实 PID 可处理），TIME_WAIT/CLOSE_WAIT 不影响新 bind，
    避免 Experience ID 504295 的失败模式："把 TIME_WAIT 误判为进程占用并全杀"。"""
    import socket
    # Fast-path: 纯 socket connect 判断（跨平台）
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.3)
            if s.connect_ex((host if host != "0.0.0.0" else "127.0.0.1", port)) == 0:
                pass  # 有东西在监听，继续走详细诊断
            else:
                return None
    except OSError:
        pass

    # Slow-path: Windows 专用（WMI + Get-NetTCPConnection）拿 PID / 进程信息
    info: dict = {"pid": None, "process_name": None, "cmdline": None,
                  "is_our_project": False, "status_url_body": None}
    try:
        if os.name == "nt":
            import subprocess as _sp
            # 用 netstat 拿 PID（比 WMI 轻量且稳定）
            out = _sp.run(
                ["netstat", "-ano", "-p", "TCP"],
                capture_output=True, text=True, timeout=8,
            ).stdout or ""
            target_lh = f"{host}:{port}"
            target_any = f"0.0.0.0:{port}"
            target_loop = f"127.0.0.1:{port}"
            pid_ = None
            for line in out.splitlines():
                cols = line.split()
                if len(cols) < 5 or cols[0].upper() != "TCP":
                    continue
                if cols[-2].upper() != "LISTENING":
                    continue
                local = cols[1]
                if local in (target_lh, target_any, target_loop):
                    try:
                        pid_ = int(cols[-1])
                        break
                    except (TypeError, ValueError):
                        continue
            info["pid"] = pid_
            if pid_:
                try:
                    proc = _sp.run(
                        ["powershell", "-NoProfile", "-Command",
                         "$p=Get-Process -Id %d -ErrorAction Stop; "
                         "%s|%s|{0}" % (
                             pid_, "$p.ProcessName",
                             "(Get-CimInstance Win32_Process -Filter \"ProcessId=$($p.Id)\").CommandLine"
                         )],
                        capture_output=True, text=True, timeout=10,
                    ).stdout.strip()
                    parts = proc.split("|", 2)
                    if len(parts) >= 2:
                        info["process_name"] = parts[0] or None
                        info["cmdline"] = parts[1] or None
                except Exception:
                    pass
            # 最后探 /health 判断是否是本项目实例（避免误杀其他 8000 端口用户）
            try:
                import urllib.request as _ur
                probe_url = f"http://127.0.0.1:{port}/health"
                with _ur.urlopen(probe_url, timeout=0.8) as resp:
                    body = (resp.read() or b"").decode("utf-8", errors="replace")
                    info["status_url_body"] = body
                    if "MOSS-Finance-Assistant" in body or "moss-finance-assistant" in body.lower():
                        info["is_our_project"] = True
            except Exception:
                pass
    except Exception:
        pass
    return info


def _find_free_port(host: str, start_port: int, max_attempts: int = 100) -> int:
    """从 start_port 开始逐个 +1，返回第一个空闲端口（最多尝试 max_attempts 个）。"""
    import socket
    for p in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.3)
                if s.connect_ex((host if host != "0.0.0.0" else "127.0.0.1", p)) != 0:
                    return p
        except OSError:
            continue
    raise RuntimeError(f"在 {host} 的 {start_port}~{start_port+max_attempts-1} 范围内没有找到空闲端口")


def cmd_server(args) -> int:
    """启动 FastAPI HTTP 服务（等价于 uvicorn interfaces.api.server:app）。

    WinError 10048 自愈（参考 Experience 504295）：
      ① 启动前先探测端口是否真的 LISTENING（非 TIME_WAIT，避免误判）；
      ② 若是本项目旧实例 → 打印占用信息并自动杀（仅限 project=yes，不碰其他用户服务）；
      ③ 若 --kill-conflicts=no 或占用者不是本项目 → 默认报友好诊断；
      ④ 若 --auto-port → 冲突时自动从 --port 起 +1 找空闲端口；
      ⑤ uvicorn 层注入 SO_REUSEADDR/SO_EXCLUSIVEADDRUSE（Windows）：
         解决"Ctrl+C 退出后立刻重启仍报 10048"的高频开发场景。
    """
    # 启动前：确保 .env 被加载（dotenv 不强制）
    try:
        from dotenv import load_dotenv  # type: ignore
        env_path = Path(__file__).resolve().parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except Exception:
        pass

    host = args.host
    port = int(args.port)
    kill_conflicts = str(getattr(args, "kill_conflicts", "project")).strip().lower() \
        in ("1", "yes", "true", "project", "always")
    kill_always = str(getattr(args, "kill_conflicts", "project")).strip().lower() \
        in ("1", "yes", "true", "always")
    auto_port = bool(getattr(args, "auto_port", False))

    # ---- ① 启动前冲突诊断 ----
    info = _port_is_listening(host, port)
    if info is not None:
        pid = info.get("pid")
        pname = info.get("process_name") or "?"
        cmdline = info.get("cmdline") or "?"
        ours = bool(info.get("is_our_project"))
        print("=" * 72, file=sys.stderr)
        print(f"⚠️  端口 {host}:{port} 已被占用（LISTENING），不能启动新的服务实例：",
              file=sys.stderr)
        print(f"   占用者 PID          : {pid}", file=sys.stderr)
        print(f"   占用者 进程名       : {pname}", file=sys.stderr)
        print(f"   占用者 命令行       : {cmdline}", file=sys.stderr)
        print(f"   /health 是否是本项目: {'是 ✅ MOSS-Finance-Assistant' if ours else '否 ⚠️（其他程序/业务，不会自动处理）'}",
              file=sys.stderr)
        if info.get("status_url_body"):
            body_snip = (info["status_url_body"] or "").strip().replace("\n", " ")
            if len(body_snip) > 200:
                body_snip = body_snip[:200] + "…"
            print(f"   占用者 /health 响应 : {body_snip}", file=sys.stderr)
        print("=" * 72, file=sys.stderr)

        handled = False
        if ours and (kill_conflicts or kill_always):
            # 安全：仅杀本项目旧实例（/health 签名匹配 MOSS-Finance-Assistant）
            try:
                import subprocess as _sp2
                print(f"💡 检测到是本项目旧实例，自动释放端口 {port}（kill PID={pid}）...",
                      file=sys.stderr)
                _sp2.run(["taskkill", "/F", "/PID", str(pid)],
                         capture_output=False, timeout=10, check=False)
                import time as _t
                for _ in range(10):
                    if _port_is_listening(host, port) is None:
                        break
                    _t.sleep(0.2)
                info2 = _port_is_listening(host, port)
                if info2 is None:
                    print(f"✅ 端口 {host}:{port} 已成功释放，可以启动新服务。", file=sys.stderr)
                    handled = True
                else:
                    print(f"❌ 自动释放失败，端口仍被 PID={info2.get('pid')} 占用。", file=sys.stderr)
            except Exception as e2:
                print(f"❌ 自动释放端口失败：{type(e2).__name__}: {e2}", file=sys.stderr)
        elif not ours and kill_always:
            # 杀无差别（由用户显式 --kill-conflicts=always 触发）
            try:
                import subprocess as _sp3
                print(f"⚠️  --kill-conflicts=always：强制释放端口 {port}（PID={pid}，非本项目）",
                      file=sys.stderr)
                _sp3.run(["taskkill", "/F", "/PID", str(pid)], capture_output=False,
                         timeout=10, check=False)
            except Exception as e3:
                print(f"❌ 强制释放失败：{type(e3).__name__}: {e3}", file=sys.stderr)

        # ---- ④ --auto-port 兜底：自动 +1 找空闲端口 ----
        if (not handled) and auto_port:
            old_port = port
            port = _find_free_port(host, port + 1, 200)
            print(f"🔀 已启用 --auto-port，跳过被占用的 {old_port} → 改用空闲端口 {port}",
                  file=sys.stderr)
            handled = True

        if not handled:
            print("", file=sys.stderr)
            print("💡 推荐处理方式（任选其一）：", file=sys.stderr)
            if ours:
                print(f"   1) 保留此旧实例直接使用即可（127.0.0.1:{port} 已经在响应 /health）",
                      file=sys.stderr)
                print(f"   2) 自动释放并重启 → 加参数：--kill-conflicts=project（默认，已自动启用）",
                      file=sys.stderr)
            print(f"   3) 换端口启动    → python main.py server --port {port+1}", file=sys.stderr)
            print(f"   4) 自动找空闲端口 → python main.py server --auto-port", file=sys.stderr)
            print(f"   5) 强制杀任何占用者 → python main.py server --kill-conflicts=always（⚠️危险）",
                  file=sys.stderr)
            sys.stderr.flush()
            return 2  # 绑定失败专用 exit code：后续脚本可区分

    # ---- ⑤ uvicorn Config 注入 SO_REUSEADDR ----
    #    Windows 上 uvicorn 默认 TCPServer 使用 socket.SO_EXCLUSIVEADDRUSE，
    #    导致 Ctrl+C 后立刻重启仍会报 10048；这里显式把 socket_options 改为
    #    SO_REUSEADDR=1（Linux）/ Windows Server 2019+ 也已兼容 SO_REUSEADDR，
    #    从而 3~5 秒内能重绑（参考 CPython socket 文档 + FastAPI discussions 8489）。
    import uvicorn

    # 构造并调用 Config.setup_socket() 之前注入 SO_REUSEADDR：
    # 直接用 uvicorn.run() 传 socket_options 最快（uvicorn 0.19+ 支持此 kwargs 透传）
    try:
        import socket as _sk
        sock_opts = [(_sk.SOL_SOCKET, _sk.SO_REUSEADDR, 1)]
        # 以下两个 kwargs 在较新的 uvicorn 中会被转发到 h11/httptools Server；
        # 老版本自动忽略，用户无感知，不报错即生效。
        # pyright: reportGeneralTypeIssues=false
        # —— uvicorn stubs 未声明 kwargs 透传，老版本自动忽略；TypeError 兜底分支在下
        _extra_run_kwargs: dict = {
            "socket_options": sock_opts,
            "reuse_port": (os.name != "nt"),  # Linux SO_REUSEPORT；Windows 不支持留 False
        }
        uvicorn.run(
            "interfaces.api.server:app",
            host=host,
            port=port,
            reload=bool(args.reload),
            log_level=args.log_level,
            **_extra_run_kwargs,  # type: ignore[arg-type]
        )
    except TypeError:
        # 老版本 uvicorn 不支持 socket_options 参数 → 走原逻辑（少一个自愈能力但不阻塞启动）
        print("ℹ️  当前 uvicorn 版本不支持 socket_options 参数，跳过 SO_REUSEADDR 注入。",
              file=sys.stderr)
        uvicorn.run(
            "interfaces.api.server:app",
            host=host,
            port=port,
            reload=bool(args.reload),
            log_level=args.log_level,
        )
    return 0


# ============================================================
# argparse CLI 总装
# ============================================================
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="main",
        description="MOSS Finance Assistant — 统一入口（HTTP 服务 + CLI 工具）",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # 1) server
    ps = sub.add_parser("server", help="启动 uvicorn FastAPI HTTP 服务")
    ps.add_argument("--host", default="127.0.0.1")
    ps.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    ps.add_argument("--reload", action="store_true", help="开发模式：文件变动自动重启")
    ps.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error", "critical"])
    ps.add_argument(
        "--kill-conflicts", dest="kill_conflicts", default="project",
        choices=["no", "project", "always"],
        help="端口被占用时的策略：no=只报友好诊断不动手；project=（默认）仅杀「/health 签名为 MOSS-Finance-Assistant」的本项目旧实例；always=强制杀任何占用者（⚠️危险）",
    )
    ps.add_argument(
        "--auto-port", dest="auto_port", action="store_true",
        help="端口占用时从当前 port+1 起自动找 200 范围内第一个空闲端口并绑定（最省事，不杀任何进程）",
    )
    ps.set_defaults(func=cmd_server)

    # 2) router
    pr = sub.add_parser("router", help="测试 Router 路由决策（输出 JSON）")
    pr.add_argument("query", help="用户 query 字符串")
    pr.add_argument("--no-gemma4", action="store_true", help="只走纯规则，不调本地 gemma4:e4b 级联")
    pr.set_defaults(func=cmd_router)

    # 3) task / task-stream / task-coder / task-reasoning
    #    agent_hint 三元组作为人工阅读友好的路由注释保留；程序级 override 走下面 _OVERRIDE_AGENT_MAP。
    #    使用 _ 前缀占位变量避免 pyright reportUnusedVariable。
    for name, help_txt, _agent_hint in [
        ("task", "跑一次完整 workflow（非流式输出）", None),
        ("task-stream", "跑一次 workflow（CLI简化版：完成后打印）", None),
        ("task-coder", "跑 workflow + 强制覆盖到 coder Agent(qwen2.5-coder)", "coder"),
        ("task-reasoning", "跑 workflow + 强制覆盖到 reasoning Agent(deepseek-r1)", "reasoning"),
    ]:
        pt = sub.add_parser(name, help=help_txt)
        pt.add_argument("query")
        pt.add_argument("--thread-id", default=None)
        pt.add_argument("--user-id", default=None)
        pt.add_argument("--no-gemma4", action="store_true")
        pt.add_argument("-v", "--verbose", action="store_true", help="广播中间事件（CLI 下默认 false）")
        pt.set_defaults(func=cmd_task, _subcommand=name)

    # 4) cache-premarket
    pcm = sub.add_parser("cache-premarket", help="强制刷新盘前新闻缓存（管理员预热）")
    pcm.set_defaults(func=cmd_cache_premarket)

    # 5) skills-scan
    psk = sub.add_parser("skills-scan", help="扫描技能清单")
    psk.set_defaults(func=cmd_skills_scan)

    # 6) scheduler-next
    psn = sub.add_parser("scheduler-next", help="查看 scheduler 下一次触发时间")
    psn.set_defaults(func=cmd_scheduler_next)

    # 7) test-imports
    pti = sub.add_parser("test-imports", help="架构层最小冒烟（import 新路径 + 旧路径兼容别名）")
    pti.set_defaults(func=cmd_test_imports)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()

    # --- 无参友好：打印完整帮助 + 退出码 0，而不是 argparse 默认的 error 退出码 2 ---
    use_argv = list(argv) if argv is not None else sys.argv[1:]
    if not use_argv:
        parser.print_help()
        print()
        print("=" * 72)
        print("💡 常用命令速查（无需死记参数）：")
        print("-" * 72)
        print("  1) 启动 API 服务     →  python main.py server --host 0.0.0.0 --port 8000")
        print("  2) Router 规则冒烟   →  python main.py router \"盘前新闻\" --no-gemma4")
        print("  3) 完整 Task 冒烟    →  python main.py task \"盘前新闻\" --no-gemma4")
        print("  4) import 链自测     →  python main.py test-imports")
        print("  5) 盘前缓存预热     →  python main.py cache-premarket")
        print("  6) 技能清单扫描     →  python main.py skills-scan")
        print("  7) Scheduler 下次   →  python main.py scheduler-next")
        print("-" * 72)
        print("   查看子命令帮助: python main.py <子命令> -h")
        print("   pytest 11条 Router: python -m pytest tests/test_router_smoke.py -v")
        print("=" * 72)
        return 0

    args = parser.parse_args(use_argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    sys.exit(main())
