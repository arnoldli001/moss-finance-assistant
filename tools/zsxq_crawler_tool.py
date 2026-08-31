"""
知识星球"最新小作文热度总结"抓取工具。

定位：
  - 独立工具文件，供主 Agent（LangChain @tool）、调度协调 Agent、
    或 server.py 的 `_run_zsxq_analysis` 编排链路直接复用。
  - 封装了用户需求的两段逻辑（见 project_root/interfaces/api/server.py L2083-2084）：
        ① 若当天已有 txt 总结 → 直接复用并返回，跳过抓取；
        ② 否则运行 `tools/zsxq_analysis_runner.py` 完整流程
          （Playwright 知识星球抓取 + 本地 Ollama LLM 分析）。
  - 不做 UI 气泡推送、不写会话历史（那些是编排层/展示层职责）；
    只专注「抓取 + 返回 txt 总结」一件事，可被任意调用方灵活组合。

对外三种入口：
  [A] async def fetch_zsxq_latest_summary_async(...)
        协程原生接口，支持可选 progress_callback(msg) 推进度；
        Server / 调度协程场景直接 await 调用。返回 str：txt 或 ""。
  [B] def fetch_zsxq_latest_summary(...) -> str
        同步包装器：主 Agent 工具调用环境（同步线程）内安全运行 A。
  [C] LangChain @tool 版 zsxq_fetch_latest_hot_summary() -> str
        供主 Agent 工具发现 + 自动调用。Tool 名长名语义清晰，
        tool.description 明确说明什么时候该用它、什么时候不要用。

依赖约定（和同级 tavily_tool.py / zsxq_tool.py 完全一致）：
  - 模块级：自动把 <project_root> 加入 sys.path；加载 find_dotenv() + load_dotenv；
            可选导入 langchain_core.tools.tool，不可用时退化为 @identity 装饰器，
            保证 `python tools/zsxq_crawler_tool.py` 也能独立运行（CLI 自检）。
  - 常量全部从 config.constants 集中 import，不抄魔鬼数字。
  - 进度回调（默认 = 打印到 stderr）：可由 server 端替换为 monitor._emit(...)，
    或主 agent 场景留默认打印（主 agent 工具通常不看这些细粒度进度，也可关 quiet=True）。
  - Ollama 准备（定位/探活/list/ensure）：直接委托 shared/utils/ollama_helper.py，
    一处改动、全局生效，避免 zsxq_crawler_tool 与 server.py 双份实现漂移。
"""
from __future__ import annotations

import os
import sys
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

# ---------------------------------------------------------------------------
# 项目根查找 + sys.path 注入（鲁棒：AGENTS.md 标志兜底）
# ---------------------------------------------------------------------------
def _find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for _ in range(12):
        if (cur / "AGENTS.md").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    # 兜底：本文件 = <root>/tools/zsxq_crawler_tool.py → 上 1 层
    return start.resolve().parents[1]


PROJECT_ROOT: Path = _find_project_root(Path(__file__))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv, find_dotenv  # noqa: E402
    _env_p = find_dotenv(str(PROJECT_ROOT / ".env"))
    if _env_p:
        load_dotenv(_env_p)
except Exception:  # pragma: no cover - dotenv 缺失不致命
    pass

# LangChain @tool 可选导入：不可用时退化为恒等装饰器，保证脚本可裸跑
try:  # pragma: no cover - 分支取决于运行环境
    from langchain_core.tools import tool as _lc_tool  # type: ignore
except Exception:  # pragma: no cover
    def _lc_tool(_fn):
        return _fn

try:  # pragma: no cover
    from api.monitor import monitor  # type: ignore  # noqa: F401
    _HAS_MONITOR = True
except Exception:  # pragma: no cover
    _HAS_MONITOR = False

# ---------------------------------------------------------------------------
# 常量集中引用（避免魔鬼数字，与 server.py 同源）
# ---------------------------------------------------------------------------
from config.constants import (  # noqa: E402
    OLLAMA_DEFAULT_BASE_URL,
    SERVER_OUTPUT_MAX_STDOUT_TAIL_LINES,
    SERVER_FINAL_RETURN_LINE_MIN_LEN,
    SERVER_JSON_DEBUG_LINE_MIN_LEN,
    SERVER_PROGRESS_SAFE_TRUNCATE_LEN,
)
from shared.utils.zsxq_paths import (  # noqa: E402
    get_zsxq_news_dir as _get_zsxq_news_dir,
    ensure_zsxq_news_dir_ready as _ensure_zsxq_news_dir,
)
ensure_zsxq_news_dir_ready = _ensure_zsxq_news_dir  # re-export，便于外部调用方触发
ZSXQ_NEWS_DIR: Path = _ensure_zsxq_news_dir(PROJECT_ROOT)  # 首次加载自动搬运
ZSXQ_RUNNER_SCRIPT: Path = PROJECT_ROOT / "tools" / "zsxq_analysis_runner.py"
ZSXQ_DEFAULT_MODEL: str = "qwen3:8b"

# ---------------------------------------------------------------------------
# Ollama 通用 helper：单一实现来自 shared/utils/ollama_helper.py
#   ProgressFn 在此 re-export（便于工具内部、旧调用方 import path 继续有效）
#   find/probe/list/ensure 也 re-export（后两者用 ZSXQ 默认值再包一层）
# ---------------------------------------------------------------------------
from shared.utils.ollama_helper import (  # noqa: E402
    ProgressFn,
    find_ollama_exe,
    probe_ollama,
    ollama_models_list,
    ensure_ollama_ready as _shared_ensure_ollama_ready,
)


def _default_progress(msg: str) -> None:
    """zsxq 工具默认进度输出：带 [ZSXQ-Crawler] 前缀打到 stderr，避免污染 stdout。"""
    sys.stderr.write(f"[ZSXQ-Crawler] {msg}\n")
    sys.stderr.flush()


async def ensure_ollama_ready(
    model: str = ZSXQ_DEFAULT_MODEL,
    *,
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
    emit_progress: ProgressFn = _default_progress,
) -> tuple[bool, str]:
    """zsxq 侧默认值包装：默认 qwen3:8b + [ZSXQ-Crawler] 进度前缀。
    实际定位/探活/拉起服务/模型拉取的实现 100% 委托 shared.utils.ollama_helper。"""
    return await _shared_ensure_ollama_ready(
        model, base_url=base_url, emit_progress=emit_progress,
    )


# ---------------------------------------------------------------------------
# 辅助：同步环境安全执行协程（完全复用 tavily_tool / zsxq_tool 的同模式）
# ---------------------------------------------------------------------------
def _run_sync(coro):
    """在同步工具环境中安全执行 async 协程：有运行 loop 就 ThreadPoolExecutor+asyncio.run，
    否则直接 asyncio.run()。避免同 loop 里嵌套事件循环 RuntimeError。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is None:
        return asyncio.run(coro)
    import concurrent.futures as _cf
    with _cf.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(lambda: asyncio.run(coro)).result()


# ---------------------------------------------------------------------------
# 辅助：找最新的"今天"总结文件
# ---------------------------------------------------------------------------
def find_latest_today_txt(news_dir: Path, today_prefix: Optional[str] = None) -> Optional[Path]:
    """查找当天最新的 txt 总结文件（文件名以 YYYYMMDD 开头，按名字降序取第 1 个）。

    参数：
        news_dir:     存放 txt 的目录（通常就是 ZSXQ_NEWS_DIR）
        today_prefix: 如 "20260830"；None 时自动取今天。
    返回：找到返回 Path；没找到返回 None。
    """
    if today_prefix is None:
        today_prefix = datetime.now().strftime("%Y%m%d")
    if not news_dir.exists():
        return None
    candidates = sorted(
        [f for f in news_dir.glob(f"{today_prefix}*.txt") if f.is_file()],
        key=lambda f: f.name,
        reverse=True,
    )
    return candidates[0] if candidates else None


# ---------------------------------------------------------------------------
# [A] 主协程：抓 + 返回 txt 总结
# ---------------------------------------------------------------------------
async def fetch_zsxq_latest_summary_async(
    *,
    news_dir: Optional[Path] = None,
    runner_script: Optional[Path] = None,
    today_prefix: Optional[str] = None,
    model: str = ZSXQ_DEFAULT_MODEL,
    ollama_base_url: str = OLLAMA_DEFAULT_BASE_URL,
    emit_progress: Optional[ProgressFn] = None,
    quiet: bool = False,
) -> str:
    """【函数 1 / 2（纯数据抓取）】知识星球盘前小作文热度最新总结。

    两段执行（完全对应 server.py L2083-2084）：
        ① 当天已有 txt 总结 → 读取并直接返回，跳过抓取。
        ② 否则：
            a. Ollama 自动准备：定位 / 拉起服务 / 检查或 pull 模型（qwen3:8b）；
            b. 以独立子进程运行 `tools/zsxq_analysis_runner.py --quiet`
               （Playwright sync API 不能在 async 上下文里直接跑，
               必须子进程隔离，这也是 AGENTS.md 的约定）；
            c. 读取刚生成的当日最新 txt 总结，返回其完整内容。

    参数：
        news_dir       : 存放 zsxq txt 总结的目录；默认 PROJECT_ROOT/zsxq_news。
        runner_script  : zsxq_analysis_runner.py 的绝对路径；默认 PROJECT_ROOT/tools/…
        today_prefix   : 当日 YYYYMMDD 前缀；None 时按本机时间取。
        model          : Ollama 模型名；默认 qwen3:8b。
        ollama_base_url: 默认 http://localhost:11434。
        emit_progress  : 回调 (msg:str)->None，默认打印到 stderr；quiet=True 时静默。
        quiet          : True 时不推任何 progress；仅最终返回 str。
    返回：
        成功 → 非空 str（UTF-8 的 txt 总结内容）；失败 → ""（错误原因已通过 emit_progress 或 stderr 输出）。
    """
    _ndir = Path(news_dir) if news_dir else ZSXQ_NEWS_DIR
    _runner = Path(runner_script) if runner_script else ZSXQ_RUNNER_SCRIPT
    _prefix = today_prefix if today_prefix else datetime.now().strftime("%Y%m%d")
    progress: ProgressFn = (lambda _m: None) if quiet else (emit_progress or _default_progress)

    # ---- ① 复用当日已有总结 ----
    latest_txt = find_latest_today_txt(_ndir, _prefix)
    if latest_txt:
        progress(f"✅ 检测到当日已有总结 {latest_txt.name}，跳过抓取直接返回")
        try:
            content = latest_txt.read_text(encoding="utf-8")
            if content.strip():
                return content
        except OSError as e:
            progress(f"⚠️  读取已有总结失败，将尝试重建：{e}")

    # ---- ② 没有 → 跑 runner ----
    # 2a. Ollama 预检 + 自动启动 + 模型拉取（委托 shared/utils/ollama_helper）
    ok, err_msg = await ensure_ollama_ready(
        model, base_url=ollama_base_url, emit_progress=progress,
    )
    if not ok:
        print(f"[ZSXQ-Crawler] Ollama 准备失败: {err_msg}", file=sys.stderr)
        return ""

    progress("📡 开始抓取知识星球 + 调用 Ollama 分析")

    if not _runner.exists():
        progress(f"❌ 找不到 runner 脚本：{_runner}")
        return ""

    # 2b. 启动子进程 --quiet（抑制模型 stdout 里巨大 JSON 污染调用方管道）
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable, str(_runner), "--quiet",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(PROJECT_ROOT),
        )
    except Exception as e:
        progress(f"❌ 启动 runner 失败: {type(e).__name__}: {e}")
        return ""

    # 2c. 实时读取 stdout 并推里程碑/进度；大段 JSON / 分隔线跳过
    stdout_lines: list[str] = []
    if process.stdout is None:
        progress("❌ runner 子进程 stdout 未就绪")
        try:
            process.kill()
        except Exception:
            pass
        return ""

    try:
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            stdout_lines.append(text)
            stripped = text.strip()
            if not stripped:
                continue

            # ---- 里程碑：落成独立 progress 消息 ----
            milestone: Optional[str] = None
            if "⚠ Ollama 调用失败" in stripped or "⚠ Ollama 预检异常" in stripped \
                    or "⚠ 分析过程出错" in stripped:
                milestone = stripped
            elif stripped.startswith("[分析结果]"):
                milestone = stripped
            elif stripped.startswith("═══ 抓取完成") or \
                    ("════════════" in stripped and "抓取完成" in stripped):
                milestone = stripped

            # ---- 跳过噪音：超大 最终返回 / JSON 调试转储 ----
            if "最终返回" in stripped and len(stripped) > SERVER_FINAL_RETURN_LINE_MIN_LEN:
                continue
            if len(stripped) > SERVER_JSON_DEBUG_LINE_MIN_LEN and stripped.lstrip() \
                    and stripped.lstrip()[0] in '{[':
                continue

            # ---- 超长行截断为安全长度 ----
            safe = stripped if len(stripped) <= SERVER_PROGRESS_SAFE_TRUNCATE_LEN \
                else stripped[:SERVER_PROGRESS_SAFE_TRUNCATE_LEN] + "…"

            if milestone:
                progress(f"🏁 {safe}")
            elif any(kw in stripped for kw in ("[抓取]", "[分析]", "[ZSXQ]", "分析结果", "总结已保存")):
                # 中间进度：统一前缀，避免调用方以为是最终结果
                progress(f"⏳ 盘前小作文热度：{safe}")
    except Exception as e:
        print(f"[ZSXQ-Crawler] 读取 runner stdout 异常: {e}", file=sys.stderr)

    await process.wait()
    rc = int(process.returncode or -1)
    if rc != 0:
        tail = "\n".join(stdout_lines[-SERVER_OUTPUT_MAX_STDOUT_TAIL_LINES:])
        print(
            f"[ZSXQ-Crawler] runner 失败（exit={rc}）末尾输出：\n{tail}",
            file=sys.stderr,
        )
        combined = tail
        if "WinError 10061" in combined or "Ollama 连接失败" in combined or "Ollama 服务未启动" in combined:
            progress("❌ Ollama 服务未启动，请在终端运行 `ollama serve` 并 `ollama pull qwen3:8b` 后重试。")
        elif "未找到" in combined or "FileNotFoundError" in combined:
            progress("❌ 分析脚本或依赖未找到，请联系管理员")
        else:
            progress("❌ 分析失败，请稍后重试")
        return ""

    # 2d. 读取 runner 刚生成的最新 txt 总结
    latest_txt = find_latest_today_txt(_ndir, _prefix)
    if not latest_txt:
        progress("❌ 未找到总结文件（runner 可能未写出，或写在其他目录）")
        return ""
    try:
        content = latest_txt.read_text(encoding="utf-8")
    except OSError as e:
        progress(f"❌ 读取总结文件失败: {e}")
        return ""
    if not content.strip():
        progress("❌ 总结文件内容为空")
        return ""
    progress(f"✅ 总结生成成功（{len(content)} 字符，文件 {latest_txt.name}）")
    return content


# ---------------------------------------------------------------------------
# [B] 同步包装：供同步调用方 / Agent Tool 使用
# ---------------------------------------------------------------------------
def fetch_zsxq_latest_summary(
    *,
    news_dir: Optional[Path] = None,
    runner_script: Optional[Path] = None,
    today_prefix: Optional[str] = None,
    model: str = ZSXQ_DEFAULT_MODEL,
    ollama_base_url: str = OLLAMA_DEFAULT_BASE_URL,
    emit_progress: Optional[ProgressFn] = None,
    quiet: bool = True,
) -> str:
    """同步版：fetch_zsxq_latest_summary_async(...) 的包装。

    默认 quiet=True，即"返回 txt，不给 stdout/stderr 打进度日志"，
    符合 LangChain Tool "同步函数返回结果字符串"的主流约定。
    若你需要看进度，传 quiet=False 或自定义 emit_progress=print。
    """
    return _run_sync(fetch_zsxq_latest_summary_async(
        news_dir=news_dir, runner_script=runner_script, today_prefix=today_prefix,
        model=model, ollama_base_url=ollama_base_url, emit_progress=emit_progress, quiet=quiet,
    ))


# ---------------------------------------------------------------------------
# [C] LangChain @tool 版：主 Agent 可直接调用（长名高辨识度，description 写清何时用）
# ---------------------------------------------------------------------------
@_lc_tool
def zsxq_fetch_latest_hot_summary() -> str:
    """盘前小作文热度总结抓取工具。

    什么时候使用：
      - 用户问"今天知识星球有什么热帖 / 小作文热度 / 盘前情绪 / 散户观点"时使用本工具；
      - 执行"复盘预测"等需要盘前小作文热度作为输入的流程时，第一步调用本工具取结果；
      - 快捷按钮"盘前小作文热度"对应的后台工作，最终也走到本工具的 async 版本。

    什么时候 **不** 要用：
      - 用户在问"某只具体股票在知识星球里的研报" → 用 tools.zsxq_tool.search_zsxq_by_stock；
      - 用户要的是新闻、公告、估值 → 用 tavily / db / ragflow 其他工具。

    执行逻辑（两段）：
      ① 若今天（YYYYMMDD）的总结 txt 已经存在于 zsxq_news/ 目录，直接复用；
      ② 否则启动 Playwright + Ollama 完整抓取分析流程，生成 txt 总结。

    返回：非空字符串 = 最终 txt 总结（Markdown 段落，可读）；空字符串 = 执行失败。
    """
    return fetch_zsxq_latest_summary(quiet=True)


# ---------------------------------------------------------------------------
# CLI 自检：python tools/zsxq_crawler_tool.py → 打印基本情况 + today.txt 命中数
#     加 --run 则真正执行一次抓取（耗时较长）
# ---------------------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover - 手动自检入口
    def _cli_main() -> int:
        print(f"[ZSXQ-Crawler] PROJECT_ROOT   = {PROJECT_ROOT}")
        print(f"[ZSXQ-Crawler] ZSXQ_NEWS_DIR  = {ZSXQ_NEWS_DIR} (exists={ZSXQ_NEWS_DIR.exists()})")
        print(f"[ZSXQ-Crawler] ZSXQ_RUNNER    = {ZSXQ_RUNNER_SCRIPT} (exists={ZSXQ_RUNNER_SCRIPT.exists()})")
        print(f"[ZSXQ-Crawler] ollama exe     = {find_ollama_exe()}")
        today = datetime.now().strftime("%Y%m%d")
        hit = find_latest_today_txt(ZSXQ_NEWS_DIR, today)
        print(f"[ZSXQ-Crawler] today {today} latest_txt = {hit}")
        if "--run" in sys.argv:
            import time as _t
            s = _t.time()
            txt = _run_sync(fetch_zsxq_latest_summary_async(quiet=False))
            dt = _t.time() - s
            print(f"\n[ZSXQ-Crawler] fetch finished in {dt:.1f}s, result len={len(txt)}")
            if txt:
                preview = txt.strip().splitlines()[:8]
                print("---- first 8 lines preview ----")
                print("\n".join(preview))
        return 0
    raise SystemExit(_cli_main())
