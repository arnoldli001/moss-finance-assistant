"""
Ollama 通用工具（跨层复用）。

定位：把「定位可执行文件、探测服务在线、读取已安装模型列表、
确保服务+模型就绪」4 个 Ollama 场景操作抽象为独立 helper。

为什么要单独一个文件（对应本项目现状 / 之前的双份实现教训）：
  - 之前 server.py 的 _ensure_ollama_ready 和 tools/zsxq_crawler_tool.py
    里各写了一份 Ollama 检查/拉起/pull 逻辑——功能同源却双份维护，
    后续修改超时时间、拉取进度心跳、错误分类都要改两处，容易漂移。
  - 本文件放到 shared/utils/，作为跨 Agent / 跨工具 / 跨 HTTP 接口的
    通用基础设施：任何模块只要需要"确保本地 Ollama 的 X 模型可用"，
    直接 `from shared.utils.ollama_helper import ensure_ollama_ready` 即可。

对外 API（最小、稳定，后续只增不删）：
    ProgressFn = Callable[[str], None]
    _default_progress(msg: str) -> None                 # 打印到 stderr
    find_ollama_exe() -> Optional[str]                   # 定位 ollama 可执行文件
    probe_ollama(base_url, timeout) -> bool              # GET /api/tags 探活
    ollama_models_list(exe, timeout) -> list[str]        # ollama list → 模型名
    ensure_ollama_ready(model, *, base_url, emit_progress) -> tuple[bool, str]

默认值来源：全部从 config.constants 导入（OLLAMA_* 常量家族），
保证和 server.py / zsxq_crawler_tool.py 的默认值 100% 一致，不出现"同模型不同默认"。

兼容加载：模块加载时自己做 AGENTS.md 锚的 PROJECT_ROOT 搜索，并注入 sys.path；
这样 `python -c 'from shared.utils.ollama_helper import ensure_ollama_ready'`
或 `python shared/utils/ollama_helper.py`（CLI 自检）都能直接运行。
"""
from __future__ import annotations

import os
import sys
import asyncio
from pathlib import Path
from typing import Callable, Optional

# ---------------------------------------------------------------------------
# 项目根 + sys.path 自举（确保"裸脚本/非项目根 cwd 导入"仍能找到 config.*）
# ---------------------------------------------------------------------------
def _find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for _ in range(12):
        if (cur / "AGENTS.md").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    # 兜底：shared/utils/ollama_helper.py → 上 2 层 = <project_root>/
    return start.resolve().parents[2]


PROJECT_ROOT: Path = _find_project_root(Path(__file__))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:  # pragma: no cover - .env 加载是锦上添花
    from dotenv import load_dotenv, find_dotenv  # type: ignore
    _env_path = find_dotenv(str(PROJECT_ROOT / ".env"))
    if _env_path:
        load_dotenv(_env_path)
except Exception:  # pragma: no cover
    pass

# ---------------------------------------------------------------------------
# 常量集中引用（魔鬼数字唯一来源 = config.constants）
# ---------------------------------------------------------------------------
from config.constants import (  # noqa: E402
    OLLAMA_DEFAULT_BASE_URL,
    OLLAMA_PROBE_TIMEOUT_SEC,
    OLLAMA_LAUNCH_POLL_INTERVAL_SEC,
    OLLAMA_LAUNCH_POLL_MAX_ROUNDS,
    OLLAMA_PULL_TIMEOUT_SEC,
    OLLAMA_MODELS_LIST_TIMEOUT_SEC,
    OLLAMA_PULL_LOG_TAIL_KEEP,
    OLLAMA_PULL_PROGRESS_INTERVAL_SEC,
    OLLAMA_PULL_PROGRESS_LINE_MAX_CHARS,
    OLLAMA_PULL_HARD_TIMEOUT_SEC,
)

DEFAULT_MODEL: str = "qwen3:8b"  # 与盘前小作文、复盘预测链路保持一致

# ---------------------------------------------------------------------------
# ProgressFn 类型别名 + 默认实现
# ---------------------------------------------------------------------------
ProgressFn = Callable[[str], None]


def _default_progress(msg: str) -> None:
    """默认进度输出：打到 stderr（不污染调用方的 stdout 结果管道），
    带 [Ollama] 前缀，便于区分是 helper 的日志还是上层业务日志。"""
    sys.stderr.write(f"[Ollama] {msg}\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# 1. find_ollama_exe —— 定位 ollama 可执行文件绝对路径
# ---------------------------------------------------------------------------
def find_ollama_exe() -> Optional[str]:
    """跨平台定位 ollama 可执行文件绝对路径。找不到返回 None。

    优先级：
      1) 环境变量 OLLAMA_BIN（显式 override，方便打包/便携部署场景）
      2) shutil.which("ollama") 查 PATH
      3) Windows 常见目录：%LOCALAPPDATA%\\Programs\\Ollama\\ollama.exe、
         C:\\Program Files\\Ollama\\ollama.exe
      4) macOS/Linux 常见目录：/usr/local/bin/ollama、/usr/bin/ollama、
         /opt/homebrew/bin/ollama（Apple Silicon 常见）
    """
    # 1) env override
    env_bin = os.environ.get("OLLAMA_BIN")
    if env_bin:
        p = Path(env_bin)
        if p.exists():
            return str(p.resolve())
    # 2) PATH
    try:
        import shutil
        which = shutil.which("ollama")
        if which:
            return which
    except Exception:
        pass
    # 3) Windows 常见安装路径
    if sys.platform.startswith("win"):
        candidates = [
            Path(os.path.expandvars(r"%LOCALAPPDATA%\Programs\Ollama\ollama.exe")),
            Path(r"C:\Program Files\Ollama\ollama.exe"),
        ]
        for c in candidates:
            if c.exists():
                return str(c)
    else:  # 4) Unix
        for c in ("/usr/local/bin/ollama", "/usr/bin/ollama", "/opt/homebrew/bin/ollama"):
            p = Path(c)
            if p.exists():
                return str(p)
    return None


# ---------------------------------------------------------------------------
# 2. probe_ollama —— 快速探测服务是否在线（GET /api/tags 2xx）
# ---------------------------------------------------------------------------
async def probe_ollama(
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
    timeout: float = OLLAMA_PROBE_TIMEOUT_SEC,
) -> bool:
    """探测 Ollama 服务是否在线：HTTP GET /api/tags 返回 2xx 即 True。
    任何网络/超时/解析异常返回 False（永不抛异常，方便用在 if 分支里）。"""
    import urllib.request
    import urllib.error
    try:
        url = base_url.rstrip("/") + "/api/tags"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 200))
            return 200 <= status < 300
    except (urllib.error.URLError, TimeoutError, OSError, ValueError, Exception):
        return False


# ---------------------------------------------------------------------------
# 3. ollama_models_list —— ollama list → 已安装模型名列表
# ---------------------------------------------------------------------------
async def ollama_models_list(
    ollama_exe: str,
    timeout: float = OLLAMA_MODELS_LIST_TIMEOUT_SEC,
) -> list[str]:
    """调用 `ollama list` 取本地已安装模型名。失败（返回码非0/超时/异常）返回 []。

    返回的名字是 CLI 输出里 NAME 列的第一段，例如：
        NAME            ID              SIZE    MODIFIED
        qwen3:8b        abc123          5.2 GB  2 days ago
    → 返回 ["qwen3:8b", ...]
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            ollama_exe, "list",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except Exception as e:
        print(f"[Ollama] ollama list 调用失败: {e}", file=sys.stderr)
        return []
    try:
        stdout_bytes, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        return []
    if (proc.returncode or 0) != 0:
        return []
    names: list[str] = []
    for raw in (stdout_bytes or b"").decode("utf-8", errors="replace").splitlines():
        parts = raw.split()
        if parts:
            names.append(parts[0])
    return names


# ---------------------------------------------------------------------------
# 4. ensure_ollama_ready —— 三位一体：定位 CLI + 启动服务 + 安装模型
#    返回 (ok: bool, message: str)。ok=False 时 message 可直接对用户展示。
# ---------------------------------------------------------------------------
async def ensure_ollama_ready(
    model: str = DEFAULT_MODEL,
    *,
    base_url: str = OLLAMA_DEFAULT_BASE_URL,
    emit_progress: ProgressFn = _default_progress,
) -> tuple[bool, str]:
    """确保 Ollama 服务在线 + 指定模型已拉取。

    阶段 1：定位 ollama CLI → 找不到返回 False + 明确安装提示
    阶段 2：探测 base_url → 未启动则后台 `ollama serve` 拉起
             最多等 OLLAMA_LAUNCH_POLL_MAX_ROUNDS * OLLAMA_LAUNCH_POLL_INTERVAL_SEC
    阶段 3：`ollama list` 检查模型是否已安装 → 未安装则 `ollama pull <model>`

    参数：
        model         – 必装模型名（默认 qwen3:8b，盘前小作文/复盘预测默认）。
                        支持 hash/tag 匹配：`qwen3:8b` 与 registry 返回的
                        `registry.ollama.ai/library/qwen3:8b` 视为同一模型（
                        按 ":" 前的基础名 + ":" 后的精确 tag 双维度匹配）。
        base_url      – 默认 http://localhost:11434，可通过 env 或远端 Ollama 改写。
        emit_progress – ProgressFn；默认打印到 stderr；
                        若你在 server 里调用，请传 ToolMonitor 的包装函数，
                        这样进度会直接渲染到前端对话区，不会在 CLI 吞掉。
    返回：
        (True, "ok")                        → 服务在线 + 模型已就绪
        (False, <面向用户的中文错误说明>)    → 失败；说明里通常包含"请执行 xxx"
    """
    # ============== 阶段 1：定位 ollama 可执行文件 ==============
    ollama_exe = find_ollama_exe()
    if not ollama_exe:
        hint = (
            "未找到 ollama 可执行文件。请先安装 Ollama：https://ollama.com/download 。"
            "安装完成后若仍提示此错误，请关闭并重新打开本程序（刷新 PATH），"
            "或将安装目录加入系统环境变量 PATH。"
        )
        emit_progress(f"❌ {hint}")
        return False, hint

    # ============== 阶段 2：Ollama 服务未启动 → 后台拉起 ==============
    if not await probe_ollama(base_url, timeout=OLLAMA_PROBE_TIMEOUT_SEC):
        emit_progress(f"🔧 Ollama 服务未运行，正在后台启动 `{ollama_exe} serve` …")
        print(f"[Ollama] 自动后台启动服务: {ollama_exe} serve", file=sys.stderr)
        try:
            import subprocess as _sp
            kw: dict = {
                "stdout": _sp.DEVNULL, "stderr": _sp.DEVNULL, "stdin": _sp.DEVNULL,
                "close_fds": True,
            }
            if sys.platform.startswith("win"):
                # CREATE_NO_WINDOW = 0x08000000；Windows 下避免 ollama serve 弹黑窗
                kw["creationflags"] = 0x08000000
            _sp.Popen([ollama_exe, "serve"], **kw)
        except Exception as e:
            msg = f"❌ 无法启动 Ollama 服务: {e}"
            emit_progress(msg)
            return False, msg

        ready = False
        for i in range(OLLAMA_LAUNCH_POLL_MAX_ROUNDS):
            await asyncio.sleep(OLLAMA_LAUNCH_POLL_INTERVAL_SEC)
            if await probe_ollama(base_url, timeout=OLLAMA_PROBE_TIMEOUT_SEC):
                ready = True
                break
            if i % 5 == 4:
                emit_progress(f"⏳ Ollama 服务启动中…已等待 {i + 1} 秒")
        if not ready:
            msg = (
                f"❌ Ollama 服务在 {OLLAMA_LAUNCH_POLL_MAX_ROUNDS} 秒内未就绪，"
                "可能是首次启动较慢或被防火墙拦截。请手动在终端执行 `ollama serve` 后重试。"
            )
            emit_progress(msg)
            return False, msg
        emit_progress("✅ Ollama 服务已就绪")
    else:
        emit_progress("✅ Ollama 服务已运行")

    # ============== 阶段 3：模型未安装 → ollama pull ==============
    installed = await ollama_models_list(ollama_exe)
    # 模型匹配：全名相等 或 ":" 之前的 library 名相等（兼容用户省略 tag 的写法）
    model_base = model.split(":", 1)[0]
    has_model = False
    for n in installed:
        if n == model:
            has_model = True
            break
        if n.split(":", 1)[0] == model_base:
            # 如果用户写了精确 tag，这里还要 tag 相等；否则就 accept
            if ":" not in model or (":" in n and n.split(":", 1)[1] == model.split(":", 1)[1]):
                has_model = True
                break
    if not has_model:
        emit_progress(
            f"📥 尚未拉取模型 `{model}`，正在后台下载并解压"
            "（~5GB，首次可能 10-30 分钟，请耐心等待）…"
        )
        print(f"[Ollama] 开始拉取模型: {ollama_exe} pull {model}", file=sys.stderr)
        try:
            proc = await asyncio.create_subprocess_exec(
                ollama_exe, "pull", model,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except Exception as e:
            msg = f"❌ 无法调用 ollama pull: {e}"
            emit_progress(msg)
            return False, msg

        last_lines: list[str] = []
        import time as _t
        pull_start = _t.time()

        async def _reader() -> None:
            assert proc.stdout is not None
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                t = line.decode("utf-8", errors="replace").rstrip()
                if not t:
                    continue
                last_lines.append(t)
                if len(last_lines) > OLLAMA_PULL_LOG_TAIL_KEEP:
                    last_lines[:] = last_lines[-OLLAMA_PULL_LOG_TAIL_KEEP:]

        reader_task = asyncio.create_task(_reader())
        timed_out = False
        try:
            while True:
                await asyncio.sleep(OLLAMA_PULL_PROGRESS_INTERVAL_SEC)
                if proc.returncode is not None:
                    break
                elapsed = int(_t.time() - pull_start)
                last = last_lines[-1] if last_lines else "下载中…"
                if len(last) > OLLAMA_PULL_PROGRESS_LINE_MAX_CHARS:
                    last = last[:OLLAMA_PULL_PROGRESS_LINE_MAX_CHARS] + "…"
                emit_progress(f"📥 模型下载中（已等 {elapsed}s）：{last}")
                if elapsed > OLLAMA_PULL_HARD_TIMEOUT_SEC:  # 硬上限
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    timed_out = True
                    break
            try:
                await asyncio.wait_for(proc.wait(), timeout=OLLAMA_PULL_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                timed_out = True
                try:
                    proc.kill()
                except Exception:
                    pass
        finally:
            if not reader_task.done():
                reader_task.cancel()
            try:
                await asyncio.wait_for(reader_task, timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

        rc = int(proc.returncode or -1)
        if timed_out or rc != 0:
            tail = "\n".join(last_lines[-10:])
            msg = (
                f"❌ 拉取模型失败（exit={rc}，超时={timed_out}）。"
                f"请手动执行：`{ollama_exe} pull {model}` 后重试。\n最近输出：\n{tail}"
            )
            emit_progress(msg)
            return False, msg
        emit_progress(f"✅ 模型 {model} 已就绪")
    else:
        emit_progress(f"✅ 模型 {model} 已安装")

    return True, "ok"


# ---------------------------------------------------------------------------
# CLI 自检：python shared/utils/ollama_helper.py → 打印环境+探活+模型列表
# ---------------------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    def _selfcheck() -> int:
        import json
        print(f"[Ollama-Helper] PROJECT_ROOT = {PROJECT_ROOT}")
        exe = find_ollama_exe()
        print(f"[Ollama-Helper] exe          = {exe}")
        online = asyncio.run(probe_ollama())
        print(f"[Ollama-Helper] server_online= {online}")
        models: list[str] = []
        if exe:
            models = asyncio.run(ollama_models_list(exe))
        print(f"[Ollama-Helper] installed    = {json.dumps(models, ensure_ascii=False)}")
        # 显式 --ensure 才真跑 ensure（可能耗时拉起服务 + pull，默认不触发）
        if "--ensure" in sys.argv:
            ok, msg = asyncio.run(ensure_ollama_ready(DEFAULT_MODEL))
            print(f"[Ollama-Helper] ensure_ok    = {ok}")
            print(f"[Ollama-Helper] ensure_msg   = {msg}")
        return 0
    raise SystemExit(_selfcheck())
