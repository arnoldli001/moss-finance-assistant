"""
全局子进程注册表 —— 端到端进程生命周期的最后防线。

背景（Ctrl+C 后进程残留的根因）：
  - asyncio 的 create_subprocess_exec / subprocess.Popen 拉起的子进程
    （zsxq_analysis_runner、ollama pull、playwright 子进程树等）在父进程
    事件循环被取消/关闭时**不会**被自动杀死（Windows 尤甚，直接变孤儿进程）。
  - 各工具内部的 try/except Exception 兜不住 CancelledError（py3.8+ 是
    BaseException），超时/关停路径经常跳过 kill。
  - 因此除了"各调用点自己的正常清理"外，还需要一张全局登记表：
    任何子进程出生即登记，退出即注销；服务关闭时统一树杀仍存活的残留。

设计原则（最小机制）：
  - track(proc, name) / untrack(proc)：登记 + 注销（幂等、线程安全）。
    proc 同时兼容 asyncio.subprocess.Process 与 subprocess.Popen。
  - kill_all_tracked()：关闭阶段调用，逐个树杀（Windows taskkill /T /F，
    POSIX 优先 killpg 回退 kill），每进程限时 SHUTDOWN_PROC_KILL_WAIT_SEC。
  - 不做守护线程、不做自动重杀：只在 lifespan 关闭时由 server 显式调用一次。
"""
from __future__ import annotations

import asyncio
import subprocess
import sys
import threading
import time
from typing import Dict, List, Optional

from config.constants import SHUTDOWN_PROC_KILL_WAIT_SEC  # noqa: E402

# ---------------------------------------------------------------------------
# 注册表本体：{id(proc): entry}
#   entry = {"proc": Any, "pid": int, "name": str, "born": float}
# 线程安全：ollama_helper 的 serve 拉起发生在同步上下文（Popen），而
# kill_all_tracked 在事件循环里跑，故用一把进程内互斥锁保护 dict。
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_registry: Dict[int, Dict] = {}


def _pid_of(proc) -> Optional[int]:
    pid = getattr(proc, "pid", None)
    return int(pid) if pid is not None else None


def _is_alive(proc) -> bool:
    """兼容 asyncio.Process（returncode）与 subprocess.Popen（poll）。"""
    try:
        if hasattr(proc, "poll"):
            return proc.poll() is None
        rc = getattr(proc, "returncode", None)
        return rc is None
    except Exception:
        return False


def track(proc, name: str = "") -> None:
    """登记一个子进程（幂等；重复登记同一对象只刷新 name）。"""
    pid = _pid_of(proc)
    if pid is None:
        return
    with _lock:
        _registry[id(proc)] = {
            "proc": proc,
            "pid": pid,
            "name": name or type(proc).__name__,
            "born": time.time(),
        }


def untrack(proc) -> None:
    """注销（进程正常退出后由调用方调用；kill_all 也会自动清理）。"""
    with _lock:
        _registry.pop(id(proc), None)


def _tree_kill_sync(pid: int) -> None:
    """树杀一个进程（含其所有子进程）。永不抛异常。"""
    try:
        if sys.platform.startswith("win"):
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True, timeout=8,
            )
        else:
            import signal
            try:
                import os
                os.killpg(os.getpgid(pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                import os
                os.kill(pid, signal.SIGKILL)
    except Exception:
        pass


async def kill_all_tracked() -> List[int]:
    """关闭阶段统一清理：树杀所有仍存活的已登记子进程。

    返回被杀掉的 pid 列表（用于日志）。单个进程超时未确认死亡也继续下一个
    （taskkill /F 是强杀，正常即刻生效；等待仅防极端句柄挂起）。
    """
    with _lock:
        entries: List[Dict] = list(_registry.values())
        _registry.clear()

    killed: List[int] = []
    for e in entries:
        proc = e["proc"]
        pid = e["pid"]
        if not _is_alive(proc):
            continue
        _tree_kill_sync(pid)
        # 同步等待确认退出（不阻塞事件循环太久：上限 SHUTDOWN_PROC_KILL_WAIT_SEC）
        try:
            if isinstance(proc, asyncio.subprocess.Process):
                await asyncio.wait_for(proc.wait(), timeout=SHUTDOWN_PROC_KILL_WAIT_SEC)
            else:
                # subprocess.Popen.wait 是阻塞调用 → 丢线程池
                await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(
                        None, lambda p=proc: p.wait(timeout=SHUTDOWN_PROC_KILL_WAIT_SEC)
                    ),
                    timeout=SHUTDOWN_PROC_KILL_WAIT_SEC + 1.0,
                )
        except Exception:
            pass
        if _is_alive(proc):
            # 树杀后仍存活（罕见）：直接对主进程补一刀
            try:
                proc.kill()
            except Exception:
                pass
        killed.append(pid)
        print(f"[ProcRegistry] 已清理残留子进程 pid={pid} name={e['name']}")
    return killed
