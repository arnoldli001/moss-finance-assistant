"""shared/console_encoding.py — 控制台编码自愈（Windows GBK 下 emoji 崩溃的单一真源）。

背景（实测失败模式）：
    Windows 中文环境控制台默认代码页 GBK(cp936)，`sys.stdout.encoding` 即为 'gbk'。
    本项目 CLI 大量使用 emoji（💡 ⚠️ ✅），`print()` 时抛
        UnicodeEncodeError: 'gbk' codec can't encode character '\\U0001f4a1'
    表现为 `python main.py` 直接崩溃（exit code 1），必须靠外部设置
    `PYTHONIOENCODING=utf-8` 才能跑通 —— 把环境准备成本转嫁给了使用者。

设计原则：
    1. **不改已有约定**：调用方（main.py / interfaces.api.server）自带的
       `reconfigure(line_buffering=True)` 继续保留，本函数只补编码维度；
    2. **可用性优先**：任何失败（流不支持 reconfigure / PyInstaller 冻结 /
       重定向到不支持的对象）都静默跳过，绝不因为控制台设置阻断启动 ——
       与治理层 LLM 分类器 `fail-open` 的既有取向一致；
    3. **优先 UTF-8**：UTF-8 能编码全部 emoji；退化候选 cp65001 是同一代码页的
       别名（老 Python 只认这个名字），二者都不可用时保持原编码不动。

唯一真源：main.py（CLI 入口）与 interfaces/api/server.py（reload 子进程 /
python interfaces/api/server.py 直跑）都调用本函数，避免两处各写一份。
"""
from __future__ import annotations

import sys
from typing import List, Optional, Tuple

# 候选编码（按优先级）：utf-8 覆盖全部 emoji；cp65001 为其代码页别名
CONSOLE_ENCODING_CANDIDATES: Tuple[str, ...] = ("utf-8", "utf8", "cp65001")

# 归一化比较用：这些别名视为「已经是 UTF-8」，无需 reconfigure
_UTF8_ALIASES = frozenset({"utf-8", "utf8", "cp65001", "u8"})


def _normalize(encoding: Optional[str]) -> str:
    """编码名归一化：小写 + 去分隔符（'UTF-8'/'utf_8'/'utf8' → 'utf8'）。"""
    return (encoding or "").strip().lower().replace("-", "").replace("_", "")


def _is_utf8(encoding: Optional[str]) -> bool:
    """判断给定编码名是否已经是 UTF-8 家族（含 cp65001 别名）。"""
    return _normalize(encoding) in {_normalize(a) for a in _UTF8_ALIASES}


def _reconfigure_encoding(stream: object, encoding: str) -> bool:
    """对单个流尝试切换编码；成功返回 True。

    不支持的流（无 reconfigure / 被替换为 StringIO）返回 False，由调用方决定是否继续。
    """
    reconfigure = getattr(stream, "reconfigure", None)
    if not callable(reconfigure):
        return False
    try:
        reconfigure(encoding=encoding)
    except (ValueError, LookupError, OSError, AttributeError, TypeError):
        # 流已关闭/不支持该编码/已写入过数据 → 不阻断启动
        return False
    return True


def ensure_utf8_console(streams: Optional[List[object]] = None) -> List[str]:
    """把 stdout/stderr 编码切换为 UTF-8（幂等），返回「实际完成切换的流」清单。

    :param streams: 待处理的流列表；默认 ``[sys.stdout, sys.stderr]``
    :return: 已完成编码切换的流（长度 0 表示无需切换或全部跳过）
    """
    targets = [sys.stdout, sys.stderr] if streams is None else list(streams)
    switched: List[str] = []
    for stream in targets:
        if stream is None:
            continue
        current = getattr(stream, "encoding", "") or ""
        if _is_utf8(current):
            continue  # 已是 UTF-8（含调用方设了 PYTHONIOENCODING 的情况）
        for candidate in CONSOLE_ENCODING_CANDIDATES:
            if _reconfigure_encoding(stream, candidate):
                switched.append(getattr(stream, "name", None) or "<stream>")
                break
    return switched
