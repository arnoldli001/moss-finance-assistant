"""shared.utils.zsxq_paths — 知识星球抓取结果目录的统一路径 + 自动迁移。

背景（用户本轮明确要求）：
    原先所有抓取、分析结果都放在项目根下的 zsxq_news/ 文件夹。
    按 AGENTS.md L55-56 「output/ = 生成的文件输出」语义，现统一迁移到
    项目根下的 output/zsxq_news/。

本文件提供的三个公共能力：
    1. ZSXQ_NEWS_DIR_NAME = "zsxq_news" 常量（避免各模块硬编码字符串）
    2. get_zsxq_news_dir(project_root)       → project_root / output / zsxq_news
    3. ensure_zsxq_news_dir_ready(project_root, *, emit=None)
         ① mkdir -p 新目录（含 output/）
         ② 若旧目录 project_root/zsxq_news/ 存在且新目录为空，
            自动把旧内容整体 MOVE 到新位置（Windows 同卷原子性 rename）。
            兼容旧数据：登录态 .zsxq_state.json、历史抓取 JSON/TXT、
                       search_*.json、analysis_*.json 全部保留。
         ③ 幂等：多进程/多模块重复调用安全，不会重复搬运或抛错。

隐藏状态文件名常量（避免 3 处各自写字符串飘移）：
    ZSXQ_STATE_FILE_NAME    = ".zsxq_state.json"
    ZSXQ_HISTORY_FILE_NAME  = ".zsxq_history.json"

使用（三文件一致，只需改两行）：
    # --- 原代码（硬编码 / 各自算路径）---
    # ZSXQ_NEWS_DIR = PROJECT_ROOT / "zsxq_news"
    # --- 新代码 ---
    from shared.utils.zsxq_paths import (
        get_zsxq_news_dir, ensure_zsxq_news_dir_ready,
        ZSXQ_STATE_FILE_NAME, ZSXQ_HISTORY_FILE_NAME,
    )
    ZSXQ_NEWS_DIR = get_zsxq_news_dir(PROJECT_ROOT)
    ensure_zsxq_news_dir_ready(PROJECT_ROOT)  # 幂等、首次触发迁移
"""
from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Callable, Optional


# ---------------------------------------------------------------------------
# 目录名 / 隐藏文件名常量（全局唯一，避免 3 模块各自写字符串漂移）
# ---------------------------------------------------------------------------

ZSXQ_NEWS_DIR_NAME: str = "zsxq_news"

# 知识星球 Playwright storage_state（保存登录 Cookie）
ZSXQ_STATE_FILE_NAME: str = ".zsxq_state.json"

# 已抓取 topic_id 增量判断历史记录
ZSXQ_HISTORY_FILE_NAME: str = ".zsxq_history.json"


ProgressLogger = Callable[[str], None]


# ---------------------------------------------------------------------------
# 两个对外函数（所有 zsxq_*.py 只应该 import 这里）
# ---------------------------------------------------------------------------

def get_zsxq_news_dir(project_root: Path) -> Path:
    """返回【新的、符合 AGENTS.md L55-56 output/ 语义】的知识星球抓取结果目录。

    固定结构：<PROJECT_ROOT>/output/zsxq_news/
    """
    return Path(project_root).resolve() / "output" / ZSXQ_NEWS_DIR_NAME


def get_zsxq_state_file(project_root: Path) -> Path:
    """返回 storage_state.json 的标准位置（新目录下）。"""
    return get_zsxq_news_dir(project_root) / ZSXQ_STATE_FILE_NAME


def get_zsxq_history_file(project_root: Path) -> Path:
    """返回抓取历史 JSON 的标准位置（新目录下）。"""
    return get_zsxq_news_dir(project_root) / ZSXQ_HISTORY_FILE_NAME


def ensure_zsxq_news_dir_ready(
    project_root: Path,
    *,
    emit: Optional[ProgressLogger] = None,
) -> Path:
    """确保新目录存在 + 首次运行时自动从老位置（<root>/zsxq_news）MOVE 所有内容。

    幂等：任何进程/任何模块任何时机重复调用均安全（第二次及以后 = 纯 mkdir + 立即返回）。
    返回：新目录 Path（可直接 mkdir / write / glob）。
    """
    root = Path(project_root).resolve()
    old_dir = root / ZSXQ_NEWS_DIR_NAME
    new_dir = get_zsxq_news_dir(root)

    def _log(msg: str) -> None:
        if emit is not None:
            try:
                emit(msg)
            except Exception:
                pass
        logging.getLogger("zsxq_paths").info(msg)

    # 1) 先 mkdir -p output/zsxq_news/（即使最后会 move 整个目录，
    #    mkdir 在 Windows 上是幂等的，父目录 output/ 也会一起被创建）
    try:
        new_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:  # pragma: no cover - 文件系统异常不应该阻断业务
        _log(f"[zsxq_paths] mkdir {new_dir} 失败（仍将尝试使用）：{e}")

    # 2) 自动迁移触发条件（缺一不可，避免误搬）：
    #      a) 老目录存在；
    #      b) 老目录非空（至少 1 条条目，包括隐藏文件）；
    #      c) 新目录为空（没有任何抓取/分析新产物落下，防止和旧内容混在一起后覆盖）。
    if old_dir.exists() and old_dir.is_dir():
        new_is_empty = not any(new_dir.iterdir())
        old_has_any = any(old_dir.iterdir())
        if old_has_any and new_is_empty:
            try:
                # 注意：shutil.move(src, dst_parent) 的语义是「把 src 本身（含目录名）
                # 搬到 dst_parent/ 下」。但我们前面已经调用了 new_dir.mkdir() 创建了
                # 一个空的 output/zsxq_news/ 目录 → shutil.move 会因"目标已存在"抛错。
                # 因此当 new_dir 确实为空时，先把这个空壳 rmdir 掉（仅当空目录时成功），
                # 再执行 move，Windows 同 NTFS 卷下退化为原子 rename。
                if new_dir.exists() and new_dir.is_dir():
                    try:
                        new_dir.rmdir()  # 仅空目录可删除
                    except OSError:
                        # 删不掉（突然又有文件写入了）：放弃自动迁移，安全退出
                        _log(
                            "[zsxq_paths] 自动迁移取消：准备搬旧目录时发现新目录瞬间有内容，"
                            "为避免覆盖已终止（后续所有写入自动走新路径，业务不受影响）"
                        )
                        return new_dir
                shutil.move(str(old_dir), str(new_dir.parent))
                _log(
                    f"[zsxq_paths] 已自动迁移知识星球结果目录："
                    f"{old_dir}  →  {new_dir}（内容：{ZSXQ_STATE_FILE_NAME} + JSON/TXT/search/analysis 共 N 项，"
                    f"如需回滚，把 {new_dir} 整体移回 <项目根> 即可）"
                )
            except Exception as e:  # pragma: no cover - 任何迁移异常都必须吞掉，业务继续写新目录
                _log(
                    f"[zsxq_paths] 自动迁移失败（不影响运行，新结果会写到 {new_dir}）："
                    f"{type(e).__name__}: {e}"
                )
        else:
            # 老目录空或者新目录已有内容：两种情况都不能动，安静退出
            if not old_has_any:
                try:
                    # 老目录空：顺手 rmdir（只读空目录，非空时 OSError 直接跳过）
                    old_dir.rmdir()
                except OSError:
                    pass

    return new_dir
