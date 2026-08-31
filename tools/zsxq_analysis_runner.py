"""知识星球抓取 + 本地 qwen3:8b 金融分析 Runner（工具脚本）

说明：此文件是 **tools 层的独立可执行脚本**，承担两条职责：
  1) 被 interfaces/api.server._run_zsxq_analysis() 在运行时通过
     asyncio.create_subprocess_exec 以 --quiet 模式调用（子进程隔离，
     避免 Playwright sync API 与 asyncio 主线程冲突 + Ollama 长时阻塞）。
  2) 运维/调试手动运行：
        python -m tools.zsxq_analysis_runner         # 普通模式，打印全部
        python tools/zsxq_analysis_runner.py --quiet # 静默模式，仅里程碑
历史说明：此脚本曾经放在项目根叫 test_zsxq.py，但它不含任何 pytest
test_* 用例，纯属工具脚本，现按代码层级迁到 tools/ 下与 zsxq_tool.py
（抓取/搜索工具）做兄弟管理。
"""
import sys
import json
import re
from pathlib import Path
from datetime import datetime

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except AttributeError:
    pass


# ---------------------------------------------------------------------------
# PROJECT_ROOT 鲁棒算法：向上查找标志性文件 AGENTS.md（项目独有）
# 算法与 tools/zsxq_tool.py._find_project_root 保持一致，
# 确保 tools/zsxq_analysis_runner.py 无论被
#   - 子进程 cwd=项目根 直接 runpy
#   - 手动任意 cwd `python -m tools.zsxq_analysis_runner`
#   - pytest monkeypatch chdir
# 都能找到真正的项目根（zsxq_news/、data/、config/ 所在目录）。
# ---------------------------------------------------------------------------
def _find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for _ in range(12):
        if (cur / "AGENTS.md").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    # 兜底：按本文件位于 <项目根>/tools/ 计算 → 上 1 层
    try:
        return start.resolve().parents[1]
    except (IndexError, Exception):
        return start.resolve().parent


_PROJECT_ROOT = str(_find_project_root(Path(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from tools.zsxq_tool import fetch_zsxq_group_topics

from config.constants import (
    TEST_ZXSQ_OLLAMA_ENTRY_TRUNCATE_CHARS,
    TEST_ZXSQ_OLLAMA_TIMEOUT_SEC,
    TEST_ZXSQ_OLLAMA_TEMPERATURE,
    TEST_ZXSQ_CLI_TIMEOUT_SEC,
    TEST_ZXSQ_OLLAMA_CONTENT_COMPRESS_THRESHOLD,
    TEST_ZXSQ_DEBUG_LINE_JSON_LEN,
    TEST_ZXSQ_DEBUG_LINE_LONG_LEN,
    TEST_ZXSQ_FINAL_SUMMARY_PREVIEW_TRUNCATE,
)

# 运行时静默标志（由 --quiet CLI 参数设置），True 时抑制原始模型输出、
# 巨大 JSON 转储等非必要打印，避免 server.py 子进程推到前端造成视觉空白。
_QUIET = False


def _log(msg: str = "", *, always: bool = False) -> None:
    """进度日志打印。quiet 模式下只打印 always=True 的关键行。"""
    if _QUIET and not always:
        return
    print(msg)


# ---------------------------------------------------------------------------
# Ollama 调度 + 分析解析：全部委托 shared/utils/ollama_analyzer.py（独立文件）。
# 保留 _check_ollama_available / _call_ollama_chat / _parse_analysis / _run_financial_analysis
# 的函数签名（作为薄包装），保证任何直接 import 这些函数的旧代码不破。
# ---------------------------------------------------------------------------
from shared.utils.ollama_helper import probe_ollama  # noqa: E402
from shared.utils.ollama_analyzer import (  # noqa: E402
    ollama_analyze,
    parse_jsonish,
    parse_stock_sentiment_items,
    truncate_entries_for_prompt,
    analyze_zsxq_hot_news as _shared_analyze_hot,
)
from shared.utils.zsxq_paths import (  # noqa: E402
    get_zsxq_news_dir,
    ensure_zsxq_news_dir_ready,
)
ensure_zsxq_news_dir_ready(Path(_PROJECT_ROOT))  # 幂等、首次自动迁移
_ZSXQ_OUTPUT_DIR = get_zsxq_news_dir(Path(_PROJECT_ROOT))


def _check_ollama_available(base_url: str = "http://localhost:11434") -> None:
    """预检 Ollama 服务是否在线。调用 ollama_helper.probe_ollama（单一实现）。
    失败抛 RuntimeError（与原函数语义一致）。"""
    import asyncio as _aio
    try:
        ok = _aio.run(probe_ollama(base_url=base_url, timeout=5.0))
    except Exception as e:
        raise RuntimeError(f"Ollama 预检异常: {e}")
    if not ok:
        raise RuntimeError(
            f"Ollama 服务未启动（{base_url}），请运行 `ollama serve` 并拉取 qwen3:8b 模型。"
        )


def _find_latest_news_json() -> Path | None:
    """找到 output/zsxq_news/ 文件夹中最新的 json 文件（排除 analysis_ 前缀输出）。"""
    news_dir = get_zsxq_news_dir(Path(_PROJECT_ROOT))
    if not news_dir.exists():
        return None
    files = [
        f for f in news_dir.glob("*.json")
        if not f.name.startswith("analysis_") and not f.name.startswith(".")
    ]
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def _call_ollama_chat(model: str, user_prompt: str, system_prompt: str = "",
                      base_url: str = "http://localhost:11434",
                      timeout: int = TEST_ZXSQ_OLLAMA_TIMEOUT_SEC,
                      temperature: float = TEST_ZXSQ_OLLAMA_TEMPERATURE,
                      force_json: bool = False,
                      json_schema: dict | None = None) -> str:
    """兼容包装：老签名 → 新 ollama_analyze。

    与原函数 100% 等价：成功返回模型 content 字符串；
    HTTP 失败 / 返回异常 → raise RuntimeError（老调用方 try/except 分支不破）。
    """
    res = ollama_analyze(
        user=user_prompt, system=system_prompt, model=model, base_url=base_url,
        timeout=timeout, temperature=temperature,
        schema=json_schema, force_json=force_json,
    )
    status = int((res.meta or {}).get("status") or 0)
    err = (res.meta or {}).get("error")
    if err:
        raise RuntimeError(str(err))
    if status < 200 or status >= 300:
        raise RuntimeError(f"Ollama HTTP {status}")
    return res.final_text


def _parse_analysis(raw: str) -> list[dict]:
    """解析 LLM 返回的盘前小作文热度输出 → list[dict(name, sentiment, count)]。

    旧函数：本文件内 300+ 行 4 重回退解析。现在 100% 委托
    shared.utils.ollama_analyzer.parse_stock_sentiment_items（共享 4 重回退链，
    单一实现，主 Agent/调度/脚本三方同逻辑）。"""
    return parse_stock_sentiment_items(raw)


def _run_financial_analysis(news_json_path: Path) -> list[dict]:
    """对 JSON 内容做金融分析师分析，返回按出现次数从高到低排序的 list。

    现在实现优先走 shared.utils.ollama_analyzer.analyze_zsxq_hot_news（封装好的
    盘前小作文热度模板，system prompt / schema / parse 回退链都已统一），
    之后再做"真实文本计数重算 + 去重 + 排序"，与旧输出排序 100% 等价。
    """
    data = json.loads(news_json_path.read_text(encoding="utf-8"))

    # 条目格式化：与原来的 1. xxx / 2. xxx 完全一致；但实现也复用 analyzer 的截断策略
    content_text, _ = truncate_entries_for_prompt(data)

    _log("\n" + "=" * 60)
    _log("[分析] 调用本地 Ollama Qwen3-8B 进行金融分析...", always=True)
    _log(f"[分析] 资讯条数: {len(data)}，文本长度: {len(content_text)} 字符")

    def _progress(msg: str) -> None:
        # analyzer 侧进度消息通过 _log 打印（quiet 模式下会被 stdout 过滤层正确收敛）
        if "[分析] " in msg or "🧠" in msg or "✅" in msg or "❌" in msg:
            _log(f"[分析] {msg.replace('🧠 ', '').replace('✅ ', '').replace('❌ ', '')}", always=True)
        else:
            _log(f"[分析] {msg}")

    # 走统一模板（schema/低温度/回退解析链 4 层都在内）
    parsed = _shared_analyze_hot(
        content_text, model="qwen3:8b",
        timeout=TEST_ZXSQ_CLI_TIMEOUT_SEC, progress_cb=_progress,
    )

    # 诊断：最终结果的第一条完整 JSON（与旧 [分析] 模型原始输出 打印格式对齐）
    try:
        raw_view = json.dumps(
            parsed if isinstance(parsed, (list, dict)) else {"stocks": parsed},
            ensure_ascii=False, indent=2,
        )
    except Exception:
        raw_view = str(parsed)
    _log(f"[分析] 模型原始输出（前 500 字符）：\n{raw_view[:500]}")

    if not parsed:
        _log("[分析] ⚠ 未能解析到股票条目，返回空列表")
        return []

    # 真实文本计数 + 去重合并（保留旧排序算法，输出与历史版本完全可比）
    full_text = "\n".join(str(v) for v in data.values())
    for item in parsed:
        name = item.get("name") or ""
        actual_count = full_text.count(name) if len(name) >= 2 else 0
        item["count"] = max(int(item.get("count") or 0), actual_count, 1)
    merged: dict[tuple, int] = {}
    for item in parsed:
        name = item.get("name") or ""
        sentiment = item.get("sentiment") or ""
        if not name or sentiment not in ("利好", "利空"):
            continue
        key = (name, sentiment)
        merged[key] = merged.get(key, 0) + int(item.get("count") or 1)
    final = [{"name": k[0], "sentiment": k[1], "count": v} for k, v in merged.items()]
    final.sort(key=lambda x: x["count"], reverse=True)
    return final


def _format_output_list(analysis_list: list[dict]) -> list[str]:
    return [f"{x['name']}:{x['sentiment']}{x['count']}" for x in analysis_list]


def main() -> int:
    """子进程入口（runpy 也会走这里）。返回值 = sys.exit code。"""
    global _QUIET
    import argparse
    parser = argparse.ArgumentParser(description="知识星球抓取 + Ollama 金融分析")
    parser.add_argument(
        "--quiet", action="store_true",
        help="静默模式：仅放行对 server 子进程有意义的里程碑/结果行，"
             "丢弃 [ZSXQ] 内部调试、巨大 JSON dump、Playwright 杂项输出。"
             "供 server.py 子进程调用，避免前端视觉空白。",
    )
    args = parser.parse_args()
    _QUIET = args.quiet

    # =============== Quiet 模式：全局 stdout 过滤包装器 =================
    if _QUIET:
        import io as _io

        _ALLOWED_PREFIXES_QUIET = (
            "[分析]", "[抓取] 最终返回", "[抓取] 最终返回(截断)",
            "[分析结果]", "知识星球抓取工具", "=" * 10,
        )

        class _QuietStdoutWrapper(_io.TextIOWrapper):
            """拦截 write()：仅放行对 server.py 有意义的关键行。

            放行：[分析] 错误/进度、[分析结果] 排名表、标题/分隔线。
            丢弃：[ZSXQ]/[ZSXQ-Search] 调试行、JSON dump（大括号开头且
                   > TEST_ZXSQ_DEBUG_LINE_JSON_LEN 字符的行）、过长杂项输出。
            """
            __slots__ = ("_underlying", "_buffer")

            def __init__(self, underlying):
                self._underlying = underlying
                self._buffer = ""
                try:
                    super().__init__(
                        _io.BytesIO(),
                        encoding=getattr(underlying, "encoding", "utf-8"),
                        errors="replace",
                        newline="",
                        line_buffering=True,
                        write_through=True,
                    )
                except Exception:
                    pass

            def write(self, s: str):
                if not s:
                    return 0
                self._buffer += s
                while "\n" in self._buffer:
                    line, self._buffer = self._buffer.split("\n", 1)
                    self._emit_line(line + "\n")
                return len(s)

            def _emit_line(self, line_with_nl: str) -> None:
                text = line_with_nl.rstrip("\r\n")
                stripped = text.strip()
                if not stripped:
                    return

                # 里程碑/标题行：始终放行
                if (stripped.startswith("[分析]")
                        or stripped.startswith("[分析结果]")
                        or stripped.startswith("[抓取] 最终返回")
                        or stripped.startswith("知识星球抓取工具")
                        or stripped.startswith("=" * 10)):
                    self._underlying.write(line_with_nl)
                    self._underlying.flush()
                    return

                # [ZSXQ] / [ZSXQ-Search] 内部调试行：丢弃
                if stripped.startswith("[ZSXQ]") or stripped.startswith("[ZSXQ-Search]"):
                    return

                # 巨大 JSON dump：丢弃
                if (len(stripped) > TEST_ZXSQ_DEBUG_LINE_JSON_LEN
                        and (stripped[0] in '{['
                             or (stripped[0].isdigit() and '{' in stripped))):
                    return

                # 超长杂项：丢弃
                if len(stripped) > TEST_ZXSQ_DEBUG_LINE_LONG_LEN:
                    return

            def flush(self):
                if self._buffer:
                    self._emit_line(self._buffer + "\n")
                    self._buffer = ""
                try:
                    self._underlying.flush()
                except Exception:
                    pass

            def __getattr__(self, name):
                return getattr(self._underlying, name)

        try:
            _real_stdout = sys.stdout
            sys.stdout = _QuietStdoutWrapper(_real_stdout)
        except Exception as _wrap_err:
            print(f"[zsxq_analysis_runner] 注意：quiet 包装 stdout 失败"
                  f"（降级不启用）：{_wrap_err}")

    # =================== Step 0: Ollama 预检（fail fast）====================
    try:
        _check_ollama_available()
    except RuntimeError as e:
        _log(f"[分析] ⚠ Ollama 调用失败：{e}", always=True)
        return 1
    except Exception as e:
        _log(f"[分析] ⚠ Ollama 预检异常：{type(e).__name__}: {e}", always=True)
        return 1

    # =================== Step 1: 抓取 ===================
    _log("=" * 60, always=True)
    _log("知识星球抓取工具（Playwright 浏览器自动化版）", always=True)
    _log("=" * 60, always=True)

    params = {
        "max_topics": 200,
        "incremental": True,
        "save_to_db": False,
        "max_scrolls": 20,
    }
    if hasattr(fetch_zsxq_group_topics, "invoke"):
        result = fetch_zsxq_group_topics.invoke(params)
    else:
        result = fetch_zsxq_group_topics(**params)
    # quiet 模式下截断 result 转储，避免巨大 JSON 被推到前端造成空白
    if _QUIET:
        result_preview = str(result)
        if len(result_preview) > TEST_ZXSQ_FINAL_SUMMARY_PREVIEW_TRUNCATE:
            result_preview = (
                result_preview[:TEST_ZXSQ_FINAL_SUMMARY_PREVIEW_TRUNCATE]
                + f"...(共 {len(result_preview)} 字符已截断)"
            )
        _log(f"\n[抓取] 最终返回(截断): {result_preview}")
    else:
        _log(f"\n[抓取] 最终返回: {result}")

    # =================== Step 2: 金融分析 ===================
    news_path = _find_latest_news_json()
    if news_path is None:
        _log("\n[分析] ⚠ zsxq_news 文件夹中未找到抓取结果 JSON，跳过分析",
             always=True)
        return 0
    _log(f"\n[分析] 读取最新抓取的 JSON：{news_path.name}", always=True)

    try:
        analysis = _run_financial_analysis(news_path)
    except RuntimeError as e:
        _log(f"\n[分析] ⚠ Ollama 调用失败：{e}", always=True)
        return 1
    except Exception as e:
        _log(f"\n[分析] ⚠ 分析过程出错：{type(e).__name__}: {e}", always=True)
        import traceback
        traceback.print_exc()
        return 1

    formatted = _format_output_list(analysis)

    # =================== Step 3: 打印 & 保存 ===================
    _log("\n" + "=" * 60, always=True)
    _log("[分析结果] 股票热度 & 多空判断（按出现次数降序）", always=True)
    _log("=" * 60, always=True)
    for i, line in enumerate(formatted, 1):
        _log(f"{i:>3}. {line}", always=True)

    # 统一时间戳，确保 json 与 txt 文件名一致（精确到秒）
    now_ts = datetime.now().strftime('%Y%m%d%H%M%S')
    now_display = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    analysis_payload = {
        "generated_at": now_display,
        "source_news": news_path.name,
        "total_stocks": len(analysis),
        "sorted_list": formatted,
        "details": analysis,
    }
    analysis_file = (
        _ZSXQ_OUTPUT_DIR
        / f"analysis_{now_ts}.json"
    )
    analysis_file.write_text(
        json.dumps(analysis_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _log(f"\n[分析] JSON 结果已保存至：{analysis_file}", always=True)

    # =================== Step 4: 写入以日期命名的 txt 总结 ===================
    txt_lines = [
        f"知识星球财经资讯分析总结",
        f"生成时间：{now_display}",
        f"数据来源：{news_path.name}",
        f"涉及股票数：{len(analysis)}",
        "",
        "【股票热度 & 多空判断（按出现次数降序）】",
    ]
    for i, line in enumerate(formatted, 1):
        txt_lines.append(f"{i:>3}. {line}")
    txt_content = "\n".join(txt_lines) + "\n"

    txt_file = (
        _ZSXQ_OUTPUT_DIR
        / f"{now_ts}.txt"
    )
    txt_file.write_text(txt_content, encoding="utf-8")
    _log(f"[分析] 总结已保存至：{txt_file}", always=True)
    return 0


if __name__ == "__main__":
    # 允许：
    #   python tools/zsxq_analysis_runner.py [--quiet]
    #   python -m tools.zsxq_analysis_runner [--quiet]
    sys.exit(main())
