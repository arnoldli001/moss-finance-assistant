"""知识星球抓取 + Qwen3-8B 金融分析脚本

说明：此文件既是独立测试脚本，也被 api/server.py 的 _run_zsxq_analysis() 在
运行时通过 asyncio.create_subprocess_exec 调用（路径硬编码为项目根目录下
test_zsxq.py）。因此保留在根目录，不迁入 tests/。tests/ 目录仅存放纯单元测试。
"""
import sys
import json
import re
from pathlib import Path
from datetime import datetime

# Windows 控制台默认 gbk 编码，知识星球内容含 \xa0 等字符会导致 UnicodeEncodeError
# 强制 stdout/stderr 使用 utf-8，确保所有 Unicode 字符都能正确输出
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except AttributeError:
    pass

_PROJECT_ROOT = str(Path(__file__).resolve().parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from tools.zsxq_tool import fetch_zsxq_group_topics

from config.constants import (
    TEST_ZXSQ_OLLAMA_ENTRY_TRUNCATE_CHARS,
    TEST_ZXSQ_OLLAMA_TIMEOUT_SEC,
    TEST_ZXSQ_OLLAMA_TEMPERATURE,
    TEST_ZXSQ_CLI_TIMEOUT_SEC,
    TEST_ZXSQ_PREVIEW_VALUE_TRUNCATE_CHARS,
    TEST_ZXSQ_OLLAMA_CONTENT_COMPRESS_THRESHOLD,
    TEST_ZXSQ_DEBUG_LINE_JSON_LEN,
    TEST_ZXSQ_DEBUG_LINE_LONG_LEN,
    TEST_ZXSQ_FINAL_SUMMARY_PREVIEW_TRUNCATE,
    TEST_ZXSQ_UNVERIFIED_NUMS_MAX_DISPLAY,
)


# 运行时静默标志（由 --quiet CLI 参数设置）
# True 时抑制原始模型输出、巨大 JSON 转储等非必要打印，
# 避免 server.py 把这些行推送到前端造成视觉空白
_QUIET = False


def _log(msg: str = "", *, always: bool = False) -> None:
    """进度日志打印。quiet 模式下只打印 always=True 的关键行。"""
    if _QUIET and not always:
        return
    print(msg)


def _check_ollama_available(base_url: str = "http://localhost:11434") -> None:
    """预检 Ollama 服务是否在线（GET /api/tags），失败时立即抛 RuntimeError。

    在抓取前调用，避免 Ollama 未启动时白白浪费 30-60s 抓取时间。
    """
    from urllib import request, error
    req = request.Request(f"{base_url}/api/tags", method="GET")
    try:
        with request.urlopen(req, timeout=5) as resp:
            resp.read()
    except error.URLError as e:
        # WinError 10061 / Connection refused 都走这里
        raise RuntimeError(
            f"Ollama 服务未启动（{base_url}），请运行 `ollama serve` 并拉取 qwen3:8b 模型: {e}"
        )
    except Exception as e:
        raise RuntimeError(f"Ollama 预检失败: {e}")


def _find_latest_news_json() -> Path | None:
    """找到 zsxq_news 文件夹中最新的 json 文件（非 analysis_ 开头）"""
    news_dir = Path(_PROJECT_ROOT) / "zsxq_news"
    if not news_dir.exists():
        return None
    files = [
        f for f in news_dir.glob("*.json")
        if not f.name.startswith("analysis_")
        and not f.name.startswith(".")  # 排除 .zsxq_history.json / .zsxq_state.json 等隐藏文件
    ]
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def _call_ollama_chat(model: str, user_prompt: str, system_prompt: str = "",
                      base_url: str = "http://localhost:11434",
                      timeout: int = TEST_ZXSQ_OLLAMA_TIMEOUT_SEC, temperature: float = TEST_ZXSQ_OLLAMA_TEMPERATURE,
                      force_json: bool = False,
                      json_schema: dict | None = None) -> str:
    """通过 Ollama 原生接口调用本地模型，支持强制 JSON 输出和 Schema 约束"""
    import json as _json
    from urllib import request, error

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user_prompt})

    payload_dict = {
        "model": model,
        "messages": messages,
        "options": {"temperature": temperature},
        "stream": False,
    }
    if json_schema:
        # 使用 JSON Schema 严格约束输出结构
        payload_dict["format"] = json_schema
    elif force_json:
        payload_dict["format"] = "json"

    payload = _json.dumps(payload_dict).encode("utf-8")

    req = request.Request(
        f"{base_url}/api/chat",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    body = None  # 预声明，确保异常分支中可安全引用
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            body = _json.loads(resp.read().decode("utf-8"))
        return body["message"]["content"]
    except error.URLError as e:
        raise RuntimeError(f"Ollama 连接失败，请确认已运行 `ollama serve` 并拉取了模型: {e}")
    except (KeyError, IndexError, _json.JSONDecodeError) as e:
        raise RuntimeError(f"Ollama 返回格式异常: {e}, 原始响应: {body}")


def _extract_item(item: dict) -> dict | None:
    """从单个条目 dict 中提取并归一化 name/sentiment/count 三字段，无效则返回 None。"""
    # 兼容多种字段名
    name = str(
        item.get("name") or item.get("股票名") or item.get("stock")
        or item.get("股票") or item.get("公司") or item.get("公司名")
        or ""
    ).strip()
    sentiment = str(
        item.get("sentiment") or item.get("利好利空") or item.get("分析")
        or item.get("判断") or item.get("情绪") or item.get("倾向")
        or item.get("类型") or item.get("方向") or ""
    ).strip()
    count_raw = (
        item.get("count") or item.get("次数") or item.get("出现次数")
        or item.get("提及次数") or 0
    )
    try:
        count = int(count_raw)
    except (TypeError, ValueError):
        m = re.search(r'\d+', str(count_raw))
        count = int(m.group(0)) if m else 1
    if not name or not sentiment:
        return None
    # 归一化情绪：只保留"利好"/"利空"，其他跳过
    if "利空" in sentiment:
        sentiment = "利空"
    elif "利好" in sentiment:
        sentiment = "利好"
    else:
        return None
    return {"name": name, "sentiment": sentiment, "count": count}


def _parse_analysis(raw: str) -> list[dict]:
    """
    解析 LLM 返回的分析文本，提取股票名、利好/利空、出现次数。
    返回 list 形如:
      [{"name": "贵州茅台", "sentiment": "利好", "count": 8}, ...]
    """
    results = []

    # 预处理：去除 markdown 代码块标记 ```json ... ```
    cleaned = re.sub(r'```(?:json)?\s*', '', raw)
    cleaned = re.sub(r'```\s*$', '', cleaned).strip()

    # 尝试解析 JSON（可能是数组、{"stocks":[...]} 对象、或按行业分组的嵌套对象）
    try:
        # 先尝试解析整个字符串为 JSON
        parsed_obj = json.loads(cleaned)

        # 收集所有可能的条目列表（处理各种嵌套结构）
        def _collect_items(obj):
            """递归收集所有 dict 条目"""
            items = []
            if isinstance(obj, list):
                for x in obj:
                    items.extend(_collect_items(x))
            elif isinstance(obj, dict):
                # 如果这个 dict 本身看起来像一个条目（有 name/公司 字段）
                if any(k in obj for k in ("name", "股票名", "stock", "股票", "公司", "公司名")):
                    items.append(obj)
                # 递归检查所有值
                for v in obj.values():
                    if isinstance(v, (list, dict)):
                        items.extend(_collect_items(v))
            return items

        arr = _collect_items(parsed_obj)
        for item in arr:
            if isinstance(item, dict):
                extracted = _extract_item(item)
                if extracted:
                    results.append(extracted)
    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f"[解析] 回退失败: {e}")

    if results:
        return results

    # 回退：正则提取 JSON 数组
    try:
        first_json = re.search(r'\[[\s\S]*\]', cleaned)
        if first_json:
            arr = json.loads(first_json.group(0))
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, dict):
                        extracted = _extract_item(item)
                        if extracted:
                            results.append(extracted)
    except Exception as e:
        print(f"[解析] 回退失败: {e}")

    if results:
        return results

    def _strip_name(name: str) -> str:
        """去除名字前后的序号、列表符号、括号装饰等"""
        if not name:
            return ""
        # 去除前后空白和装饰符
        name = name.strip().strip('【】"\'「」()（）[]<>《》·•').strip()
        # 去除开头的列表序号，如 "1.", "2、", "3)", "①", "-", "•"
        name = re.sub(r'^[\s]*([一二三四五六七八九十百0-9]+[\.、\)\）·]|[①-⑳]|[\-*•▲■▶◆])\s*', '', name)
        # 再次去除前后空白和装饰符
        return name.strip().strip('【】"\'「」()（）[]<>《》·•').strip()

    # 回退：正则逐行解析 {股票名}:{利好/利空}{出现次数}
    pattern = re.compile(r'[【"\'\s]*([^\s：:{}【】"\'<>·][^：:{}【】"\'<>·]{0,20}?)[】"\'\s]*[：:]\s*[【"\'\s]*([利好利空]{2})[】"\'\s]*[（\(\s]*(\d+)[\)\）\s]*')
    for m in pattern.finditer(raw):
        name = _strip_name(m.group(1))
        sentiment = m.group(2).strip()
        try:
            count = int(m.group(3))
        except (TypeError, ValueError):
            count = 1
        if len(name) >= 2 and sentiment in ("利好", "利空"):
            results.append({"name": name, "sentiment": sentiment, "count": count})

    if results:
        return results

    # 再次回退：更宽松的逐行扫描
    lines = raw.splitlines()
    for line in lines:
        s_line = line.strip().lstrip('-*•\t ')
        if not s_line:
            continue
        m = re.search(r'(.{1,20}?)\s*[：:]\s*.*?(利好|利空).*?(\d+)', s_line)
        if not m:
            m = re.search(r'(.{1,20}?)\s*[：:]\s*(利好|利空)\D*(\d+)', s_line)
        if m:
            name = _strip_name(m.group(1))
            sentiment = m.group(2)
            try:
                count = int(m.group(3))
            except (TypeError, ValueError):
                count = 1
            if len(name) >= 2 and sentiment in ("利好", "利空"):
                results.append({"name": name, "sentiment": sentiment, "count": count})

    return results


def _run_financial_analysis(news_json_path: Path) -> list[dict]:
    """对 JSON 内容进行金融分析师分析，返回按次数从高到低排序的 list"""
    # 读取内容并截取（避免超过模型上下文）
    data = json.loads(news_json_path.read_text(encoding="utf-8"))
    # 取每条 value 为纯文本条目，控制总长度避免 8B 模型丢失指令
    entries = []
    for idx, (ts, val) in enumerate(data.items(), 1):
        # 每条限制 TEST_ZXSQ_OLLAMA_ENTRY_TRUNCATE_CHARS 字，避免单条过长
        short_val = val[:TEST_ZXSQ_OLLAMA_ENTRY_TRUNCATE_CHARS] + ("..." if len(val) > TEST_ZXSQ_OLLAMA_ENTRY_TRUNCATE_CHARS else "")
        entries.append(f"{idx}. {short_val}")
    content_text = "\n".join(entries)
    # 8B 模型上下文有限，截断至 TEST_ZXSQ_OLLAMA_CONTENT_COMPRESS_THRESHOLD 字符
    if len(content_text) > TEST_ZXSQ_OLLAMA_CONTENT_COMPRESS_THRESHOLD:
        content_text = content_text[:TEST_ZXSQ_OLLAMA_CONTENT_COMPRESS_THRESHOLD] + "\n...(内容截断)"

    system_prompt = (
        "你是A股金融分析师。从财经资讯中提取上市公司股票名，判断利好或利空。"
        "只提取上市公司，不提取行业名或指数名。"
        "利好=涨价/业绩增长/推荐/订单增长，利空=降价/下滑/风险提示。"
    )
    user_prompt = f"""从以下资讯中提取所有被提到的上市公司股票名，并判断对该公司是利好还是利空。

只提取上市公司（如贵州茅台、宁德时代、比亚迪、五粮液、古井贡酒、药明康德、迈瑞医疗等）。
不要提取行业名（白酒、AI、半导体）、指数名（上证、恒生）。

资讯（共{len(data)}条）：
{content_text}"""

    # JSON Schema 严格约束输出格式
    schema = {
        "type": "object",
        "properties": {
            "stocks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "上市公司股票名称"
                        },
                        "sentiment": {
                            "type": "string",
                            "enum": ["利好", "利空"],
                            "description": "利好或利空"
                        }
                    },
                    "required": ["name", "sentiment"]
                }
            }
        },
        "required": ["stocks"]
    }

    _log("\n" + "=" * 60)
    _log("[分析] 调用本地 Ollama Qwen3-8B 进行金融分析...", always=True)
    _log(f"[分析] 资讯条数: {len(data)}，文本长度: {len(content_text)} 字符")

    raw = _call_ollama_chat(
        model="qwen3:8b",
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        timeout=TEST_ZXSQ_CLI_TIMEOUT_SEC,
        temperature=0.1,
        json_schema=schema,
    )
    # 原始模型输出含大量 JSON 语法噪声，quiet 模式下抑制避免前端空白
    _log(f"[分析] 模型原始输出（前 500 字符）：\n{raw[:500]}")

    parsed = _parse_analysis(raw)
    if not parsed:
        _log("[分析] ⚠ 未能解析到股票条目，返回空列表")
        return []

    # 用 Python 统计每个股票名在原文中的实际出现次数（比 LLM 计数更准确）
    full_text = "\n".join(str(v) for v in data.values())
    for item in parsed:
        name = item["name"]
        # 统计股票名在全文中的出现次数
        actual_count = full_text.count(name) if len(name) >= 2 else 0
        item["count"] = max(actual_count, 1)  # 至少为 1

    # 去重（同名同情绪合并次数）
    merged: dict[tuple, int] = {}
    for item in parsed:
        key = (item["name"], item["sentiment"])
        merged[key] = merged.get(key, 0) + item["count"]
    parsed = [
        {"name": k[0], "sentiment": k[1], "count": v}
        for k, v in merged.items()
    ]
    # 按次数从高到低排序
    parsed.sort(key=lambda x: x["count"], reverse=True)
    return parsed


def _format_output_list(analysis_list: list[dict]) -> list[str]:
    """按要求格式化成 list: ['{股票名}:{利好/利空}{出现次数}', ...]"""
    return [f"{x['name']}:{x['sentiment']}{x['count']}" for x in analysis_list]


if __name__ == "__main__":
    # =================== CLI 参数 ===================
    import argparse
    parser = argparse.ArgumentParser(description="知识星球抓取 + Ollama 金融分析")
    parser.add_argument(
        "--quiet", action="store_true",
        help="静默模式：抑制原始模型输出/巨大 JSON 转储等非必要打印，"
             "供 server.py 子进程调用时使用，避免前端视觉空白",
    )
    args = parser.parse_args()
    _QUIET = args.quiet  # 设置模块级静默标志

    # =================== Quiet 模式：全局 stdout 过滤包装器 ====================
    # 被 import 的 tools/zsxq_tool.py 内部有 80+ 处 print([ZSXQ] ...)，
    # 这些调试信息 --quiet 时不能泄漏到 server.py 的前端推送通道。
    # 在导入 zsxq_tool / 调用前就把 sys.stdout 包一层 TextIOWrapper，
    # 确保 zsxq_tool 内部任何 print 都会经过我们的白名单判断。
    if _QUIET:
        import io as _io

        _ALLOWED_PREFIXES_QUIET = (
            "[分析]", "[抓取] 最终返回", "[抓取] 最终返回(截断)",
            "[分析结果]", "知识星球抓取工具", "=" * 10,  # 允许分隔线
        )

        class _QuietStdoutWrapper(_io.TextIOWrapper):
            """拦截 write()：仅放行对 server.py 有意义的关键行。

            放行：[分析] 错误/进度、[分析结果] 排名表、标题/分隔线。
            丢弃：[ZSXQ]/[ZSXQ-Search] 调试行、JSON dump（含 '{', '[' 且 > 500 字符的行）、
                   纯空行以外的 Playwright 调试输出。
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
                    # 某些环境下 TextIOWrapper 不能包 BytesIO，退化为 duck-typing
                    pass

            def write(self, s: str):
                if not s:
                    return 0
                # 按行分段处理（print 可能分多次 write 最后写 \n）
                self._buffer += s
                while "\n" in self._buffer:
                    line, self._buffer = self._buffer.split("\n", 1)
                    self._emit_line(line + "\n")
                return len(s)

            def _emit_line(self, line_with_nl: str) -> None:
                text = line_with_nl.rstrip("\r\n")
                stripped = text.strip()

                # 空行：不推前端、也不写
                if not stripped:
                    return

                # 始终放行的关键行（[分析] 错误/进度、分析结果排名、标题）
                if stripped.startswith("[分析]") or stripped.startswith("[分析结果]") \
                        or stripped.startswith("[抓取] 最终返回") \
                        or stripped.startswith("知识星球抓取工具") \
                        or stripped.startswith("=" * 10):
                    self._underlying.write(line_with_nl)
                    self._underlying.flush()
                    return

                # 丢弃：[ZSXQ] / [ZSXQ-Search] 调试行（85+ 条，都是内部日志）
                if stripped.startswith("[ZSXQ]") or stripped.startswith("[ZSXQ-Search]"):
                    return

                # 丢弃：明显是 JSON dump 的行（以 '{' 或 '[' 开头且长度大）
                if len(stripped) > TEST_ZXSQ_DEBUG_LINE_JSON_LEN and (stripped[0] in '{[' or stripped[0].isdigit() and '{' in stripped):
                    return

                # 丢弃：数字序号排名表之外的长行
                if len(stripped) > TEST_ZXSQ_DEBUG_LINE_LONG_LEN:
                    return

                # 其余全部丢弃，--quiet 时 stdout 只包含必须信息
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
            print(f"[test_zsxq] 注意：quiet 包装 stdout 失败（降级不启用）：{_wrap_err}")

    # =================== Step 0: Ollama 预检（fail fast）====================
    # 在抓取前检查 Ollama 是否在线，避免未启动时白白浪费 30-60s 抓取时间
    try:
        _check_ollama_available()
    except RuntimeError as e:
        # 关键错误，always 打印让 server.py 能识别
        _log(f"[分析] ⚠ Ollama 调用失败：{e}", always=True)
        sys.exit(1)
    except Exception as e:
        _log(f"[分析] ⚠ Ollama 预检异常：{type(e).__name__}: {e}", always=True)
        sys.exit(1)

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
            result_preview = result_preview[:TEST_ZXSQ_FINAL_SUMMARY_PREVIEW_TRUNCATE] + f"...(共 {len(result_preview)} 字符已截断)"
        _log(f"\n[抓取] 最终返回(截断): {result_preview}")
    else:
        _log(f"\n[抓取] 最终返回: {result}")

    # =================== Step 2: 金融分析 ===================
    news_path = _find_latest_news_json()
    if news_path is None:
        _log("\n[分析] ⚠ zsxq_news 文件夹中未找到抓取结果 JSON，跳过分析", always=True)
        sys.exit(0)
    _log(f"\n[分析] 读取最新抓取的 JSON：{news_path.name}", always=True)

    try:
        analysis = _run_financial_analysis(news_path)
    except RuntimeError as e:
        # Ollama 连接失败等关键错误，always 打印让 server.py 能识别
        _log(f"\n[分析] ⚠ Ollama 调用失败：{e}", always=True)
        sys.exit(1)
    except Exception as e:
        _log(f"\n[分析] ⚠ 分析过程出错：{type(e).__name__}: {e}", always=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)

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
        Path(_PROJECT_ROOT) / "zsxq_news"
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

    # 以日期+时间精确到秒为文件名，如 20260810214947.txt
    txt_file = (
        Path(_PROJECT_ROOT) / "zsxq_news"
        / f"{now_ts}.txt"
    )
    txt_file.write_text(txt_content, encoding="utf-8")
    _log(f"[分析] 总结已保存至：{txt_file}", always=True)
