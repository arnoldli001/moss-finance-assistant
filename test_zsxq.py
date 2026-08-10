"""知识星球抓取 + Qwen3-8B 金融分析脚本"""
import sys
import json
import re
from pathlib import Path
from datetime import datetime

_PROJECT_ROOT = str(Path(__file__).resolve().parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from tools.zsxq_tool import fetch_zsxq_group_topics


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
                      timeout: int = 300, temperature: float = 0.2,
                      force_json: bool = False,
                      json_schema: dict = None) -> str:
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
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            body = _json.loads(resp.read().decode("utf-8"))
        return body["message"]["content"]
    except error.URLError as e:
        raise RuntimeError(f"Ollama 连接失败，请确认已运行 `ollama serve` 并拉取了模型: {e}")
    except (KeyError, IndexError, _json.JSONDecodeError) as e:
        raise RuntimeError(f"Ollama 返回格式异常: {e}, 原始响应: {body if 'body' in dir() else 'N/A'}")


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
                if name and sentiment:
                    if "利空" in sentiment:
                        sentiment = "利空"
                    elif "利好" in sentiment:
                        sentiment = "利好"
                    else:
                        continue
                    results.append({"name": name, "sentiment": sentiment, "count": count})
    except json.JSONDecodeError:
        pass
    except Exception:
        pass

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
                        if name and sentiment:
                            if "利空" in sentiment:
                                sentiment = "利空"
                            elif "利好" in sentiment:
                                sentiment = "利好"
                            else:
                                continue
                            results.append({"name": name, "sentiment": sentiment, "count": count})
    except Exception:
        pass

    if results:
        return results

    def _strip_name(name: str) -> str:
        """去除名字前后的序号、列表符号、括号装饰等"""
        if not name:
            return ""
        # 去除前后空白和装饰符
        name = name.strip().strip('【】"\'「」()（）[]<>《》·•').strip()
        # 去除开头的列表序号，如 "1.", "2、", "3)", "①", "-", "•"
        name = re.sub(r'^[\s]*([一二三四五六七八九十百0-9]+[\.、\)\）·]|[①-⑳]|[\-*•▲■▶◆▶])\s*', '', name)
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
        # 每条限制 300 字，避免单条过长
        short_val = val[:300] + ("..." if len(val) > 300 else "")
        entries.append(f"{idx}. {short_val}")
    content_text = "\n".join(entries)
    # 8B 模型上下文有限，截断至约 15k 字符
    if len(content_text) > 15000:
        content_text = content_text[:15000] + "\n...(内容截断)"

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

    print("\n" + "=" * 60)
    print("[分析] 调用本地 Ollama Qwen3-8B 进行金融分析...")
    print(f"[分析] 资讯条数: {len(data)}，文本长度: {len(content_text)} 字符")

    raw = _call_ollama_chat(
        model="qwen3:8b",
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        timeout=600,
        temperature=0.1,
        json_schema=schema,
    )
    print(f"[分析] 模型原始输出（前 500 字符）：\n{raw[:500]}")

    parsed = _parse_analysis(raw)
    if not parsed:
        print("[分析] ⚠ 未能解析到股票条目，返回空列表")
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
    # =================== Step 1: 抓取 ===================
    print("=" * 60)
    print("知识星球抓取工具（Playwright 浏览器自动化版）")
    print("=" * 60)

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
    print("\n[抓取] 最终返回:", result)

    # =================== Step 2: 金融分析 ===================
    news_path = _find_latest_news_json()
    if news_path is None:
        print("\n[分析] ⚠ zsxq_news 文件夹中未找到抓取结果 JSON，跳过分析")
        sys.exit(0)
    print(f"\n[分析] 读取最新抓取的 JSON：{news_path.name}")

    try:
        analysis = _run_financial_analysis(news_path)
    except RuntimeError as e:
        print(f"\n[分析] ⚠ Ollama 调用失败：{e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[分析] ⚠ 分析过程出错：{type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    formatted = _format_output_list(analysis)

    # =================== Step 3: 打印 & 保存 ===================
    print("\n" + "=" * 60)
    print("[分析结果] 股票热度 & 多空判断（按出现次数降序）")
    print("=" * 60)
    for i, line in enumerate(formatted, 1):
        print(f"{i:>3}. {line}")

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
    print(f"\n[分析] JSON 结果已保存至：{analysis_file}")

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
    print(f"[分析] 总结已保存至：{txt_file}")
