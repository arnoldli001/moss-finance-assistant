# -*- coding: utf-8 -*-
"""
LLM 输出质量评估脚本（CI 集成用）：结构化回归测试，度量 LLM 输出质量。

工作流程：
  1) 加载 golden_set.json
  2) 对每个样本调用被测 Agent（默认调本地 http://localhost:8000/chat 接口）
  3) 把 (样本, Agent 输出) 喂给 LLM-as-judge 评分
  4) 输出 JSON 报告 + Markdown 摘要
  5) 根据阈值返回退出码：0=通过 1=失败（CI 阻断）

用法：
  python -m tests.eval.run_eval                       # 默认跑全部
  python -m tests.eval.run_eval --category valuation  # 只跑某类
  python -m tests.eval.run_eval --limit 3             # 只跑前 3 条
  python -m tests.eval.run_eval --agent-url http://localhost:8000/chat
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

# 让脚本可独立运行（不依赖项目根目录在 PYTHONPATH）
_THIS_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _THIS_DIR.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# 自动加载 .env（如果存在）
try:
    from dotenv import load_dotenv
    load_dotenv(_PROJECT_ROOT / ".env")
except ImportError:
    pass  # 无 python-dotenv 时回退到系统环境变量

from config.constants import (
    EVAL_GOLDEN_SET_PATH,
    EVAL_RESULTS_DIR,
    EVAL_JUDGE_MODEL,
    EVAL_JUDGE_TIMEOUT_SEC,
    EVAL_JUDGE_TEMPERATURE,
    EVAL_PASS_SCORE_THRESHOLD,
    EVAL_HALLUCINATION_RATE_BLOCK_THRESHOLD,
    EVAL_CONCURRENCY,
    EVAL_SAMPLE_MAX_RETRIES,
    EVAL_HTTP_TASK_TIMEOUT_SEC,
    EVAL_WS_CONNECT_TIMEOUT_SEC,
    EVAL_AGENT_TASK_PATH,
    EVAL_AGENT_WS_PATH_PREFIX,
)
from tests.eval.judge_prompt import (
    JUDGE_SYSTEM_PROMPT,
    JUDGE_USER_PROMPT_TEMPLATE,
    HALLUCINATION_CHECK_PROMPT,
)


# ======================================================================
# 数据结构
# ======================================================================

@dataclass
class SampleResult:
    """单个样本的评估结果。"""
    sample_id: str
    category: str
    input: str
    agent_output: str
    judge_raw: str = ""
    coverage: float = 0.0
    forbidden_violations: int = 0
    forbidden_hits: List[str] = field(default_factory=list)
    must_contain_hit: int = 0
    must_contain_total: int = 0
    hallucination: float = 0.0
    hallucination_evidence: List[str] = field(default_factory=list)
    risk_compliance: int = 0
    overall_score: float = 0.0
    comment: str = ""
    error: Optional[str] = None  # 调用失败时填充


@dataclass
class EvalReport:
    """整体评估报告。"""
    total: int = 0
    passed: int = 0
    failed: int = 0
    errored: int = 0
    avg_score: float = 0.0
    hallucination_rate: float = 0.0
    avg_coverage: float = 0.0
    by_category: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    samples: List[SampleResult] = field(default_factory=list)


# ======================================================================
# 被测 Agent 调用
# ======================================================================

async def call_agent(
    agent_url: str,
    user_input: str,
    timeout: float = 120.0,
    mode: str = "http",
) -> str:
    """调用被测 Agent。

    mode:
      - http: POST agent_url，期望同步返回 {"answer"|"response"|"content": ...}
              若服务端是异步任务模式（返回 {"status":"started","thread_id":...}），
              自动通过 WebSocket 监听 monitor_event 直到 event=task_result
      - direct: 直接用 OpenAI 兼容接口（DeepSeek）当被测 Agent 答题
                —— 用于无 server 时验证评估框架本身
    """
    if mode == "direct":
        return await _call_deepseek_direct(user_input, timeout)

    # 把 agent_url 规范化成 base_url（去掉尾部 /chat 等路径，便于拼接 /api/task 和 /ws/）
    base_url = agent_url.rstrip("/")
    if base_url.endswith("/chat"):
        base_url = base_url[: -len("/chat")]
    elif base_url.endswith(EVAL_AGENT_TASK_PATH):
        base_url = base_url[: -len(EVAL_AGENT_TASK_PATH)]
    task_url = base_url + EVAL_AGENT_TASK_PATH

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            task_url,
            json={
                "query": user_input,
                "thread_id": f"eval-{int(time.time() * 1000)}",
                "user_id": "eval_runner",
            },
        )
        resp.raise_for_status()
        data = resp.json()

    # 路径 A：旧同步 server 直接返回答案
    direct_answer = data.get("answer") or data.get("response") or data.get("content")
    if direct_answer:
        return direct_answer

    # 路径 B：异步任务模式，需要 WS 等待最终 task_result
    thread_id = data.get("thread_id")
    if not thread_id:
        raise RuntimeError(f"Agent 响应既无 answer 也无 thread_id: {data}")

    return await _wait_for_task_result_via_ws(base_url, thread_id)


async def _wait_for_task_result_via_ws(base_url: str, thread_id: str) -> str:
    """通过 WebSocket 监听 /ws/{thread_id}，等到 event=task_result 或 error。

    Agent 服务端通过 ConnectionManager 推送 {type:monitor_event, event, data, ...}，
    最终答案在 event=task_result 的 data.result 字段。
    """
    import websockets

    # http(s)://host:port → ws(s)://host:port
    ws_base = base_url.replace("https://", "wss://").replace("http://", "ws://")
    ws_url = ws_base + EVAL_AGENT_WS_PATH_PREFIX + thread_id

    try:
        async with websockets.connect(
            ws_url,
            open_timeout=EVAL_WS_CONNECT_TIMEOUT_SEC,
            close_timeout=5,
        ) as ws:
            deadline = time.time() + EVAL_HTTP_TASK_TIMEOUT_SEC
            while time.time() < deadline:
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=deadline - time.time()
                    )
                except asyncio.TimeoutError:
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if msg.get("type") != "monitor_event":
                    continue
                event = msg.get("event", "")
                if event == "task_result":
                    return (msg.get("data") or {}).get("result", "")
                if event == "error":
                    err_msg = (msg.get("data") or {}).get("message") or msg.get("message") or "error"
                    raise RuntimeError(f"Agent error: {err_msg}")
            raise TimeoutError(
                f"WS 等待 task_result 超时（{EVAL_HTTP_TASK_TIMEOUT_SEC}s）thread_id={thread_id}"
            )
    except websockets.exceptions.InvalidURI:
        raise RuntimeError(f"无效 WS URI: {ws_url}")


# ======================================================================
# LLM-as-judge 调用
# ======================================================================

_DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "") or os.environ.get("OPENAI_API_KEY", "")
_raw_base_url = os.environ.get(
    "DEEPSEEK_BASE_URL", os.environ.get("OPENAI_BASE_URL", "https://api.deepseek.com")
)
# 自动补全 /v1 后缀（DeepSeek 兼容 OpenAI 协议）
_DEEPSEEK_BASE_URL = _raw_base_url.rstrip("/")
if not _DEEPSEEK_BASE_URL.endswith("/v1"):
    _DEEPSEEK_BASE_URL = _DEEPSEEK_BASE_URL + "/v1"
# 被测 Agent 的系统 prompt（金融投研助手角色设定）
_AGENT_SYSTEM_PROMPT = (
    "你是一名金融投研助手 MOSS。请基于 AGENTS.md 规范回答用户：\n"
    "1) 个股新闻速览：每只个股汇总≤200字，必须判定利空/利多，列出来源\n"
    "2) 估值分析：必须列出 PE/PB/ROE/营收增速，与行业均值对比\n"
    "3) 护城河分析：从品牌/技术/成本/网络效应/转换成本五维度评估\n"
    "4) 散户数据：只用中登公司或交易所公开数据\n"
    "5) 涉及买卖建议时必须附带风险声明：⚠️ 以上信息来自互联网公开资料，仅供参考，"
    "不构成投资建议。投资有风险，入市需谨慎，盈亏自负。\n"
    "6) 检索不到数据时明确说『未找到相关数据』，禁止编造股票代码或财务数据\n"
    "7) 来自股吧/论坛/自媒体的信息必须标注『信息来源可靠性待验证』"
)


async def _call_deepseek_direct(user_input: str, timeout: float) -> str:
    """direct 模式：用 DeepSeek 兼容接口当被测 Agent 答题。

    与 judge 用同一模型（但角色不同）——这是 LLM 评估的常见做法，
    既能验证评估框架本身，也能反映模型在该任务上的真实表现。
    """
    if not _DEEPSEEK_API_KEY:
        raise RuntimeError(
            "DEEPSEEK_API_KEY 或 OPENAI_API_KEY 未设置；"
            "请在 .env 中配置或运行时 $env:OPENAI_API_KEY='sk-...'"
        )
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            f"{_DEEPSEEK_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {_DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": _AGENT_SYSTEM_PROMPT},
                    {"role": "user", "content": user_input},
                ],
                "temperature": 0.3,
                "max_tokens": 800,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]


async def call_judge(
    sample: Dict[str, Any],
    agent_output: str,
    max_retries: int = EVAL_SAMPLE_MAX_RETRIES,
) -> str:
    """让裁判 LLM 对 Agent 输出打分，返回裁判原始字符串。"""
    expected_points = sample.get("expected_points", [])
    forbidden = sample.get("forbidden_patterns", [])
    must_contain = sample.get("must_contain", [])

    user_prompt = JUDGE_USER_PROMPT_TEMPLATE.format(
        sample_id=sample.get("id", ""),
        category=sample.get("category", ""),
        risk_level=sample.get("risk_level", "low"),
        input=sample.get("input", ""),
        expected_total=len(expected_points),
        expected_points_text="\n".join(f"- {p}" for p in expected_points) or "（无）",
        forbidden_patterns_text="\n".join(f"- {p}" for p in forbidden) or "（无）",
        must_contain_text="\n".join(f"- {m}" for m in must_contain) or "（无）",
        agent_output=agent_output,
    )

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=EVAL_JUDGE_TIMEOUT_SEC) as client:
                resp = await client.post(
                    f"{_DEEPSEEK_BASE_URL}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {_DEEPSEEK_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": EVAL_JUDGE_MODEL,
                        "messages": [
                            {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                            {"role": "user", "content": user_prompt},
                        ],
                        "temperature": EVAL_JUDGE_TEMPERATURE,
                        "response_format": {"type": "json_object"},
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"]
        except Exception as e:
            last_err = e
            await asyncio.sleep(1.0 * attempt)
    raise RuntimeError(f"Judge 调用失败 {max_retries} 次: {last_err}")


# ======================================================================
# 裁判结果解析
# ======================================================================

def parse_judge_output(raw: str) -> Dict[str, Any]:
    """从裁判输出中提取 JSON。容忍 ```json 包裹和首尾多余文本。"""
    if not raw:
        return {}
    text = raw.strip()
    # 去掉 markdown code fence
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    # 找到第一个 { 到最后一个 }
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        text = text[start:end + 1]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"parse_error": True, "raw": raw}


# ======================================================================
# 单样本评估流程
# ======================================================================

async def evaluate_one(
    sample: Dict[str, Any],
    agent_url: str,
    semaphore: asyncio.Semaphore,
    mode: str = "http",
) -> SampleResult:
    """评估单个样本：调 Agent → 调裁判 → 解析。"""
    result = SampleResult(
        sample_id=sample.get("id", ""),
        category=sample.get("category", ""),
        input=sample.get("input", ""),
        agent_output="",
    )

    async with semaphore:
        # 1) 调用被测 Agent
        try:
            result.agent_output = await call_agent(agent_url, sample["input"], mode=mode)
        except Exception as e:
            result.error = f"Agent 调用失败: {type(e).__name__}: {e}"
            return result

        # 2) 调用裁判 LLM
        try:
            result.judge_raw = await call_judge(sample, result.agent_output)
        except Exception as e:
            result.error = f"Judge 调用失败: {type(e).__name__}: {e}"
            return result

        # 3) 解析裁判输出
        parsed = parse_judge_output(result.judge_raw)
        if parsed.get("parse_error"):
            result.error = f"裁判输出 JSON 解析失败"
            return result

        result.coverage = float(parsed.get("coverage", 0.0))
        result.forbidden_violations = int(parsed.get("forbidden_violations", 0))
        result.forbidden_hits = parsed.get("forbidden_hits", [])
        result.must_contain_hit = int(parsed.get("must_contain_hit", 0))
        result.must_contain_total = int(parsed.get("must_contain_total", 0))
        result.hallucination = float(parsed.get("hallucination", 0.0))
        result.hallucination_evidence = parsed.get("hallucination_evidence", [])
        result.risk_compliance = int(parsed.get("risk_compliance", 0))
        result.overall_score = float(parsed.get("overall_score", 0.0))
        result.comment = parsed.get("comment", "")

    return result


# ======================================================================
# 主流程
# ======================================================================

async def run_eval(
    agent_url: str,
    category_filter: Optional[str] = None,
    limit: Optional[int] = None,
    mode: str = "http",
    ids_filter: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
) -> EvalReport:
    """主评估流程。返回 EvalReport。"""
    # 1) 加载 golden set
    with open(EVAL_GOLDEN_SET_PATH, "r", encoding="utf-8") as f:
        golden_set: List[Dict[str, Any]] = json.load(f)
    if category_filter:
        golden_set = [s for s in golden_set if s.get("category") == category_filter]
    if ids_filter:
        id_set = set(ids_filter)
        golden_set = [s for s in golden_set if s.get("id") in id_set]
    if limit:
        golden_set = golden_set[:limit]

    print(f"[eval] 加载 {len(golden_set)} 个样本  mode={mode}")

    # 2) 并发评估
    conc = concurrency if concurrency and concurrency > 0 else EVAL_CONCURRENCY
    semaphore = asyncio.Semaphore(conc)
    tasks = [evaluate_one(s, agent_url, semaphore, mode=mode) for s in golden_set]
    t0 = time.time()
    results: List[SampleResult] = await asyncio.gather(*tasks)
    elapsed = time.time() - t0

    # 3) 聚合报告
    report = EvalReport(total=len(results))
    for r in results:
        report.samples.append(r)
        if r.error:
            report.errored += 1
            continue
        if r.overall_score >= EVAL_PASS_SCORE_THRESHOLD and r.forbidden_violations == 0:
            report.passed += 1
        else:
            report.failed += 1
        report.avg_score += r.overall_score
        report.avg_coverage += r.coverage
        if r.hallucination > 0.5:
            report.hallucination_rate += 1

        # 分类聚合
        cat = r.category
        if cat not in report.by_category:
            report.by_category[cat] = {"total": 0, "passed": 0, "avg_score": 0.0}
        report.by_category[cat]["total"] += 1
        if r.overall_score >= EVAL_PASS_SCORE_THRESHOLD:
            report.by_category[cat]["passed"] += 1
        report.by_category[cat]["avg_score"] += r.overall_score

    valid = report.total - report.errored
    if valid > 0:
        report.avg_score /= valid
        report.avg_coverage /= valid
        report.hallucination_rate /= valid
    for cat_data in report.by_category.values():
        if cat_data["total"] > 0:
            cat_data["avg_score"] /= cat_data["total"]

    print(f"[eval] 完成 耗时={elapsed:.1f}s passed={report.passed}/{report.total} "
          f"avg_score={report.avg_score:.2f} halluc_rate={report.hallucination_rate:.2f}")
    return report


# ======================================================================
# 报告输出
# ======================================================================

def save_report(report: EvalReport) -> Path:
    """保存 JSON 报告 + Markdown 摘要。返回 JSON 文件路径。"""
    out_dir = Path(EVAL_RESULTS_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")

    # JSON
    json_path = out_dir / f"eval_{ts}.json"
    report_data = {
        "summary": {
            "total": report.total,
            "passed": report.passed,
            "failed": report.failed,
            "errored": report.errored,
            "avg_score": round(report.avg_score, 4),
            "avg_coverage": round(report.avg_coverage, 4),
            "hallucination_rate": round(report.hallucination_rate, 4),
            "threshold_pass_score": EVAL_PASS_SCORE_THRESHOLD,
            "threshold_hallucination_block": EVAL_HALLUCINATION_RATE_BLOCK_THRESHOLD,
        },
        "by_category": report.by_category,
        "samples": [
            {
                "id": r.sample_id,
                "category": r.category,
                "input": r.input,
                "agent_output": r.agent_output[:500],  # 截断防止报告过大
                "overall_score": round(r.overall_score, 4),
                "coverage": round(r.coverage, 4),
                "forbidden_violations": r.forbidden_violations,
                "hallucination": round(r.hallucination, 4),
                "risk_compliance": r.risk_compliance,
                "comment": r.comment,
                "error": r.error,
            }
            for r in report.samples
        ],
    }
    json_path.write_text(json.dumps(report_data, ensure_ascii=False, indent=2), encoding="utf-8")

    # Markdown 摘要
    md_path = out_dir / f"eval_{ts}.md"
    md_lines = [
        f"# 评估报告 {ts}",
        "",
        "## 概览",
        f"- 总样本数：{report.total}",
        f"- 通过：{report.passed} ({report.passed/max(report.total,1)*100:.1f}%)",
        f"- 失败：{report.failed}",
        f"- 错误：{report.errored}",
        f"- 平均分：{report.avg_score:.2f}（阈值 {EVAL_PASS_SCORE_THRESHOLD}）",
        f"- 平均覆盖度：{report.avg_coverage:.2f}",
        f"- 幻觉率：{report.hallucination_rate:.2%}（阻断阈值 {EVAL_HALLUCINATION_RATE_BLOCK_THRESHOLD:.0%}）",
        "",
        "## 分类统计",
        "| 类别 | 总数 | 通过 | 平均分 |",
        "|---|---|---|---|",
    ]
    for cat, d in report.by_category.items():
        md_lines.append(f"| {cat} | {d['total']} | {d['passed']} | {d['avg_score']:.2f} |")
    md_lines.extend(["", "## 失败样本详情"])
    for r in report.samples:
        if r.error or r.overall_score < EVAL_PASS_SCORE_THRESHOLD:
            md_lines.extend([
                f"### {r.sample_id} ({r.category})",
                f"- 输入：{r.input}",
                f"- 分数：{r.overall_score:.2f}",
                f"- 违规：{r.forbidden_violations}",
                f"- 幻觉：{r.hallucination:.2f}",
                f"- 错误：{r.error or '无'}",
                f"- 评论：{r.comment}",
                "",
            ])
    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    return json_path


# ======================================================================
# CLI 入口
# ======================================================================

def main() -> int:
    """CI 入口。返回 0=通过 1=失败。"""
    parser = argparse.ArgumentParser(description="LLM 输出质量评估")
    parser.add_argument("--agent-url", default="http://localhost:8000",
                        help="被测 Agent 基地址（http 模式自动 POST {base}/api/task + 监听 {base}/ws/{thread_id}）")
    parser.add_argument("--mode", default="direct", choices=["http", "direct"],
                        help="http=POST /api/task + WS 等待结果 / direct=直接用 DeepSeek API 当被测 Agent（默认）")
    parser.add_argument("--category", default=None, help="只评估某类别")
    parser.add_argument("--limit", type=int, default=None, help="只跑前 N 条")
    parser.add_argument("--ids", default=None,
                        help="只跑指定 ID 列表（逗号分隔，如 eval_014,eval_015）")
    parser.add_argument("--concurrency", type=int, default=None,
                        help=f"覆盖评估并发数（默认 {EVAL_CONCURRENCY}）")
    args = parser.parse_args()

    if not _DEEPSEEK_API_KEY:
        print("[eval] 警告：DEEPSEEK_API_KEY/OPENAI_API_KEY 未设置，judge+direct 都会失败")
        print("       解决：在 .env 设 OPENAI_API_KEY=sk-... 或运行时 $env:OPENAI_API_KEY='sk-...'")

    ids_list = None
    if args.ids:
        ids_list = [s.strip() for s in args.ids.split(",") if s.strip()]

    report = asyncio.run(run_eval(
        agent_url=args.agent_url,
        category_filter=args.category,
        limit=args.limit,
        mode=args.mode,
        ids_filter=ids_list,
        concurrency=args.concurrency,
    ))
    json_path = save_report(report)
    print(f"[eval] 报告已保存：{json_path}")

    # CI 阈值判定
    if report.hallucination_rate > EVAL_HALLUCINATION_RATE_BLOCK_THRESHOLD:
        print(f"[eval] ❌ 幻觉率 {report.hallucination_rate:.2%} 超阈值，CI 阻断")
        return 1
    pass_rate = report.passed / max(report.total, 1)
    if pass_rate < 0.7:
        print(f"[eval] ❌ 通过率 {pass_rate:.1%} 低于 70%，CI 阻断")
        return 1
    print(f"[eval] ✅ 评估通过（通过率 {pass_rate:.1%}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
