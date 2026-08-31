# -*- coding: utf-8 -*-
"""bench_judge_consistency.py — LLM-as-Judge 一致性实测。

把 LLM 当裁判前，先量化裁判自身是否可靠——裁判不一致时，下游 eval 指标全是噪声。

方法：5 个金融问答对比案例（按项目金融业务规则构造：来源标注/风险声明/数据真实性/
利空利多判定），本地 Ollama 裁判只输出 JSON 判定。
  - 自一致性：同案例重复 K 次，算多数派占比与两两一致率
  - 位置偏差：交换 A/B 再判，判定随位置翻转即有偏差
  - 准确率：多数派判定 == 人工期望

输出：控制台明细 + benchmarks/results/judge_consistency_latest.json
运行：python benchmarks/bench_judge_consistency.py [--k 5]（依赖本地 Ollama）
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import statistics
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

RESULTS_DIR = _HERE / "results"

OLLAMA_BASE_URL = "http://localhost:11434"
JUDGE_MODEL = os.getenv("JUDGE_MODEL", "qwen3:8b")
JUDGE_TIMEOUT_SEC = 120

JUDGE_SYSTEM_PROMPT = (
    "你是严格的问答质量裁判。对比两个 AI 助手对同一金融投研问题的回答，"
    "依据以下标准判定哪个更好：\n"
    "1) 事实与数据是否可靠（编造股票代码/财务数据 = 严重缺陷）\n"
    "2) 是否引用信息来源、是否附投资风险声明（金融合规要求）\n"
    "3) 是否直接回答问题、有无关键遗漏（如利空/利多判定）\n"
    "4) 冗余、跑题、空话算减分\n"
    '只输出一行 JSON，不要解释：{"winner": "A"或"B"或"tie", "confidence": 0.0~1.0}'
)

# ======================================================================
# 标注集：(问题, 回答A, 回答B, 期望判定, 考察点)
# ======================================================================

CASES: List[Dict[str, str]] = [
    {
        "question": "贵州茅台最近有什么新闻？",
        "answer_a": (
            "根据10月28日上海证券交易所公告和证券时报报道：贵州茅台前三季度营收1207.76亿元，"
            "同比增长10.31%；归母净利润608.28亿元。整体属于利多消息。\n"
            "⚠ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。"
        ),
        "answer_b": (
            "茅台最近挺好的，一直在涨，很多人都在买，建议赶紧上车，晚了就买不到了。"
        ),
        "expected": "A",
        "focus": "数据来源标注 + 风险声明（B 无来源且诱导买入）",
    },
    {
        "question": "宁德时代最新股价是多少？",
        "answer_a": (
            "根据最新行情，宁德时代（300750）当前股价 258.60 元，日内涨跌幅 +1.2%。"
            "数据为盘中实时数据，可能存在延迟。\n"
            "⚠ 以上信息仅供参考，不构成投资建议。"
        ),
        "answer_b": (
            "宁德时代（300751）当前股价 258.60 元，趋势向好，可以重点关注。"
        ),
        "expected": "A",
        "focus": "股票代码准确性（B 编造代码 300751，真实为 300750）",
    },
    {
        "question": "帮我看看中国平安的估值水平",
        "answer_a": (
            "中国平安（601318）当前 PE 约 9.2 倍，PB 约 1.05 倍，ROE 约 11.5%，"
            "显著低于保险行业均值（PE 约 12 倍）。从估值看处于历史偏低区间。"
            "（数据来源：交易所公开数据）"
        ),
        "answer_b": (
            "中国平安是一家领先的综合性金融集团，成立于1988年，总部在深圳，"
            "业务涵盖保险、银行、投资等多个领域，服务数千万客户，是一家非常优秀的公司。"
        ),
        "expected": "A",
        "focus": "回答针对性（B 跑题：只有公司介绍，无估值数据）",
    },
    {
        "question": "五粮液今天为什么跌？",
        "answer_a": (
            "白酒板块今日整体走弱，五粮液跌幅约 2.3%。可能原因：一是今日公布的高端白酒"
            "批价数据走弱；二是板块轮动资金流出。属于利空信号，建议关注后续批价走势。\n"
            "⚠ 以上信息来自互联网公开资料，仅供参考，不构成投资建议，盈亏自负。"
        ),
        "answer_b": (
            "五粮液今日下跌 2.3%，主要原因是大盘走弱。"
        ),
        "expected": "A",
        "focus": "利空/利多判定完整性（B 缺关键归因分析）",
    },
    {
        "question": "散户开户数量最近有变化吗？",
        "answer_a": (
            "根据中登公司最新月度统计，10月新增开户数 285 万户，环比增长约 15%，"
            "同比增长约 32%，显示散户入场意愿回升。数据来源：中国结算官网。"
        ),
        "answer_b": (
            "根据中登公司最新月度统计，10月新增开户数 285 万户，环比增长约 15%，"
            "同比增长约 32%，显示散户入场意愿回升，市场情绪持续升温，"
            "大量增量资金正在跑步进场，A股即将迎来历史性大牛市。"
        ),
        "expected": "tie",
        "focus": "同数据基础上 B 附加无依据的趋势断言（考察裁判对'客观陈述 vs 煽动'的分辨）",
    },
]

# ======================================================================
# Ollama 裁判调用
# ======================================================================

def _extract_verdict(content: str) -> Optional[Dict[str, Any]]:
    if not content:
        return None
    cleaned = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL)
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        obj = json.loads(cleaned[start:end + 1])
    except Exception:
        return None
    winner = str(obj.get("winner", "")).upper()
    if winner not in ("A", "B", "TIE"):
        return None
    return {"winner": winner, "confidence": float(obj.get("confidence", 0.0) or 0.0)}


def _ollama_judge(question: str, answer_a: str, answer_b: str) -> Dict[str, Any]:
    """同步调用 Ollama /api/chat（非流式），返回解析后的判定或 {"error": ...}。"""
    import urllib.request as _urlreq
    user_content = (
        f"问题：{question}\n\n【回答A】\n{answer_a}\n\n【回答B】\n{answer_b}\n\n"
        "请判定哪个回答更好。"
    )
    payload = json.dumps({
        "model": JUDGE_MODEL,
        "messages": [
            {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "stream": False,
        # qwen3 类思考模型会先在 <think> 消耗大量 token，预算必须给足，
        # 否则思考未结束就被截断 → content 为空或 JSON 被切半
        "options": {"temperature": 0.1, "num_predict": 1024},
    }).encode("utf-8")
    req = _urlreq.Request(
        OLLAMA_BASE_URL.rstrip("/") + "/api/chat",
        data=payload, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with _urlreq.urlopen(req, timeout=JUDGE_TIMEOUT_SEC) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        content = (data.get("message") or {}).get("content") or ""
        verdict = _extract_verdict(content)
        if verdict is None:
            return {"error": f"unparseable: {content[:120]}"}
        return verdict
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


async def judge_once(question: str, a: str, b: str) -> Dict[str, Any]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _ollama_judge, question, a, b)


# ======================================================================
# 基准执行
# ======================================================================

async def run_benchmark(k: int) -> Tuple[int, Dict]:
    t0 = time.monotonic()
    # 可用性探测
    probe = await judge_once("探测", "A回答", "B回答")
    if "error" in probe and "unparseable" not in probe["error"]:
        print(f"裁判模型不可用（{probe['error']}）。请确认 Ollama 已启动并执行: ollama pull {JUDGE_MODEL}")
        return 2, {}

    details = []
    all_verdicts = 0
    consistent_cases = 0
    acc_correct = 0

    for idx, case in enumerate(CASES, 1):
        q, a, b = case["question"], case["answer_a"], case["answer_b"]
        print(f"\n案例{idx} [{case['focus']}]  期望={case['expected']}")
        verdicts = []
        for i in range(k):
            v = await judge_once(q, a, b)
            verdicts.append(v.get("winner") if "winner" in v else None)
            conf = v.get("confidence", 0) if "winner" in v else v.get("error", "")
            print(f"  run{i + 1}: {verdicts[-1]} (conf={conf})")

        valid = [x for x in verdicts if x]
        all_verdicts += len(valid)
        majority, majority_n = ("", 0)
        if valid:
            cnt = Counter(valid)
            majority, majority_n = cnt.most_common(1)[0]
        pairwise_agree = (
            sum(1 for i in range(len(valid)) for j in range(i + 1, len(valid)) if valid[i] == valid[j])
            / (len(valid) * (len(valid) - 1) / 2) if len(valid) >= 2 else 0.0
        )
        is_consistent = len(set(valid)) <= 1 and len(valid) == k
        consistent_cases += int(is_consistent)
        correct = majority == case["expected"]
        acc_correct += int(correct)

        # 位置偏差：交换 A/B 再判一次，winner 应镜像翻转（A<->B，tie 不变）
        vs = await judge_once(q, b, a)
        swapped = vs.get("winner")
        mirrored = {"A": "B", "B": "A", "TIE": "TIE"}.get(majority, "")
        no_bias = (swapped == mirrored) if swapped and majority else None
        print(f"  多数派={majority or 'N/A'}  两两一致率={pairwise_agree:.0%}  "
              f"交换A/B后={swapped}  位置偏差={'无' if no_bias else ('有' if no_bias is not None else '未测')}")

        details.append({
            "case": idx, "focus": case["focus"], "expected": case["expected"],
            "verdicts": verdicts, "majority": majority,
            "majority_ratio": round(majority_n / k, 2),
            "pairwise_agreement": round(pairwise_agree, 3),
            "self_consistent": is_consistent,
            "swapped_verdict": swapped, "position_bias_free": no_bias,
            "accuracy_hit": correct,
        })

    n = len(CASES)
    summary = {
        "judge_model": JUDGE_MODEL,
        "k_repeats": k,
        "self_consistency_rate": round(consistent_cases / n, 3),
        "avg_pairwise_agreement": round(
            statistics.mean(d["pairwise_agreement"] for d in details), 3),
        "position_bias_free_rate": round(
            sum(1 for d in details if d["position_bias_free"] is True) / n, 3),
        "accuracy_vs_expected": round(acc_correct / n, 3),
        "valid_verdict_ratio": round(
            all_verdicts / (n * k + n), 3),
        "verdict": (
            "裁判可靠" if consistent_cases / n >= 0.8 and acc_correct / n >= 0.8
            else "裁判一致性/准确性不足，eval 指标需谨慎解读"
        ),
    }

    print("\n" + "=" * 60)
    print(f"汇总: 自一致率={summary['self_consistency_rate']:.0%}  "
          f"平均两两一致={summary['avg_pairwise_agreement']:.0%}  "
          f"位置无偏率={summary['position_bias_free_rate']:.0%}  "
          f"与人工标注一致率={summary['accuracy_vs_expected']:.0%}")
    print(f"结论: {summary['verdict']}")
    return 0, {"benchmark": "judge_consistency",
               "generated_at": datetime.now().isoformat(timespec="seconds"),
               "summary": summary, "details": details,
               "elapsed_sec": round(time.monotonic() - t0, 2)}


def main() -> int:
    k = 3
    if "--k" in sys.argv:
        try:
            k = max(2, int(sys.argv[sys.argv.index("--k") + 1]))
        except (IndexError, ValueError):
            pass
    print(f"LLM-as-Judge 一致性实测（judge={JUDGE_MODEL}, K={k}, 案例={len(CASES)}）")
    code, out = asyncio.run(run_benchmark(k))
    if code == 0:
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = RESULTS_DIR / "judge_consistency_latest.json"
        out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"结果已写入: {out_path}")
    return code


if __name__ == "__main__":
    sys.exit(main())
