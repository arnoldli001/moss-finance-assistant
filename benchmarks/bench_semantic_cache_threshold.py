# -*- coding: utf-8 -*-
"""bench_semantic_cache_threshold.py — 语义缓存相似度阈值实测。

为 SEMANTIC_CACHE_SIMILARITY_THRESHOLD（默认 0.92）提供实测校准依据，替代拍脑袋阈值；离线可跑。

方法：金融投研查询对标注集分两类——equivalent（同义改写，应命中缓存）/
different（换股票/换指标/换意图，不应命中）；阈值扫描 0.70→0.98（步长 0.02），
逐档计算 TP/FP/Accuracy/F1/Youden's J，推荐 Accuracy 最高档（并列取 J 大者）。

输出：控制台指标表 + benchmarks/results/semantic_cache_threshold_latest.json
运行：python benchmarks/bench_semantic_cache_threshold.py
（sentence-transformers 与 Ollama embedding 均不可用时退出——hash 兜底嵌入无语义，扫描无意义）
"""
from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from governance.guardrails.semantic_cache import Embedder  # noqa: E402  真源复用
from config.constants import SEMANTIC_CACHE_SIMILARITY_THRESHOLD  # noqa: E402

RESULTS_DIR = _HERE / "results"

# Ollama 嵌入回退后端（sentence-transformers 未安装时使用，需 Ollama 有 embedding 模型）
_OLLAMA_BASE_URL = "http://localhost:11434"
_OLLAMA_EMBED_MODEL = "nomic-embed-text"

# ======================================================================
# 标注集（equivalent=应命中缓存 / different=不应命中）
# ======================================================================

EQUIVALENT_PAIRS: List[Tuple[str, str, str]] = [
    ("贵州茅台最近有什么新闻", "茅台最近的新闻动态有哪些", "同股同问"),
    ("宁德时代今天的股价是多少", "宁德时代现价多少", "同股同问"),
    ("帮我分析一下比亚迪的护城河", "比亚迪的竞争壁垒怎么样", "同义术语"),
    ("腾讯控股的估值高不高", "腾讯现在的估值水平如何", "同义术语"),
    ("招商银行最新财报业绩怎么样", "招行最近一期财报表现如何", "简称/全称"),
    ("五粮液今年的营收增速是多少", "五粮液本年度营业收入增长情况", "同义改写"),
    ("中国平安的ROE是多少", "平安的净资产收益率是多少", "中英术语"),
    ("隆基绿能光伏组件的市场份额", "隆基在光伏组件市场的占有率", "同义改写"),
    ("最近有什么利好医药板块的政策", "医药行业近期有哪些政策利好", "语序调换"),
    ("美联储加息对A股有什么影响", "美国加息会如何影响A股市场", "同义改写"),
]

DIFFERENT_PAIRS: List[Tuple[str, str, str]] = [
    ("贵州茅台最近有什么新闻", "五粮液最近有什么新闻", "换股票"),
    ("宁德时代今天的股价是多少", "宁德时代去年的股价是多少", "换时间(缓存TTL外仍有风险)"),
    ("比亚迪的护城河分析", "比亚迪最新销量数据", "换意图"),
    ("腾讯控股的估值高不高", "腾讯控股的股价走势", "换指标"),
    ("招商银行财报业绩", "工商银行财报业绩", "换股票"),
    ("五粮液营收增速", "五粮液净利润增速", "换指标"),
    ("中国平安ROE", "中国平安PB", "换指标"),
    ("光伏行业最新政策", "锂电行业最新政策", "换行业"),
    ("美联储加息对A股影响", "美联储加息对美股影响", "换市场"),
    ("茅台批发价格体系", "茅台股价走势", "一词多义"),
    ("查询股票600519的行情", "查询股票000858的行情", "换代码"),
    ("新能源车销量数据", "新能源车充电桩数据", "换实体"),
]


def _cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0


async def _pair_sims(embed: Any, pairs) -> List[Tuple[float, str, str, str]]:
    sims = []
    for a, b, note in pairs:
        va = await embed(a)
        vb = await embed(b)
        sims.append((_cosine(va, vb), a, b, note))
    return sims


# ======================================================================
# Ollama 嵌入后端（/api/embed，回退方案）
# ======================================================================

class OllamaEmbedder:
    """基于 Ollama /api/embed 的嵌入后端（与 Embedder 接口兼容，供基准独立校准）。"""

    def __init__(self, model: str = _OLLAMA_EMBED_MODEL):
        self.model = model
        self.model_name = f"ollama:{model}"

    async def embed(self, text: str) -> List[float]:
        import urllib.request as _urlreq
        payload = json.dumps({"model": self.model, "input": text}).encode("utf-8")
        req = _urlreq.Request(
            _OLLAMA_BASE_URL.rstrip("/") + "/api/embed",
            data=payload, method="POST",
            headers={"Content-Type": "application/json"},
        )
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None, lambda: json.loads(_urlreq.urlopen(req, timeout=30).read().decode("utf-8")))
        vec = data.get("embeddings", [[]])[0] or data.get("embedding") or []
        if not vec:
            raise RuntimeError(f"Ollama embed 返回空向量: {data}")
        return vec


async def _detect_backend() -> Tuple[Optional[Any], str]:
    """按优先级探测可用嵌入后端：sentence-transformers → Ollama embed → 无。"""
    embedder = Embedder()
    probe_text = "探测句子"
    vec = await embedder.embed(probe_text)
    if not embedder._init_failed and vec != embedder._hash_embedding(probe_text):
        return embedder, embedder.model_name

    # sentence-transformers 不可用 → 试 Ollama embedding 模型
    try:
        oe = OllamaEmbedder()
        await oe.embed(probe_text)
        return oe, oe.model_name
    except Exception as e:
        print(f"Ollama 嵌入模型不可用（{e}）。可执行: ollama pull {_OLLAMA_EMBED_MODEL}")
    return None, ""


def sweep(equ_sims: List[float], diff_sims: List[float]) -> List[Dict]:
    rows = []
    thresholds = [round(0.70 + 0.02 * i, 2) for i in range(15)]  # 0.70..0.98
    for t in thresholds:
        tp = sum(1 for s in equ_sims if s >= t)
        fn = len(equ_sims) - tp
        fp = sum(1 for s in diff_sims if s >= t)
        tn = len(diff_sims) - fp
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        accuracy = (tp + tn) / (len(equ_sims) + len(diff_sims))
        youden_j = recall + tn / (tn + fp) - 1 if (tn + fp) else 0.0
        rows.append({
            "threshold": t, "tp": tp, "fn": fn, "fp": fp, "tn": tn,
            "precision": round(precision, 3), "recall": round(recall, 3),
            "f1": round(f1, 3), "accuracy": round(accuracy, 3),
            "youden_j": round(youden_j, 3),
        })
    return rows


async def run_benchmark() -> Tuple[int, Dict]:
    t0 = time.monotonic()
    backend, backend_name = await _detect_backend()
    if backend is None:
        print("无可用语义嵌入后端（sentence-transformers 未安装 且 Ollama 无 embedding 模型）。")
        print("阈值扫描需要语义向量。安装任一后端后重跑：")
        print(f"  a) pip install sentence-transformers")
        print(f"  b) ollama pull {_OLLAMA_EMBED_MODEL}")
        return 2, {}
    print(f"嵌入后端: {backend_name}")

    equ_sims_raw = await _pair_sims(backend.embed, EQUIVALENT_PAIRS)
    diff_sims_raw = await _pair_sims(backend.embed, DIFFERENT_PAIRS)
    equ_sims = [s for s, *_ in equ_sims_raw]
    diff_sims = [s for s, *_ in diff_sims_raw]

    rows = sweep(equ_sims, diff_sims)

    print(f"\n{'阈值':>6}{'TP':>4}{'FN':>4}{'FP':>4}{'TN':>4}{'P':>7}{'R':>7}{'F1':>7}{'Acc':>7}{'J':>7}")
    for r in rows:
        print(f"{r['threshold']:>6.2f}{r['tp']:>4}{r['fn']:>4}{r['fp']:>4}{r['tn']:>4}"
              f"{r['precision']:>7.3f}{r['recall']:>7.3f}{r['f1']:>7.3f}"
              f"{r['accuracy']:>7.3f}{r['youden_j']:>7.3f}")

    print(f"\n等价对相似度: min={min(equ_sims):.3f} mean={statistics.mean(equ_sims):.3f} | "
          f"不同对: max={max(diff_sims):.3f} mean={statistics.mean(diff_sims):.3f}")

    best_acc = max(rows, key=lambda r: (r["accuracy"], r["youden_j"]))
    best_f1 = max(rows, key=lambda r: r["f1"])

    # 区分度健全性检查：不同对的最高相似度若 >= 等价对的最高相似度，
    # 说明该嵌入后端对中文实体/内容差异不敏感（entity-blind），阈值结论不可迁移。
    blind = max(diff_sims) >= max(equ_sims)
    applicability_note = ""
    if blind:
        applicability_note = (
            "警告：该嵌入后端区分度不足（不同对最高相似度 %.3f >= 等价对最高 %.3f，"
            "换实体对得分几乎相同）——阈值结论仅对该后端有效，不可迁移到生产 Embedder。"
            "中文语义缓存建议使用多语言模型（如 paraphrase-multilingual-MiniLM-L12-v2）重新校准。"
            % (max(diff_sims), max(equ_sims))
        )
        print(f"\n[重要]{applicability_note}")

    print(f"\n推荐: Accuracy最优阈值={best_acc['threshold']} (acc={best_acc['accuracy']}), "
          f"F1最优阈值={best_f1['threshold']} (f1={best_f1['f1']})")
    cur = SEMANTIC_CACHE_SIMILARITY_THRESHOLD
    cur_row = next((r for r in rows if abs(r["threshold"] - cur) < 0.011), None)
    if cur_row:
        print(f"当前默认 {cur}: acc={cur_row['accuracy']}, f1={cur_row['f1']}, "
              f"fp={cur_row['fp']}, fn={cur_row['fn']}")
        if not blind:
            print("  <- 实测支持当前默认" if cur_row["accuracy"] >= best_acc["accuracy"]
                  else f"  <- 建议调整为 {best_acc['threshold']}（注意：仅当生产 Embedder 与本后端一致时成立）")

    out = {
        "benchmark": "semantic_cache_threshold",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "embed_model": backend_name,
        "backend_applicability_note": applicability_note,
        "current_default_threshold": cur,
        "recommended_threshold_accuracy": best_acc["threshold"],
        "recommended_threshold_f1": best_f1["threshold"],
        "equivalent_pair_sims": [
            {"sim": round(s, 4), "a": a, "b": b, "note": n} for s, a, b, n in equ_sims_raw],
        "different_pair_sims": [
            {"sim": round(s, 4), "a": a, "b": b, "note": n} for s, a, b, n in diff_sims_raw],
        "equ_sim_stats": {"min": round(min(equ_sims), 3), "mean": round(statistics.mean(equ_sims), 3)},
        "diff_sim_stats": {"max": round(max(diff_sims), 3), "mean": round(statistics.mean(diff_sims), 3)},
        "sweep": rows,
        "elapsed_sec": round(time.monotonic() - t0, 2),
    }
    return 0, out


def main() -> int:
    print(f"语义缓存阈值实测（当前默认={SEMANTIC_CACHE_SIMILARITY_THRESHOLD}）")
    code, out = asyncio.run(run_benchmark())
    if code == 0:
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = RESULTS_DIR / "semantic_cache_threshold_latest.json"
        out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"结果已写入: {out_path}")
    return code


if __name__ == "__main__":
    sys.exit(main())
