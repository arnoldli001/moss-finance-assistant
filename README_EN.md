<p align="center">
  <h1 align="center">🤖 MOSS Finance Assistant</h1>
  <p align="center"><b>Enterprise-grade Multi-Agent Financial Research Assistant — Four-Layer Evolutionary Architecture (Prompt → Context → Harness → Loop Engineering)</b></p>
  <p align="center">
    <a href="https://github.com/arnoldli001/moss-finance-assistant/actions/workflows/ci.yml"><img src="https://github.com/arnoldli001/moss-finance-assistant/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://codecov.io/gh/arnoldli001/moss-finance-assistant"><img src="https://codecov.io/gh/arnoldli001/moss-finance-assistant/graph/badge.svg" alt="Coverage"></a>
    <img src="https://img.shields.io/badge/Python-3.13-blue.svg" alt="Python">
    <img src="https://img.shields.io/badge/FastAPI-0.129.2-green.svg" alt="FastAPI">
    <img src="https://img.shields.io/badge/LangChain-1.2.10-orange.svg" alt="LangChain">
    <img src="https://img.shields.io/badge/deepagents-0.4.3-purple.svg" alt="deepagents">
    <img src="https://img.shields.io/badge/Ollama-local%20inference-success.svg" alt="Ollama">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License">
  </p>
  <p align="center">English | <a href="README.md">中文</a></p>
</p>

---

## 🎯 What Is This

An **enterprise-grade multi-agent financial research system** for retail investors: DeepSeek web search + Zsxq (knowledge-community) report scraping + RAGFlow knowledge base + local LLM analysis, providing stock news, valuation analysis, moat assessment, retail-investor data, and other research services.

**A complete blueprint for turning an "Agent demo" into an enterprise system** — the focus is not on how many APIs are wired together, but on the enterprise fundamentals: reliability engineering, security hardening, observability, and evaluation regression. Every design decision is backed by measured data.

---

## ✨ Highlights

### 1. Multi-Agent Collaboration (deepagents + LangChain)
An orchestrator agent coordinates three sub-agents: web search (DeepSeek + Tavily, auto-filtering research reports older than 2 months), database queries (MySQL), and knowledge-base retrieval (RAGFlow), plus Zsxq scraping via Playwright with local Ollama analysis.

### 2. Progressive Tool Disclosure (PTD, benchmark-driven Token optimization)
Two-stage routing: Stage 0 injects a minimal tool menu for the model to pick tools → Stage 1 injects full schemas of only the selected tools → Stage 2 appends missing tools as a fallback.
**Adaptive gating**: benchmarks revealed PTD is counterproductive for a 4-tool pool (full schema ~607 tok < menu ~678 tok), so the runtime bypasses it automatically; the 9-tool pool saves 50.2%–53.6% on average.

### 3. Reliability Engineering Suite (Layer 3 Harness)
| Component | Capability | Key Parameters |
|------|------|---------|
| Error quadrant classifier | Retryable hard errors / non-retryable soft errors / unrecoverable / config errors; mandatory idempotency checks | Exponential backoff vs. no-retry |
| Three-state circuit breaker | CLOSED → OPEN → HALF_OPEN with anti-flap recovery | Breaks after 3 failures in 60s, 30s cooldown |
| Four-level degradation chain | DeepSeek → IMA knowledge base → local Qwen3-8B → static template | Dual hard caps: ≤150s / ≤1M tokens per task |
| Hallucination guard | RAG citation tracing + JSON Schema validation + LLM-as-Judge triple pipeline | Warning-attached by default, never blocks output |
| Output validation & retry | Five-dimension scoring (data/integrity/risk/source/hallucination) with auto-retry prompts | Per-request retry counter |
| SLO monitoring | Availability / latency / hallucination pass rate + 30-day error budget | ≥99% / P95≤30s / ≥95% |
| OTel tracing | agent.run → llm.chat → tool.call three-layer spans with cross-coroutine propagation | console/OTLP configurable |
| Actor model | Serialized mailbox queues for session registry / WS connections / SLO writes + snapshot persistence | Eliminates cancellation races and concurrent writes |

### 4. Enterprise-Grade Security
- **JWT auth + RBAC**: register/login/refresh/guest endpoints issue token pairs; role-based rate limiting (owner 600 / admin 120 / user 60 / guest 10 QPM); user_id always taken from the JWT to prevent horizontal privilege escalation
- **Dual-layer prompt-injection protection**: regex fast path (zero cost) → local LLM classifier slow path (semantic second check, rejects at confidence ≥0.7) → JSONL audit logging; LLM failure defaults to fail-open (availability first), switchable to fail-closed

### 5. LLM Evaluation Regression (CI-blocking)
26 structured golden-set samples (10 multi-stock comparisons, 10 industry analyses, etc.), LLM-as-Judge six-dimension scoring (coverage / violations / must-contain / hallucination / risk compliance / overall). **Judge reliability measured first**: qwen3:8b achieves 100% self-consistency, 100% position-bias-free rate, 80% agreement with human labels — because when the judge is inconsistent, downstream eval metrics are pure noise. CI blocks when pass rate <70% or hallucination rate >5%.

### 6. Context Engineering
Sliding window of 10 turns + auto-compression into 3-part summaries after 20 turns + permanent retention of key decisions + relevance filtering (Jaccard + stock-code weighting, threshold 0.15) + 2000-char context trimming + semantic cache (embedding cosine matching with benchmark-calibrated threshold).

---

## 📊 Benchmarks & Measured Data

Quantified measurements replace "roughly X% saved" claims; all scripts run offline and reproducibly (result JSONs land in `benchmarks/results/`, not tracked in git):

| Script | What It Measures | Key Findings (2026-08-31) |
|------|--------|--------------------------|
| `bench_ptd_tokens.py` | PTD token savings (real tiktoken counting, 10 scenarios × dual routing paths) | 4-tool pool: PTD counterproductive → adaptive gating bypasses automatically; 9-tool pool: 50.2%–53.6% average savings |
| `bench_semantic_cache_threshold.py` | Semantic-cache threshold sweep (0.70–0.98, TP/FP/F1) | nomic-embed-text lacks Chinese entity discrimination (different-pair similarity ≈1.0); production calibration needs multilingual embeddings — threshold conclusions don't transfer across backends |
| `bench_judge_consistency.py` | LLM-as-Judge self-consistency (K repeats + A/B position swap) | qwen3:8b: 100% self-consistency, 100% position-bias-free, 80% agreement with human labels |
| `k6/smoke.js` | Full-chain HTTP smoke (1 VU, 13 assertions: health/register/row-level privilege/task start-stop/RBAC) | 13/13 passed; health p95 **1ms**, register **88ms**, authenticated reads **1.5ms**; task acceptance **10ms** (warm), graceful cancel **12.2s** |
| `k6/load.js` | Staged load (1→5→10→20 VUs, 3m20s, SLO threshold assertions) | **4,761 requests / error rate 0.00% / 4,721 rate-limited (429, user 60 QPM enforced) / rate-limit path p95 2.2ms / checks 100%** |

```bash
python benchmarks/bench_ptd_tokens.py
python benchmarks/bench_semantic_cache_threshold.py   # needs an embedding backend
python benchmarks/bench_judge_consistency.py          # needs local Ollama
k6 run benchmarks/k6/smoke.js                         # smoke + full-chain assertions
k6 run benchmarks/k6/load.js                          # staged load + SLO assertions
```

**Eval measurements** (20 new samples, http mode, serial): direct mode average score 0.60 / pass rate 50%; http mode (web + knowledge base) average score **0.825** / pass rate **100%**; 4 previously failing samples improved by +0.20–0.28 after enabling web search; hallucination rate 0%, risk compliance consistently 1.

The "why" behind architecture decisions is recorded in ADRs (`docs/adr/`): layered source-of-truth, single source for constants, PTD adaptive gating, dual-layer injection protection, test gating, and CI strategy.

---

## 💰 Business Value (Cost Quantification)

The first-principles question for any enterprise AI application is the **unit economics model**. All three token-optimization mechanisms are backed by measured benchmarks:

| Mechanism | Measured Effect | Cost Impact |
|---------|---------|---------|
| PTD progressive tool disclosure | 50.2%–53.6% tool-schema token savings on a 9-tool pool (4-tool pool bypassed automatically to avoid negative optimization) | Input tokens roughly halved → near-halved metered API cost |
| Semantic cache | Zero LLM calls on hit (threshold 0.92 benchmark-calibrated; real-time/single-stock price questions skipped automatically) | Marginal cost of repeated questions ≈ 0 |
| Model routing | Splits by cost/SLA/complexity (easy questions go to cheaper models) | Lower blended price per query |

**Cost formula** (tunable parameters, no fabricated absolutes):

```
Annual cost ≈ daily queries × avg input tokens × 365 × price(¥/token)
PTD saving ≈ annual cost × 50% (midpoint of measured 9-tool range)
Stacked cache saving ≈ repeated-question ratio × annual cost
```

Worked example: 10k DAU × 5 queries/day at 3k avg input tokens and volume-tier DeepSeek pricing → PTD alone offsets substantial API spend; semantic cache and model routing stack further savings. **Exact amounts float with vendor pricing; the formula and measured ratios hold** (per-query tokens/cost are already captured in OTel span `llm.chat:<model>`, ready for a future `/api/metrics/cost` dashboard).

Business metric tree (north star: weekly active research users): adoption rate / first-token latency (SSE first packet <50ms) / hallucination rate (measured 0%) / cost per query — all four are instrumented or aggregable from existing span/SLO data.

---

## 🏭 Production Topology & Scaling Path

**Current shape** (single host, `docker compose up -d --build`): app (uvicorn) + MySQL + Redis + Jaeger. Reliability components (rate limiting / breakers / degradation / SLO / OTel / Actor snapshots) are built in — scaling requires no business-code changes.

| Stage | Scale | Architecture Action | Status |
|------|------|---------|------|
| L0 | dev/demo | Single process + SQLite + in-process Actors | ✅ current |
| L1 | ~1k QPS | MySQL replaces SQLite; Redis backs semantic/session cache (`CACHE_BACKEND=redis`) | ✅ config switch |
| L2 | ~10k QPS | Stateless app layer: rate-limit counters to Redis → multi-instance + LB; cross-instance WS broadcast via Redis Pub/Sub stream_bus | rate-limit/cache have Redis backends; stream_bus Pub/Sub is an evolution item |
| L3 | 100k DAU | Agent tasks queued (dedicated worker pool), standalone WS gateway, MySQL read/write split | evolution item |
| L4 | multi-region | Regional deployment + CDN for static assets + global traffic steering | evolution item |

**Why this order**: the Actor mailbox already converges concurrent writes to a single point, so externalizing state is the only blocker for multi-instance — which sets the cost ranking of the scaling path (externalize state to Redis first, scale instances next, queue tasks last).

---

## 🏗️ Architecture Overview

| Layer | Name | Core Capability | Source Modules |
|------|------|---------|---------|
| Layer 1 | **Prompt Engineering** | Eight-part XML structure, CoT, few-shot, risk disclaimer | `prompt/prompts.yml` |
| Layer 2 | **Context Engineering** | Timeliness dedup, source vetting, 2000-char trimming, relevance filtering, PTD, semantic cache | `shared/llm_client/*`, `governance/guardrails/semantic_cache.py` |
| Layer 3 | **Harness Engineering** | Error classification, circuit breaking, degradation, hallucination guard, SLO, tracing, Actors | `governance/guardrails/*`, `governance/monitor/*`, `shared/actors/*` |
| Layer 4 | **Loop Engineering** | Scheduling, state persistence, skill encoding, stream resume | `orchestration/*`, `skills/` |

```
User request (POST /api/task or pre-market button)
    │
    ▼
interfaces/api/server.py ← FastAPI routes + WebSocket + scheduler + SLO endpoints
    │  POST /api/auth/*            ← JWT auth (register/login/refresh/guest)
    │  GET /api/slo/status         ← SLO monitoring + error budget + circuit breakers
    │  GET /api/traces/{sid}       ← OTel distributed tracing queries
    │
    ├─ api/middleware/             ← RBAC + role-based rate limiting + prompt-injection protection
    │
    ├──→ Layer1 Prompt: prompt/prompts.yml (eight-part XML + CoT + few-shot + Judge prompts)
    ├──→ Layer2 Context:
    │      shared/llm_client/tool_router.py        ← PTD two-stage routing (adaptive gating)
    │      agents/reasoning/memory_manager.py      ← sliding window + summaries + relevance filter
    │      governance/guardrails/semantic_cache.py ← semantic cache (embedding cosine match)
    │      shared/llm_client/model_router.py       ← model routing: cost/SLA/complexity
    ├──→ Layer3 Harness (8-piece reliability suite, see table above)
    │      governance/guardrails/{error_classifier,circuit_breaker,degradation_chain,
    │                             hallucination_guard,output_validator,maker_checker}.py
    │      governance/monitor/{slo_monitor,tracing,stream_resume}.py
    │      shared/actors/* + CancellationToken trinity (token + metadata + timeout)
    ├──→ Layer4 Loop:
    │      orchestration/scheduler|workflows|skills/ + skills/*
    ├──→ Orchestrator agent agents/analyst/agent.py
    │      circuit-breaker admission → semantic cache → model routing → sub-agents (search/DB/KB)
    │      → tools → StreamResume → maker-checker → hallucination check → spans → SLO events
    ├──→ Eval regression (offline CI): tests/eval/{golden_set.json,run_eval.py}
    └──→ Pre-market notes: tools/zsxq_tool.py (Playwright + browser mutex) → Ollama qwen3:8b
    │
    ▼ (every step streamed to the frontend via WebSocket api/monitor.py)
Frontend shows progress + tool results + final answer + hallucination warnings + stream resume
```

> The repo follows a "layered structure + compatibility shims" convention: **each real implementation lives in exactly one place (the source of truth)**, while legacy paths are pure re-export shims (headed with `[兼容垫片]`, aliased at runtime by `shared/compat_bootstrap.py`). **Only edit the source of truth.**

---

## 🚀 Quick Start

### Requirements

- Python 3.10+ / OpenAI-compatible LLM API key (DeepSeek etc.) / Tavily API key ([free registration](https://tavily.com))
- [Ollama](https://ollama.com) (local LLM, required for Zsxq analysis) / Playwright (required for Zsxq scraping)

### Steps

```bash
# 1) Clone + install dependencies
git clone https://github.com/arnoldli001/moss-finance-assistant.git
cd moss-finance-assistant
pip install -r requirements.txt

# 2) Playwright (needed for Zsxq scraping)
playwright install chromium

# 3) Ollama local model
ollama pull qwen3:8b

# 4) Environment variables
cp .env.example .env
```

Key `.env` settings:

```env
# LLM service (required)
OPENAI_BASE_URL=https://api.deepseek.com
OPENAI_API_KEY=your-api-key

# Web search (required)
TAVILY_API_KEY=your-tavily-key

# Ollama local LLM (needed for Zsxq analysis)
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=qwen3:8b

# Zsxq (needed for report search)
ZSXQ_ACCESS_TOKEN=your-zsxq-token
ZSXQ_GROUP_ID=your-group-id

# Feature switches (optional; defaults apply)
PTD_ENABLED=1                    # Progressive tool disclosure
TRACE_ENABLED=1                  # OTel tracing
SCHEDULER_ENABLED=1              # Scheduler
```

> Runs without RAGFlow / MySQL: the orchestrator skips unconfigured services automatically; LLM + Tavily alone cover the core experience.

```bash
# 5) Start
python main.py server              # Option 1: unified entry (recommended)
docker compose up -d --build       # Option 2: Docker (app + MySQL + Redis + Jaeger)
```

Open `http://localhost:8000` for the frontend; first-time users can click "guest login" for an instant account, or register via `POST /api/auth/register`. Jaeger UI (OTel tracing) is at `http://localhost:16686`.

### Verify the Deployment

```bash
python main.py test-imports               # import-chain smoke test
pytest tests/ -q                          # unit tests (network tests skipped by default)
k6 run benchmarks/k6/smoke.js             # full-chain HTTP smoke test (requires k6)
python -m tests.eval.run_eval --mode direct --limit 3   # LLM eval sampling
```

---

## 🔌 API Endpoints

| Method | Path | Description |
|------|------|------|
| **POST** | **`/api/task`** | **Generic task (multi-agent collaboration; returns thread_id, results stream over WS)** |
| POST | `/api/task/stream` | SSE streaming endpoint (first packet <50ms, Last-Event-ID resume) |
| POST | `/api/task/stop` | Stop the current task (CancellationToken cascading cancel) |
| POST | `/api/auth/register` / `login` / `refresh` / `guest` | JWT auth endpoints (guest = one-click, 10 QPM) |
| POST | `/api/zsxq-analysis` | Pre-market note heatmap analysis |
| POST | `/api/review-prediction` | Review & prediction (injects Beijing time + auto search window) |
| GET/POST/DELETE | `/api/users/*` `/api/sessions/*` | User/session management (JWT + row-level ownership checks) |
| **GET** | **`/api/slo/status`** | **SLO snapshot (availability/error budget/degradation/breakers; owner/admin)** |
| **GET** | **`/api/circuit-breakers`** | **Real-time breaker states (CLOSED/OPEN/HALF_OPEN)** |
| GET | `/api/traces/{session_id}` | Trace records (full span tree) + `/latency` (P50/P95/P99) |
| DELETE/POST | `/api/sessions/{sid}/turns/{i}` / `messages/batch-delete` | Single-turn / batch message deletion (three-layer persistence sync) |
| **WS** | **`/ws/{thread_id}`** | **Real-time push (tool_start/tool_end/task_result/thinking/error)** |

> All endpoints except the whitelist (health/docs/auth/static) require `Authorization: Bearer <token>`; user_id is always taken from the JWT.

---

## 📁 Project Structure (Source-of-Truth Map)

| Responsibility | Source of Truth | Compatibility Shim (do not edit) |
|------|----------------|----------------|
| Global constants (235+, flat) | `config/constants.py` | `shared/config/constants.py` (flat re-export + grouped views TIMEOUTS/SLO_TARGETS) |
| API server entry | `interfaces/api/server.py` (`python main.py server`) | — |
| Streaming protocol/bus/WS push | `api/stream_protocol.py`, `api/stream_bus.py`, `api/monitor.py` | same-named files under `interfaces/api/` |
| User/session storage | `interfaces/api/storage.py` | `api/storage.py` |
| Orchestrator agent | `agents/analyst/agent.py` | `agent/main_agent.py` |
| Memory/context engineering | `agents/reasoning/*` | `agent/memory_manager.py` etc. |
| LLM client/PTD/model routing | `shared/llm_client/*` | `agent/llm.py`, `agent/tool_router.py`, etc. |
| Governance (breakers/degradation/hallucination/validation/SLO/cache/RBAC) | `governance/guardrails/*`, `governance/monitor/*` | `agent/circuit_breaker.py` and 10+ more shims |
| Actor concurrency | `shared/actors/*` | `agent/actors/*` |
| Scheduling/workflows/skills | `orchestration/{scheduler,workflows,skills}/` | `agent/scheduler.py` |
| Data sources/tools | `shared/data_sources/*`, `tools/*` | a few re-exports |

```
moss_finance_assistant/
├── main.py                  # Unified entry: server / task / router / scheduler-next / test-imports
├── shared/                  # Shared layer: actors / data_sources / llm_client / utils / models / compat_bootstrap
├── agents/                  # Agent definitions: analyst (orchestrator) / router / reasoning
├── agent/                   # Compatibility shims + true submodules (subagents/, request_context, skill_manager)
├── governance/              # Governance: guardrails (breakers, degradation, hallucination) / monitor (OTel/SLO)
├── orchestration/           # Orchestration: scheduler / workflows / skills / loop
├── interfaces/api/          # API layer: server (single entry) / storage
├── api/                     # API sources: context / monitor / stream_bus / stream_protocol / middleware
├── tools/                   # LangChain tools (zsxq/tavily/ragflow/db/pdf etc.)
├── config/                  # constants.py — single source for global constants + rbac_policy.json
├── prompt/ skills/          # Eight-part XML prompts / domain skill encoding
├── benchmarks/              # Benchmarks (PTD / semantic cache threshold / judge consistency) + k6/ (HTTP load tests)
├── static/                  # Frontend single-page app
├── tests/                   # Unit tests + tests/eval (LLM eval regression, CI-blocking)
├── docs/adr/                # Architecture decision records (the only docs tracked in git)
├── .github/workflows/ci.yml # CI: ruff fatal + import smoke + pytest(cov) + Codecov + LLM eval sampling
├── Dockerfile / docker-compose.yml   # Containers: app + MySQL + Redis + Jaeger
└── data/ output/            # Runtime data & artifacts (gitignored)
```

---

## 🔧 Tech Stack

| Layer | Technology |
|------|------|
| Agent framework | deepagents (LangChain) + custom orchestration, multi-agent collaboration |
| Web | FastAPI + Uvicorn (async HTTP + WebSocket + SSE) |
| LLM | DeepSeek / OpenAI-compatible APIs with cost/SLA/complexity routing; local inference via Ollama qwen3:8b |
| Security | JWT + RBAC (four roles, QPM rate limits) + dual-layer prompt-injection protection (regex + LLM, JSONL audit) |
| Observability | OpenTelemetry (console/OTLP) + SLO monitoring + error budget + breaker endpoints |
| Reliability | Three-state breakers + four-level degradation + triple hallucination guard + five-dimension output validation retry + Actor snapshots + stream resume |
| Testing | pytest (93+ cases) + k6 load tests + LLM eval regression (26 golden samples, CI-blocking) |
| Cache/storage | Semantic cache (memory/redis), MySQL, SQLite (LangGraph checkpointer + SLO events) |
| Token optimization | PTD progressive tool disclosure (adaptive gating, measured 50%+ savings) |

## 🎤 Interview Q&A Map

The 10 questions interviewers are most likely to probe, with evidence locations in the repo:

| # | Question | Evidence |
|---|------|---------|
| 1 | Why is PTD a negative optimization for a 4-tool pool? | `benchmarks/bench_ptd_tokens.py` measurement (607 vs 678 tok) + `docs/adr/adr-0003` |
| 2 | How was the semantic cache threshold chosen? | `bench_semantic_cache_threshold.py` sweep 0.70–0.98 + embedding-discrimination lessons |
| 3 | Can you trust an LLM judge? | `bench_judge_consistency.py`: 100% self-consistency / 100% position-bias-free / 80% human agreement |
| 4 | How do you prevent hallucinations? Does it block answers? | Triple pipeline + "attach warning, never block" strategy (FAQ) |
| 5 | Why fail-open for injection protection? | Availability first + JSONL audit fallback + switchable fail-closed (Highlights §4) |
| 6 | How does this scale to 100k DAU? | "Production Topology & Scaling Path" L0→L4 above |
| 7 | What if an agent task runs away? | 150s/1M-token dual hard caps + error quadrants + breaker/degradation chain (Highlights §3) |
| 8 | What happens when WebSocket drops? | StreamResume + SSE Last-Event-ID resume (API table) |
| 9 | How do you keep evaluation honest? | Judge reliability measured first + golden-set threshold blocking in CI (Highlights §5) |
| 10 | Is the load-test data real? | `benchmarks/results/k6_*_summary.json` + measured numbers in the benchmark table, reproducible live |

## ✅ Pre-Interview Checklist

A demo repo's credibility depends on "everything clickable works"; any 404 or empty badge costs points:

- [ ] **Make the repo Public** (GitHub → Settings → General → Danger Zone) — badges/CI/ADR links must resolve
- [ ] **Push the latest code** (commit & push P1/P2/P3 changes; `git status` should be clean)
- [ ] Configure GitHub Actions **secrets**: `DEEPSEEK_API_KEY` (so eval sampling doesn't skip), `CODECOV_TOKEN` (coverage upload)
- [ ] CI shows green: confirm ruff + pytest(cov) + eval all pass on the Actions page
- [ ] Live-demo fallback: `python main.py server` + `k6 run benchmarks/k6/smoke.js` re-runs all assertions within 3 minutes

---

## ❓ FAQ

<details>
<summary><b>How does memory relevance filtering work?</b></summary>

Keywords are extracted (stock codes, Chinese phrases, finance abbreviations) and matched against conversation history using Jaccard overlap + stock-code exact-match weighting. History below the threshold (0.15) stays out of context. Example: if the user previously discussed Moutai and CATL and now asks about a Moutai report, only Moutai-related history is kept.
</details>

<details>
<summary><b>Do Zsxq search and bulk scraping conflict?</b></summary>

No. A `threading.Lock` browser mutex ensures `search_zsxq_by_stock` and `fetch_zsxq_group_topics` never run the browser simultaneously; bulk scraping returns "browser busy" immediately while a search is in progress.
</details>

<details>
<summary><b>Can circuit-breaker thresholds be tuned?</b></summary>

Yes. Defaults live in `governance/guardrails/circuit_breaker.py` under `CircuitBreakerRegistry.DEFAULTS` (five independent breakers: deepseek/ima/qwen8b/zsxq/main_agent), and `get_or_create(name, failure_threshold=X, failure_window_sec=Y, recovery_cooldown_sec=Z)` supports runtime overrides.
</details>

<details>
<summary><b>Does the hallucination guard block answers?</b></summary>

No. It follows a "conservative default + attach warning" strategy: unverified numbers or missing sources append a visible warning section (e.g., "⚠️ Hallucination guard notice: the following numbers were not found in tool results...") instead of blocking. Confidence decays 0.15 per failed item.
</details>

<details>
<summary><b>How do I observe SLO error budget consumption?</b></summary>

`GET /api/slo/status`: `error_budget.consumed_pct` shows consumption over a 30-day window (99% availability → budget 1% ≈ 432 minutes of acceptable downtime); above 80% warrants manual intervention. `slo_violations` lists current violations.
</details>

<details>
<summary><b>What if Ollama isn't running when I click "pre-market note heatmap"?</b></summary>

The system auto-starts it: locate the CLI (PATH + user-directory fallback) → background `ollama serve` (CREATE_NO_WINDOW on Windows) → poll for readiness up to 30s → auto `ollama pull` with download progress if the model is missing. All visible in the frontend.
</details>

<details>
<summary><b>Do deleted messages reappear after refresh? Do turn indexes get gaps?</b></summary>

No, on both counts. Three-layer synchronized persistence: `memory_turns` rows deleted with later turn_index shifted up (kept contiguous) + key_decisions/expired summaries cleaned + LangGraph checkpointer `RemoveMessage`. The frontend DOM indexes shift accordingly.
</details>

<details>
<summary><b>How do I add evaluation samples to the golden set?</b></summary>

Edit `tests/eval/golden_set.json`; each sample requires `_description` / `id` (contiguous eval_XXX) / `category` / `input` / `expected_points` / `forbidden_patterns` / `risk_level`; for buy/sell advice add `must_contain: ["仅供参考", "不构成投资建议"]`. Then verify with `python -m tests.eval.run_eval --mode direct --ids <newId> --limit 1`.
</details>

<details>
<summary><b>How does run_eval's http mode guarantee a final answer?</b></summary>

Three-stage pipeline: POST `/api/task` → `websockets.connect("ws://host/ws/{thread_id}")` → loop until `task_result` or `error` or a 180s timeout. Timeouts are marked errored without blocking other samples. Use `--concurrency 1` for long Q&A.
</details>

<details>
<summary><b>Does the semantic cache leak private data?</b></summary>

No. `should_cache_query` filters first (real-time questions / stock prices / holdings → skip), hits require cosine similarity above the threshold (default 0.92) plus a TTL staleness check. No user identity is recorded, and a `memory` backend keeps everything in-process.
</details>

<details>
<summary><b>How do I connect OTel to Jaeger/Grafana?</b></summary>

Two `.env` settings: `OTEL_EXPORTER_TYPE=otlp` + `OTEL_OTLP_ENDPOINT=http://<host>:4317`. Default sampling is 1.0; production recommends 0.05–0.1. Three span layers: `agent.run` (request_id/user_id) → `llm.chat:<model>` (tokens/cost) → `tool.call:<name>` (duration). Search by request_id in Jaeger for the full waterfall.
</details>

<details>
<summary><b>How do Actor snapshot recoveries work after a crash?</b></summary>

At startup `recover(actor_id)`: load the latest full snapshot → replay subsequent incremental snapshots → return state + version. If the file backend is corrupted but redis dual-writing is enabled, switch `backend=redis`; if both are corrupted, the Actor starts empty (equivalent to first run) without blocking the main flow.
</details>

---

## 📄 License

MIT License
