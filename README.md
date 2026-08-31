<p align="center">
  <h1 align="center">🤖 MOSS Finance Assistant</h1>
  <p align="center"><b>企业级多智能体金融资讯助手 —— 四层演进嵌套架构 (Prompt → Context → Harness → Loop Engineering)</b></p>
  <p align="center">
    <a href="https://github.com/arnoldli001/moss-finance-assistant/actions/workflows/ci.yml"><img src="https://github.com/arnoldli001/moss-finance-assistant/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://codecov.io/gh/arnoldli001/moss-finance-assistant"><img src="https://codecov.io/gh/arnoldli001/moss-finance-assistant/graph/badge.svg" alt="Coverage"></a>
    <img src="https://img.shields.io/badge/Python-3.10+-blue.svg" alt="Python">
    <img src="https://img.shields.io/badge/FastAPI-0.129.2-green.svg" alt="FastAPI">
    <img src="https://img.shields.io/badge/LangChain-1.2.10-orange.svg" alt="LangChain">
    <img src="https://img.shields.io/badge/deepagents-0.4.3-purple.svg" alt="deepagents">
    <img src="https://img.shields.io/badge/Ollama-本地推理-success.svg" alt="Ollama">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License">
  </p>
  <p align="center"><a href="README_EN.md">English</a> | 中文</p>
</p>

---

## 🎯 这是什么

面向散户用户的**企业级金融资讯多智能体系统**：DeepSeek 联网搜索 + 知识星球研报抓取 + RAGFlow 知识库 + 本地大模型分析，提供股票新闻、估值分析、护城河评估、散户数据等投研服务。

**一个"把 Agent demo 做成企业级系统"的完整样本**——重点不在接了几个 API，而在可靠性工程、安全防护、可观测性、评估回归这套企业级基建，且每个设计决策都有实测数据支撑。

---

## ✨ 核心亮点

### 1. 多智能体协作（deepagents + LangChain）
主智能体调度三个子智能体：网络搜索（DeepSeek + Tavily，自动过滤 2 个月前旧研报）/ 数据库查询（MySQL）/ 知识库检索（RAGFlow），外加知识星球 Playwright 抓取 + Ollama 本地分析。

### 2. 渐进式工具披露 PTD（Token 优化，实测驱动）
两阶段路由：Stage 0 注入极简工具菜单让模型选工具 → Stage 1 只注入选中工具的完整 Schema → Stage 2 兜底追加遗漏工具。
**自适应门控**：实测发现 4 工具池下 PTD 是负优化（全量 Schema ~607 tok < 菜单 ~678 tok），运行时自动旁路；9 工具池平均节省 50.2%~53.6%。

### 3. 可靠性工程八件套（Layer 3 Harness）
| 组件 | 能力 | 关键参数 |
|------|------|---------|
| 错误四象限分类 | 可重试硬错误/不应重试软错误/不可重试/配置类，幂等性强制校验 | 指数退避 vs 禁止重试 |
| 三态熔断器 | CLOSED → OPEN → HALF_OPEN 防抖恢复 | 60s 内 3 次失败熔断，冷却 30s |
| 四级降级链 | DeepSeek → IMA 知识库 → Qwen3-8B 本地 → 静态模板 | 单任务 ≤150s / ≤1M Token 双硬上限 |
| 幻觉防护 | RAG 引用追踪 + JSON Schema 校验 + LLM-as-Judge 三重管道 | 默认警示附加，不阻断输出 |
| 输出校验重试 | 数据/完整性/风险/来源/幻觉五维评分，未达标自动构造改进提示重调 | 每 request 限重试次数 |
| SLO 监控 | 可用性/延迟/幻觉通过率 + 30 天错误预算 | ≥99% / P95≤30s / ≥95% |
| OTel 追踪 | agent.run → llm.chat → tool.call 三层 span，跨协程传播 | console/OTLP 可配 |
| Actor 模型 | 会话注册/WS 连接/SLO 写入邮箱队列串行化 + 快照持久化 | 消除取消竞态与并发写 |

### 4. 企业级安全
- **JWT 认证 + RBAC**：register/login/refresh/guest 四端点签发 token 对；按角色限流 owner 600 / admin 120 / user 60 / guest 10 QPM；API 层强制从 JWT 取 user_id，杜绝水平越权
- **Prompt 注入双层防护**：正则快路（零成本）→ 本地 LLM 分类器慢路（语义二判，confidence≥0.7 拒绝）→ JSONL 审计；LLM 故障默认 fail-open（可用性优先），可切 fail-closed

### 5. LLM 评估回归体系（CI 可阻断）
26 条结构化 golden set（多股对比×10 / 行业分析×10 等），LLM-as-Judge 六维评分（覆盖度/违规/必须包含/幻觉/风险合规/综合分）。**Judge 可靠性先行实测**：qwen3:8b 自一致率 100%、位置无偏率 100%、与人工标注一致率 80%——裁判不一致时下游 eval 指标全是噪声。CI 通过率 <70% 或幻觉率 >5% 即阻断。

### 6. Context Engineering
滑窗 10 轮 + 20 轮自动压缩 3 段摘要 + 关键决策永久保留 + 关联度过滤（Jaccard + 股票代码加权，阈值 0.15）+ 2000 字精简裁剪 + 语义缓存（Embedding 余弦命中，阈值经基准校准）。

---

## 📊 实测数据与基准

量化数据替代"约节省 X%"式口径，全部脚本可离线复跑（结果 JSON 落 `benchmarks/results/`，不追 git）：

| 脚本 | 测什么 | 关键实测结论（2026-08-31） |
|------|--------|--------------------------|
| `bench_ptd_tokens.py` | PTD token 节省（tiktoken 真实计数，10 场景 × 双路由路径） | 4 工具池：PTD 负优化 → 自适应门控自动旁路；9 工具池：平均节省 50.2%~53.6% |
| `bench_semantic_cache_threshold.py` | 语义缓存阈值扫描（0.70~0.98，TP/FP/F1） | nomic-embed-text 对中文实体无区分度（不同对相似度≈1.0），生产校准需多语言嵌入模型——阈值结论不可跨后端迁移 |
| `bench_judge_consistency.py` | LLM-as-Judge 自一致性（K 次重复 + A/B 位置互换） | qwen3:8b：自一致率 100%、位置无偏率 100%、与人工标注一致率 80% |
| `k6/smoke.js` | HTTP 全链路冒烟（1 VU，13 项断言：health/注册/行级越权/任务启停/RBAC） | 13/13 通过；health p95 **1ms**、register **88ms**、鉴权读 **1.5ms**；任务受理 **10ms**（预热后）、优雅取消 **12.2s** |
| `k6/load.js` | 阶梯负载（1→5→10→20 VU，3m20s，SLO 阈值断言） | **4761 请求 / 错误率 0.00% / 429 限流 4721 次（user 60 QPM 生效）/ 限流链路 p95 2.2ms / 断言 100%** |

```bash
python benchmarks/bench_ptd_tokens.py
python benchmarks/bench_semantic_cache_threshold.py   # 需嵌入后端
python benchmarks/bench_judge_consistency.py          # 需本地 Ollama
k6 run benchmarks/k6/smoke.js                         # 冒烟 + 全链路断言
k6 run benchmarks/k6/load.js                          # 阶梯负载 + SLO 断言
```

**Eval 实测**（20 条新增样本 http 模式串行）：direct 模式平均分 0.60 / 通过率 50%；http 模式（联网 + 知识库）平均分 **0.825** / 通过率 **100%**；4 条无联网失败样本联网后提升 +0.20~+0.28 分；幻觉率 0%，风险合规一致为 1。

架构决策的"为什么"记录在 ADR（`docs/adr/`）：分层真源、常量唯一真源、PTD 自适应门控、注入双层防护、测试门控与 CI 策略。

---

## 💰 业务价值（成本量化）

企业级 AI 应用的第一性问题是**单位经济模型**。本项目的 Token 优化三件套均有基准实测背书：

| 优化手段 | 实测效果 | 成本影响 |
|---------|---------|---------|
| PTD 渐进式工具披露 | 9 工具池节省 50.2%~53.6% tool-schema token（4 工具池自动旁路避免负优化） | 输入 token 减半 → 按量计费 API 成本近似减半 |
| 语义缓存 | 命中即零 LLM 调用（阈值 0.92 经基准校准；实时/个股价格类问题自动跳过） | 重复问题边际成本 ≈ 0 |
| 模型路由 | 按成本/SLA/复杂度分流（简单问题走低成本模型） | 混合负载下均价下移 |

**成本换算公式**（参数可调，避免拍脑袋绝对值）：

```
年成本 ≈ 日查询量 × 平均输入token × 365 × 单价(¥/token)
PTD 节省 ≈ 年成本 × 50%（9 工具池实测区间取中）
缓存叠加节省 ≈ 重复问题占比 × 年成本
```

示例口径：1 万 DAU × 5 次查询/日、平均 3k 输入 token、DeepSeek 量贩单价 → PTD 一项即可覆盖大量 API 开销；叠加语义缓存与模型路由后综合降幅更高。**精确金额随厂商定价浮动，公式与实测比例长期有效**（每查询实际 token/cost 已通过 OTel span `llm.chat:<model>` 采集，为 `/api/metrics/cost` 成本仪表预留了数据源）。

业务指标树（北极星：周活跃投研用户）：采纳率 / 首答延迟（SSE 首包 <50ms）/ 幻觉率（eval 实测 0%）/ 单查询成本——四项均已埋点或可从现有 span/SLO 数据聚合。

---

## 🏭 生产部署拓扑与扩容路径

**当前形态**（单机，`docker compose up -d --build`）：app（uvicorn）+ MySQL + Redis + Jaeger。可靠性组件（限流/熔断/降级/SLO/OTel/Actor 快照）均已内置，扩容时无需改业务代码。

| 阶段 | 规模 | 架构动作 | 现状 |
|------|------|---------|------|
| L0 | 开发/演示 | 单进程 + SQLite + 进程内 Actor | ✅ 当前 |
| L1 | ~1k QPS | MySQL 替换 SQLite；Redis 承接语义缓存与会话缓存（`CACHE_BACKEND=redis`） | ✅ 配置即切换 |
| L2 | ~10k QPS | 应用层无状态化：限流计数器外置 Redis → 多实例 + LB；WS 跨实例广播用 Redis Pub/Sub 版 stream_bus | 限流/缓存有 Redis 后端，stream_bus Pub/Sub 版为演进项 |
| L3 | 10 万 DAU | Agent 任务队列化（独立 worker 池消费），WS 独立连接网关，MySQL 读写分离 | 演进项 |
| L4 | 多地域 | 多区域部署 + 静态资源 CDN + 全球流量调度 | 演进项 |

**演进判断依据**：Actor 邮箱队列已把并发写收敛到单点，状态外置是唯一阻塞多实例的改造点——这决定了扩容路径的成本排序（先 Redis 化状态，再水平扩实例，最后队列化任务）。

---

## 🏗️ 架构总览

| 层级 | 名称 | 核心能力 | 真源模块 |
|------|------|---------|---------|
| Layer 1 | **Prompt Engineering** | XML 八段式结构化、CoT、Few-shot、风险声明 | `prompt/prompts.yml` |
| Layer 2 | **Context Engineering** | 时效性去重、来源甄别、2000 字裁剪、关联度过滤、PTD、语义缓存 | `shared/llm_client/*`、`governance/guardrails/semantic_cache.py` |
| Layer 3 | **Harness Engineering** | 错误分类、熔断、降级、幻觉防护、SLO、Trace、Actor | `governance/guardrails/*`、`governance/monitor/*`、`shared/actors/*` |
| Layer 4 | **Loop Engineering** | 定时调度、状态持久化、Skill 编码、流式续流 | `orchestration/*`、`skills/` |

```
用户请求 (POST /api/task 或盘前按钮)
    │
    ▼
interfaces/api/server.py ← FastAPI 路由 + WebSocket + 定时调度 + SLO 端点
    │  POST /api/auth/*            ← JWT 认证（register/login/refresh/guest）
    │  GET /api/slo/status         ← SLO 监控 + 错误预算 + 熔断器状态
    │  GET /api/traces/{sid}       ← OTel 分布式追踪查询
    │
    ├─ api/middleware/             ← RBAC 鉴权 + 按角色限流 + Prompt 注入双层防护
    │
    ├──→ Layer1 Prompt: prompt/prompts.yml（XML 八段式 + CoT + Few-shot + Judge Prompt）
    ├──→ Layer2 Context:
    │      shared/llm_client/tool_router.py        ← PTD 两阶段路由（自适应门控）
    │      agents/reasoning/memory_manager.py      ← 滑窗+摘要+优先级+关联度过滤
    │      governance/guardrails/semantic_cache.py ← 语义缓存（Embedding 余弦命中）
    │      shared/llm_client/model_router.py       ← 多模型路由：成本/SLA/复杂度
    ├──→ Layer3 Harness（可靠性八件套，见上表）
    │      governance/guardrails/{error_classifier,circuit_breaker,degradation_chain,
    │                             hallucination_guard,output_validator,maker_checker}.py
    │      governance/monitor/{slo_monitor,tracing,stream_resume}.py
    │      shared/actors/* + CancellationToken 三位一体（令牌+元数据+超时）
    ├──→ Layer4 Loop:
    │      orchestration/scheduler|workflows|skills/ + agent/stream_resume.py + skills/*
    ├──→ 主智能体 agents/analyst/agent.py
    │      熔断准入 → 语义缓存 → 模型路由 → 子智能体（搜索/DB/知识库）
    │      → 工具集 → StreamResume → Maker-Checker → 幻觉校验 → Span 埋点 → SLO 事件
    ├──→ 评估回归（离线 CI）: tests/eval/{golden_set.json,run_eval.py}
    └──→ 盘前小作文: tools/zsxq_tool.py（Playwright + 浏览器互斥锁）→ Ollama qwen3:8b
    │
    ▼ (每步通过 WebSocket 实时推送 api/monitor.py)
前端实时显示进度 + 工具结果 + 最终回答 + 幻觉防护警示 + 续流
```

> 仓库采用「新分层结构 + 兼容垫片」：**真实实现只保留一份（真源）**，旧路径为纯 re-export 垫片（头部有 `[兼容垫片]` 标注，由 `shared/compat_bootstrap.py` 运行时别名），**改代码只改真源**。

---

## 🚀 快速开始

### 环境要求

- Python 3.10+ / OpenAI 兼容 LLM API Key（DeepSeek 等）/ Tavily API Key（[免费注册](https://tavily.com)）
- [Ollama](https://ollama.com)（本地大模型，知识星球分析需要）/ Playwright（知识星球抓取需要）

### 启动步骤

```bash
# 1) 克隆 + 装依赖
git clone https://github.com/arnoldli001/moss-finance-assistant.git
cd moss-finance-assistant
pip install -r requirements.txt

# 2) Playwright（知识星球抓取功能需要）
playwright install chromium

# 3) Ollama 本地模型
ollama pull qwen3:8b

# 4) 环境变量
cp .env.example .env
```

编辑 `.env` 核心配置：

```env
# LLM 服务（必填）
OPENAI_BASE_URL=https://api.deepseek.com
OPENAI_API_KEY=your-api-key

# 网络搜索（必填）
TAVILY_API_KEY=your-tavily-key

# Ollama 本地大模型（知识星球分析需要）
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=qwen3:8b

# 知识星球（研报搜索功能需要）
ZSXQ_ACCESS_TOKEN=your-zsxq-token
ZSXQ_GROUP_ID=your-group-id

# 功能开关（可选，均有默认值）
PTD_ENABLED=1                    # 渐进式工具披露
TRACE_ENABLED=1                  # OTel 追踪
SCHEDULER_ENABLED=1              # 定时调度
```

> 没有配置 RAGFlow / MySQL 也能跑：主智能体自动跳过未配置的服务，只配 LLM + Tavily 即可体验核心链路。

```bash
# 5) 启动
python main.py server              # 方式一：统一入口（推荐）
docker compose up -d --build       # 方式二：Docker 一键起（app + MySQL + Redis + Jaeger）
```

访问 `http://localhost:8000` 打开前端；首次使用点「游客登录」一键获取账号，或 `POST /api/auth/register` 注册。Jaeger UI（OTel 追踪）在 `http://localhost:16686`。

### 验证部署

```bash
python main.py test-imports               # import 链冒烟
pytest tests/ -q                          # 单元测试（网络测试默认跳过）
k6 run benchmarks/k6/smoke.js             # HTTP 全链路冒烟（需 k6）
python -m tests.eval.run_eval --mode direct --limit 3   # LLM 评估抽样
```

---

## 🔌 API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| **POST** | **`/api/task`** | **通用任务（多智能体协作，返回 thread_id 后走 WS 推最终结果）** |
| POST | `/api/task/stream` | SSE 流式端点（首包 <50ms，断点续传 Last-Event-ID） |
| POST | `/api/task/stop` | 停止当前任务（CancellationToken 级联取消） |
| POST | `/api/auth/register` / `login` / `refresh` / `guest` | JWT 认证四端点（guest 一键游客，10 QPM） |
| POST | `/api/zsxq-analysis` | 盘前小作文热度分析 |
| POST | `/api/review-prediction` | 复盘预测（注入北京时间 + 自动搜索时间窗口） |
| GET/POST/DELETE | `/api/users/*` `/api/sessions/*` | 用户/会话管理（JWT + 行级越权校验） |
| **GET** | **`/api/slo/status`** | **SLO 快照（可用性/错误预算/降级链/熔断器，owner/admin）** |
| **GET** | **`/api/circuit-breakers`** | **熔断器实时状态（CLOSED/OPEN/HALF_OPEN）** |
| GET | `/api/traces/{session_id}` | Trace 记录（全链路 Span 树）+ `/latency`（P50/P95/P99） |
| DELETE/POST | `/api/sessions/{sid}/turns/{i}` / `messages/batch-delete` | 单轮/批量消息删除（持久化三层同步） |
| **WS** | **`/ws/{thread_id}`** | **实时推送（tool_start/tool_end/task_result/thinking/error）** |

> 除白名单（health/docs/auth/static）外全部端点要求 `Authorization: Bearer <token>`；user_id 一律取自 JWT。

---

## 📁 项目结构（真源地图）

| 职责 | 真源（唯一实现） | 兼容垫片（勿改） |
|------|----------------|----------------|
| 全局常量（平铺定义 235+） | `config/constants.py` | `shared/config/constants.py`（flat re-export + 分组视图 TIMEOUTS/SLO_TARGETS） |
| API 服务入口 | `interfaces/api/server.py`（`python main.py server`） | — |
| 流式协议/总线/WS推送 | `api/stream_protocol.py`、`api/stream_bus.py`、`api/monitor.py` | `interfaces/api/` 同名文件 |
| 用户/会话存储 | `interfaces/api/storage.py` | `api/storage.py` |
| 主智能体 | `agents/analyst/agent.py` | `agent/main_agent.py` |
| 记忆/上下文工程 | `agents/reasoning/*` | `agent/memory_manager.py` 等 |
| LLM 客户端/PTD/模型路由 | `shared/llm_client/*` | `agent/llm.py`、`agent/tool_router.py` 等 |
| 治理层（熔断/降级/幻觉/校验/SLO/缓存/RBAC） | `governance/guardrails/*`、`governance/monitor/*` | `agent/circuit_breaker.py` 等 10+ 垫片 |
| Actor 并发 | `shared/actors/*` | `agent/actors/*` |
| 调度/工作流/Skills | `orchestration/{scheduler,workflows,skills}/` | `agent/scheduler.py` |
| 数据源/工具 | `shared/data_sources/*`、`tools/*` | 少数 re-export |

```
moss_finance_assistant/
├── main.py                  # 统一入口：server / task / router / scheduler-next / test-imports
├── shared/                  # 共享层：actors / data_sources / llm_client / utils / models / compat_bootstrap
├── agents/                  # 智能体定义：analyst(主) / router / reasoning
├── agent/                   # 兼容垫片层 + 真源子模块（subagents/、request_context、skill_manager）
├── governance/              # 治理层：guardrails(熔断降级幻觉校验) / monitor(OTel/SLO)
├── orchestration/           # 编排层：scheduler / workflows / skills / loop
├── interfaces/api/          # API 层：server(唯一入口) / storage
├── api/                     # API 真源：context / monitor / stream_bus / stream_protocol / middleware
├── tools/                   # LangChain 工具（zsxq/tavily/ragflow/db/pdf 等）
├── config/                  # constants.py 全局常量唯一真源 + rbac_policy.json
├── prompt/ skills/          # XML 八段式提示词 / 领域 Skill 编码
├── benchmarks/              # 量化基准（PTD/语义缓存阈值/Judge 一致性）+ k6/（HTTP 压测）
├── static/                  # 前端单页应用
├── tests/                   # 单元测试 + tests/eval（LLM 评估回归，CI 阻断）
├── docs/adr/                # 架构决策记录（唯一入库文档）
├── .github/workflows/ci.yml # CI：ruff fatal + import 冒烟 + pytest(cov) + Codecov + LLM 评估抽样
├── Dockerfile / docker-compose.yml   # 容器化：app + MySQL + Redis + Jaeger
└── data/ output/            # 运行时数据与生成物（gitignored）
```

---

## 🔧 技术栈

| 层级 | 技术 |
|------|------|
| Agent 框架 | deepagents (LangChain) + 自研编排，多智能体协作 |
| Web | FastAPI + Uvicorn（异步 HTTP + WebSocket + SSE） |
| LLM | DeepSeek / OpenAI 兼容协议，多模型按成本/SLA/复杂度路由；Ollama qwen3:8b 本地推理 |
| 安全 | JWT + RBAC 四角色按 QPM 限流 + Prompt 注入双层防护（正则+LLM，JSONL 审计） |
| 可观测性 | OpenTelemetry（console/OTLP）+ SLO 监控 + 错误预算 + 熔断器端点 |
| 可靠性 | 三态熔断 + 四级降级 + 幻觉三重防护 + 输出五维校验重试 + Actor 快照 + 流式续流 |
| 测试 | pytest（93+ 用例）+ k6 压测 + LLM 评估回归（26 条 golden set，CI 阻断） |
| 缓存/存储 | Semantic Cache（memory/redis）、MySQL、SQLite（LangGraph checkpointer + SLO 事件） |
| Token 优化 | PTD 渐进式工具披露（自适应门控，实测节省 50%+） |

---

## 🎤 面试 Q&A 地图

面试官最可能追问的 10 个问题，以及答案在仓库中的证据位置：

| # | 追问 | 证据位置 |
|---|------|---------|
| 1 | 为什么 4 工具池 PTD 是负优化？ | `benchmarks/bench_ptd_tokens.py` 实测（607 vs 678 tok）+ `docs/adr/adr-0003` |
| 2 | 语义缓存阈值怎么定的？ | `bench_semantic_cache_threshold.py` 扫描 0.70~0.98 + 嵌入区分度教训 |
| 3 | LLM 当裁判可信吗？ | `bench_judge_consistency.py`：自一致 100% / 位置无偏 100% / 与人工对齐 80% |
| 4 | 幻觉怎么防？会不会阻断回答？ | 三重管道 + "警示附加不阻断"策略（FAQ §幻觉防护） |
| 5 | 注入防护为什么 fail-open？ | 可用性优先 + JSONL 审计兜底 + 可切 fail-closed（核心亮点 §4） |
| 6 | 10 万 DAU 怎么扩？ | 本文「生产部署拓扑与扩容路径」L0→L4 |
| 7 | 任务失控怎么办？ | 150s/1M token 双硬上限 + 错误四象限 + 熔断降级链（核心亮点 §3） |
| 8 | WS 断线了怎么办？ | StreamResume + SSE Last-Event-ID 断点续传（API 表） |
| 9 | 评估怎么防止"自嗨"？ | Judge 可靠性先行实测 + golden set 阈值阻断 CI（核心亮点 §5） |
| 10 | 压测数据是真的吗？ | `benchmarks/results/k6_*_summary.json` + 本文基准表实测数字，可现场复跑 |

## ✅ 面试前自证清单

演示项目的可信度取决于"点开都能看"，任何 404/空 badge 都会减分：

- [ ] **仓库设为 Public**（GitHub → Settings → General → Danger Zone）——badge/CI/ADR 链接才可访问
- [ ] **推送最新代码**（P1/P2/P3 改动提交并 push，`git status` 应为 clean）
- [ ] GitHub Actions **配 secrets**：`DEEPSEEK_API_KEY`（eval 抽样才不 skip）、`CODECOV_TOKEN`（覆盖率上传）
- [ ] CI 首页绿勾：Actions 页确认 ruff + pytest(cov) + eval 三步全绿
- [ ] 现场演示兜底：`python main.py server` + `k6 run benchmarks/k6/smoke.js` 3 分钟内可复跑全部断言

---

## ❓ FAQ

<details>
<summary><b>记忆管理的关联度过滤如何工作？</b></summary>

提取关键词（股票代码、中文词组、金融缩写），与历史对话计算 Jaccard 重叠度 + 股票代码精确匹配加权。低于阈值（0.15）的历史不放入 context。例：之前聊过茅台和宁德时代，现在问茅台研报，只保留茅台相关对话。
</details>

<details>
<summary><b>知识星球搜索和全量抓取会冲突吗？</b></summary>

不会。`threading.Lock` 浏览器互斥锁，`search_zsxq_by_stock` 与 `fetch_zsxq_group_topics` 不会同时运行浏览器，搜索进行中时全量抓取快速返回"浏览器正忙"。
</details>

<details>
<summary><b>熔断阈值可以调整吗？</b></summary>

可以。默认在 `governance/guardrails/circuit_breaker.py` 的 `CircuitBreakerRegistry.DEFAULTS`（deepseek/ima/qwen8b/zsxq/main_agent 五个独立），也支持 `get_or_create(name, failure_threshold=X, failure_window_sec=Y, recovery_cooldown_sec=Z)` 运行时覆盖。
</details>

<details>
<summary><b>幻觉防护会阻止回答输出吗？</b></summary>

不会。"默认保守 + 警示附加"策略：检测到未验证数字或缺失来源时，在回答末尾附加警示段落（如"⚠️ 幻觉防护提示：以下数字未在工具结果中找到..."），置信度按未通过项数量 0.15/项衰减。
</details>

<details>
<summary><b>如何观察 SLO 错误预算消耗？</b></summary>

`GET /api/slo/status`：`error_budget.consumed_pct` 为 30 天窗口错误预算消耗（可用性 99% → 预算 1% ≈ 432 分钟可接受停机），超 80% 需人工介入；`slo_violations` 列出当前违规项。
</details>

<details>
<summary><b>点击「盘前小作文热度」时 Ollama 没启动会怎样？</b></summary>

系统自动后台拉起：定位 CLI（PATH + 用户目录兜底）→ 后台 `ollama serve`（Windows 下 `CREATE_NO_WINDOW`）→ 轮询 30 秒等就绪 → 模型未拉取自动 `ollama pull` 并推下载进度。全程前端可见。
</details>

<details>
<summary><b>右键删除消息后，刷新页面还会出现吗？turn_index 会有间隙吗？</b></summary>

不会出现、不会间隙。三层同步持久化：`memory_turns` 行删除且后段 turn_index 前移 1（保持连续）+ 同步清理 key_decisions/过期 summaries + LangGraph checkpointer `RemoveMessage`。前端 DOM 索引同步前移。
</details>

<details>
<summary><b>如何给 golden set 新增评估样本？</b></summary>

编辑 `tests/eval/golden_set.json`，每条必填 `_description` / `id`（eval_XXX 连续）/ `category` / `input` / `expected_points` / `forbidden_patterns` / `risk_level`；涉及买卖建议的追加 `must_contain: ["仅供参考", "不构成投资建议"]`。然后 `python -m tests.eval.run_eval --mode direct --ids <新ID> --limit 1` 验证。
</details>

<details>
<summary><b>run_eval 的 http 模式怎么确保拿到最终答案？</b></summary>

三流程串联：POST `/api/task` 提交 → `websockets.connect("ws://host/ws/{thread_id}")` → 循环接收直到 `task_result` 或 `error` 或 180s 超时。超时标记 errored 不阻塞其他样本。长问答建议 `--concurrency 1`。
</details>

<details>
<summary><b>语义缓存会泄露隐私吗？</b></summary>

不会。`should_cache_query` 先过滤（实时问题/个股价格/具体持仓 → 跳过），命中需余弦相似度超阈值（默认 0.92）且通过 TTL 陈旧检查。不记录用户身份，可切 `memory` 后端进程内缓存。
</details>

<details>
<summary><b>OTel 如何接入 Jaeger/Grafana？</b></summary>

`.env` 两项：`OTEL_EXPORTER_TYPE=otlp` + `OTEL_OTLP_ENDPOINT=http://<host>:4317`。默认全量采样，生产建议 0.05~0.1。Span 三层：`agent.run`（request_id/user_id）→ `llm.chat:<model>`（tokens/cost）→ `tool.call:<name>`（耗时），Jaeger 搜 request_id 看全链路瀑布。
</details>

<details>
<summary><b>Actor 持久化快照崩溃后如何恢复？</b></summary>

启动时 `recover(actor_id)`：取最新全量快照反序列化 → 依次回放其后增量快照 → 返回状态 + 版本号。file 后端损坏可切 redis 双写恢复；双后端都损坏时从空状态启动（等价首次运行），不阻塞主流程。
</details>

---

## 📄 License

MIT License
