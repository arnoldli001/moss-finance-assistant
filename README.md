<p align="center">
  <h1 align="center">🤖 MOSS Finance Assistant</h1>
  <p align="center"><b>企业级多智能体金融资讯助手 —— 四层演进嵌套架构 (Prompt → Context → Harness → Loop Engineering)</b></p>
  <p align="center">
    <img src="https://img.shields.io/badge/Python-3.10+-blue.svg" alt="Python">
    <img src="https://img.shields.io/badge/FastAPI-0.129.2-green.svg" alt="FastAPI">
    <img src="https://img.shields.io/badge/LangChain-1.2.10-orange.svg" alt="LangChain">
    <img src="https://img.shields.io/badge/deepagents-0.4.3-purple.svg" alt="deepagents">
    <img src="https://img.shields.io/badge/Playwright-1.62.0-red.svg" alt="Playwright">
    <img src="https://img.shields.io/badge/Ollama-本地推理-success.svg" alt="Ollama">
    <img src="https://img.shields.io/badge/PTD-渐进式工具披露-yellow.svg" alt="PTD">
  </p>
</p>

---

## 🎯 项目简介

MOSS Finance Assistant 是一个面向散户用户的**企业级金融资讯多智能体系统**。采用业界最新的四层演进嵌套架构设计，集成 DeepSeek 联网搜索、知识星球研报抓取、RAGFlow 知识库、本地大模型分析，提供股票新闻、估值分析、护城河评估、散户数据等投研服务。

### 四层演进架构

| 层级 | 名称 | 核心能力 | 关键模块 |
|------|------|---------|---------|
| Layer 1 | **Prompt Engineering** | 语言能力：角色设定、CoT 推理链、Few-shot、结构化 XML | `prompt/prompts.yml` |
| Layer 2 | **Context Engineering** | 上下文工程：时效性去重、来源甄别、2000字精简裁剪、关联度过滤 | `agent/memory_manager.py`、`agent/context_engineer.py` |
| Layer 3 | **Harness Engineering** | 可靠性：错误四象限分类、三态熔断器、四级降级链、幻觉防护、SLO 监控、Trace、Maker-Checker | `agent/error_classifier.py`、`agent/circuit_breaker.py`、`agent/degradation_chain.py`、`agent/hallucination_guard.py`、`agent/slo_monitor.py` |
| Layer 4 | **Loop Engineering** | 持续迭代：定时调度、状态持久化、Skill 编码、子 Agent 分离 | `agent/scheduler.py`、`agent/state_store.py`、`skills/` |

---

## ✨ 核心功能

### 1. 多智能体协作系统

主智能体像团队负责人一样调度三个子智能体协同工作：
- **网络搜索助手**（DeepSeek + Tavily）：联网搜索券商研报、业绩预估（自动过滤 2 个月前旧研报）
- **数据库查询助手**（MySQL）：散户数据、业务数据查询
- **知识库检索助手**（RAGFlow）：专有知识库检索

### 2. 知识星球研报搜索

按股票名搜索知识星球内容（研报/小作文/新闻）：
- Playwright 浏览器自动化：登录 → 搜索框输入股票名 → 点"当前星球" → 点"最新"排序
- 逐条点击打开详情 → 提取完整正文 → 退出 → 下一条（共 5 条）
- 调用 Ollama Qwen3-8B 本地大模型分析汇总
- 浏览器互斥锁防止并发冲突

### 3. 盘前小作文热度分析

一键抓取知识星球金融资讯，本地大模型分析股票热度，生成总结报告。

### 4. 渐进式工具披露（PTD）

业界最新 Token 优化方案，两阶段路由减少 Tool Schema Token 消耗：
- **Stage 0**（路由）：注入极简工具菜单（~200 tokens），模型选择所需工具
- **Stage 1**（执行）：只注入选中工具的完整 Schema（节省 48%~74% Token）
- **Stage 2**（兜底）：自动追加遗漏工具，最多 2 次重试

### 5. 记忆管理（Context Engineering）

防止多轮对话内容丢失：
- **滑窗**：完整保留最近 10 轮对话
- **摘要压缩**：超过 20 轮自动压缩为 3 段摘要（早期/中期/近期）
- **优先级排序**：关键决策永久保留，闲聊丢弃
- **关联度过滤**：与当前问题无关的历史对话不放入 context（Jaccard + 股票代码匹配加权）

### 6. 停止任务功能

前端"停止"按钮一键终止当前检索查询，后端 `task.cancel()` + WebSocket 通知，停止后立即允许重新输入。

### 7. Ollama 自动启动与模型管理

点击「盘前小作文热度」时若 Ollama 服务未启动，系统**自动后台拉起**（Windows 下 `CREATE_NO_WINDOW` 不弹黑框），轮询 30 秒等就绪；模型未拉取时自动 `ollama pull qwen3:8b`，每 20 秒推下载进度。三阶段流水线：定位 CLI（PATH + 用户目录兜底）→ 启动服务 → 检查/拉取模型，全程前端可见进度。

### 8. 右键消息删除

前端消息气泡右键/长按菜单新增「🗑️ 删除该消息」功能：
- **整轮删除**：一轮问答（user + assistant）一起删，保持 `turn_index` 连续（后段前移 1）
- **持久化**：同步删除 `memory_turns` / `memory_key_decisions` / 过期 `memory_summaries` + LangGraph checkpointer 对应消息
- **越权校验**：必须传 `user_id`，`storage.verify_session_owner` 防止删除他人会话
- **实时消息可删**：刚发送未刷新的消息也能删除（`pendingTurnIndex` 实时计数）
- **DOM 索引同步**：删除后剩余 `data-turn-index > N` 的全部前移 1，保证下次右键删除索引仍正确

### 10. Actor 模型（并发安全 + 状态原子性）

针对多 Agent 协作中「异步工具回调直接改状态 → 状态机确定性被破坏 → 取消竞态 / 重复注册 / 连接字典被并发修改」等问题，用 Actor 模型重构并发路径：

- **SessionRegistryActor**（`agent/actors/session_registry_actor.py`）：会话级任务注册原子化。旧/新任务切换、停止、反注册全部走邮箱队列串行处理，`REGISTER_AGENT_TASK → cancel 旧 task → 登记新 task` 在同一消息内完成，无需外部锁。
- **ConnectionManagerActor**（`agent/actors/connection_manager_actor.py`）：WebSocket 连接字典并发安全。所有 active_connections 修改（connect/disconnect/send_to_thread）在单协程内串行执行，send 失败不会影响其他连接。
- **SloMonitorActor**（`agent/actors/slo_monitor_actor.py`）：SLO 事件写入与滚动窗口计算串行化，避免多线程写 Deque 导致统计偏差。

配合全局 `CancellationToken` 三位一体（取消令牌 + 元数据 + 超时控制），连接断开 / STOP 接口 / 超时任一路径触发后，所有执行深度主动检查令牌状态，毫秒级资源回收。

### 11. 企业级 8 大纵深加固：

| 编号 | 加固方向 | 核心模块 | 能力要点 |
|------|---------|---------|---------|
| 1 | **OpenTelemetry 分布式追踪** | `agent/observability/tracing.py` | OTel SDK 接入（console/OTLP），`agent_span`/`tool_span`/`llm_span` 上下文跨协程传播，采样率/span 属性前缀可配置 |
| 2 | **LLM 评估集 + LLM-as-Judge** | `tests/eval/` + `config/constants.py` | 26 条结构化 golden set（新闻/估值/护城河/幻觉/多股对比×10/行业分析×10），覆盖率/违规/必须包含/幻觉四维评分，CI 阈值阻断（通过率≥70% / 幻觉率≤5%） |
| 3 | **Actor 状态持久化快照** | `agent/actor_persistence.py` | 全量 + 增量快照双模式，file/redis/memory 三种后端，FIFO 保留最近 N 个版本，崩溃后从最近快照恢复 Actor 状态 |
| 4 | **RBAC 权限中间件 + 注入防护** | `api/middleware/rbac.py` | 角色权限映射（admin/analyst/viewer/guest），`require_permission("xxx")` 装饰器细粒度接口鉴权，Prompt Injection 检测（ignore_upstream_instructions / role_reset / system_prompt_override） |
| 5 | **语义缓存（LLM 响应级）** | `agent/semantic_cache.py` | Embedding + 余弦相似度匹配，Redis 与内存双后端，查询白名单 / 正则过滤敏感问题不缓存，缓存失效 + 允许陈旧读取策略可配置 |
| 6 | **多模型动态路由（成本/SLA 感知）** | `agent/model_router.py` | 简单/复杂三级分类器，`cost_aware` 按预算限流，`sla_aware` 黑名单降级，路由决策含 estimated_cost_usd 可审计 |
| 7 | **流式输出续流（断点恢复）** | `agent/stream_resume.py` | 中断后 `resume_stream(session_id, msg_id)` 从已落地 chunk 回放，再消费剩余队列；过期 TTL 自动清理，避免磁盘膨胀 |
| 8 | **输出 Schema 校验 + 智能重试** | `agent/output_validator.py` | 数据/完整性/风险/来源/幻觉五维校验，未通过时自动「构造改进提示 → 重调 LLM」，每 request 计数器限重试，超上限输出兜底歉意文案 |

### 12. LLM 评估回归体系（CI 可阻断）

在 `tests/eval/` 中提供完整 LLM 输出质量评估，解决「大模型迭代没法做回归测试」的行业痛点：

- **结构化 Golden Set**（[tests/eval/golden_set.json](file:///d:/code/moss-finance-assistant/tests/eval/golden_set.json)）：26 条样本覆盖 8 大类
  - 个股新闻速览 1 条 / 估值分析 1 条 / 护城河 1 条 / 幻觉防护 1 条 / 风险声明 1 条 / 信息时效性 1 条
  - **多股对比 10 条**：白酒、新能源、银行、互联网、光伏、创新药、家电、地产、煤炭、半导体
  - **行业分析 10 条**：白酒、新能源车、创新药、存储芯片、AI算力、旅游、煤炭、快递、储能、人形机器人
- **LLM-as-Judge 评分维度**：`coverage`（期望点覆盖度）、`forbidden_violations`（违规条数）、`must_contain_hit/total`（必须包含）、`hallucination`（0~1 幻觉强度）、`risk_compliance`（风险声明合规）、`overall_score`（综合分）
- **两种评估模式**：
  - `--mode direct`：直接用 DeepSeek API 答题，验证评估框架本身（无 server 时也能跑）
  - `--mode http`：POST `/api/task` + WebSocket `/ws/{thread_id}` 监听 `task_result`，测真实 MOSS Agent（联网 + 知识库）
- **便捷参数**：`--category` 按类别过滤、`--ids eval_014,eval_015` 按 ID 指定、`--limit N` 只跑前 N 条、`--concurrency 1` 覆盖默认并发

典型用法：

```bash
# 1) 验证评估框架（无需 server）
python -m tests.eval.run_eval --mode direct --limit 5

# 2) 测真实 Agent，只跑之前失败的 4 条，串行避免压力过大
python -m tests.eval.run_eval --mode http --ids eval_014,eval_015,eval_021,eval_024 --concurrency 1

# 3) 跑完整多股对比 + 行业分析 20 条新样本
python -m tests.eval.run_eval --mode http --category stock_compare --concurrency 1
python -m tests.eval.run_eval --mode http --category industry_analysis --concurrency 1
```

**实测结果**（20 条新增样本 http 模式串行）：
- direct 模式平均分 0.60、通过率 50%
- http 模式（联网 + 知识库）平均分 **0.825**、通过率 **100%**
- 4 条无联网失败样本（地产财务/煤炭股息/AI算力/快递价格战）联网后提升 +0.20~+0.28 分
- 幻觉率 0%，风险合规一致为 1

### 9. 可靠性工程（Layer 3 Harness）

**错误四象限分类器**（`agent/error_classifier.py`）：
- A 可重试硬错误（网络瞬态/504/429 → 指数退避重试）
- B 不应重试软错误（模型幻觉/参数错误 → **禁止重试**）
- C 不可重试错误（写超时/OOM → Fail-Closed）
- D 配置类错误（API Key 过期/401 → **永不重试**，立即告警）
- `IdempotencyChecker` 强制幂等性检查（敏感操作下单/转账等）

**三态时间窗口熔断器**（`agent/circuit_breaker.py`）：
- 三态：**CLOSED → OPEN（60秒3次失败）→ HALF_OPEN（冷却30秒）→ CLOSED**
- 预置 5 个熔断器配置：deepseek / ima / qwen8b / zsxq / main_agent
- HALF_OPEN 需要连续 2 次成功才恢复 CLOSED，防抖动

**四级降级链**（`agent/degradation_chain.py`）：
- 四级自动降级：**DeepSeek → IMA 知识库 → Qwen3-8B 本地 → 静态模板**
- **双重硬上限**：单任务执行时间 ≤ 150s，Token 消耗 ≤ 1,000,000
- 每层前熔断器准入检查 + asyncio.wait_for 超时保护 + 错误分类决定熔断计数

**幻觉防护三重管道**（`agent/hallucination_guard.py`）：
- **Tier 1 RAG 引用追踪**：输出数字/股票代码必须可在工具结果中找到，新闻/数据必须标注来源
- **Tier 2 JSON Schema 验证**：必填字段存在性 + 类型校验（不引入 jsonschema 依赖）
- **Tier 3 LLM-as-Judge**：独立轻量模型二次审查（可选）
- 未通过时自动附加用户可见警示，置信度按未通过项数量衰减

**SLO 监控聚合器**（`agent/slo_monitor.py`）：
- 可用性 ≥ 99% / P95 延迟 ≤ 30s / 幻觉通过率 ≥ 95%
- 30 天滚动窗口错误预算计算（99% 可用性 → 432 分钟可接受停机）
- 内存 Deque（实时查询）+ SQLite（跨重启持久化，`data/slo_events.db`）双保险
- 监控端点：`GET /api/slo/status`、`GET /api/circuit-breakers`

**输出质量自动校验 + 智能重试**（`agent/output_validator.py` — 可靠性第 6 件套）：
- 五维评分：数据可信 / 结构完整 / 风险声明合规 / 信息来源标注 / 幻觉程度
- 任意维度不达标 → 自动构造改进提示 → 重新调用 LLM
- 每 request 独立计数器限制重试次数，超限返回兜底歉意
- 与 hallucination_guard + maker_checker 联动：三重防护一致才能放行

**OpenTelemetry 分布式追踪**（`agent/observability/tracing.py` — 可靠性第 7 件套：可观测性）：
- OTel 官方 SDK，`console` / `OTLP gRPC` / `OTLP HTTP` 三种导出
- Span 分层：`agent.run`（主流程）→ `llm.chat`（LLM 调用）→ `tool.call`（工具执行）
- 跨协程上下文传播（ContextVar），采样率可配置（默认 1.0，生产建议 0.05~0.1）
- 追踪端点：`GET /api/traces/{session_id}`、`GET /api/traces/{session_id}/latency`

**评估驱动闭环（CI 阻断）**（`tests/eval/run_eval.py` — 可靠性第 8 件套：回归测试）：
- 26 条结构化 golden set 覆盖多股对比 + 行业分析
- 通过率阈值 70% / 幻觉率阈值 5%，任一突破 → CI 返回非零退出码
- 每次 LLM 模型升级 / prompt 变更 / 工具 schema 更新 → 必须回归此评估集

---

## 🏗️ 架构总览

```
用户请求 (POST /api/task 或盘前按钮)
    │
    ▼
api/server.py                     ← FastAPI 路由 + WebSocket + 定时调度 + SLO端点
    │  GET /api/slo/status            ← SLO 监控 + 错误预算 + 熔断器状态
    │  GET /api/circuit-breakers
    │  GET /api/traces/{sid}          ← OTel 分布式追踪查询
    │  POST /api/users / {id}/sessions ← RBAC 用户/会话管理
    │
    ├─ api/middleware/rbac.py        ← RBAC 鉴权 + Prompt Injection 防护
    │    └── require_permission("xxx")装饰器：admin/analyst/viewer/guest 四角色
    │
    ├──→ Layer1: Prompt Engineering
    │    agent/prompts.py             ← XML 八段式结构化提示词
    │    prompt/prompts.yml           ← 角色/CoT/Few-shot/风险声明/Judge Prompt
    │
    ├──→ Layer2: Context Engineering
    │    agent/memory_manager.py      ← 滑窗+摘要+优先级+关联度过滤
    │    agent/context_engineer.py    ← 时效性去重+来源甄别+2000字裁剪
    │    agent/tool_router.py         ← PTD 两阶段路由(零Schema→选子集)
    │    agent/semantic_cache.py      ← 语义缓存：Embedding + 余弦相似度命中
    │    agent/model_router.py        ← 多模型路由：成本/SLA/复杂度三级分类
    │
    ├──→ Layer3: Harness Engineering（可靠性五件套 + 扩展 3 件 = 8 件）
    │    AGENTS.md                    ← 错误经验结构化沉淀（硬编码规则）
    │    agent/error_classifier.py    ← 错误四象限分类 + 幂等性检查
    │    agent/circuit_breaker.py     ← 时间窗口三态熔断器（60s/3次熔断）
    │    agent/degradation_chain.py   ← 四级降级链（150s/1M 双重硬上限）
    │    agent/hallucination_guard.py ← 幻觉防护：RAG引用追踪 + Schema + Judge
    │    agent/output_validator.py    ← 输出五维校验 + 自动构造重试提示
    │    agent/slo_monitor.py         ← SLO 聚合 + 错误预算计算 + SQLite持久化
    │    agent/trace.py / observability/tracing.py ← OTel 全链路 Trace 可观测性
    │    agent/maker_checker.py       ← 五维输出质量校验（数据/完整性/风险/来源/幻觉）
    │    agent/feedback_handler.py    ← 用户质疑检测+错误学习记忆
    │
    ├──→ Layer4: Loop Engineering
    │    agent/scheduler.py           ← 定时调度(9:13小作文/9:15新闻)
    │    agent/state_store.py         ← 跨运行状态持久化
    │    agent/actor_persistence.py   ← Actor 快照：全量/增量 + file/redis 后端
    │    agent/skill_manager.py       ← Skill 自动加载（关键词匹配注入）
    │    agent/stream_resume.py       ← 流式续流：断点恢复 + chunk TTL 清理
    │    skills/trading-reliability/  ← 金融可靠性Skill（熔断/降级/幂等/幻觉）
    │    skills/finance-analysis/     ← 金融分析Skill编码
    │
    ├──→ Actor Model（并发原子性保证）
    │    agent/actors/session_registry_actor.py   ← 任务注册/停止/反注册串行化
    │    agent/actors/connection_manager_actor.py ← WS连接字典串行访问
    │    agent/actors/slo_monitor_actor.py        ← SLO 事件原子写入
    │    └──→ CancellationToken 三位一体：取消令牌 + 元数据 + 超时控制
    │
    ├──→ 主智能体: agent/main_agent.py
    │    ├──→ 熔断器准入检查 → 熔断中直接返回静态兜底
    │    ├──→ SemanticCache 查询（语义命中直接返回）
    │    ├──→ ModelRouter 路由决策（成本/SLA 感知）
    │    ├──→ 子智能体: 网络搜索 / 数据库查询 / 知识库检索
    │    ├──→ 工具: Markdown生成 / PDF转换 / 文件读取 / 知识星球搜索
    │    ├──→ StreamResume 写入 chunk（断流可续）
    │    ├──→ Maker-Checker 规则校验 → OutputValidator 智能重试
    │    ├──→ HallucinationGuard 三重幻觉校验
    │    ├──→ Span 埋点 (agent.run/llm.chat/tool.call)
    │    └──→ SLOEvent 事件记录（成功/延迟/层级/幻觉/象限/熔断）
    │
    ├──→ 评估回归（离线 CI）
    │    tests/eval/golden_set.json   ← 26 条结构化 golden set
    │    tests/eval/judge_prompt.py   ← Judge System + User Prompt 模板
    │    tests/eval/run_eval.py       ← --mode direct/http --ids --category --concurrency
    │
    └──→ 盘前小作文: _run_zsxq_analysis()
              ├──→ tools/zsxq_tool.py (Playwright 抓取 + 浏览器互斥锁)
              └──→ Ollama qwen3:8b (LLM 分析)
    │
    ▼ (每步通过 WebSocket 实时推送)
api/monitor.py                   ← 事件埋点 + ConnectionManagerActor 推送
    │
    ▼
前端实时显示进度 + 工具结果 + 最终回答 + 幻觉防护警示 + 续流
```

---

## 🚀 快速开始

### 环境要求

- Python 3.10+
- OpenAI 兼容的 LLM API Key（DeepSeek / 阿里云百炼 / OpenAI）
- Tavily API Key（[免费注册](https://tavily.com)）
- [Ollama](https://ollama.com)（本地大模型，知识星球分析功能需要）
- Playwright 浏览器（知识星球抓取功能需要）

### 第一步：克隆 + 装依赖

```bash
git clone https://github.com/arnoldli001/moss-finance-assistant.git
cd moss-finance-assistant
pip install -r requirements.txt
```

### 第二步：安装 Playwright

```bash
pip install playwright
playwright install chromium
```

### 第三步：配置 Ollama 本地大模型

```bash
ollama pull qwen3:8b
```

### 第四步：配环境变量

```bash
cp .env.example .env
```

编辑 `.env`，核心配置：

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

# # 四层工程配置（可选，均有默认值）
PTD_ENABLED=1                    # 渐进式工具披露
MEM_RELEVANCE_THRESHOLD=0.15    # 记忆关联度过滤阈值
CONTEXT_MAX_CHARS=2000          # 上下文精简裁剪阈值
TRACE_ENABLED=1                 # Trace 可观测性
SCHEDULER_ENABLED=1             # 定时调度器

# 可靠性工程（可选，均有默认值）
# 熔断器：deepseek 默认 60秒/3次失败熔断，30秒冷却
# 降级链：单任务执行 ≤ 150s，Token ≤ 1,000,000
# SLO：可用性 ≥ 99%，P95延迟 ≤ 30s，幻觉通过率 ≥ 95%
# RBAC：默认角色 guest 可用公共接口，其他角色在 api/middleware/rbac.py 映射
# 语义缓存：默认 memory 后端（单实例），集群部署可切 redis://
# OTel 追踪：默认 console 导出；生产环境建议 OTEL_EXPORTER_TYPE=otlp + OTEL_OTLP_ENDPOINT=http://localhost:4317
# 以上参数在 config/constants.py（19功能分组/235+ 命名常量）中定义，全局修改入口唯一
```

### 第五步：启动

```bash
python api/server.py
```

访问 `http://localhost:8000` 打开前端界面。

---

## 📖 推荐阅读顺序

| 顺序 | 文件 | 重点看什么 |
|------|------|-----------|
| 1 | `agent/llm.py` | LLM 模型初始化 + PTD 包装 |
| 2 | `config/constants.py` | **235+ 魔鬼数字集中管理**（19 功能分组） |
| 3 | `prompt/prompts.yml` | XML 八段式结构化提示词 + CoT + Few-shot + Judge Prompt |
| 4 | `agent/memory_manager.py` | 滑窗+摘要+优先级+关联度过滤 |
| 5 | `agent/tool_router.py` | PTD 两阶段路由 + 启发式兜底 |
| 6 | `agent/context_engineer.py` | 时效性去重 + 来源甄别 + 2000字裁剪 |
| 7 | `agent/semantic_cache.py` | 语义缓存：Embedding + 余弦相似度命中 |
| 8 | `agent/model_router.py` | 多模型动态路由：成本/SLA/复杂度三级分类 |
| 9 | `agent/error_classifier.py` | 错误四象限分类 A/B/C/D + 幂等性检查 |
| 10 | `agent/circuit_breaker.py` | 三态时间窗口熔断器（60s/3次熔断） |
| 11 | `agent/degradation_chain.py` | 四级降级链（150s/1M 双重硬上限） |
| 12 | `agent/hallucination_guard.py` | 幻觉防护三重管道（RAG引用 + Schema + Judge） |
| 13 | `agent/output_validator.py` | 输出五维校验 + 智能重试（构造改进提示） |
| 14 | `agent/slo_monitor.py` | SLO 聚合 + 错误预算 + 监控端点 |
| 15 | `agent/observability/tracing.py` | **OTel 分布式追踪**（agent/llm/tool span） |
| 16 | `agent/actor_persistence.py` | Actor 快照持久化（全量/增量 + file/redis） |
| 17 | `agent/stream_resume.py` | 流式输出续流（中断后 chunk 回放+补推） |
| 18 | `agent/main_agent.py` | 主智能体编排 + 五层可靠性保护集成入口 |
| 19 | `agent/maker_checker.py` | 五维输出质量校验 |
| 20 | `agent/trace.py` | 全链路 Trace 可观测性（兼容旧路径） |
| 21 | `agent/feedback_handler.py` | 用户质疑检测 + 错误学习 |
| 22 | `agent/actors/session_registry_actor.py` | Actor：会话级任务注册/取消原子化 |
| 23 | `agent/actors/connection_manager_actor.py` | Actor：WS 连接字典并发安全 |
| 24 | `agent/scheduler.py` | 定时调度 + 状态持久化 |
| 25 | `agent/skill_manager.py` | Skill 自动加载管理器 |
| 26 | `api/middleware/rbac.py` | RBAC 角色权限 + Prompt Injection 防护 |
| 27 | `api/server.py` | FastAPI + 全部 API 端点 + SLO 端点 |
| 28 | `tests/eval/golden_set.json` | **26 条 golden set**（多股对比×10 / 行业×10） |
| 29 | `tests/eval/run_eval.py` | LLM 评估脚本（--mode direct/http / --ids / --concurrency） |
| 30 | `tools/zsxq_tool.py` | Playwright 知识星球搜索 + Qwen8B 分析 |
| 31 | `skills/trading-reliability/SKILL.md` | 金融可靠性规范（熔断/降级/幂等/幻觉） |
| 32 | `AGENTS.md` | 错误经验结构化沉淀（硬编码规则） |

---

## 📁 项目结构

```
moss_finance_assistant/
├── agent/                           # 智能体层
│   ├── llm.py                       # 模型初始化 + PTD 包装
│   ├── prompts.py                   # 提示词加载
│   ├── main_agent.py                # 主智能体 + 四层集成入口
│   ├── memory_manager.py            # Layer2: 滑窗+摘要+关联度过滤
│   ├── context_engineer.py          # Layer2: 时效性+来源甄别+裁剪
│   ├── tool_router.py              # Layer2: PTD 两阶段路由
│   ├── semantic_cache.py           # Layer2: 语义缓存 (Embedding+余弦)
│   ├── model_router.py             # Layer2: 多模型路由 (成本/SLA/复杂度)
│   ├── error_classifier.py         # Layer3: 错误四象限分类 + 幂等性检查
│   ├── circuit_breaker.py          # Layer3: 三态时间窗口熔断器
│   ├── degradation_chain.py        # Layer3: 四级降级链 (150s/1M)
│   ├── hallucination_guard.py      # Layer3: 幻觉防护三重管道
│   ├── output_validator.py         # Layer3: 输出五维校验 + 智能重试
│   ├── slo_monitor.py              # Layer3: SLO 聚合 + 错误预算
│   ├── trace.py                    # Layer3: Trace 可观测性 (兼容旧路径)
│   ├── feedback_handler.py         # Layer3: 质疑检测+错误学习
│   ├── maker_checker.py            # Layer3: 五维质量校验
│   ├── scheduler.py                # Layer4: 定时调度器
│   ├── state_store.py              # Layer4: 跨运行状态持久化
│   ├── actor_persistence.py        # Layer4: Actor 快照 (全量/增量 + file/redis)
│   ├── skill_manager.py            # Layer4: Skill 自动加载管理器
│   ├── stream_resume.py            # Layer4: 流式续流 (chunk 回放+TTL 清理)
│   ├── actors/                     # Actor 并发原子性保证 (独立邮箱串行)
│   │   ├── session_registry_actor.py   # 任务注册/停止/反注册
│   │   ├── connection_manager_actor.py # WS 连接字典安全访问
│   │   └── slo_monitor_actor.py        # SLO 事件原子写入
│   ├── observability/              # 可观测性
│   │   └── tracing.py              # OTel 分布式追踪 (agent/llm/tool span)
│   └── subagents/                   # 子智能体
│       ├── network_search_agent.py
│       ├── database_query_agent.py
│       └── knowledge_base_agent.py
├── api/                             # Web 接口层
│   ├── server.py                    # FastAPI 入口 + 全部 API + SLO端点
│   ├── context.py                   # ContextVar 协程隔离 (CancellationToken)
│   ├── monitor.py                   # WS 推送 + 事件埋点 + 输出内容净化
│   ├── storage.py                   # 用户/会话存储
│   └── middleware/
│       └── rbac.py                  # RBAC 四角色鉴权 + Prompt Injection 防护
├── config/
│   └── constants.py                 # 19 功能分组 235+ 命名常量 (魔鬼数字治理)
├── tools/                           # 工具函数
│   ├── tavily_tool.py               # 网络搜索
│   ├── db_tools.py                  # 数据库查询
│   ├── ragflow_tools.py             # RAGFlow 知识库
│   ├── markdown_tools.py            # Markdown 生成
│   ├── pdf_tools.py                 # PDF 转换
│   ├── upload_file_read_tool.py     # 文件读取
│   └── zsxq_tool.py                 # 知识星球搜索+抓取
├── prompt/prompts.yml               # XML 结构化提示词 + Judge Prompt
├── skills/
│   ├── finance-analysis/            # Layer4: 金融分析 Skill
│   └── trading-reliability/         # Layer4: 金融可靠性规范 (熔断/降级/幻觉)
├── tests/                           # 单元测试 + LLM 回归评估
│   ├── eval/
│   │   ├── golden_set.json          # 26 条结构化 golden set
│   │   ├── judge_prompt.py          # LLM-as-Judge System + User Prompt 模板
│   │   └── run_eval.py              # 评估主脚本 (direct/http 双模式)
│   ├── test_ptd.py                  # PTD 渐进式工具披露
│   ├── test_memory_manager.py       # 记忆管理
│   └── test_ragflow.py              # RAGFlow 集成
├── static/
│   ├── index.html                   # 前端界面 (单页应用)
│   └── js/constants.js              # 前端命名常量 (与 constants.py 保持一致)
├── data/                            # 运行时数据 (Git ignored)
│   ├── memory.db                    # 记忆 SQLite
│   ├── trace.db                     # Trace SQLite
│   ├── feedback.db                  # 反馈 SQLite
│   ├── slo_events.db                # SLO 事件 SQLite
│   └── actor_snapshots/             # Actor 持久化快照文件 (file 后端)
├── output/                          # 生成文件输出 (Git ignored)
│   └── eval/                        # run_eval 评估报告 (JSON + Markdown)
├── AGENTS.md                        # Layer3: 错误经验沉淀
├── test_cases.json                  # 测试用例
├── test_zsxq.py                     # 知识星球抓取脚本
├── requirements.txt
└── .env.example
```

---

## 🔧 技术栈

| 层级 | 技术 | 说明 |
|------|------|------|
| 并发模型 | **Actor Model** | 3 个 Actor（会话/连接/SLO）+ 邮箱队列串行，保证 next_state = f(current_state, input) |
| Agent 框架 | **deepagents** (LangChain) | 多智能体编排 |
| LLM 接入 | LangChain + OpenAI 兼容协议 | DeepSeek / 阿里云 / OpenAI，多模型按成本/SLA/复杂度路由 |
| 本地大模型 | **Ollama** (qwen3:8b) | 知识星球分析，零 API 费用 |
| 响应缓存 | **Semantic Cache** | Embedding + 余弦相似度命中（memory / redis 双后端） |
| 输出质量 | **OutputValidator + LLM-as-Judge** | 五维校验 + 智能重试；CI 26 条 golden set 回归阻断 |
| Web 框架 | FastAPI + Uvicorn | 异步 HTTP + WebSocket |
| 权限安全 | **RBAC + Prompt Injection Guard** | 四角色（admin/analyst/viewer/guest）+ 注入正则检测 |
| 可观测性 | **OpenTelemetry** | OTel SDK（console/OTLP），agent/llm/tool 三层 span |
| 状态持久化 | **Actor Snapshot** | 全量 + 增量快照，file/redis/memory 三后端 |
| 流式续流 | **StreamResume** | chunk 落地持久化 + TTL 清理，断流后补推 |
| 浏览器自动化 | **Playwright** | 知识星球内容搜索/抓取 |
| 搜索引擎 | Tavily API | AI 专用搜索 |
| 知识库 | RAGFlow | 开源 RAG 引擎 |
| 数据库 | MySQL | 业务数据查询 |
| 持久化 | AsyncSqliteSaver (LangGraph) | 对话历史跨重启保留 |
| 记忆管理 | SQLite | 滑窗+摘要+错误学习+Trace |
| 魔鬼数字治理 | **config/constants.py** | 19 功能分组，235+ 命名常量，前端 js/constants.js 同步镜像 |
| Token 优化 | PTD 渐进式工具披露 | 节省 48%~74% Schema Token |

---

## 🔌 API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| **POST** | **`/api/task`** | **通用任务（多智能体协作，返回 {thread_id} 后走 WS 推最终结果）** |
| POST | `/api/task/stop` | 停止当前任务（CancellationToken 级联取消） |
| POST | `/api/zsxq-analysis` | 盘前小作文热度分析 |
| POST | `/api/review-prediction` | 复盘预测（注入北京时间 + 自动搜索时间窗口） |
| POST | `/api/users` | 新建用户（RBAC 需要） |
| GET | `/api/users/{user_id}` | 查用户详情 |
| GET | `/api/users/{user_id}/sessions` | 用户会话列表 |
| POST | `/api/users/{user_id}/sessions` | 新建会话（自动绑定 owner） |
| GET | `/api/scheduler/status` | 调度器状态 |
| **GET** | **`/api/slo/status`** | **SLO 监控快照（可用性/错误预算/降级链/熔断器/SLO违规）** |
| **GET** | **`/api/circuit-breakers`** | **所有熔断器实时状态（CLOSED/OPEN/HALF_OPEN）** |
| GET | `/api/traces/{session_id}` | 查看 Trace 记录（全链路 Span 树） |
| GET | `/api/traces/{session_id}/latency` | 延迟统计（P50/P95/P99） |
| GET | `/api/sessions/{session_id}/history` | 获取会话历史 |
| DELETE | `/api/sessions/{session_id}` | 删除会话 |
| **DELETE** | **`/api/sessions/{session_id}/turns/{turn_index}`** | **删除单轮问答（右键消息删除，支持?role=user/assistant 粒度删除）** |
| **POST** | **`/api/sessions/{session_id}/messages/batch-delete`** | **多选批量删除消息** |
| POST | `/api/upload` | 上传文件 |
| GET | `/api/files` | 文件列表 |
| **WS** | **`/ws/{thread_id}`** | **WebSocket 实时推送（monitor_event = tool_start/tool_end/task_result/thinking/error）** |

---

## ❓ FAQ
### Q: 记忆管理的关联度过滤如何工作？

**A:** 每次用户提问时，系统提取关键词（股票代码、中文词组、金融缩写），与历史对话计算 Jaccard 重叠度 + 股票代码精确匹配加权。低于阈值（0.15）的历史对话不放入 context。例如：用户之前聊过茅台和宁德时代，现在问茅台研报，只保留茅台相关对话，过滤掉宁德时代和闲聊。

### Q: 知识星球搜索和全量抓取会冲突吗？

**A:** 不会。系统使用 `threading.Lock` 浏览器互斥锁，`search_zsxq_by_stock`（按股票搜索）和 `fetch_zsxq_group_topics`（全量抓取）不会同时运行浏览器。如果搜索正在进行，全量抓取会快速返回"浏览器正忙"而不阻塞。

### Q: 没有 RAGFlow 和 MySQL 能跑吗？

**A:** 能。主智能体会自动跳过没有的服务，只配 LLM + Tavily 就能体验核心链路。

### Q: 停止按钮如何工作？

**A:** 前端点击"停止"→ POST `/api/task/stop` → 后端 `asyncio.Task.cancel()` → `run_deep_agent` 捕获 `CancelledError` → WebSocket 推送"任务已停止" → 前端解锁允许重新输入。

### Q: 熔断器的熔断阈值可以调整吗？

**A:** 可以。默认配置在 `agent/circuit_breaker.py` 的`CircuitBreakerRegistry.DEFAULTS` 字典中，五个熔断器（deepseek/ima/qwen8b/zsxq/main_agent）各自独立。例如 deepseek 是 60 秒内 3 次失败即熔断、冷却 30 秒；如果接口相对不稳定可以把 threshold 从 3 调到 5。`CircuitBreakerRegistry.get_or_create(name, failure_threshold=X, failure_window_sec=Y, recovery_cooldown_sec=Z)` 也支持运行时覆盖。

### Q: 幻觉防护会阻止回答输出吗？

**A:** 不会。采用"默认保守 + 警示附加"策略：即使幻觉防护检测到未验证数字或缺失来源，也不会阻止输出，而是在回答末尾附加清晰的警示段落（例如"⚠️ 幻觉防护提示：以下数字未在工具结果中找到..."），由用户自行判断。置信度按未通过项数量 0.15/项衰减，低置信度可在监控端点观察。

### Q: 如何观察 SLO 错误预算消耗？

**A:** 访问 `GET /api/slo/status` 端点。`error_budget.consumed_pct` 显示按 30 天窗口计算的错误预算消耗百分比（可用性 99% → 预算 1% = 432 分钟可接受停机）。当该值超过 80% 时说明风险较高，需要人工介入。`slo_violations` 字段会列出当前违反的所有 SLO 项。

### Q: 点击「盘前小作文热度」时 Ollama 没启动会怎样？

**A:** 系统会**自动后台拉起** Ollama 服务，无需手动开 cmd：
1. 定位 ollama CLI（PATH + `%LOCALAPPDATA%\Programs\Ollama\` 兜底）
2. 后台 `ollama serve`（Windows 下 `CREATE_NO_WINDOW` 不弹黑框，脱离父进程常驻）
3. 轮询 30 秒等就绪，每 5 秒推心跳进度
4. 模型未拉取时自动 `ollama pull qwen3:8b`，每 20 秒推下载进度
5. 30 秒仍未就绪才兜底提示手动 `ollama serve`

### Q: 右键删除消息后，刷新页面还会出现吗？

**A:** 不会。删除是**三层同步持久化**的：
1. `MemoryManager.delete_turn` 删除 `memory_turns` 行 + 后段 `turn_index` 前移 1（保持连续）
2. 同步清理 `memory_key_decisions` + 过期 `memory_summaries`（下轮压缩自动重建）
3. LangGraph checkpointer 用 `RemoveMessage(id=...)` 删除对应消息（msg.id 缺失时兜底 whole-update）

刷新页面时 `get_session_history` 从 LangGraph checkpointer 读取，已删消息不会出现。即便 LangGraph 清理失败，memory 是最终数据源，下次压缩/新建会话基于 memory 渲染仍正确。

### Q: 删除消息后 turn_index 会有间隙吗？

**A:** 不会。`MemoryManager.delete_turn` 在 session 写入锁内**逐条**把 `turn_index > N` 的所有行前移 1（升序更新，主键安全），保证索引连续。前端 DOM 也同步前移 `data-turn-index`，使下次右键删除仍能命中正确索引。

### Q: 如何给 golden set 新增评估样本？

**A:** 直接编辑 `tests/eval/golden_set.json`，每条样本必填字段：`_description` / `id`（`eval_XXX` 连续）/ `category` / `input` / `expected_points`（列表）/ `forbidden_patterns`（列表，允许空）/ `risk_level`（high/medium/low）。若涉及买卖价格/长期持有建议等，追加 `must_contain: ["仅供参考", "不构成投资建议"]` 强制风险声明。保存后跑：

```bash
# 只验证 ID 连续性 + 字段
python -c "import json; d=json.load(open('tests/eval/golden_set.json',encoding='utf-8')); [print(f'OK {len(d)} samples, last id={d[-1][\"id\"]}') if all(k in s for k in ['_description','id','category','input','expected_points','forbidden_patterns','risk_level']) for s in d else exit('missing field')]"

# 跑 direct 模式 1 条确认格式兼容
python -m tests.eval.run_eval --mode direct --ids <新ID> --limit 1
```

### Q: run_eval 的 http 模式怎么确保拿到最终答案？

**A:** `run_eval.py` 的 http 模式是**三流程串联**：
1. `POST {base}/api/task` 提交 `{query, thread_id, user_id}`，服务端立即返回 `{status:started, thread_id}`
2. 同时 `websockets.connect("ws://host/ws/{thread_id}")` 建立长连接
3. 循环接收消息，直到 `event == "task_result"` 取 `data.result`，或 `event == "error"` 抛错，或 180s 超时兜底

超时/建连失败会把样本标记为 errored，不会阻塞其他样本。长问答建议 `--concurrency 1` 串行，避免并发过大导致 Agent 单任务超 180s 熔断。

### Q: 语义缓存（SemanticCache）的命中策略是什么？会泄露隐私吗？

**A:** SemanticCache 命中流程：`should_cache_query(query)` 先过滤（实时问题/个股价格/涉及具体持仓 → 直接跳过缓存 → 不写入也不读取）。通过过滤后，计算 query 向量 embedding，与缓存内 key 做余弦相似度：超过阈值（默认 `SEMANTIC_CACHE_SIMILARITY_THRESHOLD=0.92`）判定命中。命中时会先判断缓存是否陈旧（`allow_stale=False` 强制执行 TTL 检查）。未命中才调 LLM，结果异步写入缓存。整个过程不记录用户身份信息，也可切 `backend=memory` 只在进程内缓存，不落盘。

### Q: 多模型路由（ModelRouter）的 "cost_aware" 和 "sla_aware" 有何区别？

**A:** 两者都在"按复杂度（simple/medium/complex）选模型"的基础上做进一步决策：
- **cost_aware**：跟踪全局预算消耗 `budget_accumulator`，当月预算逼近上限时把 complex → cheap（牺牲延迟换省钱）
- **sla_aware**：跟踪近 5 分钟各模型 P95 延迟，命中黑名单（P95 > `MODEL_ROUTER_SLA_P95_MS`）自动 fallback 到备用模型，保证用户等待可预测

路由决策对象 `RouteDecision` 包含 `model/reason/complexity/estimated_cost_usd`，在 `/api/traces/{sid}` 的 span 标签中可审计。

### Q: OTel 分布式追踪如何接入真实后端（Jaeger/Grafana）？

**A:** 只需改 `.env` 两项：
```env
OTEL_EXPORTER_TYPE=otlp
OTEL_OTLP_ENDPOINT=http://<jaeger-or-otel-collector-host>:4317
```
默认 `OTEL_TRACE_SAMPLE_RATIO=1.0` 全量采样，生产建议 0.05~0.1。Span 三层结构：`agent.run`（根 span，含 request_id/user_id/category）→ `llm.chat:<model>`（含 prompt_tokens/estimated_cost）→ `tool.call:<name>`（含工具调用耗时）。直接在 Jaeger UI 搜 `request_id` 即可看到全链路瀑布。

### Q: `config/constants.py` 与 `static/js/constants.js` 是同步的吗？

**A:** 两者是**手动镜像**但语义一一对应：Python 版 235+ 常量（后端 19 功能分组），JS 版 24 常量（前端 UI 相关：删除模式枚举、批量删除按钮文案、动画时长等）。修改规则：后端阈值只改 Python `config/constants.py`，前端 UI 呈现只改 `static/js/constants.js`，避免耦合产生漂移。若需跨端一致（例如 WS 心跳间隔），统一在 `config/constants.py` 定义，前端初始化时通过 `GET /` 返回 HTML 中注入 `<script> window.DEFAULT_TIMEOUT = {{ TIMEOUT }} </script>`。

### Q: Actor 持久化快照崩溃后如何恢复？

**A:** `ActorPersistence` 启动时执行 `recover(actor_id)`：1. 列出该 actor 的所有历史快照版本；2. 取最新 is_full=True 的全量快照反序列化；3. 依次回放其后所有增量快照（diff apply）；4. 返回 actor 状态 + 最近成功版本号。如果 `file` 后端（`data/actor_snapshots/`）损坏但同时启用了 redis 双写，可切 `backend=redis` 再恢复；双后端都损坏时 Actor 从空状态启动（等价首次运行），不阻塞主流程。

---

## 📄 License

MIT License
