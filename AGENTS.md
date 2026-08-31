# AGENTS.md — MOSS Finance Assistant 项目规范

> 本文件的每一行都对应一个曾经的失败模式——它是错误经验的结构化沉淀。
> Agent 每次运行时读取本文件，而不是从零猜测项目习惯。

## 项目概述
金融新闻多 Agent 查询系统：DeepSeek 联网搜索 + IMA 知识库 + MySQL 业务数据，
面向散户用户提供股票新闻、估值分析、护城河评估、散户数据等投研服务。

## 绝对禁止（违反会导致严重错误）

### 1. 投资建议风险声明
- **禁止**：在涉及买卖价格、目标价、评级建议时，不附带风险声明
- **规则**：必须输出 `⚠️ 以上信息来自互联网公开资料，仅供参考，不构成投资建议。投资有风险，入市需谨慎，盈亏自负。`
- **失败案例**：曾经直接给出"建议买入茅台"而未声明风险，用户误以为是投资建议

### 2. 信息来源标注
- **禁止**：将股吧/论坛/自媒体信息当作可靠信息源，不加甄别地引用
- **规则**：来自交易所/监管机构/权威媒体的标注"可靠"，来自股吧/论坛/自媒体的标注"信息来源可靠性待验证"
- **失败案例**：曾经引用股吧帖子中的"内部消息"导致用户做出错误决策

### 3. 信息时效性
- **禁止**：使用过期信息回答实时性问题（如"今天的股价"用3天前的数据）
- **规则**：相似讨论内容，以最新日期的两项进行对比；观点不一致时搜索官方公告甄别
- **失败案例**：曾经用上周的新闻回答"今日盘前"问题

### 4. 执行顺序
- **禁止**：在获取信息之前调用文件生成工具（generate_markdown）
- **规则**：先搜索/查询 → 拿到完整信息 → 再生成文件
- **失败案例**：曾经用"等待子任务完成"占位符生成了空内容文件

### 5. 数据准确性
- **禁止**：主观臆测或瞎猜股票代码、财务数据、股价
- **规则**：所有涉及股票的数据必须来自检索结果，检索不到就明确说"未找到相关数据"
- **失败案例**：曾经把"600519"说成"000519"，用户据此操作差点出错

## 编码规范
- 所有导入使用相对包路径（`from agent.xxx import`），不硬编码项目名
- `.gitignore` 忽略 `docs/` 下所有文件，唯一白名单是 `docs/adr/`（架构决策记录入库）；`benchmarks/results/` 运行产物不入库
- API 响应包含 `X-Powered-By: MOSS-Finance-Assistant` 头
- 测试用例存储在 `test_cases.json`，每条含 `_description` 字段

## Agent 架构约定
- 主 Agent 协调三个子 Agent（网络搜索/数据库查询/RAGFlow知识库）
- 记忆管理：滑窗(10轮) + 摘要压缩(3段) + 优先级排序
- 渐进式工具披露(PTD)：两阶段路由减少 Token 消耗；带自适应门控——工具池全量 Schema 开销 ≤ 路由菜单开销时自动旁路（小池负优化，基准实测结论，见 docs/adr/adr-0003）
- Prompt 注入双层防护：统一入口 `sanitize_user_input_async`（正则快路 + 本地 LLM 分类器慢路 + JSONL 审计），请求流水线 enterprise_hooks 已接入；LLM 故障默认 fail-open（可用性优先）
- Context Engineering：2000字阈值精简裁剪，按问题关联度筛选

## 文件结构约定
- 代码真源唯一：真实实现只保留一份，旧路径为 re-export 垫片（文件头有 `[兼容垫片]` 标注，由 shared/compat_bootstrap.py 做运行时别名）；改代码前先查 README「项目结构（真源地图）」
- 全局常量平铺定义唯一编辑 `config/constants.py`；`shared/config/constants.py` 只做 re-export + 分组视图（TIMEOUTS/SLO_TARGETS），平铺新常量自动可见，新增分组键需同步该文件
- 服务统一入口 `python main.py server`（uvicorn 目标 `interfaces.api.server:app`，Docker 用 `docker compose up -d --build`）
- `agent/` — 兼容垫片层 + 真源子模块（subagents/、request_context、skill_manager）
- `agent/subagents/` — 子 Agent 定义
- `tools/` — LangChain 工具
- `api/` — 流式协议/总线/WS 推送/请求上下文真源 + middleware；服务端入口在 `interfaces/api/server.py`
- `prompt/` — Prompt 模板
- `data/` — 运行时数据（SQLite DB等，已被 gitignore）
- `output/` — 生成的文件输出
- `skills/` — 可复用技能编码（每个 skill 一个子目录，含 SKILL.md）
- `tests/` — 单元测试统一目录（`python tests/test_xxx.py` 运行）
- `benchmarks/` — 量化基准脚本（PTD token / 语义缓存阈值 / Judge 一致性），`python benchmarks/bench_xxx.py` 独立运行；`benchmarks/k6/` — HTTP 压测（k6 冒烟+阶梯负载，SLO 阈值断言，`k6 run benchmarks/k6/xxx.js`）；结果 JSON 写 `benchmarks/results/`（gitignored），基准得出的行为改动须同步 ADR
- `docs/adr/` — 架构决策记录（唯一入库文档）：高影响/不可逆决策先写 ADR 再改代码，编号递增不改旧决策
- `tools/zsxq_analysis_runner.py` — 知识星球抓取 + Ollama 金融分析的独立可执行工具脚本（与 zsxq_tool.py 同级管理），由 server.py 以子进程方式调用

## 金融业务规则
- 个股新闻速览：每只个股汇总≤200字，必须判定利空/利多
- 估值分析：必须列出 PE/PB/ROE/营收增速，与行业均值对比
- 护城河分析：从品牌/技术/成本/网络效应/转换成本五维度评估
- 散户数据：来自中登公司或交易所公开数据，不使用非公开数据
