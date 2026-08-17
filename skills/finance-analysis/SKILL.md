# SKILL.md — 金融新闻热点辅助分析技能

> Agent 每次运行时读取此 skill，把猜测替换为确定的规则。

## 适用场景
本技能适用于金融新闻资讯 Agent，覆盖以下高频任务：
1. 个股新闻速览（利空/利多判定）
2. 股票估值分析（PE/PB/ROE对比）
3. 护城河评估（五维度框架）
4. 散户数据查询（股东人数/户均持股）
5. 盘前小作文热度分析

## 构建步骤
1. 加载 `.env` 配置（LLM/Tavily/RAGFlow/MySQL/ZSXQ）
2. 初始化记忆管理（memory.db）+ PTD（tool_router）+ Context Engineer
3. 初始化 Trace Logger + Feedback Handler + Maker-Checker
4. 启动 Scheduler（9:13 盘前小作文 / 9:15 盘前新闻）
5. 启动 FastAPI 服务

## 我们不这么做（因为那次事故）

### 事故1：未标注信息来源导致误导
- **事故**：引用股吧帖子"茅台要降价30%"，未标注来源可靠性，用户恐慌
- **规则**：所有信息必须标注来源类型（可靠/待验证），不可靠信息注明"信息来源不可靠"
- **代码位置**：`agent/context_engineer.py` → `assess_source_reliability()`

### 事故2：过期数据冒充实时数据
- **事故**：用3天前的股价回答"今天茅台多少钱"，用户据此下单
- **规则**：实时性问题必须调用网络搜索获取最新数据；相似内容去重后只保留最新2条
- **代码位置**：`agent/context_engineer.py` → `deduplicate_news()`

### 事故3：Context 过长导致幻觉
- **事故**：塞入8000字检索结果，模型注意力分散，把A公司的数据说成B公司
- **规则**：Context 超过2000字时按问题关联度裁剪；可靠来源优先保留
- **代码位置**：`agent/context_engineer.py` → `trim_context()`

### 事故4：用户质疑后未修正
- **事故**：用户说"你的数据不对，茅台PE是30不是50"，Agent 回复"好的"但下次还犯
- **规则**：检测到质疑 → 提取纠正信息 → 重新搜索 → 学习错误模式 → 下次规避
- **代码位置**：`agent/feedback_handler.py` → `detect_challenge()` / `learn_error()`

### 事故5：未检查输出质量就发送
- **事故**：Agent 输出"建议买入"但无风险声明，且数字与检索结果不符
- **规则**：所有输出经过 Maker-Checker 校验（数据一致性/完整性/风险声明/幻觉检测）后才发送
- **代码位置**：`agent/maker_checker.py` → `check_output()`

## 量化工具使用约定
- `internet_search`：topic 参数必须正确（finance=股价行情，news=新闻，general=其他）
- `search_knowledge_base`：单次搜索1-2个关键问题，不重复搜索同一问题
- `execute_sql_query`：先 `list_sql_tables` 确认表名，再查询
- Tavily 重试3次（2s/4s/8s指数退避），RAGFlow 最多调用3次

## 盘前自动化心跳
- 工作日 9:13：触发"盘前小作文热度"按钮 → 调用 zsxq 抓取 + 分析
- 工作日 9:15：触发"盘前新闻"按钮 → 调用主 Agent 搜索盘前新闻
- 周末/节假日跳过（weekday_only=true）
- Scheduler 30秒轮询，防当日重复触发（last_run 检查）

## Maker-Checker 校验清单
- [ ] 数据一致性：输出中的股票代码/数字与工具检索结果一致
- [ ] 完整性：用户查询中的每只股票都被分析
- [ ] 风险声明：涉及买卖建议时附带"不构成投资建议"声明
- [ ] 来源标注：引用新闻/数据时标注来源
- [ ] 幻觉检测：输出中的百分比/价格/财务指标在检索结果中可查
