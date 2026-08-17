---
name: trading-reliability
description: 金融量化交易系统可靠性工程专家。用于指导企业级量化交易系统的错误处理、故障恢复、熔断降级、灾备切换与SLO驱动的可靠性治理。
allowed-tools: Read, Write, Bash, MCP
trigger-keywords:
  # 交易场景
  - 交易
  - 下单
  - 撤单
  - 撮合
  - 券商
  - 交易所
  - 实盘
  - 回测
  - 策略
  - 风控
  - 持仓
  - 订单
  - 幂等
  - trading
  - order
  - strategy
  - broker
  # 故障与可靠性场景
  - 故障
  - 报错
  - 错误处理
  - 熔断
  - 降级
  - 重试
  - 灾备
  - 容灾
  - SLO
  - SLA
  - 可靠性
  - 可观测性
  - 监控
  - 告警
  - 事故
  - 复盘
  - 混沌工程
  - failure
  - retry
  - circuit
  - breaker
  - degrade
  - observability
  - incident
  - postmortem
  # 代码生成/排查场景
  - 生成代码
  - 写代码
  - 代码审查
  - 排查问题
  - 问题排查
  - debug
  - 调试
  - 异常
  - exception
  - 超时
  - timeout
  - 网络抖动
  - 丢包
  - 数据校验
  - 数据一致性
  # 幻觉防护场景
  - 幻觉
  - hallucination
  - 引用追踪
  - 来源标注
  - LLM-as-Judge
  - JSON Schema
  - 输出验证
  - citation
  # 降级链场景
  - 降级链
  - degradation chain
  - 兜底
  - 静态模板
  - Token 预算
  - 硬上限
---

# 金融量化交易系统可靠性工程规范
## Trading System Reliability Engineering Specification

---

## 0. 金融量化系统的特殊挑战

金融量化交易系统与通用分布式系统存在本质差异，错误处理策略必须考虑以下特殊性：

- **资金风险**：每次错误决策都可能造成真实的财务损失，错误处理的容错边界极窄
- **低延迟约束**：高频交易中，毫秒级的阻塞可能导致策略失效，恢复时间目标（RTO）需控制在秒级甚至毫秒级
- **监管合规**：交易行为、错误记录、恢复操作必须可审计、可追溯
- **市场连续性**：金融市场7×24小时运行（部分品种），系统升级和维护窗口极其有限
- **数据确定性**：行情数据、订单状态必须精确无误，过期缓存可能比错误本身更具破坏性

> **核心原则**：在金融量化系统中，**"宁可拒绝，不可欺骗"** —— 返回明确的错误比返回过期的、可能误导决策的数据安全得多。

---

## 1. 故障分类体系（Failure Taxonomy）

金融量化系统的故障必须按以下维度精确分类，不同类别采取截然不同的处理策略：

### 1.1 按严重程度（Severity）
| 级别 | 定义 | 示例 | 响应动作 |
|------|------|------|----------|
| **CRITICAL** | 可能导致资金损失或监管违规 | 订单状态不明、风控失效 | 立即暂停交易，人工介入 |
| **HIGH** | 影响核心交易功能 | 行情断流、订单网关不可达 | 触发降级或灾备切换 |
| **MEDIUM** | 影响辅助功能 | 因子计算延迟、日志写入失败 | 记录并告警，继续运行 |
| **LOW** | 不影响交易 | 非关键指标计算异常 | 记录日志，静默处理 |

### 1.2 按故障类型（Category）—— 错误四象限分类法

**金融量化系统严禁使用"一刀切"的重试策略**。必须按"是否可重试 × 是否为硬错误"将错误划分为四个象限，分别采取不同处理动作。判定的核心原则：**"错误是否会随时间自行消失"**。

#### 象限 A：**可重试的硬错误（Retryable Hard Errors）**
**特征**：错误由瞬态外部环境导致，**重试大概率会成功**，不修改请求本身即可解决。

| 错误类型 | 示例 | HTTP/业务码 | 处理动作 |
|----------|------|------------|----------|
| 网络瞬态错误 | TCP RST、Connection reset、DNS 抖动 | 502/503/504 | 指数退避 + 抖动重试（最多 5 次） |
| 读操作超时 | 查询行情、查询持仓响应慢 | 408/504 | 可重试（读操作幂等） |
| 流控限制（未触发熔断） | 429 未达熔断阈值 | 429 + Retry-After | 按 Retry-After 头延迟重试 |
| 行情源临时不可用 | 主行情源心跳超时 | 503 | 切备用源 + 主源后台探测 |
| WebSocket 闪断 | 盘中行情断流 | 连接 1006 | 1s→2s→4s→8s→16s→30s 退避重连 |

#### 象限 B：**不应重试的软错误（Non-Retryable Soft Errors）**
**特征**：错误由请求本身的语义问题或业务状态导致，**重试同样会失败**。必须修改请求或人工介入。

| 错误类型 | 示例 | HTTP/业务码 | 处理动作 |
|----------|------|------------|----------|
| 参数校验失败 | 订单价格超涨跌停、数量非整手 | 400 / 业务码 INVALID_PARAM | 永不重试，直接拒绝 |
| 业务规则拒绝 | 账户余额不足、持仓超限、信用不足 | 400 / 业务码 INSUFFICIENT_xxx | 永不重试，告警并剔除信号 |
| 撤单超限 | 单合约撤单达 500 次 | 业务码 ORDER_CANCEL_LIMIT | 熔断该合约，次日恢复 |
| 涨跌停限制 | 触及涨停板无法买入 | 业务码 PRICE_LIMIT | 永不重试，记录后跳过 |
| 订单不存在 | 撤单时订单已成交 | 业务码 ORDER_NOT_FOUND | 永不重试，更新本地状态 |
| 合约停牌 | 标的暂停交易 | 业务码 SYMBOL_SUSPENDED | 永不重试，标记不可交易 |

#### 象限 C：**不可重试错误（Permanent Errors）**
**特征**：错误由系统级、进程级或数据级损坏导致，**任何重试都不会成功**，必须触发恢复机制。

| 错误类型 | 示例 | 处理动作 |
|----------|------|----------|
| 写操作超时 | 下单后未收到 ACK | **严禁重试提交**，走状态反查流程 |
| 状态数据库故障 | 持仓库不可读 | Fail-Closed，立即停止交易 |
| 内存溢出（OOM） | 进程被 kill | 进程自愈 + WAL 重放恢复 |
| 磁盘写满 | 日志无法落盘 | 紧急告警 + 日志轮转 |
| 静默数据损坏 | 行情时间戳错乱 | 双源交叉验证发现后告警 + 熔断 |

#### 象限 D：**配置类错误（Configuration Errors）—— 绝对不可重试**
**特征**：错误由配置、凭证、环境问题导致。**重试不仅无效，还会触发安全风控（如多次无效请求被IP封禁）**。必须先修复配置再恢复。

| 错误类型 | 示例 | HTTP/业务码 | 处理动作 |
|----------|------|------------|----------|
| **API Key 过期** | 券商 Token 超期 | 401 / 业务码 TOKEN_EXPIRED | **永不重试**，立即告警，人工刷新凭证 |
| **API Key 无效** | 错误的 AppSecret | 401 / 业务码 INVALID_KEY | **永不重试**，告警 + 暂停该券商通道 |
| **签名错误** | HMAC 签名校验失败 | 401 / 业务码 SIGN_INVALID | **永不重试**，检查时钟同步 + 密钥配置 |
| **权限不足** | 接口未授权 | 403 / 业务码 NO_PERMISSION | **永不重试**，申请权限后人工恢复 |
| **IP 白名单未通过** | 来源 IP 未在白名单 | 403 / 业务码 IP_FORBIDDEN | **永不重试**，运维添加 IP 白名单 |
| **时钟漂移** | 本地时间与交易所偏差 > 1s | 业务码 TIMESTAMP_SKEW | **永不重试**，NTP 校时后恢复 |
| **环境变量缺失** | .env 中关键配置缺失 | 启动失败 | **永不重试**，启动前必须校验 |

> **核心铁律**：**401/403 类错误 = 配置问题，不是网络问题**。出现此类错误时，重试只会触发券商的风控告警（如"5 分钟内 10 次无效凭证请求 → IP 封禁 24 小时"）。正确做法是立即告警 + 停止该通道，由人工/自动化流程修复配置后恢复。

#### 象限判定流程图（决策树）

```
捕获异常 →
├─ HTTP 401/403 或业务码 TOKEN_EXPIRED/INVALID_KEY/SIGN_INVALID/IP_FORBIDDEN？
│  └─ YES → 象限 D（配置错误）：告警 + 停止通道，**绝对不重试**
├─ HTTP 400 + 业务码 INVALID_PARAM/INSUFFICIENT_xxx/PRICE_LIMIT？
│  └─ YES → 象限 B（软错误）：永不重试，记录并剔除信号
├─ 写操作（下单/撤单/转账）超时？
│  └─ YES → 象限 C（不可重试）：走状态反查流程
├─ 系统级异常（OOM/磁盘满/状态库故障）？
│  └─ YES → 象限 C（不可重试）：进程自愈 + WAL 恢复
├─ 读操作超时 / 网络瞬态 / 429 / 503？
│  └─ YES → 象限 A（可重试硬错误）：指数退避 + 抖动重试
└─ 不确定？→ 默认按象限 C 处理（保守策略，宁停不乱）
```

### 1.3 故障的"冰山模型"
> **危险的不是报错的那些——报错只花一次算力；危险的是那些能跑出一个看起来合理、但含义和你写的不是一回事的结果**。

金融量化系统中，**静默数据错误（Silent Data Corruption）** 比显式异常更危险。例如：
- 行情数据时间戳错乱导致信号计算偏移
- 复权因子计算错误导致回测与实盘不一致
- 因子计算中的非滚动均值被误用为滚动均值

---

## 2. Fail-Closed vs Fail-Open 策略矩阵

金融量化系统中，**不同模块必须采用不同的故障处理策略**，一刀切的"全部停止"或"全部继续"都是危险的。

### 2.1 必须 Fail-Closed（停止交易）的模块
任何**直接保护资金安全**的模块，故障时必须停止：
- **风控引擎（Risk Guard）** ：持仓限额、信用检查失败 → 拒绝所有新订单
- **状态数据库（State DB）** ：无法读取当前持仓/资金状态 → 停止交易
- **止盈止损管理（Stop Order Management）** ：无法确认止损状态 → 停止交易
- **撤单管理器（Cooldown/Order Manager）** ：撤单计数状态不明 → 停止交易

### 2.2 可以 Fail-Open（降级继续）的模块
**辅助信号层**故障时，可以降级但继续交易：
- **情绪因子（Sentiment）** ：不可用 → 跳过该信号层
- **基本面因子（Fundamentals）** ：不可用 → 使用纯技术信号
- **内幕/新闻信号（Insider）** ：不可用 → 继续交易，标记为降级模式

### 2.3 分级降级（Tiered Degradation）—— 推荐模式

**不要实现二元的 Fail-Open vs Fail-Closed，而是构建明确的分级降级阶梯**：

| 层级 | 状态 | 验证范围 | 风险敞口 | 触发条件 |
|------|------|----------|----------|----------|
| **Tier 1** | 正常运行 | 全量验证：盘前风控、实时持仓、对手方信用、监管约束 | 零 | 默认状态 |
| **Tier 2** | 缓存验证 | 回退到有界陈旧的缓存快照 | 已知、可量化的有限风险 | 实时检查延迟 > 阈值 |
| **Tier 3** | 名义限额 | 仅执行硬性名义限额（如单标的$1000万上限） | 可接受的最大风险敞口 | 缓存数据不可用 |
| **Tier 4** | 人工覆写 | 需人工授权，订单标记"降级模式" | 需事后对账 | 自动决策不可用 |

每个层级必须明确记录：
- 哪些假设被违反
- 接受了什么风险
- 有哪些补偿控制措施

---

## 3. 重试策略（Retry Policy）—— 金融场景特化

### 3.1 核心铁律：区分"读"与"写"

| 操作类型 | 重试策略 | 原因 |
|----------|----------|------|
| **读操作**（查询行情、查询余额、查询持仓） | ✅ 可自动重试，指数退避 | 重复读取不改变状态 |
| **写操作**（下单、撤单、转账） | ❌ **禁止客户端自动重试** | 重复提交=重复订单/重复扣款 |
| **未知状态**（订单提交后超时） | ❌ 不重试，走**状态反查**流程 | 订单可能已成交或已拒绝 |

### 3.2 指数退避 + 抖动（Exponential Backoff with Jitter）

对于**允许重试**的场景（网络抖动、行情重连）：
初始间隔: 1s
退避因子: 2
最大间隔: 30s
最大重试次数: 5次
抖动: ±20% 随机偏移

**WebSocket 重连场景**：采用 `1s → 2s → 4s → 8s → 16s → 30s（上限）` 的指数退避策略，避免密集重连触发交易所限流。

### 3.3 订单"未知态"处理流程（Order State Reconciliation）

当订单提交后发生超时，**严禁**直接重试提交。必须执行以下流程：
1.订单提交请求发出，等待响应
2.响应超时（ReadTimeout）
3.❌ 不重试提交
4.✅ 调用订单状态查询接口（Query Order）
5.判断订单状态：
    已成交（Filled） → 更新本地状态为成功
    已拒绝（Rejected） → 更新本地状态为失败，记录原因
    已撤销（Cancelled） → 更新本地状态为取消
    不存在（Not Found） → 确认订单未进入撮合，可安全重试提交
    状态未知（Pending） → 加入待确认队列，定时轮询
6.所有状态变更记录审计日志


### 3.4 交易所特定流控处理

- **撤单次数限制**：中国期货交易所规定"客户单日单合约撤单次数超过500次将受限制"。策略必须在达到**480次**时触发预警并停止该合约交易。
- **登录频率限制**：部分券商限制每分钟登录次数（如5次/分钟），需使用**连接池**技术复用连接。
- **查询接口降频**：对资产、持仓查询接口做降频处理，限制每秒查询次数。

---

## 4. 熔断器（Circuit Breaker）—— 金融级自适应实现

### 4.1 标准熔断器三态

| 状态 | 行为 | 触发条件 |
|------|------|----------|
| **CLOSED（闭合）** | 正常处理所有请求 | 默认状态 |
| **OPEN（打开）** | 拒绝所有请求，快速失败 | 错误率超过阈值（如5% / 1分钟窗口） |
| **HALF_OPEN（半开）** | 允许少量探测请求 | 冷却时间到达（如30秒） |

### 4.2 时间窗口三态熔断器（项目运行时实现）

> **案例对齐**：用户要求"错误率超过 60 秒 3 次即熔断"。

项目运行时实现位于 [agent/circuit_breaker.py](file:///d:/code/moss-finance-assistant/agent/circuit_breaker.py)，采用**时间窗口内失败次数**触发（而非滑动窗口错误率），更贴合"60秒3次"案例：

```
三态状态机：
  CLOSED  ──(60s 内 3 次失败)──▶  OPEN
  OPEN     ──(冷却 30s 后)──▶      HALF_OPEN
  HALF_OPEN ──(探测成功 2 次)──▶   CLOSED
  HALF_OPEN ──(探测失败 1 次)──▶   OPEN
```

预置默认配置（CircuitBreakerRegistry.DEFAULTS）：

| 被保护对象 | failure_threshold | failure_window_sec | recovery_cooldown_sec |
|------------|-------------------|--------------------|-----------------------|
| deepseek   | 3                 | 60                 | 30                    |
| ima        | 3                 | 60                 | 30                    |
| qwen8b     | 5                 | 60                 | 15                    |
| zsxq       | 3                 | 120                | 60                    |
| main_agent | 3                 | 300                | 60                    |

**关键设计**：
- `_gc_failure_window()` 清理超出时间窗口的旧失败记录，避免历史失败永久累积
- HALF_OPEN 状态下连续 `half_open_success_needed` 次成功才恢复 CLOSED，防抖动
- `snapshot()` 接口供 SLO 监控端点 `/api/circuit-breakers` 暴露实时状态

### 4.3 自适应阈值（Adaptive Thresholds）

金融场景下，固定阈值可能过于死板。推荐使用**滑动窗口**计算动态错误率，具体实现请参考 `scripts/circuit_breaker.py`（基于错误率的通用版本）。

项目运行时版本（`agent/circuit_breaker.py`）基于时间窗口失败次数，两者可互补：
- `scripts/` 版本适用于交易网关、行情源等需要错误率感知的场景
- `agent/` 版本适用于 LLM API、知识库等需要快速熔断的场景

### 4.4 熔断触发后的动作

| 熔断级别 | 动作 | 恢复条件 |
|----------|------|----------|
| **行情源熔断** | 切换到备用行情源（如TickDB备源） | 主源健康探测成功 |
| **订单网关熔断** | 暂停该网关的所有订单，切换到备用券商 | 半开状态探测成功 |
| **策略级熔断** | 暂停该策略的信号生成和下单 | 人工确认或自动恢复 |
| **LLM 模型熔断** | 触发四级降级链，切换到下一层模型 | 半开探测成功 |
| **main_agent 熔断** | 拒绝新请求，返回静态兜底提示 | 冷却后半开探测 |

---

## 5. 幂等性保障（Idempotency）—— 金融系统生命线

### 5.1 强制要求
**所有订单提交、资金划转等写操作，必须携带全局唯一的幂等键（Idempotent Key）** 。
幂等键设计：
格式：{client_id}-{date}-{sequence} 或 UUID v7
有效期：至少 T+1 日（跨交易日对账）
存储：数据库唯一索引 + Redis 缓存（热数据）

### 5.2 订单幂等检查流程
1.接收订单请求，提取幂等键
2.查询本地订单表（唯一索引）：
    存在且状态为 FILLED/REJECTED/CANCELLED → 返回缓存结果
    存在且状态为 PENDING → 返回"处理中"
    不存在 → 创建订单记录（状态=PENDING），提交交易所
3.交易所返回后更新订单状态
4.任何异常不删除幂等记录

### 5.3 幂等管道的故障隔离

专业交易系统应采用**幂等管道（Idempotent Pipelines）** 设计，确保即使消息重复消费、请求重试，最终状态保持一致。

---

## 6. 可观测性（Observability）—— 金融级标准

### 6.1 必须采集的指标（SLI）

| 指标类型 | 具体指标 | 金融场景特殊说明 |
|----------|----------|------------------|
| **可用性** | 订单提交成功率 / 总提交数 | HTTP 200 + 业务成功才算"好事件" |
| **延迟** | 订单端到端延迟 P50/P95/P99 | 必须用百分位数，不能用平均值 |
| **错误率** | 按错误类型分类的错误率 | 区分"业务拒绝"（正常）和"系统错误"（异常） |
| **数据质量** | 行情 Tick 更新频率、延迟 | 延迟突增触发自动降级 |
| **滑点** | 实际成交价与信号价的偏差 | 偏差超阈值触发告警 |

### 6.2 SLO 与错误预算（Error Budget）
金融量化系统必须基于 SLO 驱动可靠性决策：
错误预算 = 1 - SLO
例如：可用性 SLO = 99.9% → 错误预算 = 0.1%
30天窗口 → 约 43.2 分钟的可接受停机时间

**错误预算是 SRE 体系的精髓**，它将可靠性问题从技术问题转化为风险管理框架，用于决定团队的工作优先级。

### 6.3 日志规范

**每条错误日志必须包含**：
- `trace_id`：全链路追踪ID
- `order_id`：订单ID（如适用）
- `symbol`：交易标的
- `error_type`：错误类型（网络/超时/业务拒绝/系统）
- `error_code`：交易所/券商返回的错误码
- `stack_trace`：完整堆栈（仅 DEBUG 级别）
- `recovery_action`：采取的恢复动作

**禁止**：`if err != nil { return err }` 以外的静默吞错。

### 6.4 项目运行时 SLO 监控实现

项目运行时 SLO 监控位于 [agent/slo_monitor.py](file:///d:/code/moss-finance-assistant/agent/slo_monitor.py)，聚合以下指标：

**SLO 目标定义（SLO_TARGETS）**：
| SLO 指标 | 目标值 | 说明 |
|----------|--------|------|
| availability | ≥ 99.0% | 成功请求数 / 总请求数 |
| latency_p95_sec | ≤ 30s | P95 延迟（单任务硬上限 240s） |
| hallucination_pass_rate | ≥ 95% | 幻觉防护通过率 |
| max_task_sec | 240s | 单任务执行时间硬上限 |
| max_tokens | 1,000,000 | 单任务 Token 硬上限 |

**错误预算计算**：
- 30 天滚动窗口（ERROR_BUDGET_WINDOW_SEC）
- 错误预算 = 1 - SLO（可用性 99% → 预算 1% → 约 432 分钟可接受停机）
- `consumed_pct` 字段实时暴露预算消耗百分比

**监控端点**：
- `GET /api/slo/status` — 完整 SLO 快照（指标 + 错误预算 + 降级链分布 + 熔断器状态 + SLO 违规列表）
- `GET /api/circuit-breakers` — 所有熔断器实时状态

**SLOEvent 记录字段**：session_id / timestamp / success / latency_sec / token_count / final_tier / hit_hard_limit / hallucination_passed / hallucination_confidence / error_quadrant / circuit_open

---

## 7. 灾难恢复（Disaster Recovery）—— 企业级标准

### 7.1 RTO 与 RPO 目标

| 系统级别 | RTO（恢复时间目标） | RPO（恢复点目标） | 架构方案 |
|----------|---------------------|-------------------|----------|
| **核心撮合/风控** | < 1秒 | 0（零数据丢失） | 热备 + 日志同步 |
| **订单管理/策略** | < 30秒 | < 1秒 | 温备 + 定期快照 |
| **行情/数据服务** | < 5秒 | < 100ms | 双活/多源 |
| **回测/分析** | < 5分钟 | < 1小时 | 冷备 + 定期备份 |

### 7.2 灾备切换触发条件
1、主→备切换触发条件（满足任一）：
2、性能劣化 > 15%（延迟/吞吐量）
3、信号生成失败（连续 N 次）
4、连续 3 次健康检查失败
5、进程异常终止（kill -9）
6、人工触发切换


### 7.3 状态恢复机制

- **WAL（Write-Ahead Log）重放**：进程异常终止后，新实例加载 WAL 恢复状态，目标 RTO < 3秒
- **热备日志同步**：热备节点重放缺失的日志条目，以连续序列号接管输出
- **订阅状态恢复**：WebSocket 重连后自动恢复订阅列表，实现行情无感知接续

### 7.4 灾备演练

**金融级要求**：定期执行"战争游戏"（War Games）级别的全规模负载测试，模拟极端市场条件下的系统表现。测试应包括：
- 主数据中心完全失效
- 网络分区（脑裂）场景
- 流量突增 10 倍以上
- 关键依赖服务全部不可用

---

## 8. 混沌工程（Chaos Engineering）—— 主动验证

### 8.1 故障注入清单

| 故障类型 | 注入方式 | 验证目标 |
|----------|----------|----------|
| 网络延迟 | 注入 100ms-5s 延迟 | 超时处理、重试策略 |
| 网络丢包 | 注入 5%-30% 丢包 | 重传、连接重建 |
| 进程终止 | kill -9 策略进程 | WAL 恢复、状态重建 |
| 磁盘满 | 填充磁盘至 95% | 日志轮转、告警 |
| 依赖超时 | 模拟上游 API 超时 | 熔断、降级 |
| 数据损坏 | 损坏行情数据包 | 数据校验、异常检测 |

### 8.2 验证通过标准

- 所有故障场景下，系统在约定 RTO 内自动恢复
- 无订单丢失或重复（幂等性验证）
- 审计日志完整记录所有异常和恢复动作
- 告警系统正确触发

---

## 9. 代码示例规范

### ❌ 严禁模式

```python
# 反模式1：吞掉异常
try:
    order = broker.submit_order(symbol, quantity, price)
except Exception:
    pass  # 订单可能已提交，但本地完全不知情 → 灾难

# 反模式2：写操作自动重试
for i in range(3):
    try:
        order = broker.submit_order(symbol, quantity, price)
        break
    except TimeoutError:
        continue  # 可能重复下单 → 灾难

# 反模式3：静默返回过期数据
try:
    price = get_realtime_price(symbol)
except TimeoutError:
    price = cached_price  # 返回过期数据 → AI/策略基于错误信号决策
```

## 10. 事故响应与复盘（Incident Response）

### 10.1 事故分级
| 级别 | 定义 | 响应时间 | 上报范围 |
|------|------|----------|----------|
| P0 | 资金损失或系统完全不可用 | 立即响应（< 5分钟） | CTO、风控、合规 |
| P1 | 核心功能严重受损 | < 15分钟 | 技术负责人、产品 |
| P2 | 非核心功能受损 | < 1小时 | 技术团队 |
| P3 | 轻微问题，不影响交易 | < 24小时 | 记录即可 |

### 10.2 无指责复盘（Blame-Free Post-Mortem）
每次事故后必须完成：
1. **时间线**：精确到秒的事件时间线
2. **根因分析**：5-Why 分析法
3. **影响范围**：受影响的订单、资金、用户
4. **改进措施**：具体、可验证的工程改进
5. **跟踪**：改进措施的责任人和完成日期

---

## 11. 四级降级链（Degradation Chain）—— 项目运行时实现

### 11.1 降级链设计

> **双重管控**：单任务执行时间 ≤ 240 秒，Token 消耗 ≤ 1,000,000

项目运行时降级链位于 [agent/degradation_chain.py](file:///d:/code/moss-finance-assistant/agent/degradation_chain.py)，按下列顺序自动降级：

```
Tier 1 DeepSeek   ──失败/熔断/超时──▶  Tier 2 IMA
Tier 2 IMA        ──失败/熔断/超时──▶  Tier 3 Qwen3-8B
Tier 3 Qwen3-8B   ──失败/熔断/超时──▶  Tier 4 静态模板
Tier 4 静态模板    ──最终兜底，无 LLM 依赖──▶  返回结构化提示
```

| 层级 | 名称 | 能力 | 依赖 | 兜底场景 |
|------|------|------|------|----------|
| Tier 1 | DeepSeek | 联网搜索 + 强推理 | 网络 + API Key | 主力模型，能力最强 |
| Tier 2 | IMA 知识库 | 检索专有文档 | RAGFlow 服务 | 主模型不可用时降级 |
| Tier 3 | Qwen3-8B | 离线本地推理 | 本地 Ollama | 网络/API 全不可用 |
| Tier 4 | 静态模板 | 结构化提示 | 无依赖 | 所有模型不可用的最终兜底 |

### 11.2 双重硬上限

```python
MAX_TASK_SECONDS: float = 150.0    # 单任务执行时间上限
MAX_TASK_TOKENS: int = 1_000_000   # 单任务 Token 上限
```

**执行流程**（`DegradationChain.execute`）：
1. 每层调用前查询对应熔断器（`cb.allow_request()`），熔断中则跳过
2. `asyncio.wait_for` 超时保护（剩余时间预算 = 240s - 已耗时）
3. Token 计数器检查（超 1M 立即终止）
4. 错误分类决定是否计入熔断器（A/C 计入，B/D 不计入）
5. 任一层成功即返回，全部失败走 Tier 4 静态模板

### 11.3 降级报告（DegradationReport）

每次降级链执行产出完整报告，供 SLO 监控记录：
- `final_tier` / `final_output`：最终命中层级与输出
- `total_latency_sec` / `total_tokens`：总耗时与总 Token
- `tier_results`：每层执行详情（success/skipped/latency/error/fallback_reason）
- `hit_hard_limit` / `limit_breached`：是否触达硬上限（"TIME" / "TOKEN"）

---

## 12. 模型幻觉防护（Hallucination Guard）—— 三重管道

### 12.1 防护设计

> **核心原则**：失败默认保守，无法判定时按"疑似幻觉"处理，附加警示而非直接交付。

项目运行时幻觉防护位于 [agent/hallucination_guard.py](file:///d:/code/moss-finance-assistant/agent/hallucination_guard.py)，实现三重管道（顺序执行，互不短路，便于完整审计）：

```
Tier 1: RAG + 引用追踪
  │  检测陈述是否标注来源
  │  关键数字（百分比/价格/金额/比率）是否可在工具结果中找到
  │  股票代码是否张冠李戴
  ▼
Tier 2: JSON Schema 输出验证
  │  若提供 schema，校验输出结构（必填字段 + 类型）
  │  支持对象与数组两种形态
  ▼
Tier 3: LLM-as-Judge 二次校验
   独立轻量模型审查 Maker 输出
   返回 {is_valid, issues, hallucination_type}
```

### 12.2 HallucinationReport 结构

| 字段 | 类型 | 说明 |
|------|------|------|
| passed | bool | 是否通过（无幻觉嫌疑） |
| confidence | float | 置信度 0.0~1.0（按未通过项数量衰减） |
| citation_gaps | List[CitationGap] | 缺少引用来源的陈述项 |
| unverified_numbers | List[str] | 工具结果中未找到的数字 |
| unverified_stock_codes | List[str] | 工具结果中未找到的股票代码 |
| llm_judge_verdict | Dict | LLM-as-Judge 返回结果 |
| schema_valid | bool | JSON Schema 校验是否通过 |
| schema_errors | List[str] | Schema 校验错误列表 |
| tiers_run | List[str] | 已执行的校验层 |

### 12.3 集成方式

在 [agent/main_agent.py](file:///d:/code/moss-finance-assistant/agent/main_agent.py) 中，agent 流式输出完成后自动执行：
1. 收集本轮所有工具结果文本（`_tool_result_texts`）
2. 调用 `get_hallucination_guard().verify()` 执行三重校验
3. 若未通过，调用 `render_warning()` 生成用户可见警示并附加到输出末尾
4. 将 `passed` / `confidence` 记录到 SLO 事件

### 12.4 引用来源标注规范

输出涉及新闻/数据/财报/业绩时，必须包含以下来源标注之一：
- `来源：xxx` / `引自：xxx` / `参考：xxx` / `消息源：xxx`
- `根据xxx公告/报道/数据` / `据xxx报道`
- `[来源：xxx]` / `Source: xxx`
- 知识星球内容须标注 `知识星球` 或 `zsxq`

缺少来源标注的陈述会被标记为 `CitationGap`，并提示期望的来源类型。

---

## 13. 参考资源

- **FINOS**（Fintech Open Source Foundation）：金融开源标准组织，提供交易系统参考架构
- **SRE 实践**：以 SLO 为核心的可靠性体系
- **LMAX/Aeron**：确定性事件溯源架构，适用于高频交易
- **混沌工程原则**：主动注入故障验证系统韧性

详细参考文档：
- 故障分类体系：`trading-reliability/references/failure-taxonomy.md`
- 分级降级策略：`trading-reliability/references/degradation-tiers.md`
- 重试策略模板：`trading-reliability/references/retry-policy-templates.md`
- HTTP 状态码特殊处理：`trading-reliability/references/error-code-glossary.md`
- 灾难恢复剧本：`trading-reliability/references/dr-playbook.md`

配套代码实现（可直接引用）：
- 自适应熔断器：`trading-reliability/scripts/circuit_breaker.py`
- 分级降级控制器：`trading-reliability/scripts/degradation_controller.py`
- 健康检查器：`trading-reliability/scripts/health_checker.py`

项目运行时实现（已在 main_agent.py 中集成）：
- 错误四象限分类器：[agent/error_classifier.py](file:///d:/code/moss-finance-assistant/agent/error_classifier.py)
- 时间窗口三态熔断器：[agent/circuit_breaker.py](file:///d:/code/moss-finance-assistant/agent/circuit_breaker.py)
- 四级降级链：[agent/degradation_chain.py](file:///d:/code/moss-finance-assistant/agent/degradation_chain.py)
- 模型幻觉防护：[agent/hallucination_guard.py](file:///d:/code/moss-finance-assistant/agent/hallucination_guard.py)
- SLO 监控聚合器：[agent/slo_monitor.py](file:///d:/code/moss-finance-assistant/agent/slo_monitor.py)
- Maker-Checker 质量校验：[agent/maker_checker.py](file:///d:/code/moss-finance-assistant/agent/maker_checker.py)

监控端点：
- `GET /api/slo/status` — SLO 完整快照（指标 + 错误预算 + 降级链 + 熔断器 + 违规列表）
- `GET /api/circuit-breakers` — 所有熔断器实时状态
- `GET /api/traces/{session_id}` — 会话级 Trace 记录
- `GET /api/traces/{session_id}/latency` — 会话级延迟统计

事故报告模板：
- `trading-reliability/assets/incident-response-template.md`
