# ADR-0005: 网络测试门控与 CI 策略

- 状态：已接受
- 日期：2026-08-30（本记录 2026-08-31 补录）

## 背景

`tests/test_ragflow.py` 依赖真实外网 API，网络环境差时曾无限阻塞（requests
无 DNS 超时兜底），拖垮整个测试套件；CI 环境无 API Key 时部分测试必失败。

## 决策

1. **网络测试门控**：真实网络测试默认 skip，`RUN_NET_TESTS=1` 显式启用；
   单元测试一律离线（mock / 本地 fake），可在 CI 稳定复跑。
2. **CI 流水线**（`.github/workflows/ci.yml`）：ruff（fatal 级）→
   import 冒烟 → pytest → LLM eval 抽样；`DEEPSEEK_API_KEY` 缺失时 eval 步骤
   自动跳过而非失败。
3. Windows 专属依赖（pywin32）加 `sys_platform == "win32"` 标记，保证
   Linux 容器/Docker 构建可安装依赖。

## 后果

- 正面：套件不再被外部网络拖死；本地/CI 行为一致。
- 负面：网络回归需手动开门控跑一次，存在"CI 全绿但线上网络故障"的盲区
  （由运行时熔断/降级补偿）。

## 替代方案

- CI 里 mock 全部外网调用：等价于门控默认态，真实链路仍需门控跑，采纳为补充而非替代。
- 测试内设大超时重试：治标不治本，波动仍会放大为套件失败，否决。
