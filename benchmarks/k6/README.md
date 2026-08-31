# k6 HTTP 压测

面向 `interfaces/api/server.py` HTTP 层的压测脚本（冒烟 + 阶梯负载），断言 SLO 阈值。

## 安装 k6

```bash
# Windows
winget install k6 --source winget        # 或 choco install k6
# macOS
brew install k6
```

## 启动被测服务

```bash
python main.py server    # 默认 http://localhost:8000
```

## 冒烟测试（1 VU，全链路断言）

覆盖：健康检查 → 注册 → 行级权限（读自己 200 / 读他人 401+）→ 任务受理与停止 → RBAC（普通用户访问 owner 端点 403）。

```bash
k6 run benchmarks/k6/smoke.js
```

## 阶梯负载（1→5→10→20 VU）

```bash
k6 run benchmarks/k6/load.js
# 导出 JSON 明细
k6 run --summary-export benchmarks/results/k6_load_summary.json benchmarks/k6/load.js
```

## SLO 断言

| 指标 | 阈值 | 说明 |
|------|------|------|
| `http_req_failed` | < 1% | 全局错误率（429 限流已豁免，单独计数） |
| `http_req_duration` p95 | ≤ 500ms | 全局 HTTP 延迟 |
| `health` p95 | ≤ 100ms | 无鉴权探针链路 |
| `sessions` p95 | ≤ 300ms | JWT + 行级校验 + SQLite 读 |
| `checks` | > 99% | 业务断言通过率 |

## 限流（429）说明

服务端按角色限流：owner 600 / admin 120 / user 60 / guest 10 QPM。负载测试用 `user` 角色，
加压到 10+ VU 后 429 是**限流生效的正常表现**（脚本已豁免并计数 `rate_limited_429`）。
需要测真实延迟上限时，调高 QPM 后再启动服务：

```bash
RATE_LIMIT_QPM_USER=100000 RATE_LIMIT_PER_MINUTE=100000 python main.py server
```

## 设计边界

- **不压 `/api/task`**：每次请求会启动真实 Agent 任务（LLM token 消耗 + 外部依赖），
  压测仅验证受理契约（冒烟），Agent 执行链路的 SLO 由 `/api/slo/status`（availability / latency_p95_sec）监控。
- `login` 端点走 bcrypt 密码哈希，是 CPU 密集代理，负载脚本中降频为每 5 次迭代 1 次。
