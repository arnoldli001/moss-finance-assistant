# 金融场景重试策略模板（Retry Policy Templates）

## 1. 重试决策矩阵

| 场景 | 重试策略 | 参数配置 | 终止条件 |
| :--- | :--- | :--- | :--- |
| **行情 WebSocket 重连** | 指数退避 + 全抖动 | `1s, 2s, 4s, 8s, 16s, 30s(上限)` | 重连成功或达到最大间隔 |
| **REST 读接口（余额/持仓）** | 指数退避 + 随机抖动 | `初始 500ms, 乘数 1.5, 最大 10s, 重试 3 次` | 收到成功响应或业务错误码（4xx） |
| **订单提交（写接口）** | **禁止重试（No Retry）** | `retry_count = 0` | 立即超时或失败，触发反查 |
| **撤单请求（写接口）** | **禁止重试（No Retry）** | `retry_count = 0` | 立即超时或失败，查询撤单结果 |

## 2. 指数退避计算公式（Python）

```python
import random
import time

def calculate_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 30.0):
    """
    计算带抖动的指数退避延迟
    :param attempt: 当前重试次数（从 0 开始）
    :param base_delay: 基础延迟（秒）
    :param max_delay: 最大延迟上限（秒）
    :return: 本次重试应等待的秒数
    """
    # 指数部分：2^attempt
    exponential_delay = base_delay * (2 ** attempt)
    # 加入 ±20% 的随机抖动（Full Jitter），防止惊群效应
    jitter = random.uniform(0.8, 1.2)
    calculated_delay = exponential_delay * jitter
    # 限制最大延迟
    return min(calculated_delay, max_delay)