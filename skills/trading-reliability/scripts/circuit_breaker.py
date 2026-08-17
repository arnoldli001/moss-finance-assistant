#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
金融级自适应熔断器实现 (Adaptive Circuit Breaker)
基于滑动窗口统计错误率，支持半开状态探测。
"""

import time
import random
from collections import deque
from threading import Lock
from typing import Tuple


class AdaptiveCircuitBreaker:
    """
    自适应熔断器
    - 滑动窗口：记录最近 N 次请求的结果
    - 错误率阈值：动态计算，默认 5%
    - 冷却时间：熔断打开后，等待 recovery_timeout 秒后进入半开状态
    """

    def __init__(self, name: str, window_size: int = 100, 
                 failure_threshold_rate: float = 0.05, 
                 recovery_timeout: float = 30.0):
        self.name = name
        self.window_size = window_size
        self.failure_threshold_rate = failure_threshold_rate
        self.recovery_timeout = recovery_timeout
        
        # 滑动窗口存储请求结果 (True=成功, False=失败)
        self._window = deque(maxlen=window_size)
        self._lock = Lock()
        
        # 熔断器状态
        self._state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._last_failure_time = 0.0
        self._half_open_success_count = 0  # 半开状态下成功次数

    def allow_request(self) -> bool:
        """判断当前请求是否允许通过"""
        with self._lock:
            if self._state == "CLOSED":
                return True
            
            elif self._state == "OPEN":
                # 检查冷却时间是否到期
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = "HALF_OPEN"
                    self._half_open_success_count = 0
                    print(f"[CircuitBreaker:{self.name}] State transition: OPEN -> HALF_OPEN")
                    return True
                else:
                    return False
            
            elif self._state == "HALF_OPEN":
                # 半开状态下，允许少量请求通过（限制并发，这里简单返回 True）
                return True
            
            return False

    def record_success(self):
        """记录一次成功请求"""
        with self._lock:
            self._window.append(True)
            
            if self._state == "HALF_OPEN":
                self._half_open_success_count += 1
                # 如果半开状态下连续成功 3 次，关闭熔断器
                if self._half_open_success_count >= 3:
                    self._state = "CLOSED"
                    print(f"[CircuitBreaker:{self.name}] State transition: HALF_OPEN -> CLOSED (Healthy)")
                    self._window.clear()  # 清空窗口，重新开始统计

    def record_failure(self):
        """记录一次失败请求"""
        with self._lock:
            self._window.append(False)
            self._last_failure_time = time.time()
            
            if self._state == "CLOSED":
                # 计算当前滑动窗口的错误率
                error_rate = self._calculate_error_rate()
                if error_rate >= self.failure_threshold_rate:
                    self._state = "OPEN"
                    print(f"[CircuitBreaker:{self.name}] State transition: CLOSED -> OPEN (Error Rate: {error_rate:.2%})")
            
            elif self._state == "HALF_OPEN":
                # 半开状态下失败，立即重新打开
                self._state = "OPEN"
                print(f"[CircuitBreaker:{self.name}] State transition: HALF_OPEN -> OPEN (Probe failed)")

    def _calculate_error_rate(self) -> float:
        """计算当前窗口内的错误率"""
        if len(self._window) < self.window_size // 2:
            # 数据不足时，保守一点，返回 0
            return 0.0
        total = len(self._window)
        failures = sum(1 for success in self._window if not success)
        return failures / total

    def get_state(self) -> Tuple[str, float]:
        """获取当前状态和错误率（用于监控）"""
        with self._lock:
            rate = self._calculate_error_rate()
            return self._state, rate


# 使用示例
if __name__ == "__main__":
    # 初始化熔断器，目标错误率 5%
    cb = AdaptiveCircuitBreaker(name="OrderGateway", failure_threshold_rate=0.05)
    
    # 模拟请求
    for i in range(200):
        # 模拟 10% 的失败率
        is_success = random.random() > 0.1
        
        if cb.allow_request():
            if is_success:
                cb.record_success()
            else:
                cb.record_failure()
        else:
            print(f"Request {i} rejected by circuit breaker.")
        
        # 打印状态
        if i % 20 == 0:
            state, rate = cb.get_state()
            print(f"Status: State={state}, ErrorRate={rate:.2%}")
        
        time.sleep(0.1)