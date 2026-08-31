#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主动健康探测服务 (Health Checker)
定期探测核心依赖（数据库、券商网关、行情源），供熔断器和降级控制器使用。
"""
import time
import threading
import requests
from typing import Dict, List

class HealthChecker:
    def __init__(self, check_interval: int = 10):
        self._endpoints = {}
        self.check_interval = check_interval
        self.status: Dict[str, bool] = {}
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def register_endpoint(self, name: str, url: str, timeout: float = 2.0):
        """注册需要探测的端点"""
        self.status[name] = True  # 初始假设健康
        self._endpoints[name] = {"url": url, "timeout": timeout}

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join()

    def _run(self):
        while not self._stop_event.is_set():
            for name, config in self._endpoints.items():
                try:
                    start = time.time()
                    resp = requests.get(config["url"], timeout=config["timeout"])
                    elapsed = time.time() - start
                    
                    # 判定条件：HTTP 200 且响应时间 < 1s (高性能要求)
                    if resp.status_code == 200 and elapsed < 1.0:
                        self.status[name] = True
                    else:
                        self.status[name] = False
                except Exception:
                    self.status[name] = False
            
            # 此处可以将健康状态推送到降级控制器或监控系统
            # print(f"[Health] Status: {self.status}")
            time.sleep(self.check_interval)

    def get_health(self) -> Dict[str, bool]:
        return self.status

    def is_healthy(self, service_name: str) -> bool:
        return self.status.get(service_name, False)

# 注意：使用前需初始化 _endpoints 字典
# HealthChecker._endpoints = {}
# 或者直接修改代码在 __init__ 中定义 self._endpoints = {}