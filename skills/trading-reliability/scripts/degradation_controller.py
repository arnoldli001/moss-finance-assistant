#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分级降级控制器 (Degradation Controller)
管理系统的运行层级（Tier 1 ~ Tier 4），并根据依赖健康状况自动调整。
"""

import time
import logging
from enum import IntEnum
from typing import Dict, Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DegradationTier(IntEnum):
    TIER_1_NORMAL = 1
    TIER_2_CACHE = 2
    TIER_3_NOTIONAL = 3
    TIER_4_OVERRIDE = 4

class DegradationController:
    def __init__(self):
        self.current_tier = DegradationTier.TIER_1_NORMAL
        self.dependencies_health: Dict[str, bool] = {
            "realtime_db": True,
            "risk_service": True,
            "trading_gateway": True
        }
        self.last_healthy_time = time.time()

    def update_health(self, service_name: str, is_healthy: bool):
        """更新依赖服务的健康状态"""
        self.dependencies_health[service_name] = is_healthy
        self._recalculate_tier()

    def _recalculate_tier(self):
        """根据依赖健康度重新计算降级层级"""
        healthy_count = sum(1 for v in self.dependencies_health.values() if v)
        
        if healthy_count == 3:
            new_tier = DegradationTier.TIER_1_NORMAL
        elif healthy_count == 2:
            # 降级到缓存验证 (假设非核心服务挂了)
            new_tier = DegradationTier.TIER_2_CACHE
        elif healthy_count == 1:
            # 仅风控或网关可用，使用名义限额
            new_tier = DegradationTier.TIER_3_NOTIONAL
        else:
            # 全部不可用，必须人工介入
            new_tier = DegradationTier.TIER_4_OVERRIDE
        
        if new_tier != self.current_tier:
            logger.critical(f"Degradation tier changed: {self.current_tier.name} -> {new_tier.name}")
            self.current_tier = new_tier
            # 触发监控告警
            self._alert_monitoring(new_tier)

    def _alert_monitoring(self, tier: DegradationTier):
        """发送降级告警"""
        if tier >= DegradationTier.TIER_3_NOTIONAL:
            # 这里可以接入钉钉/邮件告警
            print(f"[ALERT] System degraded to {tier.name}. Please check dependencies.")

    def execute_with_risk_check(self, order_request: dict, order_func: Callable):
        """
        根据不同层级执行下单前的风控检查
        """
        if self.current_tier == DegradationTier.TIER_1_NORMAL:
            # 全量风控：检查价格、持仓、资金、撤单次数
            logger.info("Tier 1: Full risk validation.")
            # ... 执行全量检查
            pass
        
        elif self.current_tier == DegradationTier.TIER_2_CACHE:
            logger.warning("Tier 2: Using cached balance/position data.")
            # 获取缓存数据校验
            pass
        
        elif self.current_tier == DegradationTier.TIER_3_NOTIONAL:
            logger.error("Tier 3: Only notional cap validation!")
            # 仅检查名义金额 (例如: abs(order_value) < 1_000_000)
            if abs(order_request['price'] * order_request['qty']) > 1_000_000:
                raise ValueError("Order exceeds notional cap under Tier 3 degradation.")
        
        elif self.current_tier == DegradationTier.TIER_4_OVERRIDE:
            logger.critical("Tier 4: MANUAL OVERRIDE MODE. No automatic checks!")
            # 必须记录操作员ID
            if 'operator_id' not in order_request:
                raise PermissionError("Manual operator authorization required for Tier 4.")
            # 直接透传订单
        
        # 执行实际下单
        return order_func(order_request)

# 使用示例
if __name__ == "__main__":
    controller = DegradationController()
    
    # 模拟服务故障
    controller.update_health("realtime_db", False)  # 数据库故障
    time.sleep(1)
    controller.update_health("risk_service", False) # 风控故障
    
    # 尝试下单
    try:
        controller.execute_with_risk_check(
            {"price": 100, "qty": 1000, "symbol": "BTCUSDT"}, 
            lambda x: print("Order executed")
        )
    except Exception as e:
        print(f"Order blocked: {e}")