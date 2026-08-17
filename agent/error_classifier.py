"""
Layer 3 - Harness Engineering: 错误四象限分类器 + 幂等性强制检查。

将所有运行时异常按"是否可重试 × 是否为硬错误"四象限归类，
并对外暴露统一的分类接口，供熔断器、降级链、重试策略使用。

象限定义（与 skills/trading-reliability/SKILL.md 第 1.2 节对齐）：
  A 可重试硬错误   — 网络瞬态、读超时、429、503、WS 闪断
  B 不应重试软错误 — 参数/业务规则拒绝、撤单超限、涨跌停、订单不存在、合约停牌
  C 不可重试错误   — 写超时、系统级、数据级损坏
  D 配置类错误     — API Key 过期/无效、签名错误、权限不足、IP 白名单、时钟漂移、env 缺失

幂等性检查：
  资金/订单类敏感操作必须在执行前调用 check_idempotency() 验证幂等键存在，
  缺失则拒绝执行（Fail-Closed），防止重复下单/重复扣款。
"""
from __future__ import annotations

import re
import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Set

from config.constants import HTTP_CODE_UNAUTHORIZED


# ======================================================================
# 错误象限枚举
# ======================================================================

class ErrorQuadrant(str, Enum):
    """错误四象限。值与 SKILL.md 保持一致，便于日志检索。"""
    A_RETRYABLE_HARD = "A"   # 可重试硬错误
    B_SOFT = "B"              # 不应重试软错误
    C_PERMANENT = "C"         # 不可重试错误
    D_CONFIG = "D"            # 配置类错误（绝对不重试）


@dataclass
class ClassifiedError:
    """分类结果。"""
    quadrant: ErrorQuadrant
    error_type: str           # 简短类型标签，如 "NETWORK_TRANSIENT"
    http_code: Optional[int]  # HTTP 状态码（如适用）
    biz_code: Optional[str]    # 业务码（如适用）
    retryable: bool            # 是否可重试
    action: str                # 建议动作
    raw_message: str           # 原始错误信息
    timestamp: float = field(default_factory=time.time)


# ======================================================================
# 错误特征模式库
# ======================================================================
# 按象限组织，每个象限一组正则。匹配顺序：D > C > B > A（严格度递减）。

_PATTERNS_D_CONFIG = [
    (re.compile(r"401|UNAUTHORIZED|TOKEN_EXPIRED|TOKEN_INVALID|INVALID_KEY", re.IGNORECASE),
     "API_KEY_EXPIRED", "刷新 API Key/Token 后人工恢复"),
    (re.compile(r"403|FORBIDDEN|NO_PERMISSION|IP_FORBIDDEN|IP_BLOCKED", re.IGNORECASE),
     "PERMISSION_DENIED", "申请权限 / 添加 IP 白名单"),
    (re.compile(r"SIGN_INVALID|SIGNATURE_MISMATCH|HMAC.*fail", re.IGNORECASE),
     "SIGN_INVALID", "检查密钥配置与时钟同步"),
    (re.compile(r"TIMESTAMP_SKEW|clock.*skew|时间偏差|时钟漂移", re.IGNORECASE),
     "CLOCK_SKEW", "NTP 校时后恢复"),
    (re.compile(r"KeyError.*env|环境变量.*缺失|missing.*env|ENV.*not.*set", re.IGNORECASE),
     "ENV_MISSING", "补齐 .env 配置后重启"),
]

_PATTERNS_C_PERMANENT = [
    (re.compile(r"WriteTimeout|write.*timeout|下单.*超时|提交.*超时", re.IGNORECASE),
     "WRITE_TIMEOUT", "走订单状态反查流程，严禁重试提交"),
    (re.compile(r"OOM|Out.*Memory|MemoryError|killed.*9", re.IGNORECASE),
     "OOM", "进程自愈 + WAL 重放恢复"),
    (re.compile(r"NoSpaceLeft|disk.*full|磁盘.*满|ENOSPC", re.IGNORECASE),
     "DISK_FULL", "紧急告警 + 日志轮转"),
    (re.compile(r"silent.*corrupt|静默.*损坏|timestamp.*skew.*data", re.IGNORECASE),
     "SILENT_DATA_CORRUPTION", "双源交叉验证后熔断"),
    (re.compile(r"ConnectionRefused.*state|state.*db.*unreachable|风控.*不可用", re.IGNORECASE),
     "STATE_DB_DOWN", "Fail-Closed 立即停止交易"),
]

_PATTERNS_B_SOFT = [
    (re.compile(r"400|INVALID_PARAM|参数.*错误|param.*invalid", re.IGNORECASE),
     "INVALID_PARAM", "永不重试，直接拒绝"),
    (re.compile(r"INSUFFICIENT|余额不足|持仓超限|信用不足", re.IGNORECASE),
     "INSUFFICIENT", "永不重试，告警并剔除信号"),
    (re.compile(r"ORDER_CANCEL_LIMIT|撤单.*超限|cancel.*limit", re.IGNORECASE),
     "CANCEL_LIMIT", "熔断该合约，次日恢复"),
    (re.compile(r"PRICE_LIMIT|涨.*停|跌.*停|limit.*up.*down", re.IGNORECASE),
     "PRICE_LIMIT", "永不重试，记录后跳过"),
    (re.compile(r"ORDER_NOT_FOUND|订单.*不存在", re.IGNORECASE),
     "ORDER_NOT_FOUND", "永不重试，更新本地状态"),
    (re.compile(r"SYMBOL_SUSPENDED|停牌|suspended", re.IGNORECASE),
     "SYMBOL_SUSPENDED", "永不重试，标记不可交易"),
    # 模型幻觉单独标记为软错误（禁止重试，重试只会产生新幻觉）
    (re.compile(r"HALLUCINATION|幻觉|hallucinated|fabricated", re.IGNORECASE),
     "MODEL_HALLUCINATION", "禁止重试，必须重新检索或人工核对"),
]

_PATTERNS_A_RETRYABLE = [
    (re.compile(r"ConnectionReset|TCP.*RST|DNS.*fail|network.*unreachable", re.IGNORECASE),
     "NETWORK_TRANSIENT", "指数退避 + 抖动重试（最多 5 次）"),
    (re.compile(r"ReadTimeout|read.*timeout|查询.*超时|408|504", re.IGNORECASE),
     "READ_TIMEOUT", "指数退避重试（最多 3 次）"),
    (re.compile(r"429|TooManyRequests|rate.*limit|流控", re.IGNORECASE),
     "RATE_LIMIT", "按 Retry-After 头延迟重试"),
    (re.compile(r"503|service.*unavailable|行情源.*不可用", re.IGNORECASE),
     "SERVICE_UNAVAILABLE", "切备用源 + 主源后台探测"),
    (re.compile(r"WebSocketDisconnect|1006|WS.*disconnect|行情.*断流", re.IGNORECASE),
     "WS_DISCONNECT", "1s→2s→4s→8s→16s→30s 退避重连"),
]


# ======================================================================
# 分类器
# ======================================================================

class ErrorClassifier:
    """
    错误分类器。

    使用方式：
        classifier = get_error_classifier()
        cls_err = classifier.classify(exc, http_code=HTTP_CODE_UNAUTHORIZED)
        if cls_err.retryable:
            await retry_with_backoff(...)
        else:
            await alert_and_stop(...)
    """

    def classify(
        self,
        exc: BaseException,
        http_code: Optional[int] = None,
        biz_code: Optional[str] = None,
    ) -> ClassifiedError:
        """
        将异常归类到四象限之一。

        匹配顺序：D → C → B → A → 默认 C（保守策略，宁停不乱）
        """
        raw_msg = f"{type(exc).__name__}: {exc}"
        # 显式传入的 http_code/biz_code 也参与匹配
        match_text = f"{http_code or ''} {biz_code or ''} {raw_msg}"

        # 象限 D：配置类错误（绝对不重试）
        for pat, etype, action in _PATTERNS_D_CONFIG:
            if pat.search(match_text):
                return ClassifiedError(
                    quadrant=ErrorQuadrant.D_CONFIG,
                    error_type=etype, http_code=http_code, biz_code=biz_code,
                    retryable=False, action=action, raw_message=raw_msg,
                )

        # 象限 C：不可重试错误
        for pat, etype, action in _PATTERNS_C_PERMANENT:
            if pat.search(match_text):
                return ClassifiedError(
                    quadrant=ErrorQuadrant.C_PERMANENT,
                    error_type=etype, http_code=http_code, biz_code=biz_code,
                    retryable=False, action=action, raw_message=raw_msg,
                )

        # 象限 B：不应重试软错误
        for pat, etype, action in _PATTERNS_B_SOFT:
            if pat.search(match_text):
                return ClassifiedError(
                    quadrant=ErrorQuadrant.B_SOFT,
                    error_type=etype, http_code=http_code, biz_code=biz_code,
                    retryable=False, action=action, raw_message=raw_msg,
                )

        # 象限 A：可重试硬错误
        for pat, etype, action in _PATTERNS_A_RETRYABLE:
            if pat.search(match_text):
                return ClassifiedError(
                    quadrant=ErrorQuadrant.A_RETRYABLE_HARD,
                    error_type=etype, http_code=http_code, biz_code=biz_code,
                    retryable=True, action=action, raw_message=raw_msg,
                )

        # 默认归入象限 C（保守策略）
        return ClassifiedError(
            quadrant=ErrorQuadrant.C_PERMANENT,
            error_type="UNKNOWN", http_code=http_code, biz_code=biz_code,
            retryable=False, action="保守处理：告警 + 不重试，等待人工判定",
            raw_message=raw_msg,
        )


# ======================================================================
# 幂等性强制检查器
# ======================================================================

class IdempotencyChecker:
    """
    敏感操作幂等键校验器。

    资金/订单/转账等写操作执行前必须调用：
        checker = get_idempotency_checker()
        key = checker.generate_key(client_id, "order", payload)
        if not checker.check(key):
            raise PermissionError("幂等键已存在或未通过校验，禁止重复执行")
        # ... 执行敏感操作 ...
        checker.mark_done(key, result)
    """

    # 触发幂等性检查的关键词
    SENSITIVE_OP_PATTERNS = re.compile(
        r"下单|撤单|转账|划转|提交订单|submit_order|cancel_order|transfer|"
        r"买入|卖出|资金|支付",
        re.IGNORECASE,
    )

    def __init__(self):
        # 已处理幂等键集合：{key: {"status": "DONE|PENDING", "result": ..., "ts": ...}}
        # 生产环境应替换为 SQLite/Redis 持久化，此处内存版仅做兜底
        self._keys: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def generate_key(client_id: str, op_type: str, payload: Dict[str, Any]) -> str:
        """生成幂等键：{client_id}-{date}-{op_type}-{payload_hash8}"""
        today = time.strftime("%Y%m%d", time.localtime())
        # payload 序列化后取 SHA1 前 8 位，避免长 key
        payload_str = str(sorted(payload.items()))
        payload_hash = hashlib.sha1(payload_str.encode("utf-8")).hexdigest()[:8]
        return f"{client_id}-{today}-{op_type}-{payload_hash}"

    def check(self, idempotency_key: str) -> bool:
        """
        校验幂等键是否可执行：
          - 不存在 → 允许执行，标记为 PENDING
          - 存在且 PENDING → 拒绝（说明上一次还没完成，可能正在执行）
          - 存在且 DONE → 拒绝（重复请求，直接返回上次结果）
        返回 True 才允许执行。
        """
        rec = self._keys.get(idempotency_key)
        if rec is None:
            self._keys[idempotency_key] = {"status": "PENDING", "result": None, "ts": time.time()}
            return True
        return False

    def mark_done(self, idempotency_key: str, result: Any = None) -> None:
        """标记幂等键对应的操作已完成。"""
        if idempotency_key in self._keys:
            self._keys[idempotency_key].update({"status": "DONE", "result": result, "ts": time.time()})

    def is_sensitive_op(self, query: str) -> bool:
        """判断用户提问是否涉及资金/订单等敏感操作（触发幂等性检查）。"""
        return bool(self.SENSITIVE_OP_PATTERNS.search(query or ""))


# ======================================================================
# 全局单例
# ======================================================================

_error_classifier: Optional[ErrorClassifier] = None
_idempotency_checker: Optional[IdempotencyChecker] = None


def get_error_classifier() -> ErrorClassifier:
    global _error_classifier
    if _error_classifier is None:
        _error_classifier = ErrorClassifier()
    return _error_classifier


def get_idempotency_checker() -> IdempotencyChecker:
    global _idempotency_checker
    if _idempotency_checker is None:
        _idempotency_checker = IdempotencyChecker()
    return _idempotency_checker
