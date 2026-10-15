"""tests/test_static_rate_limit.py — 静态资源限流/缓存回归测试。

线上缺陷（2026-10-16 实测）：
    `/static/*` 被并进匿名白名单的 guest 档（10 QPM/IP）计数，而前端首页一次加载
    需 1 个页面 + 5 个 JS → 刷新一两次即耗尽配额 → 静态 JS 返回 429 / net::ERR_ABORTED
    → `MossMonitor` / `MossMoatCard` / `MStreamClient` 全部 undefined
    → 页面能打开但「点击界面没反应、登录框不弹」。

修复判据（本文件断言的契约）：
    S1 静态资源有独立配额，guest 配额耗尽不再影响 /static/*；
    S2 静态资源确实在计数（配额小时仍会 429，DDoS 兜底未被移除）；
    S3 静态与 guest 分属不同桶（互不挤占）；
    S4 静态响应带 Cache-Control: no-cache，且条件请求命中 ETag 时返回 304
       （避免重复整包下载，同时保证前端更新能立即生效）；
    S5 RateLimiter.hit(limit_override=..., endpoint=...) 的桶隔离语义。

运行：pytest tests/test_static_rate_limit.py -v
"""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import pytest

from config import constants as C

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
STATIC_JS = "/static/js/constants.js"


@pytest.fixture()
def guest_quota_1(reset_rate_limiter):
    """把环境全局 limiter 的 guest 档压到 1 QPM（模拟配额已耗尽场景）。"""
    rl = reset_rate_limiter
    original = rl.qpm_for_role("guest")
    rl._qpm["guest"] = 1  # noqa: SLF001 — 测试直接调档，避免改 rbac_policy.json
    yield rl
    rl._qpm["guest"] = original  # noqa: SLF001


class TestStaticAssetsExemptFromGuestQuota:
    def test_static_js_served_after_guest_quota_exhausted(self, unauth_client, guest_quota_1):
        """S1（修完再犯会红）：guest 配额耗尽后，连续 5 次静态 JS 请求仍必须成功。

        修复前：这些请求与 guest 同桶 —— 第 1 次 429，后续全部 429（前端 JS 全灭）。
        """
        # 先把 guest 桶打满（用公开的 auth 之外的公开端点：/health 属 pub 桶）
        assert unauth_client.get("/health").status_code == 200
        assert unauth_client.get("/health").status_code == 429, "guest 配额应为 1 QPM，第二次应被限流"

        for i in range(5):
            resp = unauth_client.get(STATIC_JS)
            assert resp.status_code == 200, (
                f"第 {i + 1} 次静态资源请求被限流（{resp.status_code}）；"
                f"说明 /static/* 又与 guest 配额同桶了 → 前端会全灭"
            )

    def test_index_page_not_guest_limited(self, unauth_client, guest_quota_1):
        """S1b：首页（应用外壳）同样不受 guest 配额限制。

        修复前：首页计入 guest 10 QPM/IP，用户每分钟刷新约 10 次就 429，
        整个应用直接打不开（页面返回 JSON 429 而不是 HTML）。
        """
        assert unauth_client.get("/health").status_code == 200
        assert unauth_client.get("/health").status_code == 429  # guest 桶已满
        for _ in range(5):
            resp = unauth_client.get("/")
            assert resp.status_code == 200, "首页被 guest 配额限流 → 用户会被整站挡在门外"
            assert "<html" in resp.text.lower()

    def test_guest_api_still_guest_limited(self, unauth_client, guest_quota_1):
        """S1 反向断言：真正的公开 API（/health 这类）仍受 guest 配额约束（限流未被整体关掉）。"""
        assert unauth_client.get("/health").status_code == 200
        assert unauth_client.get("/health").status_code == 429

    def test_static_has_own_ceiling(self, unauth_client, reset_rate_limiter, monkeypatch):
        """S2：静态资源仍计数 —— 把静态配额压到 2，第 3 次必须 429（DDoS 兜底未丢失）。"""
        monkeypatch.setattr("interfaces.api.server.STATIC_ASSET_QPM_PER_IP", 2)
        assert unauth_client.get(STATIC_JS).status_code == 200
        assert unauth_client.get(STATIC_JS).status_code == 200
        assert unauth_client.get(STATIC_JS).status_code == 429

    def test_static_and_guest_buckets_are_separate(self, unauth_client, reset_rate_limiter, monkeypatch):
        """S3：两个桶互不挤占 —— guest 打满不影响静态；静态打满不影响 guest。"""
        monkeypatch.setattr("interfaces.api.server.STATIC_ASSET_QPM_PER_IP", 1)
        original = reset_rate_limiter.qpm_for_role("guest")
        reset_rate_limiter._qpm["guest"] = 1  # noqa: SLF001
        try:
            # 静态桶先打满
            assert unauth_client.get(STATIC_JS).status_code == 200
            assert unauth_client.get(STATIC_JS).status_code == 429
            # guest 桶仍可用（未被静态请求消耗）
            assert unauth_client.get("/health").status_code == 200
        finally:
            reset_rate_limiter._qpm["guest"] = original  # noqa: SLF001


class TestStaticCacheControl:
    def test_static_response_declares_no_cache(self, unauth_client):
        """S4a：静态响应必须显式声明缓存策略，避免浏览器启发式缓存旧 JS。"""
        resp = unauth_client.get(STATIC_JS)
        assert resp.status_code == 200
        assert resp.headers.get("cache-control") == C.STATIC_CACHE_CONTROL

    def test_conditional_request_returns_304(self, unauth_client):
        """S4b：带 If-None-Match 的重复请求应得 304（省带宽又不阻塞前端更新）。"""
        first = unauth_client.get(STATIC_JS)
        etag = first.headers.get("etag")
        assert etag, "静态响应缺少 ETag（Starlette FileResponse 应提供）"
        second = unauth_client.get(STATIC_JS, headers={"If-None-Match": etag})
        assert second.status_code == 304
        assert second.content == b""

    def test_index_page_is_not_cached(self, unauth_client):
        """首页必须 no-store：否则用户拿不到带新 ?v= 版本号的新 index.html。"""
        resp = unauth_client.get("/")
        assert resp.status_code == 200
        assert "no-store" in (resp.headers.get("cache-control") or "")


class TestRateLimiterLimitOverride:
    def test_limit_override_is_respected_and_identity_bucketed(self):
        """S5：limit_override 生效，且不同 identity 前缀的桶独立计数
        （生产用 `static:{ip}` 作为独立 identity —— 全局 limiter 未开 endpoint 分桶）。"""
        from shared.utils.rate_limiter import RateLimiter

        rl = RateLimiter()
        # 桶 A：静态配额 2
        assert rl.hit("static:ip1", None, limit_override=2)[0] is True
        assert rl.hit("static:ip1", None, limit_override=2)[0] is True
        ok_a, retry_a = rl.hit("static:ip1", None, limit_override=2)
        assert ok_a is False and retry_a >= 1
        # 桶 B：不同 identity → 独立计数，不受桶 A 影响
        assert rl.hit("pub:ip1", None, limit_override=2)[0] is True
        # 桶 C：同 identity 前缀但不同 IP → 独立计数
        assert rl.hit("static:ip2", None, limit_override=2)[0] is True

    def test_limit_override_does_not_mutate_role_config(self):
        """S5b：limit_override 不得污染角色档位表（否则会影响真实 RBAC 配额）。"""
        from shared.utils.rate_limiter import RateLimiter

        rl = RateLimiter()
        before = rl.qpm_for_role("guest")
        rl.hit("ip", "guest", limit_override=99999)
        assert rl.qpm_for_role("guest") == before

    def test_without_override_role_quota_applies(self):
        """回归：不传 limit_override 时，行为与改动前一致（按角色档位计数）。"""
        from shared.utils.rate_limiter import RateLimiter

        rl = RateLimiter(qpm={"guest": 2})
        assert rl.hit("ip", "guest")[0] is True
        assert rl.hit("ip", "guest")[0] is True
        assert rl.hit("ip", "guest")[0] is False
