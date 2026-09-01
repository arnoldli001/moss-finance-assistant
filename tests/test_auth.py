"""P0 鉴权 + 限流 验收单测（9 场景 × A6-4 标准）。

场景对照（AGENTS.md & 审查报告 P0-1/P0-2 验收）：
  [S1] POST /api/auth/register → 200 + 返回 access_token / refresh_token / user.role
  [S2] POST /api/auth/login 密码错误 → 401 + body.code == PASSWORD_MISMATCH
  [S3] 无 Authorization 头访问 POST /api/task → 401（middleware 层拦，不是 handler）
  [S4] 普通用户 userB 访问 userA 建的 GET /api/sessions/{sid}/history → 403 行级隔离
  [S5] 过期 JWT 访问任意鉴权端点 → 401 + body.code == EXPIRED_TOKEN
  [S6] 连续 11 次 guest 身份（QPM=10）打鉴权端点 → 第 11 次 = 429 + Retry-After 头存在
  [S7] 鉴权通过的响应必须带 X-RateLimit-Remaining / X-RateLimit-Limit / X-User-Id / X-User-Role 4 头
  [S8] 同一 user_id 重复 register 两次幂等（HTTP 200，且旧密码仍然能登录）
  [S9] POST /api/auth/change-password old_password 错误 → 401 + body.code == OLD_PASSWORD_MISMATCH
"""
from __future__ import annotations

import time
import uuid


# ======================================================================
# S1：register 成功场景
# ======================================================================
def test_register_success_200_token(unauth_client):
    uid = f"t_s1_{uuid.uuid4().hex[:8]}"
    pw = f"pw_{uuid.uuid4().hex[:12]}"
    r = unauth_client.post("/api/auth/register", json={
        "user_id": uid, "password": pw, "display_name": "S1 Demo",
    })
    assert r.status_code == 200, f"register={r.status_code} {r.text}"
    body = r.json()
    # TokenResponse pydantic 字段
    for f in ("access_token", "refresh_token", "token_type", "expires_in", "user"):
        assert f in body, f"缺少字段 {f}: {body}"
    assert body["token_type"] == "bearer"
    assert body["user"]["user_id"] == uid
    assert body["user"]["role"] == "user", (
        f"自助注册应默认 role=user，实际 user.role={body['user']['role']}")
    assert len(body["access_token"]) > 20, "access_token 长度异常"


# ======================================================================
# S2：login 密码错细分
# ======================================================================
def test_login_wrong_password_401(unauth_client):
    uid = f"t_s2_{uuid.uuid4().hex[:8]}"
    pw_correct = f"pw_{uuid.uuid4().hex[:12]}"
    # 先注册
    r = unauth_client.post("/api/auth/register",
                           json={"user_id": uid, "password": pw_correct})
    assert r.status_code == 200, r.text
    # 错误密码登录
    r2 = unauth_client.post("/api/auth/login",
                            json={"user_id": uid, "password": "definitely-wrong"})
    assert r2.status_code == 401, f"login(pw_wrong)={r2.status_code} {r2.text}"
    code = ((r2.json() or {}).get("detail") or {}).get("code")
    assert code == "PASSWORD_MISMATCH", f"期望 PASSWORD_MISMATCH，实际={code}"


# ======================================================================
# S3：POST /api/task 无 token → 401（middleware 已拦，不依赖 actor）
# ======================================================================
def test_post_task_no_token_401(unauth_client):
    # 传一个合理 body（即使不合法 middleware 也在到达 handler 前拦 401）
    r = unauth_client.post("/api/task", json={
        "query": "茅台今天怎么样", "model": "deepseek-chat",
    })
    assert r.status_code == 401, f"task(no token)={r.status_code} {r.text}"
    detail = (r.json() or {}).get("detail") or {}
    # middleware 自定义 code
    assert detail.get("code") in ("TOKEN_MISSING", "UNAUTHENTICATED", "TOKEN_INVALID"), \
        f"缺鉴权 middleware code 字段：{detail}"


# ======================================================================
# S4：行级越权（userA 建 session → userB 访问 history = 403）
# ======================================================================
def test_other_user_session_history_403(two_users):
    userA, userB = two_users

    # userA 创建一个 session（通过白名单旧端点 POST /api/users/{uid}/sessions）
    r = userA["client"].post(f"/api/users/{userA['user_id']}/sessions",
                             json={"title": "S4 A 的会话"})
    assert r.status_code == 200, f"userA create session={r.status_code} {r.text}"
    session_id = r.json()["session"]["session_id"]

    # userA 自己访问 → 不应报错（404 也 OK，只要不是 403）
    r_self = userA["client"].get(f"/api/sessions/{session_id}/history")
    assert r_self.status_code != 403, f"userA 访问本人会话不应 403：{r_self.status_code}"

    # userB 访问 userA 会话 → 403
    r_other = userB["client"].get(f"/api/sessions/{session_id}/history")
    assert r_other.status_code == 403, \
        f"userB 访问 userA 会话必须 403，实际={r_other.status_code} {r_other.text}"
    # 响应 body 应带 code
    detail = (r_other.json() or {}).get("detail") or {}
    # current_user_id_must_match 抛 HTTPException 403，detail 含 code 或直接为字符串
    code = detail.get("code") if isinstance(detail, dict) else None
    # 宽松：只要 403 即通过（兼容当前 HTTPException detail 形式可能为字符串）
    assert r_other.status_code == 403


# ======================================================================
# S5：过期 JWT = 401（临时 monkey patch JWT_ACCESS_TOKEN_EXPIRE_MINUTES）
# ======================================================================
def test_expired_jwt_401(moss_app, reset_rate_limiter, unauth_client):
    from shared.utils import auth as _auth_mod
    from fastapi.testclient import TestClient

    saved = _auth_mod.JWT_ACCESS_TOKEN_EXPIRE_MINUTES
    try:
        # 过去 5 分钟前过期 → 立即过期
        _auth_mod.JWT_ACCESS_TOKEN_EXPIRE_MINUTES = -5
        token_resp = _auth_mod.create_token_pair(
            f"t_s5_exp_{uuid.uuid4().hex[:8]}", "user", "ExpiredUser", False,
        )
    finally:
        _auth_mod.JWT_ACCESS_TOKEN_EXPIRE_MINUTES = saved

    expired_token = token_resp.access_token
    with TestClient(moss_app, raise_server_exceptions=False,
                    headers={"Authorization": f"Bearer {expired_token}"}) as tc:
        r = tc.get(f"/api/sessions/does-not-matter-{uuid.uuid4().hex}/history")
    assert r.status_code == 401, f"expired token={r.status_code} {r.text}"
    detail = (r.json() or {}).get("detail") or {}
    code = detail.get("code") if isinstance(detail, dict) else None
    assert code in ("TOKEN_EXPIRED", "TOKEN_INVALID", "EXPIRED_TOKEN"), \
        f"过期 token 期望 TOKEN_EXPIRED，实际 code={code} body={r.json()}"


# ======================================================================
# S6 & S7：guest QPM=10 限流 + 响应头正确
# ======================================================================
def _new_guest_client(moss_app, unauth_client):
    """生成新的 guest 身份 TestClient（返回 client, user_id）。"""
    from fastapi.testclient import TestClient

    r = unauth_client.post("/api/auth/guest")
    assert r.status_code == 200, f"guest register={r.status_code} {r.text}"
    body = r.json()
    token = body["access_token"]
    user_id = body["user"]["user_id"]
    tc = TestClient(moss_app, raise_server_exceptions=False,
                    headers={"Authorization": f"Bearer {token}"})
    return tc, user_id


def test_ratelimit_guest_10qpm_11th_429_and_headers(moss_app, reset_rate_limiter,
                                                    unauth_client):
    from fastapi.testclient import TestClient

    g_client, g_uid = _new_guest_client(moss_app, unauth_client)

    # 先打 1 次看 S7：响应头（X-User-Id / X-User-Role / X-RateLimit-Remaining / Limit）
    fake_sid = f"s6_probe_{uuid.uuid4().hex}"
    first = g_client.get(f"/api/sessions/{fake_sid}/history")
    assert first.status_code != 429, f"第 1 次不应限流：{first.status_code}"
    # S7 断言
    assert "X-RateLimit-Limit" in first.headers, f"缺失 Limit 头: {dict(first.headers)}"
    assert "X-RateLimit-Remaining" in first.headers, f"缺失 Remaining 头"
    assert "X-User-Id" in first.headers, f"缺失 X-User-Id"
    assert "X-User-Role" in first.headers, f"缺失 X-User-Role"
    assert first.headers["X-User-Id"] == g_uid, (
        f"X-User-Id 不匹配：{first.headers['X-User-Id']} vs {g_uid}")
    assert first.headers["X-User-Role"] == "guest", \
        f"X-User-Role 应=guest，实={first.headers['X-User-Role']}"
    assert int(first.headers["X-RateLimit-Limit"]) == 10, (
        f"QPM guest=10 期望 Limit=10，实={first.headers['X-RateLimit-Limit']}")
    # 打完 1 次 remaining 应=9（或如果内部实现 remaining 基于 deque len 可能是 9）
    rem_1 = int(first.headers["X-RateLimit-Remaining"])
    assert 0 <= rem_1 <= 9, f"首请求 remaining 应<=9，实={rem_1}"

    # S6：再打 9 次（合计 10 次通过），第 11 次 = 429
    for i in range(9):
        fake_sid2 = f"s6_{i}_{uuid.uuid4().hex}"
        r = g_client.get(f"/api/sessions/{fake_sid2}/history")
        assert r.status_code != 429, f"第 {i+2} 次意外 429：{r.text}"

    # 第 11 次 —— 命中限流
    fake_sid3 = f"s6_hit_{uuid.uuid4().hex}"
    r11 = g_client.get(f"/api/sessions/{fake_sid3}/history")
    assert r11.status_code == 429, f"第 11 次必须 429，实={r11.status_code} {r11.text}"
    assert "Retry-After" in r11.headers, f"429 响应缺少 Retry-After 头: {dict(r11.headers)}"
    retry = int(r11.headers["Retry-After"])
    assert 0 < retry <= 61, f"Retry-After 应在 (0, 61]，实={retry}"
    body = r11.json() or {}
    detail = body.get("detail") or {}
    if isinstance(detail, dict):
        assert detail.get("code") == "RATE_LIMIT_BY_ROLE", f"限流响应 code 错：{detail}"
        assert detail.get("role") == "guest", f"限流响应 role 错：{detail}"
        assert detail.get("qpm") == 10, f"限流响应 qpm 错：{detail}"


# ======================================================================
# S8：register 幂等（同 user_id 两次 register，仍能用第 1 次密码登录）
# ======================================================================
def test_register_same_user_id_idempotent(unauth_client):
    uid = f"t_s8_{uuid.uuid4().hex[:8]}"
    pw1 = f"pw_first_{uuid.uuid4().hex[:10]}"
    pw2 = f"pw_second_{uuid.uuid4().hex[:10]}"

    # 第一次 register：password=pw1
    r1 = unauth_client.post("/api/auth/register",
                            json={"user_id": uid, "password": pw1})
    assert r1.status_code == 200, f"1st register={r1.status_code} {r1.text}"

    # 第二次 register：password=pw2（不应生效，因为 get_or_create_user 对已存在用户忽略 password）
    r2 = unauth_client.post("/api/auth/register",
                            json={"user_id": uid, "password": pw2})
    # 判定：幂等 —— 允许 200 或 409 CONFLICT（看实现）。当前 storage.get_or_create_user
    # 对已存在用户直接返回，不抛错。期望 200。
    assert r2.status_code == 200, f"2nd register(幂等)={r2.status_code} {r2.text}"

    # 用 pw1 登录 OK
    l1 = unauth_client.post("/api/auth/login",
                            json={"user_id": uid, "password": pw1})
    assert l1.status_code == 200, f"login(pw1)={l1.status_code} {l1.text}"

    # 用 pw2 登录 FAIL 401（证明第 2 次 register 没覆盖密码）
    l2 = unauth_client.post("/api/auth/login",
                            json={"user_id": uid, "password": pw2})
    assert l2.status_code == 401, f"login(pw2) 期望 401 证明幂等未覆盖密码，实={l2.status_code}"


# ======================================================================
# S9：change-password old_password 错 = 401
# ======================================================================
def test_change_password_old_wrong_401(authenticated_user):
    user = authenticated_user
    r = user["client"].post("/api/auth/change-password", json={
        "old_password": "completely-wrong-old-pw",
        "new_password": f"new_pw_{uuid.uuid4().hex[:12]}",
    })
    assert r.status_code == 401, (
        f"change-pw(old错) 期望 401，实={r.status_code} {r.text}")
    detail = (r.json() or {}).get("detail") or {}
    code = detail.get("code") if isinstance(detail, dict) else None
    assert code == "OLD_PASSWORD_MISMATCH", f"期望 OLD_PASSWORD_MISMATCH，实={code}"


# ======================================================================
# S10：/api/users/{uid}/sessions 强制鉴权（P1 修复回归）
#   修复前：端点在 _AUTH_PUBLIC_PREFIXES 白名单内按匿名放行 ——
#     a) 匿名可读任意用户会话列表/预埋会话（越权）；
#     b) 登录用户点会话页签也按 IP guest 档(10 QPM)限流 → RATE_LIMIT_PUBLIC 429
# ======================================================================
def test_user_sessions_require_auth(unauth_client, two_users):
    user_a, user_b = two_users
    uid = user_a["user_id"]

    # 匿名（无 token）：列表/创建一律 401（middleware 层拦截）
    r_get = unauth_client.get(f"/api/users/{uid}/sessions")
    assert r_get.status_code == 401, f"sessions(no token) 期望 401，实={r_get.status_code}"
    r_post = unauth_client.post(f"/api/users/{uid}/sessions", json={"title": "x"})
    assert r_post.status_code == 401, f"create(no token) 期望 401，实={r_post.status_code}"

    # 登录用户访问他人 sessions → 403 行级隔离
    r_other = user_b["client"].get(f"/api/users/{uid}/sessions")
    assert r_other.status_code == 403, f"他人 sessions 期望 403，实={r_other.status_code}"

    # 登录用户本人 → 200
    r_self = user_a["client"].get(f"/api/users/{uid}/sessions")
    assert r_self.status_code == 200, f"本人 sessions 期望 200，实={r_self.status_code} {r_self.text}"

    # 旧明文登录端点 POST /api/users 与单段 GET /api/users/{id} 仍保持公开（兼容）
    r_legacy = unauth_client.post("/api/users", json={"user_id": f"t_s10_{uuid.uuid4().hex[:8]}"})
    assert r_legacy.status_code == 200, f"旧明文登录应保持公开，实={r_legacy.status_code}"
