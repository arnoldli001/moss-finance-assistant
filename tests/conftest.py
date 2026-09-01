"""全局 pytest 配置钩子。

解决：不依赖 pytest-asyncio 插件时，`async def test_xxx()` 函数会直接报
"async def functions are not natively supported" 的真实基础设施错误。
策略：在 pyfunc 调用时拦截，若函数是协程函数（async def），则**手动创建
新事件循环**执行，不污染全局循环状态（避免被测代码自行 new_event_loop
与 asyncio.run 内部 close 策略冲突导致挂死/超时）。
同时兼容已添加的 @pytest.mark.asyncio 装饰器：不装插件时它是普通
marker，不影响调用；装了插件会优先走插件驱动（钩子返回 True 会覆盖，
所以装插件时请在 pytest.ini 设置 ASYNCIO_LEGACY_HOOK=1 或直接删除本钩子）。
"""
from __future__ import annotations

import asyncio
import inspect
import uuid
from typing import Any

import pytest


def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> Any:
    """pytest 钩子：拦截每个 test 函数调用。

    若 test 函数为 `async def`，手动创建新事件循环执行。对同步函数返回 None，
    让 pytest 按默认方式执行。
    """
    testfunc = pyfuncitem.obj
    if not inspect.iscoroutinefunction(testfunc):
        return None

    sig = inspect.signature(testfunc)
    fixture_names = list(sig.parameters.keys())
    kwargs: dict[str, Any] = {}
    for name in fixture_names:
        try:
            kwargs[name] = pyfuncitem.funcargs[name]
        except KeyError:
            # 延迟求值 fixture（request / 自定义带 yield 的 fixture 等）
            kwargs[name] = pyfuncitem._request.getfixturevalue(name)

    # ---- Windows 事件循环策略兜底 ------------------------------------------
    # 先把当前策略保存为模块局部变量，结束时恢复，避免污染其它测试
    saved_policy: Any = None
    try:
        import sys as _sys
        if _sys.platform.startswith("win"):
            try:
                saved_policy = asyncio.get_event_loop_policy()
                # 若当前是 SelectorEventLoopPolicy，切换为 Proactor（避免某些
                # 旧 Playwright / subprocess 钩子"loop has no attribute _selector"
                # 类问题）；失败则静默退回
                if isinstance(saved_policy, asyncio.WindowsSelectorEventLoopPolicy):  # type: ignore[attr-defined]
                    asyncio.set_event_loop_policy(
                        asyncio.WindowsProactorEventLoopPolicy()  # type: ignore[attr-defined]
                    )
            except Exception:
                saved_policy = None
    except Exception:
        saved_policy = None

    loop: Any = None
    try:
        # 手动 new + run_until_complete：保证每个异步测试都拥有独立、干净的
        # loop，且不与 asyncio.run() 中 run_until_complete(shutdown_default_executor)
        # 关闭后被测代码想 new_event_loop 继续存活产生的冲突（之前 ActorSystem
        # 卡死 240s 正是由此引发）。
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(testfunc(**kwargs))
    finally:
        if loop is not None:
            try:
                loop.close()
            except Exception:
                pass
            # 清除 set_event_loop 引用，让后续下次 new_event_loop 回归默认
            try:
                asyncio.set_event_loop(None)
            except Exception:
                pass
        if saved_policy is not None:
            try:
                asyncio.set_event_loop_policy(saved_policy)
            except Exception:
                pass
    return True


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> int:
    """收尾兜底：关闭测试期间泄漏的 aiosqlite 连接。

    aiosqlite 每个连接持有一个**非 daemon** 的 _connection_worker_thread，
    若测试结束（尤其事件循环被本 conftest 每用例新建/关闭）时连接未 close，
    该线程会永久存活并阻塞解释器退出（threading._shutdown 无限 join）——
    表现为 pytest 打完汇总后进程挂死，CI 上直到 job 超时被杀。
    这里在会话结束时统一扫描并关闭残留连接（close() 不依赖原事件循环，
    可在新循环中安全执行；未真正启动线程的连接 close() 会立即返回）。
    """
    try:
        import gc

        import aiosqlite
    except ImportError:  # pragma: no cover
        return exitstatus

    leaked = [o for o in gc.get_objects() if isinstance(o, aiosqlite.Connection)]
    if not leaked:
        return exitstatus

    loop: Any = None
    try:
        loop = asyncio.new_event_loop()
        for conn in leaked:
            try:
                loop.run_until_complete(
                    asyncio.wait_for(conn.close(), timeout=5)
                )
            except Exception:
                pass
    except Exception:
        pass
    finally:
        if loop is not None:
            try:
                loop.close()
            except Exception:
                pass
    return exitstatus


# ======================================================================
# P0 鉴权 + 限流：共享 fixtures（tests/test_auth.py 复用，亦可供其它单测用）
# ======================================================================
import os as _os

# 测试模式下禁用 lifespan 中真实 Actor/Scheduler 启动。
# 纯 HTTP/Storage 单测（auth / history / 限流 / 白名单）都不依赖真 Actor。
_os.environ.setdefault("MOSS_TEST_SKIP_LIFESPAN", "1")


@pytest.fixture(scope="session")
def moss_app():
    """导入 FastAPI app 对象（仅一次，加速）。interfaces.api.server 导入会
    自动触发 storage.init_db() + compat_bootstrap 薄壳注册。"""
    from interfaces.api.server import app  # noqa: F401  side-effect import
    yield app


@pytest.fixture(scope="function")
def reset_rate_limiter():
    """每次用例前后清空 rate limiter，避免测试间 QPM 计数互相污染。"""
    from shared.utils.rate_limiter import get_rate_limiter
    rl = get_rate_limiter()
    rl.reset_all()
    yield rl
    rl.reset_all()


@pytest.fixture(scope="function")
def unauth_client(moss_app, reset_rate_limiter):
    """返回未携带任何 Authorization 头的 TestClient。"""
    from fastapi.testclient import TestClient
    with TestClient(moss_app, raise_server_exceptions=False) as c:
        yield c


def _unique(prefix: str) -> str:
    """生成测试唯一标识，避免并发/重复测试污染 SQLite DB。"""
    return f"t_{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="function")
def authenticated_user(unauth_client):
    """注册一个全新用户，返回 {user_id, password, role, token, refresh, client}。

    client 已经预注入 Authorization: Bearer <access_token> 头。
    """
    uid = _unique("u")
    pw = f"pw_{uuid.uuid4().hex[:12]}"
    resp = unauth_client.post("/api/auth/register", json={
        "user_id": uid, "password": pw, "display_name": f"Test {uid}",
    })
    assert resp.status_code == 200, f"register fail: {resp.status_code} {resp.text}"
    body = resp.json()
    access = body["access_token"]

    from fastapi.testclient import TestClient
    from interfaces.api.server import app as _app
    tc = TestClient(_app, raise_server_exceptions=False,
                    headers={"Authorization": f"Bearer {access}"})
    yield {
        "user_id": uid, "password": pw,
        "role": body.get("role", "user"),
        "token": access,
        "refresh_token": body.get("refresh_token"),
        "client": tc,
    }
    try:
        tc.close()
    except Exception:
        pass


@pytest.fixture(scope="function")
def two_users(unauth_client):
    """返回两个独立普通用户 (userA, userB)，用于行级越权场景。"""
    result = []
    for tag in ("A", "B"):
        uid = _unique(f"u{tag.lower()}")
        pw = f"pw_{uuid.uuid4().hex[:12]}"
        resp = unauth_client.post("/api/auth/register", json={
            "user_id": uid, "password": pw,
        })
        assert resp.status_code == 200, f"register user{tag} fail: {resp.text}"
        body = resp.json()
        from fastapi.testclient import TestClient
        from interfaces.api.server import app as _app
        tc = TestClient(_app, raise_server_exceptions=False,
                        headers={"Authorization": f"Bearer {body['access_token']}"})
        result.append({"user_id": uid, "password": pw, "token": body["access_token"],
                       "client": tc, "role": body.get("role", "user")})
    yield tuple(result)
    for d in result:
        try:
            d["client"].close()
        except Exception:
            pass
