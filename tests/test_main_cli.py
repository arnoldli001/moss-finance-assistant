"""tests/test_main_cli.py — CLI 启动路径回归测试（main.py + shared/console_encoding.py）。

覆盖 2026-10-16 修复的 4 个实测缺陷（每条用例都对应一个「修完再犯会红」的判据）：

  F1 Windows 中文控制台 GBK 下 `python main.py` 因 emoji 抛 UnicodeEncodeError 崩溃
     → shared/console_encoding.ensure_utf8_console()（T1~T5）
  F2 uvicorn 0.41 起已移除 socket_options，旧代码每次启动都落进 except TypeError 兜底分支，
     打印与真实原因不符的「当前 uvicorn 版本不支持 socket_options」，且 timeout_graceful_shutdown
     是否生效无从得知 → 按能力探测构建启动参数（T6~T9）
  F3 启动横幅硬编码 8000，`--port 8010` 时提示与实际监听不一致
     → resolve_server_port() 端口同源（T10~T16）+ cmd_server 回写（T17）
  F4 `PORT` 环境变量非法时 `int()` 直接抛 ValueError 裸 traceback
     → _server_port_from_env() 严格校验并给出可操作提示（T18~T19）

运行：pytest tests/test_main_cli.py -v
"""
from __future__ import annotations

import os
import subprocess
import sys
import pathlib
import shutil
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import pytest

import main
import shared.console_encoding as ce
from config import constants as C

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
PYTHON = sys.executable

_PORT_ENV_KEYS = (*C.SERVER_PORT_ENV_KEYS, C.SERVER_RUNTIME_PORT_ENV)


@pytest.fixture(autouse=True)
def _clean_port_env(monkeypatch):
    """端口相关环境变量全域隔离（等价类/边界值判定都依赖「未设置」这一初始态）。"""
    for key in _PORT_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture(autouse=True)
def _clear_probe_cache():
    """uvicorn 能力探测结果带缓存，逐个用例清空，避免用例间互相污染。"""
    C.UVICORN_RUN_KWARG_PROBE_CACHE.clear()
    yield
    C.UVICORN_RUN_KWARG_PROBE_CACHE.clear()


@pytest.fixture(autouse=True)
def _block_real_uvicorn_launch():
    """护栏：任何用例都不得真的把 HTTP 服务拉起来。

    踩坑记录（2026-10-16）：cmd_server 内部是「函数级 `import uvicorn`」，
    因此 `monkeypatch.setattr(main, "uvicorn", stub)` 无效 —— 真实 uvicorn 会被调用，
    测试会真的起服务并永久阻塞（实测卡死 10 分钟以上，且占用端口）。
    这里对 `uvicorn.run` 本体打桩：未被用例显式替换时直接抛错，让「误起服务」立刻变红。
    """
    import uvicorn

    def _forbidden(*_a, **_kw):
        raise AssertionError("测试不得真实调用 uvicorn.run（会起服务并阻塞；请打桩 uvicorn.run）")

    original = uvicorn.run
    uvicorn.run = _forbidden  # type: ignore[assignment]
    try:
        yield
    finally:
        uvicorn.run = original  # type: ignore[assignment]


class _FakeStream:
    """最小流替身：只实现本模块用到的 encoding/text_io_encoding/reconfigure/name。"""

    def __init__(self, encoding: str, *, reconfigurable: bool = True, name: str = "<fake>",
                 fail_with: Exception | None = None):
        self.encoding = encoding
        self.text_io_encoding = encoding  # real stdout 同时具备这两个属性
        self.name = name
        self._reconfigurable = reconfigurable
        self._fail_with = fail_with
        self.reconfigure_calls: list[dict] = []

    def reconfigure(self, **kwargs):  # type: ignore[no-untyped-def]
        if not self._reconfigurable:
            raise AttributeError("'%s' object has no attribute 'reconfigure'" % type(self).__name__)
        if self._fail_with is not None:
            raise self._fail_with
        self.reconfigure_calls.append(kwargs)
        if "encoding" in kwargs:
            self.encoding = kwargs["encoding"]
            self.text_io_encoding = kwargs["encoding"]


# ----------------------------------------------------------------------
# F1 控制台编码自愈（等价类：GBK 需切换 / UTF-8 族免切换 / 不可切换流静默跳过）
# ----------------------------------------------------------------------
class TestConsoleEncodingSelfHeal:
    def test_gbk_stream_reconfigured_to_utf8(self):
        """正常路径：GBK 流 → 切成 UTF-8，emoji 不再抛 UnicodeEncodeError。"""
        s = _FakeStream("gbk", name="stdout")
        switched = ce.ensure_utf8_console([s])
        assert switched == ["stdout"]
        assert s.encoding == "utf-8"
        assert s.reconfigure_calls and s.reconfigure_calls[0] == {"encoding": "utf-8"}

    def test_gbk_codec_cannot_encode_emoji_but_utf8_can(self):
        """语义前提：GBK 编不出 emoji（这正是原始崩溃原因），UTF-8 可以 —— 判据不依赖终端。"""
        with pytest.raises(UnicodeEncodeError):
            "💡 常用命令速查".encode("gbk")
        assert "💡 常用命令速查".encode("utf-8").decode("utf-8") == "💡 常用命令速查"
        # 切换后用新编码写出同样的文本不得再抛异常
        s = _FakeStream("gbk")
        ce.ensure_utf8_console([s])
        "💡 常用命令速查".encode(s.encoding)

    @pytest.mark.parametrize("encoding", ["utf-8", "UTF-8", "utf8", "cp65001", "utf_8"])
    def test_utf8_family_is_noop(self, encoding):
        """边界值：已是 UTF-8 族（含大小写/别名/下划线写法）不得重复 reconfigure。"""
        s = _FakeStream(encoding)
        assert ce.ensure_utf8_console([s]) == []
        assert s.reconfigure_calls == []

    def test_stream_without_reconfigure_is_skipped(self):
        """异常路径：StringIO / 冻结环境等无 reconfigure 的流 → 静默跳过，绝不阻断启动。"""
        class _NoReconfigure:
            encoding = "gbk"
            text_io_encoding = "gbk"

        assert ce.ensure_utf8_console([_NoReconfigure()]) == []

    @pytest.mark.parametrize("exc", [ValueError("closed"), OSError("bad fd"), LookupError("no codec")])
    def test_reconfigure_failure_is_swallowed(self, exc):
        """异常路径：reconfigure 抛错（流已关闭 / 编码不可用）→ 吞掉不抛，保持可用性优先。"""
        s = _FakeStream("gbk", fail_with=exc)
        assert ce.ensure_utf8_console([s]) == []

    def test_none_stream_ignored(self):
        """边界值：sys.stdout 被置 None（pythonw 场景）不得抛 AttributeError。"""
        assert ce.ensure_utf8_console([None]) == []


# ----------------------------------------------------------------------
# F2 uvicorn 启动参数能力探测（等价类：新 API 形态 / 旧 API 形态）
# ----------------------------------------------------------------------
class TestUvicornRunKwargs:
    def test_new_uvicorn_drops_socket_options_but_keeps_graceful_timeout(self, monkeypatch):
        """新版 uvicorn（0.41+ 显式签名，无 **kwargs）：socket_options 被剔除，
        timeout_graceful_shutdown 保留 —— 且不得再打印「版本不支持」这类错误归因。"""
        import uvicorn

        accepted = set(uvicorn.Config.__init__.__code__.co_varnames)
        monkeypatch.setattr("main._uvicorn_run_supports",
                            lambda name, value, **kw: name in accepted)
        kwargs = main._build_uvicorn_run_kwargs("127.0.0.1", 8010, False, "info")
        assert "socket_options" not in kwargs
        assert kwargs["timeout_graceful_shutdown"] == C.UVICORN_GRACEFUL_SHUTDOWN_TIMEOUT_SEC
        assert kwargs["host"] == "127.0.0.1" and kwargs["port"] == 8010
        assert kwargs["reload"] is False and kwargs["log_level"] == "info"

    def test_legacy_uvicorn_keeps_socket_options(self, monkeypatch):
        """旧版 uvicorn（Config 接受 socket_options）：SO_REUSEADDR 注入必须保留
        （Windows 10048 自愈能力不能因为重构而丢失）。"""
        monkeypatch.setattr("main._uvicorn_run_supports", lambda name, value, **kw: True)
        kwargs = main._build_uvicorn_run_kwargs("127.0.0.1", 8000, True, "debug")
        assert "socket_options" in kwargs
        import socket as _socket
        assert any(opt[1] == _socket.SO_REUSEADDR for opt in kwargs["socket_options"])
        assert kwargs["timeout_graceful_shutdown"] == C.UVICORN_GRACEFUL_SHUTDOWN_TIMEOUT_SEC

    def test_reuse_port_skipped_on_windows(self, monkeypatch):
        """边界值：Windows（os.name == 'nt'）不得传 reuse_port（uvicorn 会直接报错）。"""
        monkeypatch.setattr(main.os, "name", "nt")
        monkeypatch.setattr("main._uvicorn_run_supports", lambda name, value, **kw: True)
        kwargs = main._build_uvicorn_run_kwargs("127.0.0.1", 8000, False, "info")
        assert "reuse_port" not in kwargs

    def test_probe_result_is_cached(self, monkeypatch):
        """探测结果缓存：同名参数只实例化一次 Config（避免每次启动重复构造）。"""
        import uvicorn

        calls: list[dict] = []
        real_config = uvicorn.Config

        def _spy(app, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(kwargs)
            return real_config(app, **kwargs)

        monkeypatch.setattr(uvicorn, "Config", _spy)
        first = main._uvicorn_run_supports("timeout_graceful_shutdown", 8)
        second = main._uvicorn_run_supports("timeout_graceful_shutdown", 8)
        assert first == second
        assert len(calls) == 1
        assert calls[0].get("timeout_graceful_shutdown") == 8

    def test_probe_reports_unsupported_on_typeerror(self, monkeypatch):
        """异常路径：Config 拒绝该 kwargs（TypeError）→ 判定为不支持，且不抛给调用方。"""
        import uvicorn

        def _strict(app, **kwargs):  # type: ignore[no-untyped-def]
            raise TypeError("Config.__init__() got an unexpected keyword argument 'nope'")

        monkeypatch.setattr(uvicorn, "Config", _strict)
        assert main._uvicorn_run_supports("nope", 1) is False
        assert C.UVICORN_RUN_KWARG_PROBE_CACHE["nope"] is False

    def test_real_uvicorn_call_signature_matches_probe(self):
        """真实性校验：探测结论必须与真实 uvicorn.Config 签名一致（防止探针与调用脱节）。"""
        import inspect
        import uvicorn

        sig = inspect.signature(uvicorn.Config.__init__)
        assert main._uvicorn_run_supports("timeout_graceful_shutdown", 8) == (
            "timeout_graceful_shutdown" in sig.parameters
        )


# ----------------------------------------------------------------------
# F3 端口单一真源（等价类：未设置 / PORT / 运行时回写；边界值：1、65535、0、65536）
# ----------------------------------------------------------------------
class TestResolveServerPort:
    def test_default_when_env_unset(self):
        """未设置任何环境变量 → 常量默认端口。"""
        assert C.resolve_server_port() == C.DEFAULT_SERVER_PORT

    def test_port_env_is_honored(self, monkeypatch):
        """PORT 环境变量生效（与 cmd_server 的 --port 默认值同源）。"""
        monkeypatch.setenv("PORT", "8010")
        assert C.resolve_server_port() == 8010

    def test_runtime_port_wins_over_port(self, monkeypatch):
        """--auto-port 定案后的回写值优先于原始 PORT（否则横幅仍提示被占用的旧端口）。"""
        monkeypatch.setenv("PORT", "8000")
        monkeypatch.setenv(C.SERVER_RUNTIME_PORT_ENV, "8032")
        assert C.resolve_server_port() == 8032

    @pytest.mark.parametrize("value,expected", [
        ("1", 1),            # 边界值下界
        ("65535", 65535),    # 边界值上界
        (" 8123 ", 8123),    # 首尾空白容忍
    ])
    def test_valid_boundaries(self, monkeypatch, value, expected):
        monkeypatch.setenv("PORT", value)
        assert C.resolve_server_port() == expected

    @pytest.mark.parametrize("value", ["0", "65536", "-1", "abc", "", "80.5"])
    def test_invalid_falls_back_to_default(self, monkeypatch, value):
        """异常路径：非法/越界值不抛异常，回落默认（横幅等展示场景可用性优先）。"""
        monkeypatch.setenv("PORT", value)
        assert C.resolve_server_port() == C.DEFAULT_SERVER_PORT

    def test_runtime_invalid_falls_through_to_port(self, monkeypatch):
        """异常路径：运行时回写变量非法时，继续向下读 PORT，而不是直接回默认。"""
        monkeypatch.setenv(C.SERVER_RUNTIME_PORT_ENV, "junk")
        monkeypatch.setenv("PORT", "8201")
        assert C.resolve_server_port() == 8201


class TestCmdServerPortSingleSource:
    def test_cmd_server_writes_resolved_port_to_env(self, monkeypatch):
        """契约：cmd_server 把「最终端口」回写到 SERVER_RUNTIME_PORT_ENV，
        app 侧 resolve_server_port() 必须读到同一个值（横幅提示 == 实际监听）。"""
        import uvicorn

        calls: list[dict] = []

        def _fake_run(app, **kwargs):  # type: ignore[no-untyped-def]
            calls.append({"app": app, **kwargs})

        # 必须打在 uvicorn.run 本体上：cmd_server 内是函数级 `import uvicorn`
        monkeypatch.setattr(uvicorn, "run", _fake_run)
        monkeypatch.setattr(main, "_port_is_listening", lambda host, port: None)
        monkeypatch.setattr(main, "_build_uvicorn_run_kwargs",
                            lambda host, port, reload, level: {"host": host, "port": port})
        # 关停兜底会 taskkill 整树：patch 全局 subprocess.run（main 内部同为函数级 import）
        monkeypatch.setattr(subprocess, "run",
                            lambda *a, **kw: subprocess.CompletedProcess(a, 0))

        class _Args:
            host = "127.0.0.1"
            port = 8010
            reload = False
            log_level = "info"
            kill_conflicts = "no"
            auto_port = False

        rc = main.cmd_server(_Args())
        assert rc == 0
        assert os.environ.get(C.SERVER_RUNTIME_PORT_ENV) == "8010"
        assert C.resolve_server_port() == 8010
        assert calls and calls[0]["port"] == 8010
        assert calls[0]["app"] == "interfaces.api.server:app"

    def test_cmd_server_rejects_out_of_range_port(self, monkeypatch):
        """异常路径：--port 越界 → 退出码 2 且不调用 uvicorn（不进入绑定阶段）。"""
        import uvicorn

        called: list[str] = []
        monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: called.append("run"))

        class _Args:
            host = "127.0.0.1"
            port = 70000
            reload = False
            log_level = "info"
            kill_conflicts = "no"
            auto_port = False

        assert main.cmd_server(_Args()) == 2
        assert called == []


# ----------------------------------------------------------------------
# F4 PORT 非法时严格拒绝（真实子进程入口，断言退出码 + 提示文案）
# ----------------------------------------------------------------------
class TestPortEnvValidationCli:
    def _run(self, *args: str, env_port: str | None = None, timeout: int = 90):
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        env.pop("MOSS_RUNTIME_PORT", None)
        if env_port is None:
            env.pop("PORT", None)
        else:
            env["PORT"] = env_port
        return subprocess.run(
            [PYTHON, "main.py", *args],
            cwd=str(REPO_ROOT), env=env, capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=timeout,
        )

    @pytest.mark.parametrize("bad,label", [("abc", "非整数"), ("70000", "越界"), ("-3", "负数")])
    def test_invalid_port_env_rejected_with_hint(self, bad, label):
        proc = self._run("server", env_port=bad)
        assert proc.returncode == 1, f"{label} 端口应被拒绝：{proc.stdout} {proc.stderr}"
        assert "PORT" in (proc.stderr + proc.stdout)
        assert "非法" in (proc.stderr + proc.stdout) or "越界" in (proc.stderr + proc.stdout)

    def test_invalid_port_env_message_is_actionable(self):
        """提示必须给出下一步动作（改 PORT 或改用 --port），而不是裸 traceback。"""
        proc = self._run("server", env_port="abc")
        combined = proc.stderr + proc.stdout
        assert "--port" in combined
        assert "Traceback" not in combined


class TestConsoleEncodingRealProcess:
    """F1 端到端：清空 PYTHONIOENCODING 后真实调用 CLI，退出码必须为 0（原缺陷：崩溃 exit 1）。"""

    def test_main_without_args_exits_zero_on_gbk_console(self):
        env = dict(os.environ)
        for key in ("PYTHONIOENCODING", "PYTHONUTF8", "PYTHONLEGACYWINDOWSSTDIO"):
            env.pop(key, None)
        if sys.platform != "win32":
            pytest.skip("GBK 控制台崩溃是 Windows 专属缺陷")
        env["PYTHONIOENCODING"] = ""  # 显式置空：不做外部兜底，检验内置自愈
        proc = subprocess.run([PYTHON, "main.py"], cwd=str(REPO_ROOT), env=env,
                              capture_output=True, text=True, encoding="utf-8",
                              errors="replace", timeout=180)
        assert proc.returncode == 0, f"stdout={proc.stdout[-800:]} stderr={proc.stderr[-800:]}"
        assert "UnicodeEncodeError" not in proc.stderr
        assert "常用命令速查" in proc.stdout


def test_python_launcher_exists():
    """G1 依赖就绪：pytest 使用的解释器必须存在（防止 CI/本地解释器漂移导致假绿）。"""
    assert PYTHON and (pathlib.Path(PYTHON).exists() or shutil.which(PYTHON))
