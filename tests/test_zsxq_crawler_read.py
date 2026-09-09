#coding: utf-8
"""盘前研报热度「读取/复用」逻辑单测。

覆盖 tools/zsxq_crawler_tool.py 的纯文件系统分支（不依赖 Playwright / Ollama / 网络）：
  1. find_latest_today_txt：目录缺失 / 无匹配 / 多文件取最新 / 前缀隔离 / 子目录过滤
  2. _attach_table_marker：analysis json 附加标记 / 缺失 / 损坏 / 空 details / 幂等
  3. fetch_zsxq_latest_summary_async：
     - 当日 txt 命中 → 跳过抓取直接返回（ensure_ollama_ready 不被调用）
     - 空 txt → 降级重跑 runner
     - runner exit 0 写产物 / exit 1 但有产物兜底 / exit 1 无产物 / exit 0 无产物 / 空产物
     - runner 脚本缺失

运行：python tests/test_zsxq_crawler_read.py
"""
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import tools.zsxq_crawler_tool as crawler
from tools.zsxq_crawler_tool import (
    find_latest_today_txt,
    _attach_table_marker,
    fetch_zsxq_latest_summary_async,
)

PREFIX = "20260101"  # 固定前缀，与系统时钟解耦
TXT_BODY = "盘前研报热度总结正文\n第二行"


# ============================================================
# fake runner：用环境变量控制行为，模拟 zsxq_analysis_runner.py
#   FAKE_ZSXQ_DIR     产物目录（绝对路径）
#   FAKE_ZSXQ_PREFIX  当日 YYYYMMDD
#   FAKE_ZSXQ_ACTION  ok / fail_with_output / fail_no_output / empty / none
# ============================================================
_FAKE_RUNNER_SRC = r'''
import os, sys
from pathlib import Path
news_dir = Path(os.environ["FAKE_ZSXQ_DIR"])
prefix = os.environ["FAKE_ZSXQ_PREFIX"]
action = os.environ["FAKE_ZSXQ_ACTION"]
news_dir.mkdir(parents=True, exist_ok=True)
print("[ZSXQ] fake runner action=" + action)
if action in ("ok", "fail_with_output", "empty"):
    body = "" if action == "empty" else "RUNNER_SUMMARY_BODY"
    (news_dir / (prefix + "120000.txt")).write_text(body, encoding="utf-8")
if action == "ok":
    (news_dir / ("analysis_" + prefix + "120000.json")).write_text(
        '{"generated_at":"2026-01-01T12:00:00","total_stocks":1,'
        '"details":[{"stock":"测试股","sentiment":"利多"}]}',
        encoding="utf-8")
if action in ("fail_with_output", "fail_no_output"):
    sys.exit(1)
'''


def _make_fake_runner(dst: Path) -> Path:
    dst.write_text(_FAKE_RUNNER_SRC, encoding="utf-8")
    return dst


def _write_txt(d: Path, name: str, body: str = TXT_BODY) -> Path:
    p = d / name
    p.write_text(body, encoding="utf-8")
    return p


def _write_analysis_json(d: Path, stem: str, payload: str) -> Path:
    p = d / f"analysis_{stem}.json"
    p.write_text(payload, encoding="utf-8")
    return p


# ============================================================
# 1) find_latest_today_txt
# ============================================================
def test_find_latest_today_txt(failures):
    def check(name, cond):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        if not cond:
            failures.append(name)

    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        # 目录不存在 → None
        check("目录不存在 → None", find_latest_today_txt(d / "no_such_dir", PREFIX) is None)

        # 空目录 → None
        check("空目录 → None", find_latest_today_txt(d, PREFIX) is None)

        # 多文件按文件名降序取最新
        _write_txt(d, f"{PREFIX}090000.txt", "old")
        _write_txt(d, f"{PREFIX}153000.txt", "newest")
        _write_txt(d, f"{PREFIX}120000.txt", "mid")
        hit = find_latest_today_txt(d, PREFIX)
        check("多文件 → 取文件名最大（最新）", hit is not None and hit.name == f"{PREFIX}153000.txt")

        # 前缀隔离：昨日文件不匹配
        _write_txt(d, "20251231235959.txt", "yesterday")
        hit2 = find_latest_today_txt(d, PREFIX)
        check("昨日文件不匹配当日前缀", hit2 is not None and hit2.name.startswith(PREFIX))

        # 非 txt 文件不匹配
        _write_txt(d, f"{PREFIX}999999.json", '{"x":1}')
        hit3 = find_latest_today_txt(d, PREFIX)
        check(".json 文件不被选中", hit3 is not None and hit3.suffix == ".txt")

        # 子目录内同名文件不被选中（glob 非递归 + is_file 过滤）
        sub = d / f"{PREFIX}000000_sub"
        sub.mkdir()
        _write_txt(sub, f"{PREFIX}999999.txt", "in_subdir")
        hit4 = find_latest_today_txt(d, PREFIX)
        check("子目录内文件不被选中", hit4 is not None and hit4.name == f"{PREFIX}153000.txt")

    # prefix=None → 自动取当天
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        _write_txt(d, f"{today}101010.txt", "today")
        check("today_prefix=None → 自动取当天并命中",
              find_latest_today_txt(d) is not None
              and find_latest_today_txt(d).name == f"{today}101010.txt")


# ============================================================
# 2) _attach_table_marker
# ============================================================
def test_attach_table_marker(failures):
    def check(name, cond):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        if not cond:
            failures.append(name)

    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        txt = _write_txt(d, f"{PREFIX}120000.txt", TXT_BODY)

        # 无 analysis json → 原样返回
        out = _attach_table_marker(TXT_BODY, txt)
        check("json 缺失 → 原样返回", out == TXT_BODY and "<<<ZSXQ_TABLE:" not in out)

        # 有 details → 附加标记块
        _write_analysis_json(d, txt.stem, json.dumps({
            "generated_at": "2026-01-01T12:00:00",
            "total_stocks": 2,
            "details": [
                {"stock": "测试股A", "sentiment": "利多"},
                {"stock": "测试股B", "sentiment": "中性"},
            ],
        }, ensure_ascii=False))
        out2 = _attach_table_marker(TXT_BODY, txt)
        check("json 有 details → 附加 ZSXQ_TABLE 标记",
              "<<<ZSXQ_TABLE:" in out2 and out2.startswith(TXT_BODY))
        check("标记块含股票行数据", "测试股A" in out2 and "测试股B" in out2)

        # 幂等：已含标记不重复附加
        out3 = _attach_table_marker(out2, txt)
        check("已含标记 → 幂等不重复附加", out3.count("<<<ZSXQ_TABLE:") == 1)

        # json 损坏 → 原样返回
        _write_analysis_json(d, txt.stem, "{not a valid json")
        out4 = _attach_table_marker(TXT_BODY, txt)
        check("json 损坏 → 原样返回", out4 == TXT_BODY)

        # details 为空列表 → 原样返回
        _write_analysis_json(d, txt.stem, json.dumps({"generated_at": "x", "details": []}))
        out5 = _attach_table_marker(TXT_BODY, txt)
        check("details 为空 → 原样返回", out5 == TXT_BODY)


# ============================================================
# 3) fetch_zsxq_latest_summary_async
# ============================================================
async def _test_fetch_async(failures):
    def check(name, cond):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        if not cond:
            failures.append(name)

    # --- 3.1 当日 txt 命中 → 跳过抓取（ollama/runner 均不应被触碰） ---
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        txt = _write_txt(d, f"{PREFIX}100000.txt", TXT_BODY)
        _write_analysis_json(d, txt.stem, json.dumps({
            "generated_at": "2026-01-01T10:00:00",
            "total_stocks": 1,
            "details": [{"stock": "缓存股", "sentiment": "利多"}],
        }, ensure_ascii=False))

        ollama_called = False

        async def _fake_ensure(*_a, **_kw):
            nonlocal ollama_called
            ollama_called = True
            return True, ""

        orig_ensure = crawler.ensure_ollama_ready
        crawler.ensure_ollama_ready = _fake_ensure
        try:
            # runner_script 故意指向不存在路径：若复用分支失效会走到 runner 检查
            result = await fetch_zsxq_latest_summary_async(
                news_dir=d,
                runner_script=d / "should_never_run.py",
                today_prefix=PREFIX,
                quiet=True,
            )
        finally:
            crawler.ensure_ollama_ready = orig_ensure

        check("复用命中 → 返回 txt 正文", TXT_BODY in result)
        check("复用命中 → 附加表格标记", "<<<ZSXQ_TABLE:" in result and "缓存股" in result)
        check("复用命中 → ensure_ollama_ready 未被调用", ollama_called is False)

    # --- 3.2 runner 各分支（ollama 预检全部 mock 为成功） ---
    async def _ok_ensure(*_a, **_kw):
        return True, ""

    orig_ensure = crawler.ensure_ollama_ready
    crawler.ensure_ollama_ready = _ok_ensure
    try:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            runner = _make_fake_runner(d / "fake_runner.py")

            async def run_action(action, news_dir):
                os.environ["FAKE_ZSXQ_DIR"] = str(news_dir)
                os.environ["FAKE_ZSXQ_PREFIX"] = PREFIX
                os.environ["FAKE_ZSXQ_ACTION"] = action
                return await fetch_zsxq_latest_summary_async(
                    news_dir=news_dir,
                    runner_script=runner,
                    today_prefix=PREFIX,
                    quiet=True,
                )

            # 空目录 + runner exit 0 写产物 → 成功返回（含表格标记）
            out_dir1 = d / "case_ok"
            r = await run_action("ok", out_dir1)
            check("runner exit0 写产物 → 返回正文", "RUNNER_SUMMARY_BODY" in r)
            check("runner 产物 analysis json → 附加表格标记",
                  "<<<ZSXQ_TABLE:" in r and "测试股" in r)

            # runner exit 1 但已写出 txt → 兜底按成功返回
            out_dir2 = d / "case_fail_with_output"
            r2 = await run_action("fail_with_output", out_dir2)
            check("runner exit1 但有新产物 → 兜底成功", "RUNNER_SUMMARY_BODY" in r2)

            # runner exit 1 无产物 → 返回 ""
            out_dir3 = d / "case_fail_no_output"
            r3 = await run_action("fail_no_output", out_dir3)
            check("runner exit1 无产物 → 返回空串", r3 == "")

            # runner exit 0 无产物 → 返回 ""
            out_dir4 = d / "case_none"
            r4 = await run_action("none", out_dir4)
            check("runner exit0 无产物 → 返回空串", r4 == "")

            # runner 写出空 txt → 返回 ""
            out_dir5 = d / "case_empty"
            r5 = await run_action("empty", out_dir5)
            check("runner 产物为空 txt → 返回空串", r5 == "")

            # 当日 txt 存在但内容为空 → 降级重跑 runner 并返回新内容
            out_dir6 = d / "case_empty_then_rerun"
            out_dir6.mkdir()
            _write_txt(out_dir6, f"{PREFIX}120000.txt", "   \n  ")  # 空白内容
            r6 = await run_action("ok", out_dir6)
            check("空 txt → 降级重跑并返回 runner 正文", "RUNNER_SUMMARY_BODY" in r6)

            # runner 脚本不存在 → 返回 ""
            out_dir7 = d / "case_no_runner"
            r7 = await fetch_zsxq_latest_summary_async(
                news_dir=out_dir7,
                runner_script=d / "missing_runner.py",
                today_prefix=PREFIX,
                quiet=True,
            )
            check("runner 脚本缺失 → 返回空串", r7 == "")
    finally:
        crawler.ensure_ollama_ready = orig_ensure
        for k in ("FAKE_ZSXQ_DIR", "FAKE_ZSXQ_PREFIX", "FAKE_ZSXQ_ACTION"):
            os.environ.pop(k, None)


def main():
    failures = []
    print("[1] find_latest_today_txt")
    test_find_latest_today_txt(failures)
    print("[2] _attach_table_marker")
    test_attach_table_marker(failures)
    print("[3] fetch_zsxq_latest_summary_async")
    asyncio.run(_test_fetch_async(failures))

    print()
    if failures:
        print(f"FAIL {len(failures)} 项: {failures}")
        sys.exit(1)
    print("ALL PASS")


if __name__ == "__main__":
    main()
