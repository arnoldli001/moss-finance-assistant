# -*- coding: utf-8 -*-
"""zsxq_get_token.py — 知识星球 token 一条龙获取工具（独立脚本，不依赖 zsxq_tool.py）。

用法：
    python tools/zsxq_get_token.py

流程：
    1. 弹出有头 Chromium 打开知识星球登录页
    2. 你手动扫码/登录（最长等待 5 分钟）
    3. 检测到登录成功后，自动提取 Cookie 中的 zsxq_access_token
    4. 写入项目 .env 的 ZSXQ_ACCESS_TOKEN（已存在则原地替换，其余行不动）
    5. 之后 zsxq_tool.py 的免扫码登录即自动生效（其常量优先读环境变量）
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"
LOGIN_URL = "https://wx.zsxq.com"
TIMEOUT_SEC = 300


def _upsert_env(key: str, value: str) -> None:
    lines = []
    if ENV_PATH.exists():
        lines = ENV_PATH.read_text(encoding="utf-8-sig").splitlines()
    out, replaced = [], False
    for ln in lines:
        if ln.strip().startswith(f"{key}="):
            out.append(f"{key}={value}")
            replaced = True
        else:
            out.append(ln)
    if not replaced:
        if out and out[-1].strip():
            out.append("")
        out.append(f"{key}={value}")
    ENV_PATH.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"[OK] 已写入 {ENV_PATH.name}: {key}={value[:12]}...(共 {len(value)} 字符)")


def main() -> int:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto(LOGIN_URL, wait_until="domcontentloaded")
        print("[等待登录] 请在弹出的浏览器里完成扫码/账号登录（最长 5 分钟）...")

        import time
        token = None
        deadline = time.time() + TIMEOUT_SEC
        while time.time() < deadline:
            cookies = {c["name"]: c["value"] for c in context.cookies("https://wx.zsxq.com")}
            token = cookies.get("zsxq_access_token")
            # 已登录判定：拿到 token 且 URL 不再停留在登录页
            if token and "/login" not in page.url:
                break
            try:
                page.wait_for_load_state("networkidle", timeout=2000)
            except Exception:
                pass
            time.sleep(1.5)
        else:
            print("[失败] 超时未检测到登录，请重跑本脚本")
            browser.close()
            return 1

        if not token:
            print("[失败] 登录态 cookie 中无 zsxq_access_token")
            browser.close()
            return 1

        _upsert_env("ZSXQ_ACCESS_TOKEN", token)
        print(f"[OK] 登录成功（当前页面: {page.url}）")
        # 保持浏览器 3 秒方便确认，然后关闭
        page.wait_for_timeout(3000)
        browser.close()

    print("\n验证：python -c \"import sys; sys.path.insert(0, r'%s'); import tools.zsxq_tool as z; print(z.ZSXQ_ACCESS_TOKEN[:12])\"" % str(ROOT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
