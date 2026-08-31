# interfaces/api: FastAPI 接入层，对外暴露 REST + SSE + WebSocket（server.py 主程序）
import shared.compat_bootstrap  # noqa: F401  (保守迁移：独立 import 时确保 compat 先启动)
