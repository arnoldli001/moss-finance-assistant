# MOSS Finance Assistant — 服务镜像
# 说明：不含 Playwright 浏览器与 Ollama（知识星球抓取/本地模型按需外挂或本地运行），
#       核心链路（多智能体 + 熔断降级 + SSE/WS 流式）开箱即用。
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Shanghai

WORKDIR /app

# 依赖层独立 COPY，充分利用构建缓存
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 代码层（.dockerignore 已排除 tests/data/output/docs/.env 等）
COPY . .

# 运行时数据目录（建议挂卷持久化）
RUN mkdir -p /app/data /app/output

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).status==200 else 1)"

CMD ["python", "main.py", "server", "--host", "0.0.0.0", "--port", "8000"]
