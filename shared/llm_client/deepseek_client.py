from dotenv import load_dotenv,find_dotenv
import os
from langchain.chat_models import init_chat_model

from config.constants import LLM_CHAT_DEFAULT_TIMEOUT_SEC

# 加载配置文件
# find_dotenv() 确保找到 .env文件 递归查询当前项目文件夹
load_dotenv(find_dotenv())

_base_model = init_chat_model(
    model=os.getenv("DEEPSEEK_V4_FLASH"),
    model_provider="openai",
    # 请求超时（秒）：防止 DeepSeek API 卡死导致整个 agent 挂起
    timeout=LLM_CHAT_DEFAULT_TIMEOUT_SEC,
    # 失败重试次数：避免无限重试拖慢响应
    max_retries=2,
)

# 盘前新闻终态综答专用实例（2026-09-10）：默认客户端 read 超时 60s 会在深拥堵
# TTFT（实测 60-103s）尚未出首 token 时直接掐断流式请求，TTFT 哨兵形同虚设。
# 本实例放宽 read 超时（常量见 config/constants.py），让流式哨兵机制真正生效。
# 🚨 2026-09-10 根治：DeepSeek V4 思考模式【默认开启且 effort=high】（官方文档
# api-docs.deepseek.com/guides/thinking_mode），对 ~8K 结构化 prompt 暗推理 100-120s+
# （流式表现为全空 delta ~122个/s），是"综合推理超时"的根源。综答是给定搜索结果的
# 结构化汇总，无需深度推理——此处显式关闭思考模式，响应从 ~104s 降至数十秒。
# 注意：OpenAI SDK 须走 extra_body；reasoning_effort="none" 不被接受（400）。
from config.constants import PREMARKET_FINAL_LLM_CLIENT_TIMEOUT_SEC

_premarket_final_model = init_chat_model(
    model=os.getenv("DEEPSEEK_V4_FLASH"),
    model_provider="openai",
    timeout=PREMARKET_FINAL_LLM_CLIENT_TIMEOUT_SEC,
    max_retries=2,
    extra_body={"thinking": {"type": "disabled"}},
)

# ======================================================================
# 渐进式工具披露（Progressive Tool Disclosure）Runnable 包装
# ======================================================================
# 业界两阶段范式：
#   阶段0 路由：不暴露 Tool Schema，只在 prompt 末尾注入 ~200 字的极简菜单，
#              让模型输出 __TOOL_ROUTE__:[id...] 选择（零 Schema Token）
#   阶段1 执行：仅注入选中工具子集的完整 Schema，让模型做参数填充
#   阶段2 兜底：若模型尝试调用未披露工具，自动追加后重试（最多 2 次）
#
# 说明：此层对上层 create_deep_agent 零侵入——create_deep_agent 依然接收
# 全量 tools 参数并绑定，但所有真正的 LLM API 请求走到这里时会按阶段裁剪 tools。
from shared.llm_client.tool_router import ProgressiveToolDisclosureModel
model = ProgressiveToolDisclosureModel(_base_model, verbose=True)
