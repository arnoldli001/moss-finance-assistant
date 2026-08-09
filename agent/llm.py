from dotenv import load_dotenv,find_dotenv
import os
from langchain.chat_models import init_chat_model

# 加载配置文件
# find_dotenv() 确保找到 .env文件 递归查询当前项目文件夹
load_dotenv(find_dotenv())

model = init_chat_model(
    model=os.getenv("DEEPSEEK_V4_FLASH"),
    model_provider="openai",
    # 请求超时（秒）：防止 DeepSeek API 卡死导致整个 agent 挂起
    timeout=60,
    # 失败重试次数：避免无限重试拖慢响应
    max_retries=2,
)