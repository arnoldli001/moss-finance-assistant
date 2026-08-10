from agent.subagents.knowledge_base_agent import knowledge_base_agent
from agent.subagents.database_query_agent import database_query_agent
from agent.subagents.network_search_agent import network_search_agent
# 使用 SQLite 持久化 checkpointer：重启后对话历史不丢失，按 thread_id (= session_id) 隔离
# 注意：agent 用 astream (异步)，必须用 AsyncSqliteSaver；同步版 SqliteSaver 不支持 async 方法
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

# main_agent tool导入
from tools.markdown_tools import generate_markdown
from tools.pdf_tools import convert_md_to_pdf
from tools.upload_file_read_tool import read_file_content

from deepagents import create_deep_agent

from agent.llm import model
from agent.prompts import main_agent_content

from api.monitor import monitor
import re
import asyncio
import uuid
import shutil
from pathlib import Path

from api.context import set_session_context, reset_session_context, set_thread_context

from langchain_core.messages import AIMessage

# 【工作环境指令】及后续"规则/工作目录/上传文件"段落的过滤正则
# 匹配：换行 + 缩进空格 + 【工作环境指令】开始直到结尾的整段内容（包含上传文件、规则1-4等）
_HIDE_PROMPT_RE = re.compile(
    r"(?:\r?\n)\s*【工作环境指令】[\s\S]*$"
)

def _strip_hidden_instructions(content: str) -> str:
    """把拼接在用户消息末尾的【工作环境指令】等规则段剥离，返回纯净的用户内容。"""
    if not content:
        return ""
    s = _HIDE_PROMPT_RE.sub("", content)
    return s.strip()

# 持久化 checkpointer：消息存到 data/checkpointer.db，跨进程/重启保留
# AsyncSqliteSaver 必须在事件循环内初始化（需要 aiosqlite 连接），故 agent 改为惰性创建
import aiosqlite
_project_root = Path(__file__).resolve().parents[1]
_data_dir = _project_root / "data"
_data_dir.mkdir(parents=True, exist_ok=True)
_checkpointer_db = _data_dir / "checkpointer.db"

# 全局变量，在 get_main_agent() 中惰性初始化
_main_agent = None
_checkpointer = None
_async_sqlite_conn = None


async def get_main_agent():
    """惰性初始化并返回 main_agent。首次调用在事件循环内建立 aiosqlite 连接 + 建表。"""
    global _main_agent, _checkpointer, _async_sqlite_conn
    if _main_agent is None:
        # aiosqlite.connect 返回 Connection 对象，需在事件循环中 __aenter__ 才真正连接
        _async_sqlite_conn = aiosqlite.connect(str(_checkpointer_db))
        await _async_sqlite_conn.__aenter__()
        _checkpointer = AsyncSqliteSaver(_async_sqlite_conn)
        # 建表（幂等）
        await _checkpointer.setup()
        _main_agent = create_deep_agent(
            model=model,  # type: ignore[arg-type]
            system_prompt=main_agent_content['system_prompt'],
            tools=[generate_markdown, convert_md_to_pdf, read_file_content],
            checkpointer=_checkpointer,
            subagents=[  # type: ignore[arg-type]
                database_query_agent,
                network_search_agent,
                knowledge_base_agent
            ]
        )
        print("[main_agent] AsyncSqliteSaver 初始化完成，agent 已就绪")
    return _main_agent

# 执行
"""
  1. 执行主智能体 一定选异步，原因：对应多个客户端
  2. 什么时候触发我们智能体的调用或者执行？？？
  3. 客户端 -》 api/task -> fastapi 接口 -》 异步执行 -》 main_agent的运行 （异步方法）
  4. main_agent执行stream流式处理 -》 调用工具 -》 已经埋好了点  
                                   调用子智能体 -》 结果解析 -》 name = task -> monitor -> 发送子智能体
                                   调用最终结果 -》 结果 -》 monitor -> 发送结果的方法
                                   开启调用以后 -》 当前会话 -》 文件夹地址 -》 推送到前端
"""



project_root_path = Path(__file__).parents[1].resolve() # 绝对 解析路径标识以及软连接
# project_root_path = Path(__file__).parents[1].absolute() # 绝对
# main_agent.invoke()
# main_agent.stream()
# main_agent.astream() [选他]
async def run_deep_agent(task_query,session_id,user_id=None):
    """
    定义流式+异步执行主智能体！！
    执行过程中，返回  会话文件化返回  调用子智能体  调用最终结果 （monitor）
    task_query: 前端提问的问题
    session_id: 每个前端会话对应的标识 （1.存储session_id ContextVars 2.session_id 给他创建对应的output输出地址）
                 同时作为 LangGraph thread_id，由 SqliteSaver 持久化对话历史，实现连续对话
    user_id: 可选，所属用户 ID；传入时会更新会话标题与时间戳
    """
    print(f"当前会话的main_agent开始执行了！ 会话id:{session_id} user:{user_id}")
    # 重置 IMA 知识库搜索计数器，防止上一个请求的计数影响当前请求
    from tools.ragflow_tools import reset_call_count
    reset_call_count()
    # 更新会话元数据：首条消息时自动生成标题（user_id+关键词+日期），刷新 updated_at
    if user_id:
        try:
            from api import storage
            existing = storage.get_session(session_id)
            if existing and (existing.get("title") in (None, "", "新会话")):
                auto_title = storage.generate_default_title(user_id, task_query)
                storage.update_session_title(session_id, auto_title)
            storage.touch_session(session_id)
        except Exception as e:
            print(f"[main_agent] 更新会话元数据失败（不影响主流程）: {e}")
    # 准备工作 【1. session_dir（前端） 2. relative_session_dir (大模型) 3. 上传的文件拼接上传文件专属提示词】
    # project_root_path / output / session_session_id(uuid)
    # 当前会话存储生成文件的专属文件夹
    session_dir = project_root_path / "output" / f"session_{session_id}"
    # 文件夹可能没有，第一次请求要创建
    session_dir.mkdir(parents=True, exist_ok=True)
    # \  \n \t -> /
    session_dir_str = str(session_dir).replace("\\","/")
    # 获取相对文件夹
    # session_dir : project_root_path / output / session_session_id(uuid)
    # project_root_path : project_root_path
    # relative_session_dir_str: / output / session_session_id(uuid)
    relative_session_dir_str = str(session_dir.relative_to(project_root_path)).replace("\\","/")

    #处理上传文件 （updated / session_session_id）
    updated_dir_path = project_root_path / "updated" / f"session_{session_id}"
    updated_info_prompt = "" # 有上传文件，拼接上传文件专属解析位置的提示词
    if updated_dir_path.exists():
        files = [ f.name  for f in updated_dir_path.iterdir()  if f.is_file()]
        # 将上传文件统一赋值到 output_dir 方便前端统一读取 session_dir
        if files:
            for filename in files:
                # 将原文件 -》 复制 -》 目标文件中  （copy2 保留原文件修改时间和权限等元数据）
                shutil.copy2(updated_dir_path / filename, session_dir / filename)
            # 构建提示词！告诉大模型，有上传文件，你要读取上传文件！！
            updated_info_prompt = (f"\n    [已上传文件] 已加载到工作目录:\n" +
                             "\n".join([f"    - {f}" for f in files]) +
                             "\n    请优先使用工具（read_file_content）读取并参考这些文件。")

    # 继续准备 1. 当前会话的对应的session_id session_dir 存储到contextVars [后续工具获取，socket -> 推送消息] 2.调用monitor给前端推送session_dir信息
    session_dir_token = set_session_context(session_dir_str)  # 存储的当前会话对应的文件夹地址
    session_id_token = set_thread_context(session_id)  #获取当前会话的session_id对应socket

    monitor.report_session_dir(session_dir_str)  # 当前会话对应的文件夹地址推送给起前端！

    # 执行main_agent
    config = {
        "configurable":{
            "thread_id":session_id
        }
    }

    # 构建提示词
    path_instruction = f"""
    【工作环境指令】
    工作目录: {relative_session_dir_str}
    {updated_info_prompt}

    规则：
    1. 新生成文件必须保存到工作目录：'{relative_session_dir_str}/filename'
    2. 读取已上传的文件时，请直接将文件名（例如：'开篇.txt'）作为 filename 参数传入（read_file_content）读取工具，不要带上任何目录前缀。
    3. 使用相对路径，禁止使用绝对路径
    4. 若存在上传文件，请先分析内容
    """
    # 反馈结果
    try:
        # 惰性获取 agent（首次调用会初始化 AsyncSqliteSaver）
        agent = await get_main_agent()
        # 执行
        async for chunk in agent.astream({
            "messages":[
                {
                    "role":"user","content":task_query+path_instruction
                }
            ]
        },config=config,recursion_limit=50):  # type: ignore[call-arg]
            # {"model [大模型决定调用工具 子智能体  最终结果] / tools" : {messages:[xxx...]}}
            for node_name,state in chunk.items():
                if not state or "messages" not in state: continue
                messages = state["messages"]
                if messages and isinstance(messages,list):
                    last_msg = messages[-1]
                    if node_name == 'model':
                        if last_msg.tool_calls:
                            # 工具和子智能体
                            for tool_call in last_msg.tool_calls:
                                """
                                  tool_call = {
                                      name: task
                                      args:{
                                          subagent_type:子智能体的名字
                                          description:子智能体的描述
                                      }
                                  }                                
                                """
                                if tool_call['name'] == 'task':
                                    # 调用某个子智能体
                                    sub_name = tool_call['args']['subagent_type']
                                    monitor.report_assistant(sub_name,{'description':tool_call['args']['description']})
                                    # 子智能体即将进入 LLM 思考阶段（决定调用哪些工具），提前告知前端，消除 tool_start 前的日志空白期
                                    monitor.report_thinking(sub_name)
                        elif last_msg.content:
                            # 最终结果
                            print(f"主智能体执行结果，最终结果：{last_msg.content[:5000]}")
                            monitor.report_task_result(last_msg.content)

    except Exception as e :
        # 报错推送错误信息给前端
        monitor._emit("error",f"执行主智能发生异常信息：{str(e)}")
    finally:
        # 释放存储的地址和session_id
        reset_session_context(session_dir_token, session_id_token)


async def get_session_history(session_id: str, limit: int = 50):
    """从 LangGraph checkpointer 中读取指定会话的对话历史。

    用于前端切换会话时恢复聊天记录。返回 [{role, content, type}, ...]。
    只返回 user / assistant 的可见消息，过滤掉 tool 内部消息。
    """
    try:
        agent = await get_main_agent()
        config = {"configurable": {"thread_id": session_id}}
        # aget_state_history 从新到旧迭代，取最新的一个快照即可（它包含完整消息列表）
        latest_state = None
        async for chunk in agent.aget_state_history(config):  # type: ignore[arg-type]
            latest_state = chunk
            break  # 只取第一个（最新）
        if not latest_state or not latest_state.values:
            return []
        msgs = []
        for m in latest_state.values.get("messages", []):
            role = None
            content = ""
            # 兼容 dict / BaseMessage 两种形态
            if isinstance(m, dict):
                role = m.get("role") or m.get("type")
                content = m.get("content", "")
            else:
                role = getattr(m, "type", None)
                content = getattr(m, "content", "")
            # 只保留用户与助手可见消息，跳过 tool / tool_call 内部消息
            if role in ("user", "human"):
                # 剥掉拼接在用户问题末尾的【工作环境指令】段，避免前端显示规则
                cleaned = _strip_hidden_instructions(content)
                if cleaned:  # 被完全剥离的情况（如测试数据）不显示
                    msgs.append({"role": "user", "content": cleaned, "type": "user"})
            elif role in ("assistant", "ai") and content and not getattr(m, "tool_calls", None):
                # 盘前小作文热度总结以特定标题开头，标记 type 供前端靠右显示
                msg_type = "zsxq" if content.startswith("知识星球财经资讯分析总结") else "assistant"
                msgs.append({"role": "assistant", "content": content, "type": msg_type})
        return msgs
    except Exception as e:
        print(f"[main_agent] 读取会话历史失败: {e}")
        return []

