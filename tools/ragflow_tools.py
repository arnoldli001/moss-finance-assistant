#  search_knowledge_base 通过 IMA API 搜索个人知识库
from __future__ import annotations

from contextvars import ContextVar
from langchain_core.tools import tool
from tools.MyRAGFlow import MyRAGFlow
from api.monitor import monitor
from rawflow.rag_config import _load_ragflow_env
import json

# 创建一个 IMA 知识库客户端
api_key, base_url, client_id = _load_ragflow_env()
ragflow_client = MyRAGFlow(
    api_key=api_key,
    base_url=base_url or 'https://ima.qq.com/openapi/wiki/v1',
    ima_client_id=client_id,
    ima_api_key=api_key,
)

# 单次搜索返回结果上限，防止上下文爆炸
MAX_RESULTS = 3
# 每个会话(per-request)的调用次数上限，防止 agent 循环调用
#
# 修复说明（并发隔离）：
#   原实现使用模块级变量 `_call_count = 0`，当用户 A 与用户 B 并发请求时，
#   全局计数器被共享：
#       时序: A调用+1(1) → B调用+1(2) → A调用+1(3) → A被第4次调用时拒绝
#       后果: A/B 互相冲减对方计数配额，可能导致某用户第1次调用就被限流
#   改为使用 ContextVar（协程级局部变量），每个 asyncio Task（=会话）独立。
MAX_CALLS = 3

# Per-session 计数器：每个请求链路（asyncio Task）独立
_rag_call_count_ctx: ContextVar[int] = ContextVar("ragflow_call_count", default=0)


def reset_call_count() -> None:
    """每次新请求开始时重置计数器（当前会话独立）。"""
    _rag_call_count_ctx.set(0)


def _get_call_count() -> int:
    return _rag_call_count_ctx.get()


def _inc_call_count() -> int:
    """自增当前会话的计数器并返回新值。"""
    new_val = _rag_call_count_ctx.get() + 1
    _rag_call_count_ctx.set(new_val)
    return new_val


@tool
def search_knowledge_base(query: str, knowledge_base_name: str = "") -> str:
    """
    搜索个人IMA知识库，获取企业内部专有知识。
    当需要查询互联网上不会流通的内部文档、研报、手册等内容时使用此工具。
    传入查询问题，返回知识库中匹配的内容片段。

    :param query: 要查询的问题或关键词
    :param knowledge_base_name: 可选，指定知识库名称（如"国产替代"）。不传则搜索所有知识库。
    :return: 知识库检索到的原始信息
    """
    current_count = _inc_call_count()
    if current_count > MAX_CALLS:
        return (
            f"已达搜索次数上限（{MAX_CALLS}次），"
            f"请基于已获取的搜索结果回答用户问题，不要再调用此工具。"
        )

    monitor.report_tool(tool_name="IMA知识库搜索工具：search_knowledge_base", args={"query": query, "knowledge_base_name": knowledge_base_name})

    try:
        # 1. 获取知识库列表（带缓存）
        kb_list = ragflow_client.list_knowledge_bases()
        if not kb_list:
            return "无法获取知识库列表，请检查IMA API配置"

        # 2. 筛选要搜索的知识库
        if knowledge_base_name:
            targets = [kb for kb in kb_list if knowledge_base_name in kb.get("name", "")]
            if not targets:
                kb_names = ", ".join(kb["name"] for kb in kb_list)
                return f"未找到名称包含'{knowledge_base_name}'的知识库。可用知识库：{kb_names}"
        else:
            targets = kb_list

        # 3. 逐个知识库搜索并获取正文，总量不超过 MAX_RESULTS
        all_results = []
        for kb in targets:
            if len(all_results) >= MAX_RESULTS:
                break
            # search_knowledge_with_content: 并行获取正文，对所有结果（limit条）都尝试获取
            entries = ragflow_client.search_knowledge_with_content(query, kb["id"], limit=3, max_content=3)
            for entry in entries:
                if len(all_results) >= MAX_RESULTS:
                    break
                all_results.append({
                    "knowledge_base": kb["name"],
                    "title": entry["title"],
                    "content": entry["content"][:2000] if entry["content"] else "(无正文内容)",
                })

        if not all_results:
            kb_names = ", ".join(kb["name"] for kb in targets)
            return f"知识库[{kb_names}]中没有找到与'{query}'相关的内容"

        # 4. 格式化输出
        output_parts = [f"共找到 {len(all_results)} 条结果："]
        for i, item in enumerate(all_results, 1):
            output_parts.append(f"--- 结果 {i} [来源: {item['knowledge_base']}] ---\n标题: {item['title']}\n内容: {item['content']}")
        return "\n\n".join(output_parts)

    except Exception as e:
        return f"知识库查询异常: {str(e)}"
