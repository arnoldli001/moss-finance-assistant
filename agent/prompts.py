# 目标加载yml中的数据，供创建主和子智能体使用
import yaml # yaml配置文件读取
from pathlib import Path
from typing import Any

# 定义一个加载函数，配置文件yaml加载成字典
def load_yaml(file_path) -> dict:
    """
    加载指定位置的yaml配置文件
    :param file_path:  加载的文件的地址
    :return:  返回的加载结果 本质就是字典
    """
    with open(file_path, 'r', encoding='utf-8') as f :
        # safe_load 只会加载，不会触发！
        # load 加载过程中可能无意执行内部的嵌入函数！！ 可能发生注入脚本攻击
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"YAML 配置文件格式错误，期望字典: {file_path}")
    return data

# 尝试读取主和子智能体的配置文件和数据（供后续使用）
# 项目的根地址
# project_root_path  = Path(__file__).parent.parent
project_root_path  = Path(__file__).parents[1] # prompts -> parents -> [agent , moss_finance_assistant]
yaml_file_path = project_root_path / "prompt" / "prompts.yml"

prompt_yaml_content = load_yaml(yaml_file_path)


# main_agent_content
main_agent_content = prompt_yaml_content["main_agent"]
# sub_agents_content
sub_agents_content = prompt_yaml_content["sub_agents"]


# ======================================================================
# Runtime Prompts 访问器
# ======================================================================
# 散落在各 .py 中的运行时提示词已统一抽取到 prompts.yml 的 runtime_prompts 段。
# 通过 get_runtime_prompt(key) 获取模板，再用 str.format_map(SafeDict(**vars)) 填充变量。
# SafeDict 允许变量缺失时保留 {var_name} 原文，不抛 KeyError。
# ======================================================================

class _SafeDict(dict):
    """str.format_map 用的 dict 子类：缺失的 key 保留 {key} 原文，不抛 KeyError。"""
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


_runtime_prompts: dict = prompt_yaml_content.get("runtime_prompts", {}) or {}


def get_runtime_prompt(key: str) -> str:
    """
    根据点分 key 获取运行时提示词模板字符串。
    例如：get_runtime_prompt("main_agent.path_instruction")
    若 key 不存在，抛 KeyError 提醒开发者检查 prompts.yml。
    """
    if key not in _runtime_prompts:
        raise KeyError(
            f"运行时提示词 '{key}' 未在 prompt/prompts.yml 的 runtime_prompts 段中定义。"
            f"可用 keys: {sorted(_runtime_prompts.keys())}"
        )
    return _runtime_prompts[key]


def format_prompt(key: str, **kwargs: Any) -> str:
    """
    一步到位：获取提示词模板并填充变量。
    缺失的变量保留 {var_name} 原文，不抛异常。
    """
    tpl = get_runtime_prompt(key)
    return tpl.format_map(_SafeDict(**kwargs))