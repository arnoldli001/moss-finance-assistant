from agent.prompts import sub_agents_content
from tools.ragflow_tools import search_knowledge_base

knowledge_base_agent = {
    "name":sub_agents_content['ragflow']['name'],
    "description":sub_agents_content['ragflow']['description'],
    "system_prompt":sub_agents_content['ragflow']['system_prompt'],
    "tools":[search_knowledge_base]
}