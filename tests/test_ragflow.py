"""
测试 RAGFlow / IMA 知识库 OpenAPI 接口连通性。
运行方式：python tests/test_ragflow.py
注意：需要在 .env 中配置 IMA_CLIENT_ID 和 IMA_API_KEY。
"""
import os
import sys
import requests
from pathlib import Path

# 测试文件位于 tests/ 子目录，需要向上一级找到项目根目录
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# 加载 .env 配置
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


def test_ima_knowledge_base_search():
    """验证 IMA 知识库 OpenAPI /search_knowledge_base 接口连通性"""
    print("\n" + "=" * 60)
    print("【测试1】IMA 知识库搜索接口连通性")
    print("=" * 60)

    url = "https://ima.qq.com/openapi/wiki/v1/search_knowledge_base"
    client_id = os.environ.get("IMA_CLIENT_ID")
    api_key = os.environ.get("IMA_API_KEY")

    if not client_id or not api_key:
        print("   ⚠️ 跳过：未配置 IMA_CLIENT_ID 或 IMA_API_KEY")
        return False

    headers = {
        "ima-openapi-clientid": client_id,
        "ima-openapi-apikey": api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "query": "测试",
        "cursor": "",
        "limit": 5
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        print(f"   HTTP 状态码: {response.status_code}")
        print(f"   响应内容: {response.text[:500]}")
        assert response.status_code == 200, f"接口返回非 200: {response.status_code}"
        print("   ✅ IMA 知识库接口连通正常")
        return True
    except requests.RequestException as e:
        print(f"   ❌ 请求异常: {e}")
        return False


def main():
    print("\n" + "*" * 60)
    print("  MOSS Finance Assistant - RAGFlow/IMA 知识库接口测试")
    print("*" * 60)
    ok = test_ima_knowledge_base_search()
    print("\n" + "=" * 60)
    print(f"汇总: {'✅ 通过' if ok else '⚠️ 跳过或失败'}")
    print("=" * 60)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
