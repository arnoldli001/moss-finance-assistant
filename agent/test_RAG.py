import requests
import os

url = "https://ima.qq.com/openapi/wiki/v1/search_knowledge_base"
headers = {
    "ima-openapi-clientid": os.environ.get("IMA_CLIENT_ID"),
    "ima-openapi-apikey": os.environ.get("IMA_API_KEY"),
    "Content-Type": "application/json"
}
payload = {
    "query": "",
    "cursor": "",
    "limit": 5
}

response = requests.post(url, json=payload, headers=headers)
print(response.status_code)
print(response.json())