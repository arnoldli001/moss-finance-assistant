from ragflow_sdk import RAGFlow
import requests
import time

class MyRAGFlow(RAGFlow):
    def __init__(self, api_key, base_url, ima_client_id, ima_api_key, version='v1'):
        # 初始化父类
        super().__init__(api_key, base_url, version)
        # 存储 IMA 的凭证（使用传入的参数，而非重新读取环境变量）
        self.ima_client_id = ima_client_id
        self.ima_api_key = ima_api_key
        # 缓存知识库列表，避免每次搜索都发请求
        self._kb_cache = None

    def _ima_headers(self):
        """IMA API 请求头"""
        return {
            "ima-openapi-clientid": self.ima_client_id,
            "ima-openapi-apikey": self.ima_api_key,
            "Content-Type": "application/json"
        }

    def _request_with_retry(self, method, url, headers, json=None, params=None, timeout=15, retries=2):
        """带重试和状态码检查的 HTTP 请求封装。
        每次失败后等待 1 秒再重试。超时/网络错误/5xx 都会重试。
        """
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                if method == "POST":
                    resp = requests.post(url, json=json, headers=headers, timeout=timeout)
                else:
                    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
                if resp.status_code >= 500:
                    last_err = f"HTTP {resp.status_code}"
                    print(f"[IMA] {url} 第{attempt}次失败: HTTP {resp.status_code}，{'重试中...' if attempt < retries else '已达最大重试次数'}")
                    if attempt < retries:
                        time.sleep(1)
                    continue
                if resp.status_code >= 400:
                    print(f"[IMA] {url} 客户端错误: HTTP {resp.status_code} - {resp.text[:200]}")
                    return None
                return resp
            except requests.exceptions.Timeout:
                last_err = f"请求超时({timeout}s)"
                print(f"[IMA] {url} 第{attempt}次超时({timeout}s)，{'重试中...' if attempt < retries else '已达最大重试次数'}")
                if attempt < retries:
                    time.sleep(1)
            except (requests.exceptions.ConnectionError, ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
                last_err = f"连接错误: {e}"
                print(f"[IMA] {url} 第{attempt}次连接错误({type(e).__name__})，{'重试中...' if attempt < retries else '已达最大重试次数'}")
                if attempt < retries:
                    time.sleep(1)
            except Exception as e:
                last_err = f"未知错误: {e}"
                print(f"[IMA] {url} 第{attempt}次异常: {e}")
                if attempt < retries:
                    time.sleep(1)
        print(f"[IMA] {url} 全部重试失败: {last_err}")
        return None

    def list_knowledge_bases(self, use_cache=True):
        """获取 IMA 知识库列表，返回 [{id, name}, ...]"""
        if use_cache and self._kb_cache is not None:
            return self._kb_cache
        url = "https://ima.qq.com/openapi/wiki/v1/get_addable_knowledge_base_list"
        resp = self._request_with_retry("POST", url, self._ima_headers(), json={"cursor": "", "limit": 50}, timeout=10)
        if resp is None:
            print("[IMA] 获取知识库列表失败")
            return []
        data = resp.json()
        if data.get("code") != 0:
            print(f"[IMA] 获取知识库列表业务错误: code={data.get('code')}, msg={data.get('msg', '')}")
            return []
        self._kb_cache = data.get("data", {}).get("addable_knowledge_base_list", [])
        return self._kb_cache

    def search_knowledge(self, query, knowledge_base_id, cursor="", limit=3):
        """在指定知识库中搜索知识内容，返回 info_list（含 media_id、title）"""
        url = "https://ima.qq.com/openapi/wiki/v1/search_knowledge"
        payload = {"query": query, "cursor": cursor, "knowledge_base_id": knowledge_base_id, "limit": limit}
        t0 = time.time()
        resp = self._request_with_retry("POST", url, self._ima_headers(), json=payload, timeout=15)
        if resp is None:
            print(f"[IMA] search_knowledge 失败 (query={query})")
            return {}
        elapsed = time.time() - t0
        result = resp.json()
        info_count = len(result.get("data", {}).get("info_list", []))
        print(f"[IMA] search_knowledge 完成 ({elapsed:.1f}s, 返回{info_count}条, query={query})")
        return result

    def get_media_info(self, media_id, knowledge_base_id):
        """获取媒体详情，返回 notebook_id 和 media_type"""
        url = "https://ima.qq.com/openapi/wiki/v1/get_media_info"
        payload = {"media_id": media_id, "knowledge_base_id": knowledge_base_id}
        resp = self._request_with_retry("POST", url, self._ima_headers(), json=payload, timeout=10)
        if resp is None:
            print(f"[IMA] get_media_info 失败 (media_id={media_id})")
            return {}
        return resp.json()

    def get_doc_content(self, doc_id):
        """获取笔记正文内容，返回 content 字符串"""
        url = "https://ima.qq.com/openapi/note/v1/get_doc_content"
        t0 = time.time()
        resp = self._request_with_retry("GET", url, self._ima_headers(), params={"doc_id": doc_id}, timeout=15)
        if resp is None:
            print(f"[IMA] get_doc_content 失败 (doc_id={doc_id})")
            return ""
        elapsed = time.time() - t0
        data = resp.json()
        if data.get("code") != 0:
            print(f"[IMA] get_doc_content 业务错误: code={data.get('code')}, msg={data.get('msg', '')}")
            return ""
        content = data.get("data", {}).get("content", "")
        print(f"[IMA] get_doc_content 完成 ({elapsed:.1f}s, 正文{len(content)}字)")
        return content

    def _download_and_extract_text(self, url_info, media_type, title=""):
        """从 url_info 下载文件二进制并提取文本内容。

        支持:
        - media_type=1 (PDF): 使用 pypdf 提取文本
        - media_type=3 (Word/docx): 使用 python-docx 提取文本
        - media_type=7 (Markdown) / 13 (TXT): 直接 UTF-8 解码
        """
        import io

        url = url_info.get("url", "")
        headers = url_info.get("headers") or {}
        if not url:
            print(f"[IMA] url_info.url 为空，无法下载 (title={title})")
            return ""

        t0 = time.time()
        content_bytes = None
        # 下载部分做连接重置重试
        for attempt in range(1, 4):
            try:
                # 合并 IMA 认证头和下载所需的额外头
                download_headers = {**self._ima_headers(), **headers}
                resp = requests.get(url, headers=download_headers, timeout=30)
                if resp.status_code != 200:
                    print(f"[IMA] 下载失败: HTTP {resp.status_code} (title={title})")
                    return ""
                content_bytes = resp.content
                if attempt > 1:
                    print(f"[IMA] 下载第{attempt}次重试成功 (title={title})")
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                    ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
                backoff = 2 ** attempt
                print(f"[IMA] 下载连接异常，第{attempt}次重试 (等待{backoff}s): {type(e).__name__} (title={title})")
                time.sleep(backoff)
            except Exception as e:
                print(f"[IMA] 下载异常 (title={title}): {type(e).__name__}: {e}")
                return ""
        if content_bytes is None:
            print(f"[IMA] 下载重试3次全部失败 (title={title})")
            return ""

        try:

            # 根据媒体类型提取文本
            text = ""
            if media_type == 1:
                # PDF → pypdf
                from pypdf import PdfReader
                reader = PdfReader(io.BytesIO(content_bytes))
                text = "\n".join(page.extract_text() or "" for page in reader.pages)
            elif media_type == 3:
                # Word/docx → python-docx
                from docx import Document
                doc = Document(io.BytesIO(content_bytes))
                text = "\n".join(para.text for para in doc.paragraphs if para.text.strip())
            elif media_type in (7, 13):
                # Markdown / TXT → 直接解码
                text = content_bytes.decode("utf-8", errors="ignore")
            else:
                print(f"[IMA] 不支持的媒体类型 media_type={media_type}，跳过正文提取 (title={title})")
                return ""

            elapsed = time.time() - t0
            print(f"[IMA] 下载+提取完成 ({elapsed:.1f}s, 下载{len(content_bytes)}字节, 提取{len(text)}字, title={title})")
            return text
        except Exception as e:
            print(f"[IMA] 下载/提取异常 (title={title}): {type(e).__name__}: {e}")
            return ""

    def search_knowledge_with_content(self, query, knowledge_base_id, limit=3, max_content=2):
        """搜索知识库并获取正文内容。
        优化：只对前 max_content 条结果获取正文（三步调用），其余只保留标题。
        使用 ThreadPoolExecutor 并行获取正文，大幅减少总耗时。
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        t_total = time.time()
        search_result = self.search_knowledge(query, knowledge_base_id, limit=limit)
        if search_result.get("code") != 0:
            print(f"[IMA] search_knowledge 业务错误: code={search_result.get('code')}, msg={search_result.get('msg', '')}")
            return []

        info_list = search_result.get("data", {}).get("info_list", [])
        print(f"[IMA] 搜索返回 {len(info_list)} 条，开始获取正文（前{max_content}条）")

        def fetch_content(item):
            """对单条结果执行 get_media_info → 获取正文"""
            title = item.get("title", "")
            entry = {
                "title": title,
                "media_id": item.get("media_id", ""),
                "content": "",
            }
            try:
                media_info = self.get_media_info(item["media_id"], knowledge_base_id)
                if media_info.get("code") == 0:
                    data = media_info.get("data", {})
                    media_type = data.get("media_type", 0)
                    if media_type == 11:
                        # 笔记类型：通过 get_doc_content 获取结构化文本
                        notebook_id = data.get("notebook_ext_info", {}).get("notebook_id", "")
                        if notebook_id:
                            entry["content"] = self.get_doc_content(notebook_id)
                        else:
                            print(f"[IMA] media_type=11 但无 notebook_id (title={title})")
                    else:
                        # PDF/docx/Markdown/TXT 等：通过 url_info 下载二进制并提取文本
                        url_info = data.get("url_info", {})
                        if url_info and url_info.get("url"):
                            entry["content"] = self._download_and_extract_text(url_info, media_type, title)
                        else:
                            print(f"[IMA] media_type={media_type} 无 url_info，无法获取正文 (title={title})")
                else:
                    print(f"[IMA] get_media_info 业务错误: code={media_info.get('code')} (title={title})")
            except Exception as e:
                print(f"[IMA] fetch_content 异常 (title={title}): {type(e).__name__}: {e}")
            return entry

        # 只对前 max_content 条并行获取正文，其余只保留标题
        to_fetch = info_list[:max_content]
        rest = info_list[max_content:]

        results = []
        # 并行获取正文
        if to_fetch:
            with ThreadPoolExecutor(max_workers=min(len(to_fetch), 4)) as executor:
                futures = {executor.submit(fetch_content, item): i for i, item in enumerate(to_fetch)}
                done = {}
                for future in as_completed(futures):
                    done[futures[future]] = future.result()
                for i in range(len(to_fetch)):
                    if i in done:
                        results.append(done[i])

        # 其余只保留标题
        for item in rest:
            results.append({
                "title": item.get("title", ""),
                "media_id": item.get("media_id", ""),
                "content": "",
            })

        elapsed_total = time.time() - t_total
        has_content = sum(1 for r in results if r["content"])
        print(f"[IMA] search_knowledge_with_content 全部完成 ({elapsed_total:.1f}s, {has_content}/{len(results)}条有正文)")
        return results
