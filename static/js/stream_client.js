
(function (global) {
  "use strict";

  /**
   * EventSourceBuffer —— 后端 SSE（text/event-stream）的消费器。
   *
   * @param {Object} opts
   * @param {string} opts.url                     后端 SSE 端点
   * @param {Object} opts.body                    请求体 JSON 对象（发送前 JSON.stringify）
   * @param {Object} [opts.headers]               额外 headers
   * @param {string} [opts.method="POST"]         HTTP 方法
   * @param {number} [opts.connectTimeoutMs=5000] 建连超时（首字节到达之前）
   * @param {number} [opts.idleTimeoutMs=0]       空闲超时：收不到任何帧多少 ms 触发错误；0=关
   * @param {boolean}[opts.retry=true]            是否断线自动重连（断点续传默认开启）
   * @param {number} [opts.maxRetries=3]          最多重连次数
   * @param {number} [opts.baseDelayMs=800]       重连指数退避基底
   * @param {string} [opts.stopEndpoint="/api/task/stop"]  主动取消端点
   * @param {boolean}[opts.idempotentByEventId=true] 启用 event_id 级幂等去重（默认开：
   *                                                  避免 replay 回放时把已处理事件再追加到 DOM 一遍）
   */
  function EventSourceBuffer(opts) {
    if (!opts || !opts.url) throw new Error("[ESB] url 必传");
    this.opts = Object.assign({
      method: "POST",
      headers: { "Content-Type": "application/json" },
      connectTimeoutMs: 5000,
      idleTimeoutMs: 0,
      retry: true,
      maxRetries: 3,
      baseDelayMs: 800,
      stopEndpoint: "/api/task/stop",
      idempotentByEventId: true,
    }, opts || {});
    if (!this.opts.body || typeof this.opts.body !== "object") {
      throw new Error("[ESB] body 必须是对象");
    }

    // 事件回调：type -> [fn, fn, ...]
    this._handlers = Object.create(null);
    // wildcard: [fn,fn,...]
    this._wildcards = [];
    // 状态机
    this.state = "idle"; // idle / connecting / streaming / done / error / cancelled
    // 当前正在运行的 fetch controller（abort 用）
    this._ctrl = null;
    // 当前正在运行的 fetch promise（引用以便检测 leak）
    this._fetchPromise = null;
    // 重连尝试次数
    this._retries = 0;
    // 空闲定时器句柄
    this._idleTimer = null;
    // 最后一次收到帧的时间戳
    this._lastFrameAt = 0;
    // 清理函数（beforeunload 绑定）——只绑定一次
    this._beforeUnloadHandler = null;
    // SSE last-event-id（断点续传：重连请求带 Last-Event-ID）
    this._lastEventId = "";
    // 幂等去重集：已经 emit 过的 event id，重放阶段直接跳过
    this._seenEventIds = (typeof Set !== "undefined") ? new Set() : Object.create(null);
    // 最多保留多少 id：环形兜底，避免极端长流内存线性长（5000 ≈ 150KB）
    this._seenMax = 5000;
    // lastEventId 写入 body 的兜底路径（与服务端 STREAM_RESUME_BODY_LAST_EVENT_ID_ALLOW 对应）
    this._bodyLastEventIdFallback = true;
  }

  // ---------- 事件订阅 API ----------
  EventSourceBuffer.prototype.on = function (type, handler) {
    if (typeof handler !== "function") return this;
    if (type === "*") {
      this._wildcards.push(handler);
      return this;
    }
    (this._handlers[type] || (this._handlers[type] = [])).push(handler);
    return this;
  };
  EventSourceBuffer.prototype.off = function (type, handler) {
    if (type === "*") {
      if (!handler) this._wildcards.length = 0;
      else this._wildcards = this._wildcards.filter((f) => f !== handler);
      return this;
    }
    var list = this._handlers[type];
    if (!list) return this;
    if (!handler) list.length = 0;
    else this._handlers[type] = list.filter((f) => f !== handler);
    return this;
  };
  EventSourceBuffer.prototype._emit = function (type, payload, raw) {
    var self = this;
    var arr = this._handlers[type];
    if (arr && arr.length) {
      for (var i = 0; i < arr.length; i++) {
        try { arr[i](payload, raw); } catch (e) { setTimeout(function () { throw e; }, 0); }
      }
    }
    for (var j = 0; j < this._wildcards.length; j++) {
      try { this._wildcards[j](type, payload, raw); }
      catch (e) { setTimeout(function () { throw e; }, 0); }
    }
  };

  // ---------- 启动 ----------
  EventSourceBuffer.prototype.start = function () {
    var self = this;
    if (this.state === "streaming" || this.state === "connecting") return this;
    this.state = "connecting";
    // 绑定 beforeunload（只绑一次）
    if (!this._beforeUnloadHandler) {
      this._beforeUnloadHandler = function (ev) {
        if (self.state === "streaming" || self.state === "connecting") {
          self.abort("page_unload", true);
        }
      };
      if (typeof window !== "undefined" && window.addEventListener) {
        window.addEventListener("beforeunload", this._beforeUnloadHandler);
      }
    }
    this._doFetch();
    return this;
  };

  EventSourceBuffer.prototype._doFetch = function () {
    var self = this;
    // 建连超时定时器
    var connectTimer = null;
    var connectTimedOut = false;

    this._ctrl = typeof AbortController !== "undefined" ? new AbortController() : null;
    var signal = this._ctrl ? this._ctrl.signal : undefined;

    // body：若启用 body 兜底，注入 last_event_id（注意不能改外部引用对象，需 shallow-copy）
    var bodyToSend = Object.assign({}, this.opts.body || {});
    if (this._bodyLastEventIdFallback && this._lastEventId) {
      bodyToSend.last_event_id = this._lastEventId;
    }

    var req = {
      method: this.opts.method,
      headers: Object.assign({}, this.opts.headers || {}),
      body: JSON.stringify(bodyToSend),
      signal: signal,
      // 关闭 fetch 对跨域 credentials 的默认行为（同源默认 same-origin 即可）
      credentials: "same-origin",
    };
    if (this._lastEventId) {
      req.headers["Last-Event-ID"] = this._lastEventId;
    }

    // 建连超时：fetch 本身不支持 timeout，用 AbortController 实现
    if (this.opts.connectTimeoutMs > 0) {
      connectTimer = setTimeout(function () {
        connectTimedOut = true;
        if (self._ctrl) {
          try { self._ctrl.abort(); } catch (e) { /* noop */ }
        }
      }, this.opts.connectTimeoutMs);
    }

    // 空闲超时：每收到一帧重置
    this._resetIdleTimer();

    var p = fetch(this.opts.url, req);
    this._fetchPromise = p;

    p.then(function (resp) {
      if (connectTimer) { clearTimeout(connectTimer); connectTimer = null; }
      if (!resp.ok) {
        // HTTP 4xx/5xx：直接 error
        return resp.text().then(function (txt) {
          self._finalizeWithError("HTTP_" + resp.status,
            "HTTP " + resp.status + ": " + (txt || resp.statusText), false);
        });
      }
      var ct = (resp.headers.get("content-type") || "").toLowerCase();
      if (ct.indexOf("text/event-stream") === -1) {
        return resp.text().then(function (txt) {
          self._finalizeWithError("BAD_CONTENT_TYPE",
            "后端未返回 text/event-stream：Content-Type=" + ct + "; body=" + (txt || "").slice(0, 500),
            false);
        });
      }
      self.state = "streaming";
      self._emit("__streaming", { requestId: resp.headers.get("X-Stream-Request-Id") || "" });
      return self._consumeStream(resp.body);
    }).catch(function (err) {
      if (connectTimer) { clearTimeout(connectTimer); connectTimer = null; }
      // 建连失败/取消/网络错误：区分处理
      if (self.state === "cancelled") {
        // 主动取消 → 已在 abort 里处理
        return;
      }
      if (connectTimedOut) {
        self._finalizeWithError("CONNECT_TIMEOUT",
          "连接超时（>" + self.opts.connectTimeoutMs + "ms）", true);
      } else if (err && err.name === "AbortError") {
        // fetch 被 AbortController 中断：若非 cancel 态，视为网络断开
        self._finalizeWithError("ABORTED", "请求已中断", true);
      } else {
        self._finalizeWithError("NETWORK",
          "网络错误: " + (err && err.message ? err.message : String(err)), true);
      }
    }).finally(function () {
      self._fetchPromise = null;
    });
  };

  // ---------- 消费 ReadableStream，按 "\n\n" 分帧 ----------
  EventSourceBuffer.prototype._consumeStream = function (body) {
    var self = this;
    if (!body || !body.getReader) {
      this._finalizeWithError("NO_READER", "响应 body 不可读（浏览器不支持 ReadableStream？）", false);
      return Promise.resolve();
    }
    var reader = body.getReader();
    var decoder = new (typeof TextDecoder !== "undefined" ? TextDecoder : function () {
      // 极低版本兜底（基本不会触发）
      return { decode: function (buf) { return String.fromCharCode.apply(null, buf); } };
    })("utf-8", { fatal: false });
    var buffer = "";
    var done = false;

    function pump() {
      if (self.state === "cancelled") { reader.cancel().catch(function () { }); return Promise.resolve(); }
      return reader.read().then(function (chunk) {
        if (chunk.done) {
          // 流读完：buffer 中如果还有残余（不以 \n\n 结尾），尝试处理
          if (buffer.length > 0) {
            self._parseEventBlock(buffer);
            buffer = "";
          }
          // 正常完成不代表业务 done（浏览器可能因网络超时提前关）
          // 状态机仍由 last received 的 done/error 事件决定；没收到则视为异常中断
          if (self.state === "streaming") {
            // 未收到 done：视为网络断
            self._finalizeWithError("STREAM_CLOSED_PREMATURELY",
              "SSE 连接被提前关闭（未收到 done/error 事件）", true);
          }
          done = true;
          return;
        }
        var chunkStr = decoder.decode(chunk.value || new Uint8Array(), { stream: true });
        if (chunkStr) {
          buffer += chunkStr;
          // ---------- 核心分帧：按 "\n\n" 切 ----------
          while (true) {
            var sepIdx = buffer.indexOf("\n\n");
            if (sepIdx === -1) break;
            var rawBlock = buffer.slice(0, sepIdx);
            buffer = buffer.slice(sepIdx + 2);
            if (!rawBlock) continue; // 空帧跳过
            self._parseEventBlock(rawBlock);
          }
          // 防御：buffer 过大（比如后端 bug 不发 \n\n）—— 超过 1MB 直接砍，防止内存爆
          if (buffer.length > 1024 * 1024) {
            buffer = buffer.slice(buffer.length - 1024);
            self._emit("__warn", { kind: "buffer_too_large", info: "SSE buffer 超过 1MB，已截断尾部" });
          }
        }
        // 每帧处理完：重置空闲超时
        self._resetIdleTimer();
        return pump();
      }).catch(function (err) {
        if (self.state === "cancelled") return;
        self._finalizeWithError("READ_ERR",
          "ReadableStream 读取失败: " + (err && err.message ? err.message : String(err)),
          true);
      });
    }
    return pump();
  };

  // ---------- 幂等去重：返回 true 表示"已处理过，跳过" ----------
  EventSourceBuffer.prototype._isDuplicateEvent = function (eventId) {
    if (!this.opts.idempotentByEventId || !eventId) return false;
    if (this._seenEventIds instanceof Set) {
      if (this._seenEventIds.has(eventId)) return true;
      this._seenEventIds.add(eventId);
      this._evictOldSeen();
      return false;
    }
    if (this._seenEventIds[eventId]) return true;
    this._seenEventIds[eventId] = true;
    this._evictOldSeen();
    return false;
  };

  EventSourceBuffer.prototype._evictOldSeen = function () {
    if (this._seenEventIds instanceof Set) {
      if (this._seenEventIds.size <= this._seenMax) return;
      // 最旧的迭代器：Set.values() 按插入顺序
      var iter = this._seenEventIds.values();
      var excess = this._seenEventIds.size - Math.floor(this._seenMax * 0.7);
      for (var i = 0; i < excess; i++) {
        var r = iter.next();
        if (r.done) break;
        this._seenEventIds.delete(r.value);
      }
      return;
    }
    var keys = Object.keys(this._seenEventIds);
    if (keys.length <= this._seenMax) return;
    var excess = keys.length - Math.floor(this._seenMax * 0.7);
    for (var j = 0; j < excess; j++) delete this._seenEventIds[keys[j]];
  };

  // ---------- 重置幂等集（收到 gap/ReplayStart.mode=resync 或 full 时调用）----------
  EventSourceBuffer.prototype._resetSeenIds = function () {
    if (this._seenEventIds instanceof Set) { this._seenEventIds.clear(); return; }
    this._seenEventIds = Object.create(null);
  };

  // ---------- 解析一个以 \n 分隔的 SSE 事件块 ----------
  // SSE 规范：每行前缀 "event:" / "id:" / "data:" / ":"(comment)；
  // 多行 "data:" 按顺序拼接后加 \n；我们的后端只发送单行 data，这里做兼容。
  EventSourceBuffer.prototype._parseEventBlock = function (rawBlock) {
    var lines = rawBlock.split(/\r?\n/);
    var eventType = "message";
    var eventId = "";
    var dataLines = [];
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i];
      if (!line) continue;
      if (line.charAt(0) === ":") continue; // comment 跳过
      var colon = line.indexOf(":");
      if (colon === -1) continue;
      var key = line.slice(0, colon);
      var val = line.slice(colon + 1);
      if (val.charAt(0) === " ") val = val.slice(1); // SSE 允许 ": " 后的值首空格
      switch (key) {
        case "event": eventType = val; break;
        case "id": eventId = val; break;
        case "data": dataLines.push(val); break;
        default: break; // retry 等其他字段忽略
      }
    }
    if (eventId) this._lastEventId = eventId;
    if (dataLines.length === 0) {
      // 无 data：直接当 comment 跳过（比如 heartbeat 事件如果后端发成空 data）
      this._emit("__comment", { event: eventType });
      return;
    }
    var raw = dataLines.join("\n");
    var payload = null;
    try {
      payload = JSON.parse(raw);
    } catch (e) {
      // 非 JSON 不致命：wildcard 还能看到 raw
      this._emit("__warn", {
        kind: "bad_json",
        event: eventType,
        raw: raw.slice(0, 500),
      });
    }

    // ---- 断点续传控制事件：先于 emit 处理语义 ----
    switch (eventType) {
      case "replay_start":
        // 服务端明确告知"以下进入回放期"。
        // 如果 mode=resync（有缺口，gap 会先发）或 full（冷启动/不可续），让
        //   消费端先执行"清态"重置再接收回放事件。
        if (payload && (payload.mode === "resync" || payload.mode === "full")) {
          this._resetSeenIds();
          this._emit("__resync_required", {
            mode: payload.mode,
            has_gap: !!payload.has_gap,
            last_event_id: payload.last_event_id || "",
            request_id: payload.request_id || "",
          });
        } else {
          this._emit("__replay_start", payload || {});
        }
        break;
      case "replay_end":
        this._emit("__replay_end", payload || {});
        break;
      case "gap":
        // gap 一定先于 replay 事件，告知消费端准备"全量重同步"。
        this._resetSeenIds();
        this._emit("__gap", payload || {});
        break;
    }

    // 幂等去重：回放事件与实时事件可能有边界重叠（replay_end 前一刻 bus 又 publish），
    // 客户端以 seenIds 为准，跳过任何已经消费过的 event_id。
    var isDup = this._isDuplicateEvent(eventId);
    if (isDup) {
      this._emit("__duplicate", { id: eventId, event: eventType });
      return;
    }

    // 记录最近帧时间（idle 重置）
    this._lastFrameAt = Date.now();
    // 生命周期事件：done/error → 切换状态 + 重置重试次数
    switch (eventType) {
      case "done":
        this.state = "done";
        this._clearIdleTimer();
        // 重置：下一次用户发起 NEW 请求不再带旧 last_event_id（避免误走 RESUME）
        this._retries = 0;
        break;
      case "error":
        if (payload && payload.cancelled) this.state = "cancelled";
        else this.state = "error";
        this._clearIdleTimer();
        this._retries = 0;
        break;
    }
    this._emit(eventType, payload, { raw: raw, id: eventId, dup: false });
  };

  // ---------- 空闲超时 ----------
  EventSourceBuffer.prototype._resetIdleTimer = function () {
    this._clearIdleTimer();
    if (!this.opts.idleTimeoutMs) return;
    var self = this;
    this._idleTimer = setTimeout(function () {
      if (self.state !== "streaming") return;
      self._finalizeWithError("IDLE_TIMEOUT",
        "空闲超时：>" + self.opts.idleTimeoutMs + "ms 未收到任何事件", true);
    }, this.opts.idleTimeoutMs);
  };
  EventSourceBuffer.prototype._clearIdleTimer = function () {
    if (this._idleTimer) {
      clearTimeout(this._idleTimer);
      this._idleTimer = null;
    }
  };

  // ---------- 主动取消 ----------
  /**
   * 取消：(1) AbortController 关 fetch 连接（让后端 request.is_disconnected 变 true）；
   *       (2) 同步调用 POST stopEndpoint 触发 cancel_by_thread_id（双保险，解决
   *           "某些浏览器 Abort 后 TCP FIN 很久才到后端"的边界情况）。
   *
   * @param {string} reason            取消原因（日志用）
   * @param {boolean}[callStop=false]  是否同步 POST /api/task/stop（需 thread_id 存在 body）
   */
  EventSourceBuffer.prototype.abort = function (reason, callStop) {
    var self = this;
    var prevState = this.state;
    if (prevState === "done" || prevState === "cancelled" || prevState === "error") return this;
    this.state = "cancelled";
    this._clearIdleTimer();
    // (1) AbortController 关 fetch
    if (this._ctrl) {
      try { this._ctrl.abort(); } catch (e) { /* noop */ }
      this._ctrl = null;
    }
    // (2) POST /api/task/stop（thread_id 必填，否则跳过）
    if (callStop && this.opts.body && this.opts.body.thread_id) {
      try {
        var xhr = new XMLHttpRequest();
        xhr.open("POST", this.opts.stopEndpoint, true);
        xhr.setRequestHeader("Content-Type", "application/json");
        xhr.send(JSON.stringify({
          thread_id: this.opts.body.thread_id,
          reason: reason || "frontend_abort",
        }));
        // 不等待：beforeunload 场景下 xhr 会被浏览器自动 abort，但 keep-alive 能保证发出
        xhr = null;
      } catch (e) { /* noop */ }
    }
    // emit cancelled（供 UI 提示）
    this._emit("error", {
      message: "已取消: " + (reason || "frontend_abort"),
      code: "CANCELLED",
      cancelled: true,
      recoverable: false,
      ts_ms: Date.now(),
    });
    // 解绑 beforeunload
    this._cleanupBeforeUnload();
    return this;
  };

  // ---------- 收尾（出错 / 完成 / 重试入口）----------
  EventSourceBuffer.prototype._finalizeWithError = function (code, msg, recoverable) {
    if (this.state === "cancelled" || this.state === "done") return;
    this._clearIdleTimer();
    var self = this;
    var shouldRetry = !!this.opts.retry && recoverable && (this._retries < this.opts.maxRetries);
    if (shouldRetry) {
      this._retries++;
      var delay = this.opts.baseDelayMs * Math.pow(2, this._retries - 1);
      this._emit("__retry", { code: code, message: msg, attempt: this._retries, delay: delay });
      setTimeout(function () {
        if (self.state === "cancelled") return;
        self.state = "connecting";
        self._doFetch();
      }, delay);
      return;
    }
    this.state = "error";
    this._emit("error", {
      message: msg,
      code: code,
      cancelled: false,
      recoverable: !!recoverable,
      ts_ms: Date.now(),
    });
    this._cleanupBeforeUnload();
  };

  EventSourceBuffer.prototype._cleanupBeforeUnload = function () {
    if (this._beforeUnloadHandler && typeof window !== "undefined" && window.removeEventListener) {
      window.removeEventListener("beforeunload", this._beforeUnloadHandler);
      this._beforeUnloadHandler = null;
    }
  };

  // ---------- 销毁（显式释放所有句柄）----------
  EventSourceBuffer.prototype.dispose = function () {
    this.abort("dispose", false);
    this._handlers = Object.create(null);
    this._wildcards = [];
  };

  // 导出
  global.EventSourceBuffer = EventSourceBuffer;
})(typeof window !== "undefined" ? window : (typeof self !== "undefined" ? self : this));
