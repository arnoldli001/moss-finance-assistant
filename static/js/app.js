// static/js/app.js — 从 static/index.html <script> 抽出（原 L448-L3045，约 2597 行）
// 依赖：/static/js/constants.js（先加载）、/static/js/stream_client.js（先加载）
// 运行在浏览器，严格模式 + 对未声明 let/const 的变量做兼容（currentUserId 等）
<script>
// ============ 全局状态 ============
let currentUserId = localStorage.getItem('dsp_user_id') || '';
let currentSessionId = null;
let ws = null;
// 是否启用 SSE 流式输出（开=POST /api/task/stream，关=退回 POST /api/task + WS 一次性）
// ⚠️ 前端 feature flag：SSE_STREAM_ENABLED = false 时，所有按钮行为与原代码完全一致
const SSE_STREAM_ENABLED = true;
// 当前正在活跃的 EventSourceBuffer 实例（null=无；stop/切换会话/关闭页面 必须释放）
let currentStreamBuf = null;
// 当前正在"流式打字机渲染"的 assistant 消息 DOM（一条）
let currentAssistantMsgEl = null;
// 当前 assistant 消息的完整纯文本累积（done 事件里用来校验）
let currentAssistantText = '';
// 当前轮收集到的"来源索引条目池"：按 index 升序，和后端 ThreadStreamState._source_pool 镜像
let currentSourceRefs = [];
// 来源引用渲染：正则 [N]（不匹配 Markdown 链接里的 [text](url) 中的 text）
const SRC_REF_RE = /\[(\d{1,3})\](?![\(\uFF08])/g;
// 首包到达时间（open 事件），用于打印"首 token 时延"调试日志
let _streamOpenAt = 0;
let _streamFirstDeltaAt = 0;
let isRunning = false;

// ================================================================
//  §6 新增：检索来源面板 + 引用角标悬停卡片 + 推理 stage 映射
// ================================================================
// 6.1 实时检索池：按 channel 聚合（每次发起新请求重置）
let _currentRetrievalByChannel = { tavily: [], ima: [], zsxq: [] };
let _currentRetrievalActiveTab = 'tavily';
// 6.2 引用角标 -> metadata 1:1 映射（索引 1..N），增量合并 citation_meta 事件
let _citationsMeta = Object.create(null);
// 6.3 悬停卡片计时器：mouseenter 延时 150ms 显示，避免抖动
let _citationHoverTimer = null;

// stage -> 中文标题映射（如果后端 ReasoningPayload.title 为空则兜底）
const _REASONING_STAGE_TITLE = {
  intent_classify: '① 意图识别',
  retrieval_plan:  '② 检索计划',
  synthesis_plan:  '③ 综合推理',
  risk_check:      '④ 风险核查',
  model_coT:       '⑤ 模型 Chain-of-Thought',
};
const _REASONING_STAGE_COLOR = {
  intent_classify: '#6c5ce7',
  retrieval_plan:  '#00b894',
  synthesis_plan:  '#e17055',
  risk_check:      '#d63031',
  model_coT:       '#0984e3',
};

// 6.4 检索面板 Tab 切换绑定（页面加载后一次性）
document.addEventListener('DOMContentLoaded', () => {
  const tabs = document.querySelectorAll('#retrieval-tabs .rtab');
  tabs.forEach(t => {
    t.addEventListener('click', () => {
      const ch = t.dataset.ch;
      _currentRetrievalActiveTab = ch;
      tabs.forEach(x => {
        const active = x === t;
        x.classList.toggle('active', active);
        if (active) {
          x.style.background = '#f3f4ff';
          x.style.color = '#4950cc';
          x.style.borderBottom = '2px solid #4950cc';
        } else {
          x.style.background = 'transparent';
          x.style.color = '#888';
          x.style.borderBottom = '2px solid transparent';
        }
      });
      _retrievalRender();
    });
  });
});

// 6.5 重置检索 & 引用状态（每次 newSession / _streamClearState 时调用）
function _retrievalResetAll() {
  _currentRetrievalByChannel = { tavily: [], ima: [], zsxq: [] };
  _citationsMeta = Object.create(null);
  document.querySelectorAll('#retrieval-tabs .rtab-cnt').forEach(s => s.textContent = '0');
  const list = document.getElementById('retrieval-list');
  if (list) list.innerHTML = '<div style="padding:18px 4px;text-align:center;color:#aaa;">检索结果将实时显示在这里</div>';
  _citationHideCard();
}

// 6.6 追加一路 retrieve_result 并刷新 UI
function _retrievalAppend(channel, items, query, duration_ms) {
  if (!_currentRetrievalByChannel[channel]) _currentRetrievalByChannel[channel] = [];
  const arr = _currentRetrievalByChannel[channel];
  const seenIds = new Set(arr.map(x => x.doc_id));
  for (const it of items || []) {
    if (!it || !it.doc_id) continue;
    if (seenIds.has(it.doc_id)) continue;
    seenIds.add(it.doc_id);
    arr.push(it);
  }
  const cnt = document.querySelector(`#retrieval-tabs .rtab[data-ch="${channel}"] .rtab-cnt`);
  if (cnt) cnt.textContent = String(arr.length);
  if (_currentRetrievalActiveTab === channel) _retrievalRender();
}

// 6.7 渲染当前激活 tab 的检索条目列表
//  需求：标题 + 外链（最少 token）；snippet 默认收起，点行展开；最多 100 字双保险（JS slice + CSS clamp）
function _retrievalRender() {
  const list = document.getElementById('retrieval-list');
  if (!list) return;
  const ch = _currentRetrievalActiveTab;
  const arr = _currentRetrievalByChannel[ch] || [];
  if (!arr.length) {
    list.innerHTML = '<div style="padding:18px 4px;text-align:center;color:#aaa;">该通道暂无检索结果</div>';
    return;
  }
  // SNIPPET_MAX 与后端 CITATION_SNIPPET_MAX_CHARS=100 对齐（再做一次 JS 端硬截兜底）
  const SNIPPET_MAX = 100;
  const TITLE_MAX = 80;
  list.innerHTML = arr.map((it, i) => {
    const id = 'ret-item-' + ch + '-' + i;
    const title = _escapeHtml((it.title || '无标题').slice(0, TITLE_MAX));
    const rawContent = String(it.content || it.snippet || '');
    // 展示层"最相关片段"：如果 content 本身就是长文，优先截取开头~SNIPPET_MAX 字（避免 2000 字灌到 DOM）
    // 后端 retrieve_result 事件里 content 是原文（给 overlap fallback 用），前端显示需要自己缩
    let snippet = rawContent.length > SNIPPET_MAX
      ? (rawContent.slice(0, SNIPPET_MAX - 1) + '…')
      : rawContent;
    snippet = _escapeHtml(snippet);
    const linkHtml = it.url
      ? `<a href="${_escapeAttr(it.url)}" target="_blank" rel="noopener noreferrer"
             style="color:#1976d2;text-decoration:none;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block;max-width:100%;">🔗 ${_escapeHtml(String(it.url))}</a>`
      : '<div style="color:#bbb;font-size:10.5px;">（无外链）</div>';
    // 点行 → 展开/折叠 snippet；snippet 默认折叠（省 token + 视觉清爽）
    return `<div id="${id}" style="padding:6px 6px;border-bottom:1px dashed #eee;cursor:pointer;"
              onclick="var b=document.getElementById('${id}-body');if(b){b.style.display=(b.style.display==='none'||!b.style.display)?'block':'none';}">
      <div style="font-weight:600;color:#222;font-size:11.5px;line-height:1.35;">
        ${i + 1}. ${title}
      </div>
      <div style="margin-top:3px;">${linkHtml}</div>
      <div id="${id}-body" style="display:none;margin-top:6px;padding:4px 6px;background:#f6f8fb;border-left:3px solid #74a9f7;color:#555;font-size:11px;line-height:1.45;word-break:break-word;
                display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:4;overflow:hidden;">${snippet}</div>
    </div>`;
  }).join('');
}

// 6.8 合并 citation_meta 增量：每个 index 写入 _citationsMeta，并同步 currentSourceRefs（为了文末"来源面板"）
function _citationMetaMerge(items) {
  if (!Array.isArray(items) || !items.length) return;
  let changed = false;
  for (const it of items) {
    const idx = Number(it.index);
    if (!idx || idx < 1) continue;
    const prev = _citationsMeta[idx];
    // 只写更完整的版本（title 更长或 url 从无到有）
    if (!prev || (it.title && !prev.title) || (it.url && !prev.url) || (it.snippet && !prev.snippet)) {
      _citationsMeta[idx] = Object.assign({}, prev || {}, it);
      changed = true;
    } else if (Object.keys(it).length > Object.keys(prev).length) {
      _citationsMeta[idx] = Object.assign({}, prev, it);
      changed = true;
    }
    // 同步 currentSourceRefs：文末来源列表（兼容旧逻辑 _streamShowSourceBox）
    const entry = _citationsMeta[idx];
    const existIdx = currentSourceRefs.findIndex(s => s.index === idx);
    const srcRef = {
      index: idx,
      title: entry.title || ('来源 ' + idx),
      url: entry.url || '',
      source_type: entry.source_type || 'web',
      reliability: entry.reliability || '待验证',
      channel: entry.channel || '',
      published_at: entry.published_at || '',
      snippet: entry.snippet || '',  // 文末来源盒 snip 字段兜底，防止后端 citation_meta 漏下发时 snip 为空
    };
    if (existIdx >= 0) currentSourceRefs[existIdx] = Object.assign({}, currentSourceRefs[existIdx], srcRef);
    else currentSourceRefs.push(srcRef);
  }
  currentSourceRefs.sort((a, b) => (a.index || 0) - (b.index || 0));
  return changed;
}

// 6.9 引用角标悬停卡片：显示 / 隐藏 / 定位
// 只显示"标题 + 打开来源链接 + 最相关片段（最多100字）"，减少信息噪音和 token
function _citationShowCard(idx, anchorEl) {
  const card = document.getElementById('citation-hover-card');
  if (!card) return;
  const meta = _citationsMeta[idx];
  const TITLE_MAX = 80;
  const SNIPPET_MAX = 100;
  if (!meta) {
    card.innerHTML = `<div style="font-weight:600;color:#888;">[${idx}] 来源信息加载中…</div><div style="color:#aaa;font-size:11px;margin-top:2px;">等待后端 citation_meta 事件下发</div>`;
  } else {
    const title = _escapeHtml(String(meta.title || '无标题').slice(0, TITLE_MAX));
    const rawSnip = String(meta.snippet || '');
    const snippet = rawSnip
      ? _escapeHtml(rawSnip.length > SNIPPET_MAX ? (rawSnip.slice(0, SNIPPET_MAX - 1) + '…') : rawSnip)
      : '';
    const snippetHtml = snippet
      ? `<div style="margin-top:8px;padding:6px 8px;background:#f6f8fb;border-left:3px solid #74a9f7;color:#444;font-size:11.5px;line-height:1.5;word-break:break-word;
                 display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:4;overflow:hidden;">“${snippet}”</div>`
      : '';
    const urlHtml = meta.url
      ? `<a href="${_escapeAttr(meta.url)}" target="_blank" rel="noopener noreferrer"
             style="display:inline-block;margin-top:6px;color:#1976d2;text-decoration:none;font-size:11.5px;
                    overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:100%;vertical-align:middle;">🔗 打开来源</a>`
      : '<div style="margin-top:6px;color:#aaa;font-size:10.5px;">（无外链）</div>';
    card.innerHTML =
      `<div style="display:flex;align-items:center;gap:6px;">
         <span style="display:inline-flex;align-items:center;justify-content:center;min-width:22px;height:22px;padding:0 6px;border-radius:11px;background:#3498db;color:#fff;font-weight:700;font-size:11.5px;flex-shrink:0;">[${idx}]</span>
         <div style="font-weight:600;color:#2c3e50;font-size:12.5px;line-height:1.35;flex:1;word-break:break-word;">${title}</div>
       </div>
       ${snippetHtml}
       ${urlHtml}`;
  }
  // 定位：anchorEl 右下方，超右边界则贴右对齐
  const r = anchorEl.getBoundingClientRect();
  card.style.display = 'block';
  const cw = card.offsetWidth;
  const ch2 = card.offsetHeight;
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  let left = r.right + 8;
  let top = r.bottom + 6;
  if (left + cw > vw - 8) left = Math.max(8, r.left - cw - 8);
  if (top + ch2 > vh - 8) top = Math.max(8, r.top - ch2 - 6);
  card.style.left = left + 'px';
  card.style.top = top + 'px';
}
function _citationHideCard() {
  if (_citationHoverTimer) { clearTimeout(_citationHoverTimer); _citationHoverTimer = null; }
  const card = document.getElementById('citation-hover-card');
  if (card) card.style.display = 'none';
}
// 卡片自 hover 时不隐藏（便于用户从角标移动鼠标到卡片看内容、点链接）
document.addEventListener('DOMContentLoaded', () => {
  const card = document.getElementById('citation-hover-card');
  if (!card) return;
  card.addEventListener('mouseenter', () => {
    if (_citationHoverTimer) { clearTimeout(_citationHoverTimer); _citationHoverTimer = null; }
  });
  card.addEventListener('mouseleave', () => { _citationHideCard(); });
});
let currentTaskType = 'normal'; // 'normal' | 'zsxq'，控制 task_result 靠左/靠右显示
// ===== TaskManager：统一在飞任务管理器（2026-09-09）=====
// 所有快捷按钮（盘前新闻/盘前研报热度/复盘预测/未来新增按钮）的运行状态统一由它管理：
//   start(sid,label,taskType)  请求受理后登记
//   finish(sid)                完成/失败/手动停止时清除
//   resumeIfRunning(sid)       switchSession 拉完历史后调用，恢复"后台运行中"提示
//   _poll()                    30s 轮询兜底：任务结果已落库但 WS 推送丢失（完成时用户
//                              不在该会话/连接异常）时，检测当前会话历史增长并提示，
//                              杜绝"任务完成了前端永远看不到"。
// 【新增按钮接入清单】① fetch 成功后 TaskManager.start(当前会话,标签,taskType)
//   ② task_result/error/停止/SSE 结束四个终点调 TaskManager.finish  ③ switchSession 无需改。
const TaskManager = {
  map: new Map(), // sid -> {label, taskType, startTime, baseCount}
  STALE_MS: 950000, // 超过视为已完成（后端最长预算 900s + 缓冲）
  POLL_MS: 30000,
  _timer: null,
  start(sid, label, taskType) {
    if (!sid) return;
    const info = { label: label || '任务', taskType: taskType || 'normal',
                   startTime: Date.now(), baseCount: null };
    this.map.set(sid, info);
    if (!this._timer) this._timer = setInterval(() => this._poll(), this.POLL_MS);
    // 登记时记录历史消息基线（异步不阻塞）：切回时历史增长 = 结果已落库
    this._fetchCount(sid).then(n => { if (n != null) info.baseCount = n; }).catch(() => {});
  },
  finish(sid) {
    if (sid) this.map.delete(sid);
    if (this.map.size === 0 && this._timer) { clearInterval(this._timer); this._timer = null; }
  },
  async _fetchCount(sid) {
    const uid = encodeURIComponent(currentUserId || '');
    const r = await fetch(`/api/sessions/${sid}/history?user_id=${uid}`);
    if (!r.ok) return null;
    const data = await r.json();
    return (data.messages || []).length;
  },
  // 权威任务状态：后端 SessionRegistryActor 是否仍登记该会话的任务。
  // 历史消息数增长不能当完成信号——复盘预测阶段1 的小作文热度中间结果会先落库，
  // 那时任务还在跑（2026-09-10 复盘 11 分钟无输出 bug 的收尾环节）。
  async _fetchRunning(sid) {
    const r = await fetch(`/api/task/status?thread_id=${encodeURIComponent(sid)}`);
    if (!r.ok) return null;
    const data = await r.json();
    return data.running === true;
  },
  // 任务登记过期/结果已落库但前端运行态未复位：清登记 + 解锁按钮
  _unlock(sid) {
    const viewing = sid === currentSessionId;
    this.finish(sid);
    if (viewing) {
      setRunning(false);
      appendMessage('system', 'ℹ️ 后台任务已结束，如未看到结果请重新点击按钮发起。');
    }
  },
  async resumeIfRunning(sid) {
    const info = this.map.get(sid);
    if (!info) return;
    if (Date.now() - info.startTime > this.STALE_MS) { this._unlock(sid); return; }
    // 【关键】切回前先问后端任务是否还活着。任务在结果落库之后才注销，
    // running=false 时结果必然已在历史里；running=true 才恢复"运行中"界面。
    // 注意：只查状态（1 个请求）——游客限流 10 QPM，历史数仅在结束时查一次。
    let running = null;
    try { running = await this._fetchRunning(sid); } catch (e) { running = null; }
    // await 期间用户可能又切走了：不再操作 DOM
    if (sid !== currentSessionId) return;
    if (running === true) {
      // 任务确实还在跑：恢复用户气泡（"复盘预测（关注：xxx）"等完整 displayLabel）
      // + 后台运行提示 + stop 按钮
      pendingTurnIndex += 1;
      appendMessage('user', info.label, { turnIndex: pendingTurnIndex });
      currentTaskType = info.taskType; // 恢复结果渲染的靠左/靠右语义（zsxq 靠右）
      const mins = Math.max(1, Math.round((Date.now() - info.startTime) / 60000));
      appendMessage('system',
        `⏳ 本会话的「${info.label}」任务仍在后台运行（已约 ${mins} 分钟）`);
      setRunning(true, info.taskType); // 恢复 stop 按钮，超时保护计时器同步重启
      return;
    }
    // 任务已结束（running=false）：结果落库先于注销。历史刚由 switchSession 渲染过，
    // 这里只需处理"历史快照早于落库一个网络往返"的窄竞态 → 最多复查两次
    this.finish(sid);
    if (running === false) {
      let grew = false, n = null;
      for (let attempt = 0; attempt < 2; attempt++) {
        n = await this._fetchCount(sid).catch(() => null);
        if (sid !== currentSessionId) return;
        grew = (n != null && (info.baseCount == null || n > info.baseCount));
        if (grew) break;
        await new Promise(r => setTimeout(r, 2000));
        if (sid !== currentSessionId) return;
      }
      appendMessage('system', grew
        ? `✅ 「${info.label}」任务已在后台完成，结果见上方对话记录。`
        : `ℹ️ 「${info.label}」任务已结束，但未产生结果，请重新点击按钮发起。`);
    } else {
      // 状态查询失败（旧服务端/网络抖动/429 限流）：保守恢复运行态，交给 30s 轮询兜底
      pendingTurnIndex += 1;
      appendMessage('user', info.label, { turnIndex: pendingTurnIndex });
      currentTaskType = info.taskType;
      const mins = Math.max(1, Math.round((Date.now() - info.startTime) / 60000));
      appendMessage('system',
        `⏳ 本会话的「${info.label}」任务可能仍在后台运行（已约 ${mins} 分钟）`);
      setRunning(true, info.taskType);
    }
  },
  async _poll() {
    for (const [sid, info] of Array.from(this.map)) {
      if (Date.now() - info.startTime > this.STALE_MS) { this._unlock(sid); continue; }
      if (sid !== currentSessionId) continue; // 只需兜底用户正在看的会话
      try {
        // 轮询只查任务状态（省请求：游客 10 QPM 限流，历史接口不跟着轮）
        const running = await this._fetchRunning(sid);
        if (sid !== currentSessionId) continue; // await 期间切走了
        if (running === true) continue;         // 仍在跑
        if (running === false) {
          // 任务刚结束（WS 完成事件丢失时的兜底）：查一次历史确认结果已落库 → 解锁提示
          const n = await this._fetchCount(sid).catch(() => null);
          if (sid !== currentSessionId) continue;
          this.finish(sid);
          setRunning(false);
          const grew = (n != null && (info.baseCount == null || n > info.baseCount));
          appendMessage('system', grew
            ? `✅ 「${info.label}」任务已完成，结果已写入本会话历史。` +
              `如上方未显示，切换到其他会话再切回本会话即可查看。`
            : `ℹ️ 「${info.label}」任务已结束，如未看到结果请重新点击按钮发起。`);
          continue;
        }
        // running === null（429/网络抖动）：保持轮询下轮重试（不解锁、不误报）
      } catch (e) { /* 网络抖动，下一轮重试 */ }
    }
    if (this.map.size === 0 && this._timer) { clearInterval(this._timer); this._timer = null; }
  }
};
let wsReconnectCount = 0;
let _wsReconnectMsg = null;  // 重连提示消息元素（复用，避免多条）
let wsHeartbeatTimer = null;
let runningTimeoutTimer = null;
// 所有数字常量集中到 APP_CONSTANTS，修改见 static/js/constants.js
const WS_HEARTBEAT_INTERVAL = APP_CONSTANTS.WS_HEARTBEAT_INTERVAL_MS;  // 心跳间隔（毫秒）
const WS_MAX_RECONNECT      = APP_CONSTANTS.WS_MAX_RECONNECT;           // 最大重连次数
const RUNNING_TIMEOUT       = APP_CONSTANTS.RUNNING_TIMEOUT_MS;         // 运行硬超时（毫秒）

const $ = id => document.getElementById(id);

// ============ 初始化 ============
window.addEventListener('load', async () => {
  // —— 多选浮动操作栏：页面加载时强制隐藏，防止浏览器缓存/恢复 DOM 状态导致
  //    class=msg-multi-active 残留（刷新后仍看到"已选0条 删除选中 取消"浮窗）。
  (function _ensureMultiActionBarHiddenOnStartup() {
    const bar = document.getElementById('msg-multi-action-bar');
    if (bar) {
      bar.classList.remove('msg-multi-active');
      bar.style.display = '';  // 清掉任何遗留的 inline style.display
      const comp = window.getComputedStyle(bar);
      // 最终保险：如果 getComputedStyle 仍然显示可见（极端情况），
      // 就直接把 visibility 设成 hidden + height:0，不占空间不显示。
      if (comp && comp.display !== 'none') {
        bar.setAttribute('style', 'display:none !important; visibility:hidden; height:0; padding:0; border:none; overflow:hidden;');
        // 下一轮事件循环再清掉，让用户点击"多选"时 class 能重新正常生效
        setTimeout(() => { bar.setAttribute('style', ''); }, 0);
      }
    }
  })();

  // —— 登录态感知启动：AUTH 模式下（auth_bootstrap.js 提供 window.MossAuth），
  //    currentUserId 必须来自 JWT.sub；未登录时登录遮罩挡在最上层（auth_bootstrap 负责），
  //    此处【不跑】会话流程，等 moss:user-changed 事件（登录/注册/游客成功）再启动。
  //    否则遮罩底下裸跑 newSession() → 401 → "Cannot read properties of undefined" 弹窗。
  const _auth = window.MossAuth;
  function _syncUidFromAuth() {
    const u = _auth && _auth.currentUser && _auth.currentUser();
    if (u && u.user_id) {
      currentUserId = u.user_id;
      localStorage.setItem('dsp_user_id', currentUserId);
    }
    const inp = $('user-id-input');
    if (inp) inp.value = currentUserId;
  }
  async function _startSessionFlow() {
    _syncUidFromAuth();
    // 无会话选中：优先选最后一个已有会话，无会话才新建
    if (!currentSessionId) {
      const sessions = await loadSessions();
      if (sessions.length > 0) {
        const last = sessions[sessions.length - 1];
        switchSession(last.session_id, last.title);
      } else {
        await newSession();
      }
    }
  }
  if (_auth && _auth.enabled) {
    if (_auth.isLoggedIn()) {
      await _startSessionFlow();
    } else {
      // 未登录：等登录/注册/游客成功（auth_bootstrap 派发 moss:user-changed）后自动启动一次
      window.addEventListener('moss:user-changed', (ev) => {
        if (ev.detail && ev.detail.user_id) _startSessionFlow();
      }, { once: true });
    }
    // 常驻监听：账号切换（登录/登出）时同步身份
    window.addEventListener('moss:user-changed', (ev) => {
      // ===== 账号切换/登出：断旧 WS + 清聊天区（2026-09-09 修复跨账号消息串台） =====
      // 旧账号的在飞任务（盘前研报热度 ~5-10min / 复盘预测）完成后，task_result 会
      // 推送到仍存活的旧会话 WS；若不断开，结果会渲染进新账号的聊天区。
      // 同时复位运行状态，避免新账号登录后按钮被旧任务的超时保护卡住。
      if (ws) {
        ws.onclose = null; ws.onerror = null; ws.onmessage = null; ws.onopen = null;
        try { ws.close(); } catch (e) {}
        ws = null;
      }
      clearHeartbeat();
      isRunning = false;
      if (runningTimeoutTimer) { clearTimeout(runningTimeoutTimer); runningTimeoutTimer = null; }
      clearThinkingTimer();
      if (ev.detail && ev.detail.user_id) {
        // 切换账号：清空聊天区 + 会话选择复位（历史由 _startSessionFlow 重新加载）
        currentSessionId = null;
        const chatEl = $('chat');
        if (chatEl) chatEl.innerHTML = '';
        const titleEl = $('session-title');
        if (titleEl) titleEl.textContent = '未选择会话';
        _syncUidFromAuth();
      } else if (!ev.detail) {
        // 登出：清空聊天区 + 会话列表 + 标题（与 index.html 登出分支一致）
        currentUserId = '';
        currentSessionId = null;
        const inp = $('user-id-input');
        if (inp) { inp.value = ''; inp.removeAttribute('readonly'); inp.title = ''; }
        const listEl = $('session-list');
        if (listEl) listEl.innerHTML = '';
        const titleEl2 = $('session-title');
        if (titleEl2) titleEl2.textContent = '未选择会话';
        const chatEl2 = $('chat');
        if (chatEl2) chatEl2.innerHTML = '';
      }
    });
  } else {
    // 非 AUTH 模式（旧行为）：自造游客 ID + 旧明文登录
    if (!currentUserId) {
      const ts = Date.now().toString(36);
      const rand = Math.random().toString(36).slice(2, 6);
      currentUserId = 'guest_' + ts + rand;
      localStorage.setItem('dsp_user_id', currentUserId);
    }
    $('user-id-input').value = currentUserId;
    await _startSessionFlow();
  }
  $('login-btn').addEventListener('click', login);
  $('send-btn').addEventListener('click', sendMessage);
  $('stop-btn').addEventListener('click', stopCurrentTask);
  $('query-input').addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
  });
});

// ============ 用户登录（侧边栏输入框手动登录） ============
async function login() {
  const uid = $('user-id-input').value.trim();
  if (!uid) { alert('请输入用户 ID'); return; }
  const _auth = window.MossAuth;
  if (_auth && _auth.enabled) {
    // AUTH 模式：身份绑定 JWT，输入框仅供展示（hideLoginScreen 会设 readonly）
    if (!_auth.isLoggedIn()) { _auth.showLoginScreen('请先登录后再操作'); return; }
    const cur = _auth.currentUser();
    if (cur && cur.user_id !== uid) {
      showToast(`当前登录身份为 ${cur.user_id}，如需切换账号请点击右上角「退出」后重新登录/注册`);
      $('user-id-input').value = cur.user_id;
      return;
    }
    await loadSessions();
    return;
  }
  // 非 AUTH 模式（旧行为）：旧明文建档端点
  currentUserId = uid;
  localStorage.setItem('dsp_user_id', uid);
  try {
    const r = await fetch('/api/users', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({user_id: uid})
    });
    if (!r.ok) throw new Error('登录失败');
    await loadSessions();
  } catch (e) { alert('登录失败: ' + e.message); }
}

// ============ 会话列表（多选） ============
let sessionMultiMode = false;
let selectedSessionIds = new Set();

async function toggleSessionMultiMode(force) {
  const next = typeof force === 'boolean' ? force : !sessionMultiMode;
  sessionMultiMode = next;
  const sb = $('sidebar');
  if (next) sb.classList.add('multi-mode');
  else {
    sb.classList.remove('multi-mode');
    selectedSessionIds.clear();
  }
  // 重新渲染列表（切换 复选框 DOM 结构）
  await loadSessions();
}

function updateMultiBarCount() {
  const el = $('smb-count-num');
  if (el) el.textContent = String(selectedSessionIds.size);
  const delBtn = document.querySelector('.smb-delete');
  if (delBtn) delBtn.disabled = selectedSessionIds.size === 0;
}

function updateSessionSelectionUI() {
  document.querySelectorAll('.session-item').forEach(item => {
    const sid = item.dataset.sid;
    const chk = item.querySelector('.session-chk');
    if (selectedSessionIds.has(sid)) {
      item.classList.add('selected');
      if (chk) chk.checked = true;
    } else {
      item.classList.remove('selected');
      if (chk) chk.checked = false;
    }
  });
  updateMultiBarCount();
}

function toggleSessionSelect(sid, e) {
  if (!sessionMultiMode) return;
  if (e) e.stopPropagation();
  if (selectedSessionIds.has(sid)) selectedSessionIds.delete(sid);
  else selectedSessionIds.add(sid);
  updateSessionSelectionUI();
}

function selectAllSessions(select) {
  document.querySelectorAll('.session-item').forEach(item => {
    const sid = item.dataset.sid;
    if (!sid) return;
    if (select) selectedSessionIds.add(sid);
    else selectedSessionIds.delete(sid);
  });
  updateSessionSelectionUI();
}

let _deleteConfirmTimer = null;
async function confirmDeleteSelectedSessions() {
  const delBtn = document.querySelector('.smb-delete');
  if (selectedSessionIds.size === 0) { showToast('请先选择要删除的会话'); return; }

  // 二次确认：第一次点击改变按钮文字+样式，3 秒内再点才真正删除
  // 按钮状态变化比 toast 更直观，避免手机端用户错过提示
  if (!_deleteConfirmTimer) {
    const count = selectedSessionIds.size;
    if (delBtn) {
      delBtn.textContent = `⚠️ 再次点击确认删除 ${count} 项`;
      delBtn.style.background = '#ff9500';
      delBtn.style.border = '1px solid #ff9500';
      delBtn.style.color = '#fff';
    }
    showToast(`再次点击确认删除 ${count} 个会话`, APP_CONSTANTS.TOAST_DEFAULT_DURATION_MS);
    _deleteConfirmTimer = setTimeout(() => {
      _deleteConfirmTimer = null;
      if (delBtn) {
        delBtn.textContent = '🗑 删除所选';
        delBtn.style.background = '';
        delBtn.style.border = '';
        delBtn.style.color = '';
      }
    }, APP_CONSTANTS.DELETE_CONFIRM_WINDOW_MS);
    return;
  }
  clearTimeout(_deleteConfirmTimer);
  _deleteConfirmTimer = null;
  if (delBtn) {
    delBtn.textContent = '🗑 删除所选';
    delBtn.style.background = '';
    delBtn.style.border = '';
    delBtn.style.color = '';
  }

  const ids = Array.from(selectedSessionIds);
  const containedCurrent = ids.includes(currentSessionId);
  showToast('正在删除...');
  try {
    let okCount = 0, failCount = 0;
    const uid = encodeURIComponent(currentUserId);
    for (const sid of ids) {
      // 用户隔离校验：带上 user_id 防止越权删除
      const resp = await fetch(`/api/sessions/${sid}?user_id=${uid}`, { method: 'DELETE' });
      if (resp.ok) okCount++;
      else failCount++;
    }
    if (containedCurrent) {
      currentSessionId = null;
      $('session-title').textContent = '未选择会话';
      $('query-input').disabled = true;
      $('send-btn').disabled = true;
      $('chat').innerHTML = '<div class="msg system">会话已删除，请新建或选择其他会话。</div>';
    }
    selectedSessionIds.clear();
    sessionMultiMode = false;
    $('sidebar').classList.remove('multi-mode');
    await loadSessions();
    if (failCount > 0) showToast(`已删除 ${okCount} 个，${failCount} 个失败`, APP_CONSTANTS.TOAST_DEFAULT_DURATION_MS);
    else showToast(`已删除 ${okCount} 个会话`);
  } catch (err) {
    console.error('批量删除失败', err);
    showToast('批量删除失败: ' + (err.message || String(err)));
  }
}

async function loadSessions() {
  if (!currentUserId) return [];
  try {
    const r = await fetch(`/api/users/${currentUserId}/sessions`);
    const data = await r.json();
    const sessions = data.sessions || [];
    const list = $('session-list');
    list.innerHTML = '';
    // 过滤掉已被标记删除的选中项（防止 loadSessions 刷新后 UI 错乱）
    const remainSelected = new Set();
    (data.sessions || []).forEach(s => {
      if (selectedSessionIds.has(s.session_id)) remainSelected.add(s.session_id);
      const div = document.createElement('div');
      div.className = 'session-item' + (s.session_id === currentSessionId ? ' active' : '');
      div.dataset.sid = s.session_id;
      // 多选模式：点击整体切换选中，不切换会话；不绑定默认 onclick/ondblclick
      if (sessionMultiMode) {
        div.innerHTML = `
          <input type="checkbox" class="session-chk" ${selectedSessionIds.has(s.session_id) ? 'checked' : ''} onclick="event.stopPropagation();toggleSessionSelect('${s.session_id}',event)" />
          <span class="session-title">${escapeHtml(s.title || '新会话')}</span>
        `;
        div.onclick = (e) => { e.stopPropagation(); toggleSessionSelect(s.session_id, e); };
        if (selectedSessionIds.has(s.session_id)) div.classList.add('selected');
      } else {
        div.innerHTML = `
          <span class="session-title">${escapeHtml(s.title || '新会话')}</span>
          <span class="rename" onclick="event.stopPropagation();startRename(this,'${s.session_id}','${escapeHtml(s.title||'')}')">✏</span>
          <span class="del" onclick="event.stopPropagation();deleteSession('${s.session_id}')">🗑</span>
        `;
        div.onclick = () => switchSession(s.session_id, s.title);
        div.ondblclick = () => startRename(div.querySelector('.session-title') ? div : null, s.session_id, s.title || '');
      }
      list.appendChild(div);
    });
    selectedSessionIds = remainSelected;
    updateMultiBarCount();
    return sessions;
  } catch (e) { console.error('加载会话列表失败', e); return []; }
}

async function newSession() {
  if (!currentUserId) { alert('请先登录'); return; }
  _retrievalResetAll();
  // 生成随机会话标题：时间_uuid片段，确保唯一不重复
  const now = new Date();
  const ts = `${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${String(now.getHours()).padStart(2,'0')}${String(now.getMinutes()).padStart(2,'0')}`;
  const uuidFrag = (crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2)).slice(0, 8);
  const randomTitle = `${ts}_${uuidFrag}`;
  try {
    const r = await fetch(`/api/users/${currentUserId}/sessions`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({title: randomTitle})
    });
    const data = await r.json();
    if (!r.ok) throw new Error((data.detail && (data.detail.message || JSON.stringify(data.detail))) || `HTTP ${r.status}`);
    if (!data.session || !data.session.session_id) throw new Error('响应缺少 session 字段');
    await loadSessions();
    switchSession(data.session.session_id, data.session.title);
  } catch (e) { alert('新建会话失败: ' + e.message); }
}

let _singleDeleteConfirm = {};
async function deleteSession(sid) {
  // 二次确认（替代 confirm 弹窗）
  if (!_singleDeleteConfirm[sid]) {
    showToast('再次点击确认删除', APP_CONSTANTS.TOAST_DEFAULT_DURATION_MS);
    _singleDeleteConfirm[sid] = true;
    setTimeout(() => { delete _singleDeleteConfirm[sid]; }, APP_CONSTANTS.DELETE_CONFIRM_WINDOW_MS);
    return;
  }
  delete _singleDeleteConfirm[sid];
  try {
    // 用户隔离校验：必须带上 user_id，防止越权删除他人会话
    const uid = encodeURIComponent(currentUserId);
    const resp = await fetch(`/api/sessions/${sid}?user_id=${uid}`, {method:'DELETE'});
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      showToast('删除失败: ' + (data.detail || resp.statusText));
      return;
    }
    if (sid === currentSessionId) {
      currentSessionId = null;
      $('session-title').textContent = '未选择会话';
      $('query-input').disabled = true;
      $('send-btn').disabled = true;
      $('chat').innerHTML = '<div class="msg system">会话已删除，请新建或选择其他会话。</div>';
    }
    await loadSessions();
    showToast('已删除');
  } catch (e) {
    showToast('删除失败: ' + e.message);
  }
}

// ============ 会话重命名 ============
function startRename(elem, sid, oldTitle) {
  // elem 可能是 .rename 图标或 .session-item div
  const item = elem.classList && elem.classList.contains('session-item') ? elem : elem.closest('.session-item');
  if (!item) return;
  const titleSpan = item.querySelector('.session-title');
  if (!titleSpan) return;
  const oldText = oldTitle || titleSpan.textContent || '';
  // 替换为输入框
  const input = document.createElement('input');
  input.type = 'text';
  input.className = 'rename-input';
  input.value = oldText;
  titleSpan.replaceWith(input);
  input.focus();
  input.select();
  // 隐藏操作按钮
  const btns = item.querySelectorAll('.rename, .del');
  btns.forEach(b => b.style.display = 'none');
  // Enter 提交，Escape 取消，blur 提交
  let done = false;
  const finish = async (submit) => {
    if (done) return;
    done = true;
    const newTitle = input.value.trim();
    if (submit && newTitle && newTitle !== oldText) {
      try {
        // 用户隔离校验：带上 user_id 防止越权修改他人会话标题
        await fetch(`/api/sessions/${sid}/title`, {
          method: 'PATCH', headers: {'Content-Type':'application/json'},
          body: JSON.stringify({title: newTitle, user_id: currentUserId})
        });
        // 更新头部标题（如果是当前会话）
        if (sid === currentSessionId) $('session-title').textContent = newTitle;
      } catch (e) { alert('重命名失败: ' + e.message); }
    }
    await loadSessions(); // 刷新列表（恢复 span 显示）
  };
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter') { e.preventDefault(); finish(true); }
    else if (e.key === 'Escape') { e.preventDefault(); finish(false); }
  });
  input.addEventListener('blur', () => finish(true));
}

// ============ 切换会话 ============
async function switchSession(sid, title) {
  // ===== 切换会话竞态修复：先立即摘除旧 WS 回调并关闭，再改 currentSessionId =====
  // 修复前：currentSessionId 先被改成新会话 → await fetch 历史 → 期间旧会话 WS
  //        仍存活，其 onmessage 把旧会话消息追加进新会话聊天区（消息混淆）。
  // 修复后：摘掉 onmessage/onclose/onerror（旧 WS 残留消息不再投递到 handleWSMessage），
  //        然后才切换 currentSessionId、拉历史、建新 WS。
  if (ws) {
    ws.onclose = null;
    ws.onerror = null;
    ws.onmessage = null;
    ws.onopen = null;
    try { ws.close(); } catch(e){}
    ws = null;
  }
  clearHeartbeat();
  currentSessionId = sid;
  $('session-title').textContent = title || '未命名会话';
  $('query-input').disabled = false;
  $('send-btn').disabled = false;
  // 切换会话时重置运行状态：旧任务在后端继续跑，结果存 checkpointer，刷新历史可见
  // 这样用户可以在不同会话间快速切换并立即发新任务（支持多标签页/多会话并发）
  // 【2026-09-09 修复】必须用 setRunning(false) 完整复位：旧实现只改 isRunning 变量 +
  // 手动解 send-btn，快捷按钮（盘前新闻/盘前研报热度/复盘预测）的 disabled 是
  // setRunning(true) 设置的 DOM 属性，不调 setRunning(false) 就永远不会恢复，
  // 导致切会话后功能键全部点不了（要等 8 分钟超时或切回原会话等任务结束）。
  setRunning(false);
  wsReconnectCount = 0;
  clearThinkingTimer();
  if (_wsReconnectMsg) { _wsReconnectMsg.remove(); _wsReconnectMsg = null; }
  // 高亮当前会话
  document.querySelectorAll('.session-item').forEach(el => el.classList.remove('active'));
  // 清空聊天区
  $('chat').innerHTML = '<div class="msg system">正在加载历史记录...</div>';
  // 加载历史（429 限流/5xx 自动重试 3 次：游客 10 QPM，切会话+状态轮询同窗口可能打满）
  try {
    // 用户隔离校验：带上 user_id 防止越权读取他人对话
    const uid = encodeURIComponent(currentUserId);
    let r = null, data = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      r = await fetch(`/api/sessions/${sid}/history?user_id=${uid}`);
      if (r.ok) { data = await r.json(); break; }
      if (r.status === 429 || r.status >= 500) {
        await new Promise(res => setTimeout(res, 1500));
        continue;
      }
      break; // 4xx（403/404 等）不重试
    }
    if (!r.ok) {
      const errData = await r.json().catch(() => ({}));
      const detail = (errData && typeof errData.detail === 'string')
        ? errData.detail : r.statusText;
      $('chat').innerHTML = `<div class="msg error">加载历史失败: ${detail}` +
        `（可稍后重新切换本会话重试）</div>`;
      return;
    }
    $('chat').innerHTML = '';
    let currentTurn = 0;
    (data.messages || []).forEach(m => {
      // 轮次计数：遇到 user 开启新一轮；assistant 沿用同一 turn_index
      const role = m.type === 'zsxq' ? 'user' : m.role;
      if (m.role === 'user' || m.role === 'human') currentTurn += 1;
      const turnIdx = currentTurn >= 1 ? currentTurn : null;
      appendMessage(role, m.content, turnIdx != null ? { turnIndex: turnIdx } : {});
    });
    // 实时 pending 轮次：基于当前 DOM 最大 turnIndex 初始化（保证刚发送的未刷新消息也能有连续 turn）
    {
      let maxT = 0;
      document.querySelectorAll('.msg-wrap[data-turn-index]').forEach(w => {
        const n = Number(w.dataset.turnIndex);
        if (!isNaN(n) && n > maxT) maxT = n;
      });
      pendingTurnIndex = maxT;
    }
    if ((data.messages || []).length === 0) {
      appendMessage('system', '会话已就绪，输入问题开始对话。');
    }
    // ===== 切回会话时恢复"任务仍在后台运行"提示（2026-09-09）=====
    // 旧实现：切走再切回时运行中状态丢失，聊天区空白，用户误以为任务丢失。
    TaskManager.resumeIfRunning(sid);
  } catch (e) {
    $('chat').innerHTML = `<div class="msg error">加载历史失败: ${e.message}</div>`;
  }
  // 进入会话后才显示「消息多选」按钮
  const msBtn = $('msg-multi-select-btn');
  if (msBtn) msBtn.style.display = 'inline-block';
  // 若处于多选模式，重新渲染复选框
  if (msgMultiSelectMode) renderMsgCheckboxes();
  // 建立 WebSocket
  connectWS(sid);
  await loadSessions(); // 刷新高亮
}

// ============ WebSocket ============
function connectWS(sid) {
  // 清除旧 ws 的回调，避免关闭时触发重连/错误提示（用户主动切换会话不应被当作异常断连）
  if (ws) {
    ws.onclose = null;
    ws.onerror = null;
    ws.onmessage = null;
    ws.onopen = null;
    try { ws.close(); } catch(e){}
    ws = null;
  }
  clearHeartbeat();
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws/${sid}`);

  ws.onopen = () => {
    console.log('WS connected');
    wsReconnectCount = 0;  // 连接成功后重置重连计数
    // 连接恢复后清除重连提示
    if (_wsReconnectMsg) { _wsReconnectMsg.remove(); _wsReconnectMsg = null; }
    startHeartbeat();
  };

  ws.onmessage = e => {
    try {
      const payload = JSON.parse(e.data);
      handleWSMessage(payload);
    } catch (err) { console.warn('WS 消息解析失败', err); }
  };

  ws.onclose = () => {
    console.log('WS closed');
    clearHeartbeat();
    // 自动重连（有当前会话且未超最大重试次数）
    if (wsReconnectCount < WS_MAX_RECONNECT && currentSessionId) {
      wsReconnectCount++;
      const delay = APP_CONSTANTS.WS_RECONNECT_BASE_DELAY_MS * wsReconnectCount;
      console.log(`WS ${wsReconnectCount}/${WS_MAX_RECONNECT} 重连中（${delay}ms 后）...`);
      // 任务运行中断连：只显示一条重连提示（复用已有消息元素）
      if (isRunning) {
        if (!_wsReconnectMsg) {
          _wsReconnectMsg = appendMessage('system', `连接中断，正在尝试重连（${wsReconnectCount}/${WS_MAX_RECONNECT}）...`);
        } else if (_wsReconnectMsg) {
          _wsReconnectMsg.textContent = `连接中断，正在尝试重连（${wsReconnectCount}/${WS_MAX_RECONNECT}）...`;
        }
        setTimeout(() => {
          // 重连后若 WebSocket 恢复则清除提示；否则报错
          if (ws && ws.readyState === WebSocket.OPEN) {
            if (_wsReconnectMsg) { _wsReconnectMsg.textContent = '连接已恢复'; setTimeout(() => { if (_wsReconnectMsg) { _wsReconnectMsg.remove(); _wsReconnectMsg = null; } }, APP_CONSTANTS.WS_RECONNECT_RECOVERY_HINT_MS); }
          } else if (isRunning) {
            appendMessage('error', '⚠️ WebSocket 重连失败，任务状态可能丢失。可尝试重新发送。');
            setRunning(false);
            _wsReconnectMsg = null;
          }
        }, delay + APP_CONSTANTS.WS_RECONNECT_BASE_DELAY_MS);
      }
      setTimeout(() => { if (currentSessionId) connectWS(currentSessionId); }, delay);
    } else if (isRunning) {
      // 超过重连次数，直接报错
      appendMessage('error', '⚠️ WebSocket 连接断开，任务状态可能丢失。可尝试重新发送。');
      setRunning(false);
      _wsReconnectMsg = null;
    }
  };

  ws.onerror = err => { console.error('WS error', err); };
}

function startHeartbeat() {
  clearHeartbeat();
  wsHeartbeatTimer = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send('ping');
    }
  }, WS_HEARTBEAT_INTERVAL);
}

function clearHeartbeat() {
  if (wsHeartbeatTimer) {
    clearInterval(wsHeartbeatTimer);
    wsHeartbeatTimer = null;
  }
}

function handleWSMessage(p) {
  // 心跳响应忽略
  if (p.type === 'pong') return;
  // monitor 事件
  if (p.type === 'monitor_event') {
    const ev = p.event;
    // ===== 前端兜底：所有 WS monitor 可见文本都走一次绝对路径脱敏 =====
    const msg = maskAbsPaths(p.message || '');
    if (ev === 'session_created') {
      // 工作目录创建提示，前端静默忽略
    } else if (ev === 'assistant_call') {
      // 子智能体调用提示，前端静默忽略
    } else if (ev === 'thinking') {
      // 只显示一次"思考中"，后续只更新计时
      stopProgressTimer();  // 停止通用进度提示，避免重复
      if (!thinkingEl) {
        thinkingEl = appendMessage('event', '💭 ' + msg + '（已耗时 0 秒）');
        thinkingEl.setAttribute('data-base', '💭 ' + msg);  // 保存原始前缀
        thinkingStartTime = Date.now();
        updateThinkingTimer();
      }
    } else if (ev === 'tool_start') {
      // 工具开始执行：合并到顶部单条 thinking 提示中，永不追加新气泡
      // 【修复大段空白】之前每条 tool_start 都可能 appendMessage 独立气泡，
      // 几十条 + 每条的外边距 = 视觉上的大段空白。这里强制只改一条 thinkingEl 文本。
      const toolName = (p.data && p.data.tool_name) ? p.data.tool_name : '';
      const toolArgs = (p.data && p.data.args) ? p.data.args : {};
      let toolDesc = msg;
      if (toolName === 'search_zsxq_by_stock' && toolArgs.stock_name) {
        toolDesc = `🔍 正在知识星球搜索「${toolArgs.stock_name}」的研报/小作文...`;
      } else if (toolName === 'generate_markdown') {
        toolDesc = '📝 正在生成文档...';
      } else if (toolName === 'convert_md_to_pdf') {
        toolDesc = '📄 正在转换PDF...';
      } else if (toolName === 'read_file_content') {
        toolDesc = `📎 正在读取文件「${toolArgs.filename || ''}」...`;
      }
      toolDesc = maskAbsPaths(toolDesc);
      stopProgressTimer();
      // 确保 thinkingEl 仍然挂载在 DOM（防止被 session 切换或异常清掉）
      if (thinkingEl && !document.body.contains(thinkingEl)) {
        thinkingEl = null;
      }
      if (!thinkingEl) {
        thinkingEl = appendMessage('event', '💭 ' + toolDesc + '（已耗时 0 秒）');
        thinkingEl.setAttribute('data-base', '💭 ' + toolDesc);
        thinkingStartTime = Date.now();
        updateThinkingTimer();
      } else {
        // 已有 thinking：只更新 data-base（updateThinkingTimer 定时读它），不新建
        thinkingEl.setAttribute('data-base', '💭 ' + toolDesc);
      }
    } else if (ev === 'tool_end') {
      // 工具执行完成：显示工具返回的结果（包含知识星球搜索结果+Qwen8B分析）
      clearThinkingTimer();
      const toolResult = maskAbsPaths((p.data && p.data.result) ? p.data.result : '');
      const toolName = (p.data && p.data.tool_name) ? p.data.tool_name : '';
      // 过滤"实质空"内容：LLM/tool 偶尔返回字面 "[]"/"null"/"{}" 等空响应
      // 后端已做过滤，这里加一层前端兜底，避免任何路径漏网
      const _isEmptyLike = (s) => {
        if (!s) return true;
        const t = String(s).trim();
        if (!t) return true;
        return ['[]', 'null', 'None', '{}', '""', "''", '()'].includes(t);
      };
      if (toolResult && !_isEmptyLike(toolResult)) {
        appendMessage('assistant', toolResult,
          pendingTurnIndex >= 1 ? { turnIndex: pendingTurnIndex } : {});
      }
    } else if (ev === 'task_result') {
      // 最终结果
      clearThinkingTimer();
      const result = maskAbsPaths((p.data && p.data.result) ? p.data.result : msg);
      // 过滤"实质空"内容（与 tool_end 同样规则）
      const _isEmptyLike = (s) => {
        if (!s) return true;
        const t = String(s).trim();
        if (!t) return true;
        return ['[]', 'null', 'None', '{}', '""', "''", '()'].includes(t);
      };
      if (_isEmptyLike(result)) {
        // 净化后为空 → 不追加消息，保持运行状态（等真正的结果）
        // 但若后续再也没有真实结果，用户会看到一直 loading —— 这里不主动 setRunning(false)，
        // 因为后端最终一定会有完成事件（task_result 真值或 error）
        return;
      }
      // 盘前研报热度结果靠右显示，普通任务结果靠左
      const resultRole = currentTaskType === 'zsxq' ? 'user' : 'assistant';
      currentTaskType = 'normal';
      appendMessage(resultRole, result,
        pendingTurnIndex >= 1 ? { turnIndex: pendingTurnIndex } : {});
      setRunning(false);
      TaskManager.finish(currentSessionId); // 任务完成，清登记
      // 刷新会话列表（标题可能已更新）
      loadSessions();
    } else if (ev === 'error') {
      clearThinkingTimer();
      appendMessage('error', '❌ ' + msg);
      setRunning(false);
      TaskManager.finish(currentSessionId); // 任务失败，清登记
    } else {
      appendMessage('event', msg);
    }
  }
}

// ============ 发送消息（流式 SSE 版本·优先） ============
// _streamEnsureAssistantMsg：确保当前有且仅有一条"流式打字机占位"的 assistant 消息。
function _streamEnsureAssistantMsg(turnIndex) {
  if (currentAssistantMsgEl && document.body.contains(currentAssistantMsgEl)) {
    return currentAssistantMsgEl;
  }
  // 新建：初始内容为空字符串
  currentAssistantText = '';
  const el = appendMessage('assistant', '',
    (turnIndex != null ? { turnIndex: turnIndex } : {}));
  // 标记 stream-msg：后续增量 + 推理折叠 + 来源面板都挂在这条消息上
  const wrap = el.parentNode;  // .msg-wrap
  if (wrap) wrap.classList.add('stream-msg-wrap');

  // —— 推理过程折叠容器（默认折叠，位于正文下方）——
  const reasoningBox = document.createElement('details');
  reasoningBox.className = 'stream-reasoning-box';
  reasoningBox.style.cssText = 'margin-top:8px;padding:6px 10px;background:#f8f9fa;border:1px solid #e6e6e6;border-radius:8px;';
  const sum = document.createElement('summary');
  sum.style.cssText = 'cursor:pointer;color:#555;font-size:12px;user-select:none;';
  sum.textContent = '🧠 推理过程（0 步）';
  reasoningBox.appendChild(sum);
  const reasoningOl = document.createElement('ol');
  reasoningOl.style.cssText = 'margin:6px 0 0 18px;padding:0;color:#444;font-size:12.5px;line-height:1.6;';
  reasoningBox.appendChild(reasoningOl);
  wrap.appendChild(reasoningBox);
  reasoningBox.dataset.reasoningCount = '0';

  // —— 来源索引面板（done 后或 source_ref 事件后出现；初始隐藏）——
  const srcBox = document.createElement('div');
  srcBox.className = 'stream-sources-box';
  srcBox.style.cssText = 'display:none;margin-top:8px;padding:6px 10px;background:#f0f7ff;border:1px solid #cce1ff;border-radius:8px;font-size:12px;color:#2c3e50;';
  wrap.appendChild(srcBox);

  // 正文：contentEditable 禁用选择复制；用 span.text 承载
  el.innerHTML = '';
  const span = document.createElement('span');
  span.className = 'stream-text';
  el.appendChild(span);

  currentAssistantMsgEl = el;
  return el;
}

// _streamAppendText：往打字机 span 里追加纯文本（不重写旧内容避免光标闪烁）
function _streamAppendText(el, chunk) {
  if (!el) return;
  const span = el.querySelector('span.stream-text');
  if (!span) return;
  span.appendChild(document.createTextNode(chunk));
  // 自动滚到底
  const chat = $('chat');
  if (chat) chat.scrollTop = chat.scrollHeight;
}

// _streamRenderText：用 HTML（含引用角标）替换当前正文（仅在 [N] 出现的 done/source 刷新时调用）
// 盘前新闻等结构化输出走 markdown 渲染（表格/标题/列表/加粗），普通文本渲染结果不变
function _streamRenderText(el, text) {
  if (!el) return;
  const span = el.querySelector('span.stream-text');
  if (!span) return;
  span.innerHTML = _renderMarkdown(text);
  _streamAttachCitationLinks(el);
  const chat = $('chat');
  if (chat) chat.scrollTop = chat.scrollHeight;
}

// _escapeHtml：最小 XSS 防护（正文先 escape 再把 [N] 替换成 sup 标签）
function _escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// _renderMarkdown：轻量 markdown 渲染（盘前新闻等结构化输出用）。
// 支持：### 三级标题、**加粗**、- 列表、|...| 表格。全程先 escape 再解析，防 XSS。
function _renderMarkdown(text) {
  if (text === undefined || text === null || text === '') return '';
  const lines = String(text).split('\n');
  const out = [];
  let tableBuf = [];
  let inList = false;

  function flushTable() {
    if (tableBuf.length < 2) {
      for (const r of tableBuf) out.push('<div>' + _inlineMd(r) + '</div>');
      tableBuf = [];
      return;
    }
    const headerCells = tableBuf[0].replace(/^\||\|$/g, '').split('|').map(s => s.trim());
    const bodyRows = tableBuf.slice(2).map(r => r.replace(/^\||\|$/g, '').split('|').map(s => s.trim()));
    let t = '<table class="md-table"><thead><tr>';
    for (const c of headerCells) t += '<th>' + _inlineMd(c) + '</th>';
    t += '</tr></thead><tbody>';
    for (const row of bodyRows) {
      t += '<tr>';
      for (const c of row) t += '<td>' + _inlineMd(c) + '</td>';
      t += '</tr>';
    }
    t += '</tbody></table>';
    out.push(t);
    tableBuf = [];
  }

  // 块边界必须先闭合列表：否则后续标题/新列表会嵌套进上一个 <ul>，
  // 浏览器按嵌套列表逐级缩进（●→○→■），导致复盘预测等输出层层右移
  function closeList() {
    if (inList) { out.push('</ul>'); inList = false; }
  }

  for (const rawLine of lines) {
    const line = rawLine.replace(/\s+$/, '');
    const trimmed = line.trim();
    if (trimmed.startsWith('|') && trimmed.endsWith('|') && trimmed.length > 3) {
      closeList();
      tableBuf.push(trimmed);
      continue;
    }
    if (tableBuf.length) flushTable();

    if (trimmed === '') { closeList(); out.push('<br>'); continue; }
    // 水平分隔线 --- / *** / ___：渲染为 <hr>，不做纯文本
    if (/^(?:-{3,}|\*{3,}|_{3,})$/.test(trimmed.replace(/\s/g, ''))) {
      closeList(); out.push('<hr class="md-hr">'); continue;
    }
    if (/^📅/.test(trimmed)) { closeList(); out.push('<div class="md-meta">' + _inlineMd(trimmed) + '</div>'); continue; }
    // 三级及以上标题（兼容模型输出 ###/####，统一左对齐）
    const h = trimmed.match(/^#{3,6}\s+(.+)$/);
    if (h) { closeList(); out.push('<div class="md-h3">' + _inlineMd(h[1]) + '</div>'); continue; }
    // 列表项：按 trimmed 匹配——模型输出的缩进/嵌套行扁平化为同一级，全部左对齐
    const li = trimmed.match(/^[-*]\s+(.+)$/);
    if (li) {
      if (!inList) { out.push('<ul class="md-list">'); inList = true; }
      out.push('<li>' + _inlineMd(li[1]) + '</li>');
      continue;
    }
    closeList();
    out.push('<div>' + _inlineMd(trimmed) + '</div>');
  }
  if (tableBuf.length) flushTable();
  closeList();
  return out.join('');
}

// 内联 markdown：加粗 **xxx**（已在外层 escape）
function _inlineMd(s) {
  return _escapeHtml(s).replace(/\*\*([^*]+?)\*\*/g, '<strong>$1</strong>');
}

// ===== 前端最后一道"展示脱敏"：绝对路径（盘符 C:\ D:\…、Unix /usr…）统一
//       替换成「工作目录(…/末级名)」，避免后端服务器目录结构被浏览器端用户看到。
//       后端 monitor._emit / stream_bus 已经过了两道，这里是兜底。
var _ABS_PATH_RE = /(?:[A-Za-z]:[\\\/][^\s"'`,\)\]]+|(?:^|(?<![A-Za-z0-9_]))\/[A-Za-z0-9_.\-@][^\s"'`,\)\]]*)/g;
function maskAbsPaths(text) {
  if (text === undefined || text === null) return '';
  var s = String(text);
  if (!s) return s;
  return s.replace(_ABS_PATH_RE, function (raw) {
    var norm = raw.replace(/\\/g, '/').replace(/[\.,;:，。；：]+$/, '');
    if (!norm) return raw;
    var lastIdx = norm.lastIndexOf('/');
    var last = (lastIdx >= 0 ? norm.slice(lastIdx + 1) : norm);
    if (!last) return '';
    // 对 DIR\末级：最多保留末 2 段（sub/name）
    var secIdx = norm.lastIndexOf('/', lastIdx - 1);
    var short = last;
    if (secIdx >= 0) short = norm.slice(secIdx + 1);
    if (short.length > 64) short = last.slice(-48);
    return short;
  });
}

// _streamAttachCitationLinks：把 span 里的 "[N]" 文本节点替换成 <sup class=src-ref>
//   —— 注意：必须以"已经 escape 过 [ ⇒ &#91;"为前提，这里直接处理字符串形式
function _streamAttachCitationLinks(el) {
  if (!el) return;
  const span = el.querySelector('span.stream-text');
  if (!span) return;
  // 直接对 innerHTML 再次替换：匹配"实体 [N]"的两种可能形式（按浏览器写文本的方式）
  let html = span.innerHTML;
  html = html.replace(/\[(\d{1,3})\](?![\(\uff08])/g, (m, n) => {
    const idx = Number(n);
    // 只有 index 在 currentSourceRefs 里存在的，才变可点击角标
    const found = currentSourceRefs.find(s => s.index === idx);
    if (found) {
      return `<sup class="src-ref" data-idx="${idx}" title="${_escapeAttr(found.title || found.url || '来源' + idx)}">[${idx}]</sup>`;
    }
    return `<sup class="src-ref src-ref-missing" data-idx="${idx}">[${idx}]</sup>`;
  });
  span.innerHTML = html;
  // 绑定点击：显示 / 切换来源 popover + 悬停显示卡片
  span.querySelectorAll('sup.src-ref').forEach(sup => {
    sup.style.cssText = 'display:inline-block;margin:0 2px;padding:0 4px;border-radius:10px;background:#e0ecff;color:#1976d2;cursor:pointer;font-size:11px;line-height:1.4;transition:background .15s;';
    sup.addEventListener('mouseenter', (e) => {
      e.stopPropagation();
      sup.style.background = '#c4dafe';
      const idx = Number(sup.dataset.idx);
      if (_citationHoverTimer) { clearTimeout(_citationHoverTimer); _citationHoverTimer = null; }
      _citationHoverTimer = setTimeout(() => _citationShowCard(idx, sup), 150);
    });
    sup.addEventListener('mouseleave', () => {
      sup.style.background = '#e0ecff';
      // 延迟隐藏：给用户时间把鼠标移到卡片上（卡片自身 hover 再续）
      if (_citationHoverTimer) { clearTimeout(_citationHoverTimer); _citationHoverTimer = null; }
      _citationHoverTimer = setTimeout(() => _citationHideCard(), 250);
    });
    sup.addEventListener('click', (e) => {
      e.stopPropagation();
      _citationHideCard();
      const idx = Number(sup.dataset.idx);
      _streamShowSourceBox(el, idx);
    });
  });
}

function _escapeAttr(s) {
  return String(s || '').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// _streamShowSourceBox：在来源面板里高亮某条 + 展开
function _streamShowSourceBox(el, highlightIdx) {
  if (!el) return;
  const wrap = el.parentNode;
  const srcBox = wrap ? wrap.querySelector('.stream-sources-box') : null;
  if (!srcBox) return;
  if (!currentSourceRefs.length) {
    srcBox.style.display = 'block';
    srcBox.innerHTML = '<div>暂无来源信息</div>';
    return;
  }
  srcBox.style.display = 'block';
  srcBox.innerHTML = `<div style="font-weight:600;margin-bottom:6px;color:#1565c0;">📚 信息来源（${currentSourceRefs.length} 条 · 精简模式）</div>` +
    currentSourceRefs.map(s => {
      const hi = highlightIdx === s.index ? 'background:#fff8d6;border-color:#f0d86a;' : '';
      const urlA = s.url
        ? `<a href="${_escapeAttr(s.url)}" target="_blank" rel="noopener noreferrer" style="color:#1565c0;word-break:break-all;text-decoration:none;">🔗 打开来源</a>`
        : '<span style="color:#aaa;font-size:10.5px;">（无外链）</span>';
      // 最少 token。snippet ≤100 字；去掉 reliability/type/发布时间三行冗余元信息
      const SNIPPET_MAX = 100;
      const rawSnip = String(s.snippet || (s._snip ? s._snip : ''));
      const snipRaw = (function (idx) {
        const meta = _citationsMeta[idx];
        if (meta && meta.snippet) return meta.snippet;
        return rawSnip;
      })(s.index);
      const snipText = snipTextHelper(snipRaw, SNIPPET_MAX);
      const snip = snipText
        ? `<div style="margin-top:5px;color:#555;font-size:11.5px;word-break:break-word;line-height:1.5;padding:4px 6px;background:#f6f8fb;border-left:3px solid #74a9f7;">${snipText}</div>`
        : '';
      const TITLE_MAX = 80;
      return `<div style="padding:6px 8px;margin-bottom:4px;border:1px solid #cce1ff;border-radius:6px;${hi}">
        <div style="display:flex;align-items:baseline;gap:6px;">
          <b style="color:#1a56a3;">[${s.index}]</b>
          <span style="font-weight:600;word-break:break-word;line-height:1.35;">${_escapeHtml(String(s.title || '未命名来源').slice(0, TITLE_MAX))}</span>
          <span style="margin-left:auto;">${urlA}</span>
        </div>
        ${snip}
      </div>`;
    }).join('');
}

// 辅助：snippet 硬截 ≤ max 字（末尾补省略号），HTML escape + 换行转 <br>
function snipTextHelper(raw, max) {
  if (!raw) return '';
  const s = String(raw);
  const cut = s.length > max ? (s.slice(0, max - 1) + '…') : s;
  return _escapeHtml(cut).replace(/\n/g, '<br>');
}

// _streamAppendReasoning：往 <details> 的 <ol> 里加一条
function _streamAppendReasoning(el, title, content, elapsedMs) {
  if (!el) return;
  const box = el.parentNode ? el.parentNode.querySelector('details.stream-reasoning-box') : null;
  if (!box) return;
  const sum = box.querySelector('summary');
  const ol = box.querySelector('ol');
  if (!sum || !ol) return;
  const li = document.createElement('li');
  li.style.cssText = 'margin-bottom:4px;list-style:decimal;';
  const head = document.createElement('div');
  head.style.cssText = 'font-weight:600;color:#333;';
  const elapsedTxt = elapsedMs ? ` <span style="color:#888;font-weight:400;">(${elapsedMs}ms)</span>` : '';
  head.innerHTML = _escapeHtml(title || '') + elapsedTxt;
  const body = document.createElement('div');
  body.innerHTML = _escapeHtml(content || '').replace(/\n/g, '<br>');
  body.style.cssText = 'color:#555;';
  li.appendChild(head);
  li.appendChild(body);
  ol.appendChild(li);
  const n = (Number(box.dataset.reasoningCount) || 0) + 1;
  box.dataset.reasoningCount = String(n);
  sum.textContent = `🧠 推理过程（${n} 步）`;
}

// _streamAppendToolCall：把 tool_call/tool_result 也作为推理步追加（折叠面板复用）
function _streamAppendToolCall(el, which, info) {
  let title = '', content = '';
  if (which === 'call') {
    title = `🛠 调用工具：${info.tool_name || ''}`;
    try { content = JSON.stringify(info.args_snippet || {}, null, 2); } catch (e) { content = String(info.args_snippet || ''); }
  } else if (which === 'result') {
    title = `✅ ${info.success ? '完成' : '失败'}：${info.tool_name || ''}（${info.duration_ms || 0}ms）`;
    content = (info.result_snippet || '') + (info.error_msg ? '\n❌ ' + info.error_msg : '');
    if (info.source_refs && info.source_refs.length) {
      content += '\n📎 引用条目：' + info.source_refs.map(s => '[' + s.index + '] ' + s.title).join('；');
    }
  }
  _streamAppendReasoning(el, title, content, info.duration_ms || 0);
}

// _streamFinalizeMsg：把 currentAssistantText 重写成带 [N] 角标的 HTML + 显示来源面板
function _streamFinalizeMsg(el, finalTextOverride) {
  if (!el) return;
  const finalText = typeof finalTextOverride === 'string' ? finalTextOverride : currentAssistantText;
  currentAssistantText = finalText;
  _streamRenderText(el, finalText);
  _streamShowSourceBox(el, -1);
  // 同步写回 dataset.content：复制/多选/编辑 用
  const wrap = el.parentNode;
  if (wrap) wrap.dataset.content = finalText;
  el.dataset.content = finalText;
}

// _streamClearState：结束/取消后重置所有流式状态（不操作 DOM 以免打断）
function _streamClearState(disposeBuf) {
  if (disposeBuf && currentStreamBuf) {
    try { currentStreamBuf.dispose(); } catch (e) { /* noop */ }
  }
  currentStreamBuf = null;
  currentAssistantMsgEl = null;
  currentAssistantText = '';
  currentSourceRefs = [];
  _streamOpenAt = 0;
  _streamFirstDeltaAt = 0;
  _retrievalResetAll();
  // SSE 流结束（完成/中止/异常），清在飞任务登记（未登记时 delete 无害）
  if (currentSessionId) TaskManager.finish(currentSessionId);
}


// _streamRun：通用 SSE 启动 + 事件注册（sendMessage / sendPreMarketNews 共用）
//   query：真实发送给后端盘前新闻的 query
//   userDisplayText：用户气泡里显示的文本（为了盘前新闻显示简化名）
//   opts.taskType：'normal' | 'zsxq'（zsxq 结果靠右显示，旧行为保留；但 SSE 下默认走 normal 也 OK）
async function _streamRun(query, userDisplayText, opts) {
  opts = opts || {};
  const buf = new EventSourceBuffer({
    url: '/api/task/stream',
    body: {
      query: query,
      thread_id: currentSessionId,
      user_id: currentUserId,
      incremental: true,
    },
    connectTimeoutMs: 5000,
    idleTimeoutMs: 180_000,  // 3 分钟：超过则触发空闲超时（比 Agent 主超时 180s 略宽）
    // 默认打开自动重连 + Last-Event-ID 断点续传（3 次指数退避，约 800ms→1.6s→3.2s）
    retry: true,
    maxRetries: 3,
    baseDelayMs: 800,
    idempotentByEventId: true,
  });
  currentStreamBuf = buf;

  // 用户气泡
  pendingTurnIndex += 1;
  if (userDisplayText) appendMessage('user', userDisplayText, { turnIndex: pendingTurnIndex });
  setRunning(true);

  // ---------- 断点续传：toast / 重连 UI 反馈 ----------
  let _retryTimerShown = null;
  const _fmtDelay = (ms) => ms >= 1000 ? `${(ms/1000).toFixed(1)} 秒` : `${ms} ms`;

  buf.on('__retry', (info) => {
    // 每次重连安排：底部 toast「已断线 N/3，正在重连（第 N 次）… X 秒后」
    const msg = `🔁 已断线（${info.code || '网络错误'}）— 正在重连（第 ${info.attempt}/${buf.opts.maxRetries} 次），` +
                `预计 ${_fmtDelay(info.delay || 0)} 后恢复…`;
    showToast(msg, Math.max(1800, (info.delay || 0) + 800));
    console.debug('[SSE] __retry', info);
  });

  buf.on('__replay_start', (info) => {
    console.debug('[SSE] 续传开始（continue / 无缺口）：', info);
  });

  buf.on('__replay_end', (info) => {
    if (!info) return;
    if (info.gap_count > 0) {
      showToast(`⚠️ 已恢复连接，但回放期间有 ${info.gap_count} 条事件缺失（已用最终文本自动补全），` +
                `共回放 ${info.replay_count || 0} 条。`);
    } else if (info.replay_count > 0) {
      showToast(`✅ 连接已恢复（断点续传 OK），共回填 ${info.replay_count} 条。`);
    } else {
      showToast('✅ 连接已恢复。');
    }
    console.debug('[SSE] __replay_end', info);
  });

  buf.on('__gap', (info) => {
    // 缺口：提示用户
    const reasonMap = {
      buffer_overflow: '事件缓存已溢出（断线时间过长）',
      server_restart: '服务端已重启（缓存丢失）',
      not_found: '未找到对应缓存会话',
      session_done: '会话已结束且过期',
    };
    const reasonTxt = reasonMap[(info && info.reason) || ''] || '未知原因';
    const needRestart = (info && info.suggestion) === 'restart';
    if (needRestart) {
      showToast(`🛑 断点不可续（${reasonTxt}），请重新提问。`);
    } else {
      showToast(`⚠️ 断点存在缺口（${reasonTxt}），已自动用最新结果重同步。`);
    }
    console.debug('[SSE] __gap', info);
  });

  buf.on('__resync_required', (info) => {
    // 服务端要求"全量重同步"：清掉当前正在打出来的文本 + 来源索引池，
    //   随后 resync 模式下的 delta(index=-1 整段)/source_ref 会完整重新渲染
    console.debug('[SSE] __resync_required → 清态重置当前消息与引用：', info);
    currentAssistantText = '';
    currentSourceRefs = [];
    _retrievalResetAll();
    if (currentAssistantMsgEl) {
      const textBox = currentAssistantMsgEl.querySelector('.stream-text');
      if (textBox) textBox.textContent = '';
      const srcBox = currentAssistantMsgEl.querySelector('.stream-sources-box');
      if (srcBox) srcBox.innerHTML = '';
      // 推理面板保留（已展示的 CoT 通常不需重打；若用户感觉不对可关页面重新提问）
    }
  });

  buf.on('__duplicate', (d) => {
    // 幂等跳过：debug 日志即可，不给用户展示
    if (d) console.debug('[SSE] 跳过重复 event_id=', d.id, 'event=', d.event);
  });

  buf.on('__warn', (w) => { console.warn('[SSE] WARN', w); });

  // 用户发送新提问时：主动"重置 buf 的 last_event_id & seenIds"，避免误走 RESUME 分支
  //   （发送新 query 意味着"新一轮"，不应该接续上一轮同 thread_id 的事件）
  try {
    if (typeof buf._resetSeenIds === 'function') buf._resetSeenIds();
    buf._lastEventId = '';
  } catch (_) { /* noop */ }

  // 注册事件（open/delta/progress/tool_call/tool_result/reasoning/source_ref/done/error）
  buf.on('__streaming', (info) => {
    console.debug('[SSE] __streaming', info);
  });

  buf.on('open', (p) => {
    _streamOpenAt = performance.now();
    console.debug('[SSE] OPEN request_id=', p && p.request_id, 'thread_id=', p && p.thread_id,
      'OPEN时延=', (_streamOpenAt - performance.timeOrigin).toFixed(0), 'ms from navigation start');
    // 占位 assistant 消息：立即创建（即使还没 delta，用户也能看到"AI 冒头"）
    _streamEnsureAssistantMsg(pendingTurnIndex);
  });

  buf.on('progress', (p) => {
    // 进度：更新顶部通用 thinking 提示（与 WS thinking/tool_start 旧 UI 同一条，不额外新增气泡）
    const detail = maskAbsPaths(p.detail || p.stage || '');
    const percentTxt = p.percent ? ` ${p.percent}%` : '';
    const stageTxt = maskAbsPaths(p.stage || 'AI 处理中');
    const base = `⏳ ${stageTxt}${percentTxt}${detail ? ' — ' + detail : ''}`;
    stopProgressTimer();
    if (!thinkingEl) {
      thinkingEl = appendMessage('event', base + '（已耗时 0 秒）');
      thinkingEl.setAttribute('data-base', base);
      thinkingStartTime = Date.now();
      updateThinkingTimer();
    } else {
      thinkingEl.setAttribute('data-base', base);
    }
  });

  buf.on('delta', (p) => {
    if (!p) return;
    if (!currentAssistantMsgEl) _streamEnsureAssistantMsg(pendingTurnIndex);
    if (_streamFirstDeltaAt === 0) {
      _streamFirstDeltaAt = performance.now();
      console.debug('[SSE] 首 token 时延 (OPEN→DELTA) =',
        (_streamFirstDeltaAt - _streamOpenAt).toFixed(1), 'ms');
    }
    if (p.is_reasoning) {
      // CoT token：暂直接拼到 reasoning 专用缓存（太频繁先忽略， reasoning 事件会一次性推送整段）
      return;
    }
    const txt = typeof p.text === 'string' ? p.text : '';
    // 累积
    currentAssistantText += txt;
    _streamAppendText(currentAssistantMsgEl, txt);
    // 每 ~30 个增量 token，把 currentAssistantText 中的 [N] 渲染一次（避免打字机期间一直无角标）
    if ((p.index || 0) % 30 === 0) {
      _streamRenderText(currentAssistantMsgEl, currentAssistantText);
    }
  });

  buf.on('reasoning', (p) => {
    if (!p) return;
    if (!currentAssistantMsgEl) _streamEnsureAssistantMsg(pendingTurnIndex);
    const stage = p.stage || 'model_coT';
    const stageTitle = _REASONING_STAGE_TITLE[stage] || '推理步骤';
    const stageColor = _REASONING_STAGE_COLOR[stage] || '#6c5ce7';
    // 标题优先用后端给的 title；如果 title 为空或与默认值雷同，则加 stage 彩色标签前缀
    let title = (typeof p.title === 'string' && p.title.trim()) ? p.title : stageTitle;
    const tag = `<span style="display:inline-block;padding:1px 7px;border-radius:10px;color:#fff;background:${stageColor};font-size:11px;font-weight:500;margin-right:6px;vertical-align:1px;">${stageTitle}</span>`;
    title = tag + title;
    _streamAppendReasoning(currentAssistantMsgEl, title, p.content, p.elapsed_ms || 0);
  });

  // -------- §6 新增：实时检索来源（Tavily / IMA / ZSXQ 独立事件） --------
  buf.on('retrieve_result', (p) => {
    if (!p) return;
    const channel = p.channel || 'unknown';
    const chName = { tavily: 'Tavily', ima: 'IMA 知识库', zsxq: '知识星球' }[channel] || channel;
    const total = Array.isArray(p.items) ? p.items.length : 0;
    if (Array.isArray(p.items) && p.items.length) {
      _retrievalAppend(channel, p.items, p.query || '', p.duration_ms || 0);
    }
    // 进度条里显示"检索命中"（复用顶部 thinking）
    const dur = p.duration_ms ? `（${p.duration_ms}ms）` : '';
    const succ = p.success ? `✅ 命中 ${total} 条${dur}` : `❌ 失败 ${p.error_msg || ''}${dur}`;
    const query = p.query ? `「${String(p.query).slice(0, 24)}」` : '';
    const info = `检索 ${chName} ${query} ${succ}`;
    if (!currentAssistantMsgEl) _streamEnsureAssistantMsg(pendingTurnIndex);
    _streamAppendReasoning(currentAssistantMsgEl,
      `<span style="display:inline-block;padding:1px 7px;border-radius:10px;background:#f0f7ff;color:#1976d2;font-size:11px;margin-right:6px;">🔎 检索结果</span>${chName}`,
      info, p.duration_ms || 0);
  });

  // -------- §6 新增：引用角标 [N] → 来源卡片元数据（增量合并） --------
  buf.on('citation_meta', (p) => {
    if (!p || !Array.isArray(p.items)) return;
    const changed = _citationMetaMerge(p.items);
    if (changed && currentAssistantMsgEl) {
      // 重新渲染 [N] 角标：src-ref-missing → src-ref 可点击 + 悬停卡片生效
      _streamRenderText(currentAssistantMsgEl, currentAssistantText);
      // 同步刷新文末来源面板（若已打开则立刻显示新条目）
      _streamShowSourceBox(currentAssistantMsgEl, -1);
    }
  });

  buf.on('tool_call', (p) => {
    if (!p) return;
    if (!currentAssistantMsgEl) _streamEnsureAssistantMsg(pendingTurnIndex);
    _streamAppendToolCall(currentAssistantMsgEl, 'call', p);
  });

  buf.on('tool_result', (p) => {
    if (!p) return;
    if (!currentAssistantMsgEl) _streamEnsureAssistantMsg(pendingTurnIndex);
    _streamAppendToolCall(currentAssistantMsgEl, 'result', p);
    // 把来源引用合并进全局池（tool_result.source_refs 带 index）
    if (Array.isArray(p.source_refs) && p.source_refs.length) {
      p.source_refs.forEach(sr => {
        const existing = currentSourceRefs.find(x => x.index === sr.index);
        if (!existing) currentSourceRefs.push(Object.assign({}, sr));
      });
      currentSourceRefs.sort((a, b) => (a.index || 0) - (b.index || 0));
      // 增量刷新角标渲染
      if (currentAssistantMsgEl) {
        _streamRenderText(currentAssistantMsgEl, currentAssistantText);
      }
    }
  });

  buf.on('source_ref', (p) => {
    if (!p || !Array.isArray(p.items)) return;
    // 全量替换
    currentSourceRefs = p.items.slice().sort((a, b) => (a.index || 0) - (b.index || 0));
    if (currentAssistantMsgEl) {
      _streamRenderText(currentAssistantMsgEl, currentAssistantText);
      _streamShowSourceBox(currentAssistantMsgEl, -1);
    }
  });

  buf.on('done', (p) => {
    clearThinkingTimer();
    _streamFinalizeMsg(currentAssistantMsgEl, (p && p.final_text) ? p.final_text : undefined);
    // 日志
    if (p) {
      const usage = p.usage || {};
      console.debug(`[SSE] DONE duration=${p.total_duration_ms || 0}ms ` +
        `sources=${p.source_ref_count || 0} tools=${p.tool_call_count || 0} ` +
        `usage=${JSON.stringify(usage)}`);
    }
    // 旧链路兼容：清空 task_type（如果有）
    currentTaskType = 'normal';
    setRunning(false);
    _streamClearState(false);
    loadSessions();
  });

  buf.on('error', (p) => {
    clearThinkingTimer();
    const msg = maskAbsPaths(p && p.message ? p.message : '未知错误');
    const cancelled = !!(p && p.cancelled);
    appendMessage(cancelled ? 'system' : 'error',
      (cancelled ? '⏹ ' : '❌ ') + msg);
    // 如果流式消息创建了但没写完，也 finalize（显示已生成的部分 + 来源面板）
    if (currentAssistantMsgEl && currentAssistantText.length > 0 && !cancelled) {
      _streamFinalizeMsg(currentAssistantMsgEl);
    }
    setRunning(false);
    _streamClearState(false);
  });

  buf.on('heartbeat', () => { /* NOP：心跳只做保活，不渲染 */ });

  buf.start();
}

async function sendMessage() {
  const q = $('query-input').value.trim();
  if (!q) return;
  if (!currentSessionId) { alert('请先选择或新建会话'); return; }
  if (isRunning) { alert('当前任务进行中，请等待完成'); return; }
  $('query-input').value = '';
  if (!SSE_STREAM_ENABLED) {
    // ===== 旧分支（SSE_STREAM_ENABLED=false 或未来想一次性切回） =====
    pendingTurnIndex += 1;
    appendMessage('user', q, { turnIndex: pendingTurnIndex });
    setRunning(true);
    try {
      const r = await fetch('/api/task', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({query: q, thread_id: currentSessionId, user_id: currentUserId})
      });
      if (!r.ok) throw new Error('请求失败 ' + r.status);
    } catch (e) {
      appendMessage('error', '发送失败: ' + e.message);
      setRunning(false);
    }
    return;
  }
  // ===== 新分支：SSE 流式 =====
  try {
    await _streamRun(q, q, { taskType: 'normal' });
  } catch (e) {
    appendMessage('error', '发送失败: ' + e.message);
    setRunning(false);
    _streamClearState(true);
  }
}

// ============ 盘前新闻：前端显示简化名，后端发送完整提示词 ============
async function sendPreMarketNews() {
  if (!currentSessionId) { alert('请先选择或新建会话'); return; }
  if (isRunning) { alert('当前任务进行中，请等待完成'); return; }

  // 前端仅展示简短名称，不暴露完整提示词，
  const fullQuery = `盘前新闻`;

  if (SSE_STREAM_ENABLED) {
    // SSE 分支：走 /api/task/stream，打字机 + 来源引用 + 推理面板
    TaskManager.start(currentSessionId, '盘前新闻', 'normal');
    try {
      await _streamRun(fullQuery, '盘前新闻', { taskType: 'normal' });
    } catch (e) {
      appendMessage('error', '发送失败: ' + e.message);
      setRunning(false);
      _streamClearState(true);
      TaskManager.finish(currentSessionId);
    }
    return;
  }

  // ===== 旧分支（SSE_STREAM_ENABLED=false）=====
  // 等待 WebSocket 连接就绪（最多等 3 秒）
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    appendMessage('system', '正在建立连接...');
    let waited = 0;
    while ((!ws || ws.readyState !== WebSocket.OPEN) && waited < APP_CONSTANTS.WS_WAIT_OPEN_TIMEOUT_MS) {
      await new Promise(r => setTimeout(r, APP_CONSTANTS.WS_WAIT_OPEN_POLL_STEP_MS));
      waited += APP_CONSTANTS.WS_WAIT_OPEN_POLL_STEP_MS;
    }
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      appendMessage('error', 'WebSocket 未连接，请检查服务是否正常后重试');
      return;
    }
  }
  pendingTurnIndex += 1;
  appendMessage('user', '盘前新闻', { turnIndex: pendingTurnIndex });
  setRunning(true);
  try {
    const r = await fetch('/api/task', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({query: fullQuery, thread_id: currentSessionId, user_id: currentUserId})
    });
    if (!r.ok) throw new Error('请求失败 ' + r.status);
    TaskManager.start(currentSessionId, '盘前新闻', 'normal');
  } catch (e) {
    appendMessage('error', '发送失败: ' + e.message);
    setRunning(false);
  }
}

// ============ 盘前研报热度：调用后端 zsxq_analysis_runner（知识星球抓取 + LLM 分析）============
async function sendZsxqHotNews() {
  if (!currentSessionId) { alert('请先选择或新建会话'); return; }
  if (isRunning) { alert('当前任务进行中，请等待完成'); return; }

  // 等待 WebSocket 连接就绪（最多等 3 秒）
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    appendMessage('system', '正在建立连接...');
    let waited = 0;
    while ((!ws || ws.readyState !== WebSocket.OPEN) && waited < APP_CONSTANTS.WS_WAIT_OPEN_TIMEOUT_MS) {
      await new Promise(r => setTimeout(r, APP_CONSTANTS.WS_WAIT_OPEN_POLL_STEP_MS));
      waited += APP_CONSTANTS.WS_WAIT_OPEN_POLL_STEP_MS;
    }
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      appendMessage('error', 'WebSocket 未连接，请检查服务是否正常后重试');
      return;
    }
  }

  pendingTurnIndex += 1;
  appendMessage('user', '盘前研报热度', { turnIndex: pendingTurnIndex });
  currentTaskType = 'zsxq';
  setRunning(true, 'zsxq');
  try {
    const r = await fetch('/api/zsxq-analysis', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({thread_id: currentSessionId, user_id: currentUserId})
    });
    if (!r.ok) throw new Error('请求失败 ' + r.status);
    TaskManager.start(currentSessionId, '盘前研报热度', 'zsxq');
    // 结果通过 WebSocket 的 task_result 事件推送，由 handleWSMessage 统一处理
  } catch (e) {
    appendMessage('error', '发送失败: ' + e.message);
    setRunning(false);
  }
}

// ============ 复盘预测：串行执行小作文热度 + 盘前新闻 + DeepSeek 指数预测 ============
async function sendReviewPrediction() {
  if (!currentSessionId) { alert('请先选择或新建会话'); return; }
  if (isRunning) { alert('当前任务进行中，请等待完成'); return; }

  // 等待 WebSocket 连接就绪（最多等 3 秒）
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    appendMessage('system', '正在建立连接...');
    let waited = 0;
    while ((!ws || ws.readyState !== WebSocket.OPEN) && waited < APP_CONSTANTS.WS_WAIT_OPEN_TIMEOUT_MS) {
      await new Promise(r => setTimeout(r, APP_CONSTANTS.WS_WAIT_OPEN_POLL_STEP_MS));
      waited += APP_CONSTANTS.WS_WAIT_OPEN_POLL_STEP_MS;
    }
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      appendMessage('error', 'WebSocket 未连接，请检查服务是否正常后重试');
      return;
    }
  }

  // 读取输入框内容：若用户填写了个股名/关注点，带入复盘预测
  const userInput = $('query-input').value.trim();
  const displayLabel = userInput
    ? `复盘预测（关注：${userInput}）`
    : '复盘预测';
  pendingTurnIndex += 1;
  appendMessage('user', displayLabel, { turnIndex: pendingTurnIndex });
  $('query-input').value = '';
  setRunning(true);
  try {
    const r = await fetch('/api/review-prediction', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        thread_id: currentSessionId,
        user_id: currentUserId,
        user_query: userInput
      })
    });
    if (!r.ok) throw new Error('请求失败 ' + r.status);
    TaskManager.start(currentSessionId, displayLabel, 'normal');
    // 结果通过 WebSocket 的 task_result 事件推送，由 handleWSMessage 统一处理
  } catch (e) {
    appendMessage('error', '发送失败: ' + e.message);
    setRunning(false);
    TaskManager.finish(currentSessionId);
  }
}

function setRunning(v, taskType) {
  isRunning = v;
  $('send-btn').disabled = v;
  $('stop-btn').disabled = !v;
  const preBtn = $('pre-market-btn');
  if (preBtn) preBtn.disabled = v;
  const hotBtn = $('zsxq-hot-btn');
  if (hotBtn) hotBtn.disabled = v;
  const reviewBtn = $('review-prediction-btn');
  if (reviewBtn) reviewBtn.disabled = v;
  // 超时保护：任务开始时启动计时器，到点自动解锁避免页面永久卡死
  // zsxq 盘前研报热度实测全流程 286s，单独走 8 分钟超时（与后端 runner 内部 480s 上限对齐）
  const timeoutMs = (taskType === 'zsxq' && APP_CONSTANTS.ZSXQ_RUNNING_TIMEOUT_MS)
    ? APP_CONSTANTS.ZSXQ_RUNNING_TIMEOUT_MS
    : RUNNING_TIMEOUT;
  const timeoutLabel = (timeoutMs >= 60000)
    ? `${Math.round(timeoutMs / 60000)} 分钟`
    : `${Math.round(timeoutMs / 1000)} 秒`;
  if (runningTimeoutTimer) {
    clearTimeout(runningTimeoutTimer);
    runningTimeoutTimer = null;
  }
  if (v) {
    // 启动进度提示（5 秒后若无响应，显示"AI 正在思考中..."）
    startProgressTimer();
    runningTimeoutTimer = setTimeout(() => {
      if (isRunning) {
        appendMessage('error', `⚠️ 任务执行超时（${timeoutLabel}），已自动解锁。如需继续请重新发送。`);
        setRunning(false);
      }
    }, timeoutMs);
  } else {
    // 任务结束，停止进度提示
    stopProgressTimer();
  }
}

// ============ 停止当前任务 ============
async function stopCurrentTask() {
  const tid = currentSessionId;
  if (!tid) {
    appendMessage('error', '无法停止：未找到当前会话');
    setRunning(false);
    return;
  }

  // ---- (1) 优先：关闭 SSE 连接 + 同步 POST /api/task/stop（EventSourceBuffer.abort 双保险）----
  if (currentStreamBuf) {
    try { currentStreamBuf.abort('user_stop_clicked', true); }
    catch (e) { console.warn('[stop] ESB abort 失败:', e); }
    _streamClearState(true);
  }

  try {
    const resp = await fetch('/api/task/stop', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ thread_id: tid })
    });
    const data = await resp.json();
    if (data.status === 'stopped') {
      appendMessage('system', '⏹ 已停止当前任务，您可以重新输入问题。');
    } else if (data.status === 'not_found') {
      appendMessage('system', '当前没有正在执行的任务。');
    }
  } catch (e) {
    appendMessage('error', '停止任务失败: ' + e.message);
  } finally {
    // 无论后端是否成功，前端都解锁，允许用户再次输入
    TaskManager.finish(tid); // 手动停止，清登记
    setRunning(false);
  }
}

// ============ Toast 提示 ============
function showToast(text, duration = APP_CONSTANTS.TOAST_DEFAULT_DURATION_MS) {
  const t = $('toast');
  t.textContent = text;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), duration);
}

// ============ 上下文菜单状态 ============
let currentContextMsg = null;  // 当前操作的消息 DOM 元素
let currentContentEl = null;  // 当前操作的 .msg 元素（用于文本选择）
let currentContextContent = '';
let currentContextRole = '';
let currentTurnIndex = null;   // 当前操作消息所属轮次（1-based），null 表示尚未绑定历史
let longPressTimer = null;
let longPressStartX = 0, longPressStartY = 0;
const LONG_PRESS_DURATION = APP_CONSTANTS.LONG_PRESS_TRIGGER_MS;  // 长按判定时间(ms)
const LONG_PRESS_MOVE_TOLERANCE = APP_CONSTANTS.LONG_PRESS_MOVE_TOLERANCE_PX;  // 移动容忍像素
let msgIdCounter = 0;
let pendingTurnIndex = 0;  // 未持久化的实时聊天轮次（刷新页面后重新从 DOM 最大 turnIndex 推导）
let msgMultiSelectMode = false;  // 消息多选模式开关
let selectedMsgSet = new Set();  // 多选模式下被选中的消息 key（格式：`${turnIndex}:${role}`）

// ============ 消息多选模式 ============
function toggleMsgMultiSelectMode() {
  msgMultiSelectMode = !msgMultiSelectMode;
  const btn = $('msg-multi-select-btn');
  const bar = $('msg-multi-action-bar');
  if (msgMultiSelectMode) {
    btn.textContent = '✕ 退出多选';
    btn.style.background = '#e53935';
    btn.style.color = '#fff';
    // 加 class 显示（CSS 里写 display:flex !important），不直接改 inline style，
    // 避免与"默认 display:none !important"冲突。
    if (bar) bar.classList.add('msg-multi-active');
    selectedMsgSet.clear();
    renderMsgCheckboxes();
    updateMsgMultiCount();
  } else {
    exitMsgMultiSelectMode();
  }
}

function exitMsgMultiSelectMode() {
  msgMultiSelectMode = false;
  const btn = $('msg-multi-select-btn');
  const bar = $('msg-multi-action-bar');
  if (btn) {
    btn.textContent = '☑ 多选';
    btn.style.background = '';
    btn.style.color = '';
  }
  // 移除 msg-multi-active class，恢复 CSS 默认 display:none !important
  if (bar) {
    bar.classList.remove('msg-multi-active');
    // 保险：如果之前有代码把 inline style.display 改成了 flex，这里也清掉
    // （但主要以 class 为主，因为 CSS 里有 !important）
    bar.style.display = '';
  }
  selectedMsgSet.clear();
  // 清除所有复选框
  document.querySelectorAll('.msg-checkbox').forEach(cb => cb.remove());
  document.querySelectorAll('.msg-wrap').forEach(w => w.classList.remove('selected'));
  updateMsgMultiCount();
}

function renderMsgCheckboxes() {
  // 给每条 user/assistant 消息加复选框
  document.querySelectorAll('.msg-wrap').forEach(w => {
    const existingCb = w.querySelector('.msg-checkbox');
    if (existingCb) existingCb.remove();
    if (!msgMultiSelectMode) return;
    const role = w.dataset.role;
    if (role !== 'user' && role !== 'assistant') return;
    const turnIdx = w.dataset.turnIndex;
    if (!turnIdx) return;
    const key = `${turnIdx}:${role}`;
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.className = 'msg-checkbox';
    cb.style.cssText = 'margin-right:8px;flex-shrink:0;width:18px;height:18px;cursor:pointer;';
    cb.checked = selectedMsgSet.has(key);
    cb.addEventListener('change', () => {
      if (cb.checked) {
        selectedMsgSet.add(key);
        w.classList.add('selected');
      } else {
        selectedMsgSet.delete(key);
        w.classList.remove('selected');
      }
      updateMsgMultiCount();
    });
    // 插入到 spacer 之后
    const spacer = w.querySelector('div[style*="width: 18px"]');
    if (spacer && spacer.nextSibling) {
      w.insertBefore(cb, spacer.nextSibling);
    } else {
      w.insertBefore(cb, w.firstChild.nextSibling);
    }
  });
}

function updateMsgMultiCount() {
  const el = $('msg-multi-count');
  if (el) el.textContent = `已选 ${selectedMsgSet.size} 条`;
  const delBtn = $('msg-multi-delete-btn');
  if (delBtn) delBtn.disabled = selectedMsgSet.size === 0;
}

async function batchDeleteSelectedMsgs() {
  if (selectedMsgSet.size === 0) {
    showToast('未选择任何消息');
    return;
  }
  if (!currentSessionId) {
    showToast('未选择会话');
    return;
  }
  // 解析 selectedMsgSet → items
  const items = [];
  selectedMsgSet.forEach(key => {
    const [tiStr, role] = key.split(':');
    const ti = Number(tiStr);
    if (!isNaN(ti) && ti >= 1 && (role === 'user' || role === 'assistant')) {
      items.push({ turn_index: ti, role: role });
    }
  });
  if (items.length === 0) {
    showToast('选择无效');
    return;
  }
  if (!window.confirm(`确定批量删除 ${items.length} 条消息吗？\n（同一轮的问或答若都被删，整轮自动消失）`)) return;

  const uid = encodeURIComponent(currentUserId || '');
  try {
    showToast(`正在批量删除 ${items.length} 条...`);
    const r = await fetch(
      `/api/sessions/${currentSessionId}/messages/batch-delete`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items: items, user_id: currentUserId || '' }),
      },
    );
    if (!r.ok) {
      const data = await r.json().catch(() => ({}));
      throw new Error(data.detail || `批量删除失败（${r.status}）`);
    }
    const respData = await r.json().catch(() => ({}));
    // 后端已按 turn_index 降序处理 + memory 内部前移；前端最简单的做法是刷新当前会话
    showToast(`已删除 ${respData.ok || items.length} 条（${respData.row_removed || 0} 轮整行被删）`);
    exitMsgMultiSelectMode();
    // 重新加载该会话历史，确保 DOM turn_index 与后端 memory 一致
    if (typeof loadSession === 'function') {
      await loadSession(currentSessionId);
    }
  } catch (e) {
    showToast('批量删除失败：' + e.message);
  }
}

// ============ 消息渲染 ============
// 正则：匹配换行+【工作环境指令】开始到文本结尾的全部内容（规则1-4）
const HIDE_PROMPT_RE = /(?:\r?\n)\s*【工作环境指令】[\s\S]*$/;
function stripHiddenInstructions(text) {
  if (typeof text !== 'string') return text;
  return text.replace(HIDE_PROMPT_RE, '').trimEnd();
}

// ============ 盘前小作文结构化表格渲染 ============
// 后端在 txt 总结末尾附加单行标记块：<<<ZSXQ_TABLE:{"rows":[{name,sentiment,count,sector,summary},...])>>>
// 检测到标记 → 拆出正文文本 + 表格数据；未检测到 → 原样返回。
function _splitZsxqTable(content) {
  if (typeof content !== 'string') return { text: content, rows: null, meta: null };
  const m = content.match(/<<<ZSXQ_TABLE:(\{[\s\S]*?\})>>>/);
  if (!m) return { text: content, rows: null, meta: null };
  try {
    const payload = JSON.parse(m[1]);
    const rows = Array.isArray(payload && payload.rows) ? payload.rows : null;
    if (rows && rows.length) {
      return {
        text: content.replace(m[0], '').trimEnd(),
        rows: rows,
        meta: { total: (payload.total_stocks != null ? payload.total_stocks : rows.length), generated_at: payload.generated_at || '' },
      };
    }
  } catch (e) { /* JSON 解析失败则按原样显示 */ }
  return { text: content, rows: null, meta: null };
}

// 用 DOM API 构建表格（不拼 innerHTML，天然防 XSS）
function _buildZsxqTable(rows) {
  const table = document.createElement('table');
  table.className = 'zsxq-table';
  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');
  ['#', '股票名', '情绪', '研报数', '行业', '摘要'].forEach((h) => {
    const th = document.createElement('th');
    th.textContent = h;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);
  const tbody = document.createElement('tbody');
  rows.forEach((r, i) => {
    const tr = document.createElement('tr');
    const cells = [String(i + 1), r.name || '', r.sentiment || '', String(r.count != null ? r.count : ''), r.sector || '', r.summary || ''];
    cells.forEach((c, idx) => {
      const td = document.createElement('td');
      td.textContent = c;
      if (idx === 2) td.classList.add(c === '利好' ? 'sentiment-bull' : c === '利空' ? 'sentiment-bear' : 'sentiment-neutral');
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  return table;
}

function appendMessage(role, content, options) {
  options = options || {};
  // 前端防御：剥掉不应显示给用户的规则段落
  if (role === 'user') content = stripHiddenInstructions(content);
  if (role === 'assistant') content = stripHiddenInstructions(content);

  const chat = $('chat');
  const msgId = 'msg_' + (++msgIdCounter) + '_' + Date.now().toString(36);
  const turnIndex = options.turnIndex != null ? Number(options.turnIndex) : null;

  const wrap = document.createElement('div');
  wrap.className = 'msg-wrap ' + (role === 'user' ? 'user-msg' : role === 'assistant' ? 'ai-msg' : role + '-wrap');
  wrap.dataset.msgId = msgId;
  wrap.dataset.role = role;
  wrap.dataset.content = content;
  if (turnIndex != null && !isNaN(turnIndex)) wrap.dataset.turnIndex = String(turnIndex);

  // 占位：保持对齐（替代原复选框位置）
  const spacer = document.createElement('div');
  spacer.style.width = '18px';
  spacer.style.flexShrink = '0';
  wrap.appendChild(spacer);

  // 消息主体
  const div = document.createElement('div');
  div.className = 'msg ' + role;
  // 盘前小作文结构化表格：检测 <<<ZSXQ_TABLE:{...}>>> 标记块 → 只渲染元信息行 + 真表格
  // （txt 原文含对齐文本表格，与 HTML 表格重复，整体不显示）
  const _zsxq = _splitZsxqTable(content);
  if (_zsxq.rows) {
    const meta = document.createElement('div');
    meta.className = 'zsxq-table-meta';
    meta.textContent = '📊 盘前研报热度：共 ' + _zsxq.meta.total + ' 只股票'
      + (_zsxq.meta.generated_at ? '（生成于 ' + _zsxq.meta.generated_at + '）' : '');
    div.appendChild(meta);
    div.appendChild(_buildZsxqTable(_zsxq.rows));
    div.classList.add('zsxq-table-msg');
  } else if (role === 'assistant') {
    div.innerHTML = _renderMarkdown(content);
  } else {
    div.textContent = content;
  }
  if (turnIndex != null && !isNaN(turnIndex)) div.dataset.turnIndex = String(turnIndex);

  // 绑定长按 + 右键菜单事件（仅 user/assistant 消息）
  if (role === 'user' || role === 'assistant') {
    bindContextMenu(div, content, role, turnIndex);
  }

  wrap.appendChild(div);

  // hover 复制按钮
  if (role === 'user' || role === 'assistant') {
    const actions = document.createElement('div');
    actions.className = 'msg-actions';
    const copyBtn = document.createElement('button');
    copyBtn.className = 'msg-action-btn';
    copyBtn.title = '复制内容';
    copyBtn.innerHTML = '📋';
    copyBtn.onclick = async (e) => {
      e.stopPropagation();
      // 流式消息闭包 content 为空串，兜底读 finalize 写回的 dataset.content
      await copyText(div.dataset.content || (wrap && wrap.dataset.content) || content);
      copyBtn.classList.add('copied');
      copyBtn.innerHTML = '✓';
      setTimeout(() => { copyBtn.classList.remove('copied'); copyBtn.innerHTML = '📋'; }, APP_CONSTANTS.COPY_BTN_HIGHLIGHT_MS);
    };
    actions.appendChild(copyBtn);
    wrap.appendChild(actions);
  }

  chat.appendChild(wrap);
  chat.scrollTop = chat.scrollHeight;
  // 若处于多选模式，给新消息也加复选框
  if (msgMultiSelectMode && (role === 'user' || role === 'assistant') && turnIndex != null) {
    renderMsgCheckboxes();
  }
  return div;
}

// ============ 绑定长按/右键上下文菜单 ============
function bindContextMenu(msgEl, content, role, turnIndex) {
  // 桌面端：右键
  msgEl.addEventListener('contextmenu', (e) => {
    e.preventDefault();
    e.stopPropagation();
    showContextMenu(e.clientX, e.clientY, content, role, msgEl, turnIndex);
  });

  // 移动端：长按
  msgEl.addEventListener('touchstart', (e) => {
    const t = e.touches[0];
    longPressStartX = t.clientX;
    longPressStartY = t.clientY;
    longPressTimer = setTimeout(() => {
      // 长按触发
      msgEl.classList.add('pressing');
      setTimeout(() => msgEl.classList.remove('pressing'), APP_CONSTANTS.LONG_PRESS_PRESSING_RELEASE_MS);
      showContextMenu(longPressStartX, longPressStartY, content, role, msgEl, turnIndex);
      if (navigator.vibrate) navigator.vibrate(15);  // 触觉反馈
    }, LONG_PRESS_DURATION);
  }, { passive: true });

  msgEl.addEventListener('touchmove', (e) => {
    if (longPressTimer) {
      const t = e.touches[0];
      const dx = Math.abs(t.clientX - longPressStartX);
      const dy = Math.abs(t.clientY - longPressStartY);
      if (dx > LONG_PRESS_MOVE_TOLERANCE || dy > LONG_PRESS_MOVE_TOLERANCE) {
        clearTimeout(longPressTimer);
        longPressTimer = null;
      }
    }
  }, { passive: true });

  msgEl.addEventListener('touchend', () => {
    if (longPressTimer) {
      clearTimeout(longPressTimer);
      longPressTimer = null;
    }
  });

  msgEl.addEventListener('touchcancel', () => {
    if (longPressTimer) {
      clearTimeout(longPressTimer);
      longPressTimer = null;
    }
  });

  // 桌面端：左键长按（备选）
  msgEl.addEventListener('mousedown', (e) => {
    if (e.button !== 0) return;  // 仅左键
    longPressStartX = e.clientX;
    longPressStartY = e.clientY;
    longPressTimer = setTimeout(() => {
      showContextMenu(longPressStartX, longPressStartY, content, role, msgEl, turnIndex);
    }, LONG_PRESS_DURATION);
  });

  msgEl.addEventListener('mousemove', (e) => {
    if (longPressTimer) {
      const dx = Math.abs(e.clientX - longPressStartX);
      const dy = Math.abs(e.clientY - longPressStartY);
      if (dx > LONG_PRESS_MOVE_TOLERANCE || dy > LONG_PRESS_MOVE_TOLERANCE) {
        clearTimeout(longPressTimer);
        longPressTimer = null;
      }
    }
  });

  msgEl.addEventListener('mouseup', () => {
    if (longPressTimer) {
      clearTimeout(longPressTimer);
      longPressTimer = null;
    }
  });
}

// ============ 显示/隐藏上下文菜单 ============
function showContextMenu(x, y, content, role, msgEl, turnIndex) {
  // 流式消息创建时 content 为空串、最终文本在 finalize 时写回 dataset.content，
  // 这里兜底读取最新内容（否则右键复制/分享拿到的是空串 → 分享图空白）
  currentContextContent = content
    || (msgEl && (msgEl.dataset.content || (msgEl.parentNode && msgEl.parentNode.dataset.content)))
    || '';
  currentContextRole = role;
  currentContentEl = msgEl;
  currentTurnIndex = turnIndex != null ? Number(turnIndex) : null;
  suppressNextClick = true;  // 抑制长按后的下一个 click 事件
  const menu = $('context-menu');

  // 清理子菜单状态
  menu.querySelectorAll('.has-submenu').forEach(it => {
    it.classList.remove('open', 'open-right');
  });

  // "删除该消息" 仅 user/assistant 且已绑定 turnIndex 的历史消息可用
  const delItem = $('ctx-delete');
  const delTurnItem = $('ctx-delete-turn');
  const canDelete = (role === 'user' || role === 'assistant') && currentTurnIndex != null && !isNaN(currentTurnIndex) && currentTurnIndex >= 1;
  if (delItem) {
    if (canDelete) {
      delItem.classList.remove('disabled');
      delItem.title = `仅删除该${role === 'user' ? '提问' : '回答'}（保留另一条）`;
    } else {
      delItem.classList.add('disabled');
      delItem.title = (currentTurnIndex == null) ? '仅历史消息可删除（请先刷新页面加载历史）' : '该消息不支持删除';
    }
  }
  if (delTurnItem) {
    if (canDelete) {
      delTurnItem.classList.remove('disabled');
      delTurnItem.title = '删除这一轮问答（该消息及其对应的用户/助手配对消息）';
    } else {
      delTurnItem.classList.add('disabled');
      delTurnItem.title = '仅历史消息可删除';
    }
  }

  menu.style.display = 'block';
  // 先显示以便测量尺寸
  menu.style.left = '0px';
  menu.style.top = '0px';

  const rect = menu.getBoundingClientRect();
  const w = rect.width;
  const h = rect.height;
  const vw = window.innerWidth;
  const vh = window.innerHeight;

  // 边界检测
  if (x + w > vw - 8) x = vw - w - 8;
  if (y + h > vh - 8) y = vh - h - 8;
  if (x < 8) x = 8;
  if (y < 8) y = 8;

  menu.style.left = x + 'px';
  menu.style.top = y + 'px';

  // ---------- 子菜单弹出方向边界检测 ----------
  // 默认：子菜单向左弹出（right:100%）。若左侧空间不足 220px，则改为向右弹出
  const subW = APP_CONSTANTS.CONTEXT_MENU_MIN_LEFT_SPACE_PX;  // 子菜单最小宽度
  const margin = 14;
  const spaceLeft = x - margin;
  const shareItem = $('ctx-share');
  if (shareItem) {
    // 计算分享项在菜单中的偏移（子菜单垂直对齐该菜单项）
    const shareRect = shareItem.getBoundingClientRect();
    const spaceRight = vw - (shareRect.right + margin);
    if (spaceLeft >= subW) {
      shareItem.classList.remove('open-right');
    } else if (spaceRight >= subW) {
      shareItem.classList.add('open-right');
    } else {
      // 左右都不够，选空间更大的一侧
      shareItem.classList.toggle('open-right', spaceRight > spaceLeft);
    }
  }
}

function hideContextMenu() {
  const menu = $('context-menu');
  menu.style.display = 'none';
  currentContextContent = '';
  currentContextRole = '';
  currentContentEl = null;
  currentTurnIndex = null;
  // 清理子菜单展开状态
  menu.querySelectorAll('.has-submenu.open').forEach(it => it.classList.remove('open'));
}

// 长按/右键后抑制下一个 click 事件（防止立即关闭菜单或误操作）
let suppressNextClick = false;

// 点击外部关闭菜单
document.addEventListener('click', (e) => {
  if (suppressNextClick) { suppressNextClick = false; return; }
  const menu = $('context-menu');
  if (menu.style.display === 'block' && !menu.contains(e.target)) {
    hideContextMenu();
  }
});

// ESC 关闭
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') hideContextMenu();
});

// 绑定菜单项点击事件
document.addEventListener('click', (e) => {
  const item = e.target.closest('.ctx-item');
  if (!item) return;                              // 非菜单项点击
  if (item.classList.contains('has-submenu')) {
    // 有子菜单的：点击切换子菜单显示（移动端无 hover，需要点击触发展开）
    e.stopPropagation();
    // 先关闭其它展开的子菜单
    document.querySelectorAll('.has-submenu.open').forEach(sib => {
      if (sib !== item) sib.classList.remove('open');
    });
    item.classList.toggle('open');
    return;
  }
  const action = item.dataset.action;
  if (!action) return;
  handleContextAction(action);
  hideContextMenu();
});

// ============ 上下文菜单动作处理 ============
async function handleContextAction(action) {
  const content = currentContextContent;
  const role = currentContextRole;
  const turnIndex = currentTurnIndex;

  switch (action) {
    case 'copy':
      await copyText(content);
      break;
    case 'share-image':
      await shareAsImage([{ role, content }]);
      break;
    case 'delete': {
      // 单条删除：只删当前右键的这条（user 或 assistant），保留另一条
      if (turnIndex == null || isNaN(turnIndex) || turnIndex < 1) {
        showToast('该消息不能删除（仅已保存的历史问答可删）');
        return;
      }
      if (!currentSessionId) {
        showToast('未选择会话，无法删除');
        return;
      }
      const roleParam = role === 'user' ? 'user' : 'assistant';
      const confirmText = `确定删除这${role === 'user' ? '条提问' : '条回答'}吗？（另一条保留，刷新后仍可见）`;
      if (!window.confirm(confirmText)) return;

      const uid = encodeURIComponent(currentUserId || '');
      try {
        showToast('正在删除...');
        const r = await fetch(
          `/api/sessions/${currentSessionId}/turns/${turnIndex}?user_id=${uid}&role=${roleParam}`,
          { method: 'DELETE' },
        );
        if (!r.ok) {
          const data = await r.json().catch(() => ({}));
          throw new Error(data.detail || `删除失败（${r.status}）`);
        }
        const respData = await r.json().catch(() => ({}));
        // 删除 DOM：只删当前 role 的 wrap
        const wraps = document.querySelectorAll(`.msg-wrap[data-turn-index="${turnIndex}"][data-role="${roleParam}"]`);
        wraps.forEach(w => {
          if (thinkingEl === w || w.contains(thinkingEl)) {
            stopProgressTimer();
            thinkingEl = null;
            thinkingStartTime = 0;
          }
          w.remove();
        });
        // 若后端返回 row_removed（说明另一条也空，整行已删），需要把后续 turn_index 前移 1
        if (respData.result === 'row_removed') {
          document.querySelectorAll('.msg-wrap[data-turn-index]').forEach(w => {
            const idx = Number(w.dataset.turnIndex);
            if (!isNaN(idx) && idx > turnIndex) {
              const next = String(idx - 1);
              w.dataset.turnIndex = next;
              const msgInner = w.querySelector('.msg[data-turn-index]');
              if (msgInner) msgInner.dataset.turnIndex = next;
            }
          });
          showToast(`已删除该${role === 'user' ? '提问' : '回答'}（另一条也已空，整轮已删）`);
        } else {
          showToast(`已删除该${role === 'user' ? '提问' : '回答'}，另一条保留`);
        }
      } catch (e) {
        showToast('删除失败：' + e.message);
      }
      break;
    }
    case 'delete-turn': {
      // 整轮删除：user + assistant 一起删
      if (turnIndex == null || isNaN(turnIndex) || turnIndex < 1) {
        showToast('该消息不能删除（仅已保存的历史问答可删）');
        return;
      }
      if (!currentSessionId) {
        showToast('未选择会话，无法删除');
        return;
      }
      const confirmText = `确定删除第 ${turnIndex} 轮问答吗？（这一轮的问 + 答都会同时删除）`;
      if (!window.confirm(confirmText)) return;

      const uid = encodeURIComponent(currentUserId || '');
      try {
        showToast('正在删除整轮...');
        const r = await fetch(
          `/api/sessions/${currentSessionId}/turns/${turnIndex}?user_id=${uid}&role=all`,
          { method: 'DELETE' },
        );
        if (!r.ok) {
          const data = await r.json().catch(() => ({}));
          throw new Error(data.detail || `删除失败（${r.status}）`);
        }
        // 删除 DOM：同一 turnIndex 下的所有 msg-wrap（user+assistant 的一组）
        const wraps = document.querySelectorAll(`.msg-wrap[data-turn-index="${turnIndex}"]`);
        wraps.forEach(w => {
          if (thinkingEl === w || w.contains(thinkingEl)) {
            stopProgressTimer();
            thinkingEl = null;
            thinkingStartTime = 0;
          }
          w.remove();
        });
        // 后端删除后会把后续 turn_index 前移 1，前端 DOM 同步前移
        document.querySelectorAll('.msg-wrap[data-turn-index]').forEach(w => {
          const idx = Number(w.dataset.turnIndex);
          if (!isNaN(idx) && idx > turnIndex) {
            const next = String(idx - 1);
            w.dataset.turnIndex = next;
            const msgInner = w.querySelector('.msg[data-turn-index]');
            if (msgInner) msgInner.dataset.turnIndex = next;
          }
        });
        showToast(`已删除第 ${turnIndex} 轮问答`);
      } catch (e) {
        showToast('删除失败：' + e.message);
      }
      break;
    }
  }
}

// ============ 通用复制方法 ============
async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    showToast('已复制');
  } catch (err) {
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand('copy'); showToast('已复制'); }
    catch (_) { showToast('复制失败'); }
    document.body.removeChild(ta);
  }
}

// ============ 分享为图片（单条消息快速版） ============
async function shareAsImage(msgs) {
  showToast('正在生成分享图片...');
  try {
    const blob = await generateLongImage(msgs);
    if (!blob) {
      throw new Error('Canvas 生成失败（可能内容过长或浏览器限制）');
    }

    const ts = new Date();
    const fname = `无极股票AI助手对话_${ts.getFullYear()}${String(ts.getMonth()+1).padStart(2,'0')}${String(ts.getDate()).padStart(2,'0')}_${String(ts.getHours()).padStart(2,'0')}${String(ts.getMinutes()).padStart(2,'0')}.png`;
    const blobUrl = URL.createObjectURL(blob);

    // 显示预览覆盖层：用户可长按保存或下载
    const overlay = $('image-preview-overlay');
    const img = $('image-preview-img');
    const saveBtn = $('image-preview-save');
    const closeBtn = $('image-preview-close');

    img.src = blobUrl;
    overlay.style.display = 'flex';

    // 保存图片（桌面端下载，移动端提示长按）
    const onSave = () => {
      try {
        const a = document.createElement('a');
        a.href = blobUrl;
        a.download = fname;
        a.style.display = 'none';
        document.body.appendChild(a);
        a.click();
        // 延迟移除，确保浏览器已触发下载（部分浏览器同步移除会导致下载丢失）
        setTimeout(() => {
          if (a.parentNode) a.parentNode.removeChild(a);
        }, 1000);
        showToast('图片已开始下载');
      } catch (e) {
        console.error('下载失败', e);
        // 回退：新开窗口打开图片，用户可右键另存
        window.open(blobUrl, '_blank');
        showToast('下载失败，请右键图片另存');
      }
    };
    const onClose = () => {
      overlay.style.display = 'none';
      img.src = '';
      URL.revokeObjectURL(blobUrl);
      saveBtn.removeEventListener('click', onSave);
      closeBtn.removeEventListener('click', onClose);
    };

    saveBtn.addEventListener('click', onSave);
    closeBtn.addEventListener('click', onClose);
  } catch (err) {
    console.error('生成图片失败', err);
    showToast('生成图片失败: ' + (err.message || '未知错误'));
  }
}

// ============ 进度提示（避免用户空等待）============
let progressTimer = null;
let progressEl = null;
let thinkingStartTime = 0;

// ============ thinking 事件计时器（只显示一次，实时更新耗时）============
let thinkingEl = null;
let thinkingTimer = null;

function updateThinkingTimer() {
  if (!thinkingEl || !isRunning) {
    clearThinkingTimer();
    return;
  }
  const seconds = Math.round((Date.now() - thinkingStartTime) / 1000);
  // 保留原始消息前缀（如"💭 网络搜索助手正在思考中"），只更新耗时
  const baseText = thinkingEl.getAttribute('data-base') || '思考中';
  thinkingEl.textContent = baseText + '（已耗时 ' + seconds + ' 秒）';
  thinkingTimer = setTimeout(updateThinkingTimer, APP_CONSTANTS.THINKING_TIMER_TICK_MS);  // 每 1 秒更新
}

function clearThinkingTimer() {
  if (thinkingTimer) {
    clearTimeout(thinkingTimer);
    thinkingTimer = null;
  }
  if (thinkingEl) {
    thinkingEl.remove();
    thinkingEl = null;
  }
}

function startProgressTimer() {
  stopProgressTimer();
  thinkingStartTime = Date.now();
  // 5 秒后若无响应，显示"AI 正在思考中..."
  progressTimer = setTimeout(() => {
    if (!isRunning) return;
    progressEl = appendMessage('event', '⏳ AI 正在思考中...');
    updateProgress();
  }, 5000);
}

function updateProgress() {
  if (!isRunning || !progressEl) return;
  const seconds = Math.round((Date.now() - thinkingStartTime) / 1000);
  progressEl.textContent = '⏳ AI 正在思考中...（已思考 ' + seconds + ' 秒）';
  progressTimer = setTimeout(updateProgress, APP_CONSTANTS.PROGRESS_UPDATE_INTERVAL_MS);  // 每 5 秒更新
}

function stopProgressTimer() {
  if (progressTimer) {
    clearTimeout(progressTimer);
    progressTimer = null;
  }
  if (progressEl) {
    progressEl.remove();
    progressEl = null;
  }
  clearThinkingTimer();  // 同时清理 thinking 提示
  thinkingStartTime = 0;
}

// ============ Canvas 生成长图（分享功能核心） ============
async function generateLongImage(msgs) {
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  const DPR = Math.min(window.devicePixelRatio || 1, 4);  // 提高到 4x 提升文字清晰度
  const baseW = 750;  // 设计稿宽度（逻辑像素）
  const padding = 0;   // 去掉四周白边
  const msgMaxW = baseW - padding * 2; 
  const footerH = 0;   // 悬浮水印覆盖在内容之上，无需底部预留高度
  const bubblePadX = 18;
  const bubblePadY = 14;
  const lineH = 26;
  const fontSize = 18;
  const gap = 12;  // 消息间距（仅消息之间，不做上下外边距）

  //用离屏 canvas 度量
  const metricCanvas = document.createElement('canvas');
  const mctx = metricCanvas.getContext('2d');
  mctx.font = `${fontSize}px -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif`;

  const layoutRows = [];
  let contentH = 0;

  for (const m of msgs) {
    const text = String(m.content || '').replace(/<[^>]+>/g, '');  // 去除 HTML 标签
    const lines = splitTextToLines(mctx, text, msgMaxW - bubblePadX * 2);
    const bubbleH = lines.length * lineH + bubblePadY * 2;
    const rowH = bubbleH + gap;
    layoutRows.push({ msg: { ...m, content: text }, lines, bubbleH, rowH });
    contentH += rowH;
  }

  // ---------- 设定 canvas 尺寸（限制最大高度防止浏览器溢出）----------
  const MAX_CANVAS_H = 32767;  // 放宽画布高度上限，避免长图被整体降采样导致文字发糊
  let totalH = Math.max(0, contentH - gap);  // 去掉上下外边距（contentH 末尾多算了一个 gap）
  let scale = 1;
  const pixelH = totalH * DPR;
  if (pixelH > MAX_CANVAS_H) {
    scale = MAX_CANVAS_H / pixelH;
    totalH = totalH * scale;
  }

  canvas.width = Math.round(baseW * DPR * scale);
  canvas.height = Math.round(totalH * DPR * scale);
  canvas.style.width = baseW + 'px';
  canvas.style.height = totalH + 'px';
  // 开启高质量图像平滑（文字与图片缩放时更锐利）
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';
  if (scale < 1) {
    ctx.scale(DPR * scale, DPR * scale);
  } else {
    ctx.scale(DPR, DPR);
  }

  // ---------- 背景：渐变浅灰 ----------
  const bg = ctx.createLinearGradient(0, 0, 0, totalH);
  bg.addColorStop(0, '#f5f7fa');
  bg.addColorStop(1, '#ebeef3');
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, baseW, totalH);

  // ---------- 消息绘制（仅内容气泡：无标题栏/副标题/头像） ----------
  let y = 0;  // 从顶部开始，无上方白边
  for (const row of layoutRows) {
    const isUser = row.msg.role === 'user';

    // 气泡定位（无头像）：用户消息右对齐、AI 消息左对齐
    const bubbleW = Math.min(msgMaxW, bubblePadX * 2 + measureLinesWidth(mctx, row.lines));
    const bx = isUser ? (baseW - padding - bubbleW) : padding;
    const by = y + 4;

    drawRoundRect(ctx, bx, by, bubbleW, row.bubbleH, 14);
    if (isUser) {
      const g = ctx.createLinearGradient(bx, by, bx + bubbleW, by);
      g.addColorStop(0, '#0a84ff');
      g.addColorStop(1, '#34a3ff');
      ctx.fillStyle = g;
    } else {
      ctx.fillStyle = '#ffffff';
    }
    ctx.fill();
    // 气泡阴影
    if (!isUser) {
      ctx.strokeStyle = '#e3e6eb';
      ctx.lineWidth = 1;
      ctx.stroke();
    }

    // 气泡文字
    ctx.fillStyle = isUser ? '#fff' : '#1d1d1f';
    ctx.font = `${fontSize}px -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif`;
    ctx.textBaseline = 'top';
    let lx = bx + bubblePadX;
    let ly = by + bubblePadY;
    for (const ln of row.lines) {
      ctx.fillText(ln, lx, ly);
      ly += lineH;
    }

    y += row.rowH;
  }

  // ---------- 悬浮水印：无极股票AI助手（文档式斜向平铺，不透明度 10%），覆盖在内容之上 ----------
  const wmText = '无极股票AI助手';
  const wmFontSize = 24;
  const wmColor = '#8e8e93';
  const wmAlpha = 0.1;
  const wmAngle = -Math.PI / 6;   // -30° 斜向，文档水印常用角度
  const wmGapX = 300;             // 横向间距
  const wmGapY = 300;            // 纵向间距

  ctx.save();
  ctx.globalAlpha = wmAlpha;
  ctx.fillStyle = wmColor;
  ctx.font = `bold ${wmFontSize}px -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';

  // 以画布中心为原点，斜向平铺覆盖整张图（网格按画布对角线放大，确保旋转后无空白角）
  const wmDiag = Math.sqrt(totalH * totalH + baseW * baseW);
  const wmRows = Math.ceil(wmDiag / wmGapY) + 2;
  const wmCols = Math.ceil(wmDiag / wmGapX) + 2;
  for (let r = -wmRows; r <= wmRows; r++) {
    for (let c = -wmCols; c <= wmCols; c++) {
      ctx.save();
      ctx.translate(baseW / 2 + c * wmGapX, totalH / 2 + r * wmGapY);
      ctx.rotate(wmAngle);
      ctx.fillText(wmText, 0, 0);
      ctx.restore();
    }
  }
  ctx.restore();

  // 使用 toBlob 替代 toDataURL（内存占用更低，移动端大 canvas 兼容性更好）
  return new Promise((resolve) => {
    canvas.toBlob((blob) => {
      if (!blob || blob.size === 0) {
        console.error('toBlob 返回空数据');
        resolve(null);
      } else {
        resolve(blob);
      }
    }, 'image/png', 0.92);
  });
}

// 辅助：将文字拆成多行（按宽度 + 换行符）
function splitTextToLines(mctx, text, maxW) {
  const result = [];
  const paragraphs = String(text).split('\n');
  for (const para of paragraphs) {
    if (para === '') { result.push(''); continue; }
    let line = '';
    for (const ch of para) {
      const tryLine = line + ch;
      const w = mctx.measureText(tryLine).width;
      if (w > maxW && line) {
        result.push(line);
        line = ch;
      } else {
        line = tryLine;
      }
    }
    if (line) result.push(line);
  }
  return result.length ? result : [''];
}

// 辅助：计算多行文最大宽度
function measureLinesWidth(mctx, lines) {
  let maxW = 0;
  for (const l of lines) maxW = Math.max(maxW, mctx.measureText(l).width);
  return maxW;
}

// 辅助：圆角矩形路径
function drawRoundRect(ctx, x, y, w, h, r) {
  r = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + w - r, y);
  ctx.quadraticCurveTo(x + w, y, x + w, y + r);
  ctx.lineTo(x + w, y + h - r);
  ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
  ctx.lineTo(x + r, y + h);
  ctx.quadraticCurveTo(x, y + h, x, y + h - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
