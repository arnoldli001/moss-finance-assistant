/**
 * react_monitor.js — 多Agent并行执行监控面板（React 18，createElement 写法，无需 Babel）
 *
 * 功能：实时展示当前任务中各子Agent/数据源的执行状态（进行中/已完成/耗时）。
 * 数据源：监听全局 CustomEvent（moss:tool_call / moss:tool_result / moss:task_done），
 *          由 index.html 的 SSE 事件回调转发。
 *
 * 面试展示点：
 *   - 列表循环渲染（.map + key）
 *   - useEffect 副作用清理（事件监听 + 定时器）
 *   - 状态驱动重渲染（useState 状态变更触发卡片更新）
 *   - 条件渲染（不同状态不同颜色/图标）
 */
(function () {
  "use strict";

  if (typeof React === 'undefined' || typeof ReactDOM === 'undefined') return;

  const { useState, useEffect, useRef, useCallback } = React;
  const h = React.createElement;

  // 工具名 → 展示信息映射（图标 + 中文名称 + 分类颜色）
  const AGENT_META = {
    internet_search: { icon: "🔍", name: "网络搜索", color: "#0a84ff" },
    tavily: { icon: "🔍", name: "网络搜索", color: "#0a84ff" },
    search_knowledge_base: { icon: "📚", name: "IMA知识库", color: "#34c759" },
    ima: { icon: "📚", name: "IMA知识库", color: "#34c759" },
    search_zsxq: { icon: "💬", name: "知识星球", color: "#ff9500" },
    zsxq: { icon: "💬", name: "知识星球", color: "#ff9500" },
    sql_query: { icon: "🗄", name: "数据库查询", color: "#af52de" },
    database_query: { icon: "🗄", name: "数据库查询", color: "#af52de" },
    _default: { icon: "🛠", name: "工具调用", color: "#8e8e93" },
  };

  function resolveMeta(toolName) {
    if (!toolName) return AGENT_META._default;
    const lower = String(toolName).toLowerCase();
    for (const key of Object.keys(AGENT_META)) {
      if (key !== "_default" && lower.indexOf(key) !== -1) return AGENT_META[key];
    }
    return AGENT_META._default;
  }

  // 单个 Agent 卡片
  function AgentCard(props) {
    var agent = props.agent;
    var meta = resolveMeta(agent.name);
    var isRunning = agent.status === "running";
    var isError = agent.status === "error";

    var statusText = isRunning ? "执行中" : (isError ? "失败" : "已完成");
    var statusColor = isRunning ? meta.color : (isError ? "#ff3b30" : "#34c759");
    var progressWidth = isRunning ? 60 : 100;

    return h("div", {
      className: "react-agent-card " + (isRunning ? "running" : (isError ? "error" : "done")),
      style: { borderLeftColor: statusColor }
    },
      h("div", { className: "react-agent-header" },
        h("span", { className: "react-agent-icon" }, meta.icon),
        h("span", { className: "react-agent-name" }, meta.name),
        h("span", { className: "react-agent-status", style: { color: statusColor } }, statusText)
      ),
      h("div", { className: "react-agent-progress" },
        h("div", {
          className: "react-agent-progress-bar",
          style: {
            width: progressWidth + "%",
            background: statusColor,
            animation: isRunning ? "reactPulse 1.2s infinite" : "none"
          }
        })
      ),
      h("div", { className: "react-agent-footer" },
        agent.durationMs != null
          ? h("span", null, "⏱ " + agent.durationMs + "ms")
          : (isRunning ? h("span", null, "⏱ 计算中…") : null),
        agent.detail
          ? h("span", { className: "react-agent-detail", title: agent.detail }, agent.detail)
          : null
      )
    );
  }

  // 主面板
  function AgentMonitor() {
    var _useState = useState([]);
    var agents = _useState[0];
    var setAgents = _useState[1];

    var _useState2 = useState(false);
    var visible = _useState2[0];
    var setVisible = _useState2[1];

    var _useState3 = useState(0);
    var completedCount = _useState3[0];
    var setCompletedCount = _useState3[1];

    var hideTimerRef = useRef(null);

    var updateAgent = useCallback(function (callId, patch) {
      setAgents(function (prev) {
        var idx = -1;
        for (var i = 0; i < prev.length; i++) {
          if (prev[i].callId === callId) { idx = i; break; }
        }
        if (idx === -1) {
          var next = prev.slice();
          next.push(Object.assign({ callId: callId }, patch));
          return next;
        }
        var next2 = prev.slice();
        next2[idx] = Object.assign({}, next2[idx], patch);
        return next2;
      });
    }, []);

    useEffect(function () {
      var onToolCall = function (e) {
        var p = e.detail || {};
        var callId = p.call_id || ("auto_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6));
        var meta = resolveMeta(p.tool_name);
        updateAgent(callId, {
          name: p.tool_name || meta.name,
          status: "running",
          startedAt: Date.now(),
          detail: p.args_snippet ? JSON.stringify(p.args_snippet) : ""
        });
        setVisible(true);
        if (hideTimerRef.current) {
          clearTimeout(hideTimerRef.current);
          hideTimerRef.current = null;
        }
      };

      var onToolResult = function (e) {
        var p = e.detail || {};
        var callId = p.call_id;
        if (!callId) return;
        updateAgent(callId, {
          status: p.success === false ? "error" : "done",
          durationMs: p.duration_ms || 0
        });
      };

      var onTaskDone = function () {
        setCompletedCount(function (c) { return c + 1; });
        hideTimerRef.current = setTimeout(function () {
          setVisible(false);
          setAgents([]);
        }, 3000);
      };

      window.addEventListener("moss:tool_call", onToolCall);
      window.addEventListener("moss:tool_result", onToolResult);
      window.addEventListener("moss:task_done", onTaskDone);

      return function () {
        window.removeEventListener("moss:tool_call", onToolCall);
        window.removeEventListener("moss:tool_result", onToolResult);
        window.removeEventListener("moss:task_done", onTaskDone);
        if (hideTimerRef.current) clearTimeout(hideTimerRef.current);
      };
    }, [updateAgent]);

    if (!visible) return null;

    var runningCount = 0;
    var doneCount = 0;
    for (var j = 0; j < agents.length; j++) {
      if (agents[j].status === "running") runningCount++;
      else if (agents[j].status === "done") doneCount++;
    }

    var cards = agents.map(function (agent) {
      return h(AgentCard, { key: agent.callId, agent: agent });
    });

    return h("div", { className: "react-monitor-panel" },
      h("div", { className: "react-monitor-header" },
        h("span", { className: "react-monitor-title" }, "🤖 多Agent执行监控"),
        h("span", { className: "react-monitor-count" },
          (runningCount > 0 ? runningCount + " 个执行中" : doneCount + " 个已完成")
          + (completedCount > 0 ? " · 本轮完成 " + completedCount + " 次" : "")
        )
      ),
      h("div", { className: "react-monitor-grid" }, cards)
    );
  }

  // 注入样式
  var STYLE = [
    ".react-monitor-panel { margin: 0 0 16px 0; padding: 14px 16px; background: linear-gradient(135deg, #f8f9ff 0%, #f0f4ff 100%); border: 1px solid #e0e5f0; border-radius: 12px; box-shadow: 0 2px 8px rgba(10,132,255,0.06); }",
    ".react-monitor-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }",
    ".react-monitor-title { font-size: 14px; font-weight: 700; color: #1d1d1f; }",
    ".react-monitor-count { font-size: 12px; color: #6c6c70; }",
    ".react-monitor-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 10px; }",
    ".react-agent-card { background: #fff; border-radius: 10px; padding: 10px 12px; border-left: 3px solid #ccc; transition: all 0.2s; }",
    ".react-agent-card.running { box-shadow: 0 0 0 1px rgba(10,132,255,0.15); }",
    ".react-agent-card.done { opacity: 0.85; }",
    ".react-agent-card.error { background: #fff5f5; border-left-color: #ff3b30; }",
    ".react-agent-header { display: flex; align-items: center; gap: 6px; margin-bottom: 8px; }",
    ".react-agent-icon { font-size: 16px; }",
    ".react-agent-name { font-size: 13px; font-weight: 600; color: #1d1d1f; flex: 1; }",
    ".react-agent-status { font-size: 11px; font-weight: 600; }",
    ".react-agent-progress { height: 4px; background: #f0f0f2; border-radius: 2px; overflow: hidden; margin-bottom: 6px; }",
    ".react-agent-progress-bar { height: 100%; border-radius: 2px; transition: width 0.3s; }",
    ".react-agent-footer { display: flex; justify-content: space-between; align-items: center; font-size: 11px; color: #8e8e93; }",
    ".react-agent-detail { max-width: 90px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }",
    "@keyframes reactPulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }"
  ].join("\n");

  var styleEl = document.createElement("style");
  styleEl.textContent = STYLE;
  document.head.appendChild(styleEl);

  // 挂载到聊天区顶部（应用JS会重建#chat，需用MutationObserver保护挂载点）
  var mount = document.createElement("div");
  mount.id = "react-agent-monitor-root";

  function ensureMounted() {
    if (!document.getElementById("react-agent-monitor-root")) {
      var chat = document.getElementById("chat");
      if (chat) {
        chat.insertBefore(mount, chat.firstChild);
      } else {
        document.body.appendChild(mount);
      }
    }
  }
  ensureMounted();

  var root = ReactDOM.createRoot(mount);
  root.render(h(AgentMonitor));

  // 监听 #chat 的子节点变化，若 mount 被移除则重新插入（应用重建聊天区时触发）
  var chatEl = document.getElementById("chat");
  if (chatEl) {
    var observer = new MutationObserver(function () {
      if (!document.getElementById("react-agent-monitor-root")) {
        ensureMounted();
      }
    });
    observer.observe(chatEl, { childList: true });
  }
})();
