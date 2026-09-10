/**
 * react_moat_card.js — 护城河五维度评估卡（React 18，createElement 写法）
 *
 * 功能：当助手输出护城河分析时，自动识别五维度（品牌/技术/成本/网络效应/转换成本）
 *       的评分，渲染为可视化进度条卡片。
 *
 * react：
 *   - 列表循环渲染（5 个维度 .map）
 *   - 条件渲染（评分等级对应不同颜色）
 *   - useEffect + MutationObserver 监听 DOM 变化
 *   - 正则解析文本提取结构化数据
 */
(function () {
  "use strict";

  if (typeof React === 'undefined' || typeof ReactDOM === 'undefined') return;

  const { useState, useEffect, useRef } = React;
  const h = React.createElement;

  // 五维度定义
  const DIMENSIONS = [
    { key: 'brand',       name: '品牌',     icon: '🏷', color: '#ff6b6b' },
    { key: 'technology',  name: '技术',     icon: '🔬', color: '#4ecdc4' },
    { key: 'cost',        name: '成本',     icon: '💰', color: '#ffe66d' },
    { key: 'network',     name: '网络效应', icon: '🌐', color: '#a29bfe' },
    { key: 'switching',   name: '转换成本', icon: '🔄', color: '#fd79a8' },
  ];

  // 评分等级 → 颜色
  function scoreColor(score) {
    if (score >= 4) return '#27ae60';   // 强 - 绿
    if (score >= 3) return '#f39c12';   // 中 - 橙
    if (score >= 1) return '#e74c3c';   // 弱 - 红
    return '#bdc3c7';                    // 无 - 灰
  }

  function scoreLabel(score) {
    if (score >= 5) return '极宽';
    if (score >= 4) return '宽';
    if (score >= 3) return '中等';
    if (score >= 1) return '窄';
    return '无';
  }

  // 从文本中解析五维度评分
  // 支持格式："品牌：5/5"、"品牌(5分)"、"品牌：极强"、"品牌 5分" 等
  function parseMoatScores(text) {
    if (!text) return null;
    const scores = {};
    let found = false;

    // 维度关键词映射
    const dimMap = {
      '品牌': 'brand', 'brand': 'brand',
      '技术': 'technology', 'technology': 'technology', '研发': 'technology',
      '成本': 'cost', 'cost': 'cost', '成本优势': 'cost',
      '网络效应': 'network', 'network': 'network', '网络': 'network',
      '转换成本': 'switching', 'switching': 'switching', '转换': 'switching',
    };

    // 匹配 "维度：X分" 或 "维度 X/5" 或 "维度：极强/强/中/弱"
    const scoreWordMap = { '极宽': 5, '极强': 5, '宽': 4, '强': 4, '中等': 3, '中': 3, '窄': 2, '弱': 1, '无': 0, '极低': 0 };

    for (const [word, dim] of Object.entries(dimMap)) {
      // 匹配数字评分：品牌：4分、品牌(4/5)、品牌 4
      const numRegex = new RegExp(word + '[:：\\s]*\\(?([0-5])(?:\\s*[/／]\\s*5)?\\)?\\s*分?', 'i');
      const numMatch = text.match(numRegex);
      if (numMatch) {
        scores[dim] = parseInt(numMatch[1], 10);
        found = true;
        continue;
      }
      // 匹配文字评级：品牌：极强、品牌(强)
      const wordRegex = new RegExp(word + '[:：\\s]*\\(?(' + Object.keys(scoreWordMap).join('|') + ')\\)?', 'i');
      const wordMatch = text.match(wordRegex);
      if (wordMatch) {
        scores[dim] = scoreWordMap[wordMatch[1]];
        found = true;
      }
    }

    return found ? scores : null;
  }

  // 单个维度条
  function DimBar({ dim, score }) {
    const pct = (score / 5) * 100;
    const color = scoreColor(score);
    // 刻度点（1~5）
    const ticks = [1, 2, 3, 4, 5].map(function (t) {
      return h("span", {
        key: t,
        className: "moat-dim-tick" + (score >= t ? " active" : ""),
        style: { left: (t / 5 * 100) + "%" }
      });
    });
    return h("div", { className: "moat-dim-row" },
      h("div", { className: "moat-dim-label" },
        h("span", { className: "moat-dim-icon" }, dim.icon),
        h("span", { className: "moat-dim-name" }, dim.name)
      ),
      h("div", { className: "moat-dim-bar" },
        ticks,
        h("div", {
          className: "moat-dim-bar-fill",
          style: { width: pct + "%", background: "linear-gradient(90deg, " + color + "aa, " + color + ")" }
        })
      ),
      h("div", {
        className: "moat-dim-score-badge",
        style: { background: color + "1a", color: color, borderColor: color + "40" }
      },
        score ? score + "/5" : '—',
        score ? h("span", { className: "moat-dim-score-label" }, scoreLabel(score)) : null
      )
    );
  }

  // 主组件
  function MoatCard({ stockName, scores }) {
    if (!scores) return null;

    const totalScore = DIMENSIONS.reduce((sum, d) => sum + (scores[d.key] || 0), 0);
    const avg = (totalScore / DIMENSIONS.length).toFixed(1);
    const overall = totalScore >= 20 ? '极宽护城河' : totalScore >= 15 ? '宽护城河' : totalScore >= 10 ? '中等护城河' : '窄护城河';
    const overallColor = totalScore >= 20 ? '#27ae60' : totalScore >= 15 ? '#2ecc71' : totalScore >= 10 ? '#f39c12' : '#e74c3c';

    return h("div", { className: "moat-card" },
      h("div", { className: "moat-card-header" },
        h("div", { className: "moat-card-title-wrap" },
          h("span", { className: "moat-card-icon-badge" }, "🛡"),
          h("span", { className: "moat-card-title" }, "护城河五维度评估" + (stockName ? " · " + stockName : ""))
        ),
        h("span", {
          className: "moat-card-overall-badge",
          style: { background: overallColor + "1a", color: overallColor, borderColor: overallColor + "40" }
        },
          h("span", { className: "moat-card-overall-label" }, overall),
          h("span", { className: "moat-card-overall-score" }, avg + "/5")
        )
      ),
      h("div", { className: "moat-card-body" },
        DIMENSIONS.map(function (dim) {
          return h(DimBar, { key: dim.key, dim: dim, score: scores[dim.key] || 0 });
        })
      )
    );
  }

  // 监听聊天区新消息，自动识别护城河分析
  function MoatDetector() {
    const [moatData, setMoatData] = useState(null);
    const processedRef = useRef(new Set());

    useEffect(function () {
      function scanMessages() {
        var msgs = document.querySelectorAll('.msg.assistant, .assistant-msg, [data-role="assistant"]');
        for (var i = 0; i < msgs.length; i++) {
          var msg = msgs[i];
          var text = msg.textContent || '';
          // 检查是否包含护城河关键词
          if (text.indexOf('护城河') !== -1 || text.indexOf('品牌') !== -1 && text.indexOf('转换成本') !== -1) {
            var msgId = msg.getAttribute('data-msg-id') || msg.id || ('msg_' + i);
            if (processedRef.current.has(msgId)) continue;
            var scores = parseMoatScores(text);
            if (scores) {
              processedRef.current.add(msgId);
              // 提取股票名（尝试从消息中找）
              setMoatData({ scores: scores, stockName: '' });
            }
          }
        }
      }

      scanMessages();

      // 监听 DOM 变化
      var chat = document.getElementById('chat');
      if (chat) {
        var observer = new MutationObserver(function () {
          scanMessages();
        });
        observer.observe(chat, { childList: true, subtree: true, characterData: true });
        return function () { observer.disconnect(); };
      }
    }, []);

    if (!moatData) return null;
    return h(MoatCard, { scores: moatData.scores, stockName: moatData.stockName });
  }

  // 注入样式
  var STYLE = [
    /* ===== 卡片容器 ===== */
    ".moat-card {",
    "  margin: 14px 0; padding: 18px 20px;",
    "  background: linear-gradient(135deg, #fffdf7 0%, #fff8ed 50%, #fef6e4 100%);",
    "  border: 1px solid rgba(255, 183, 77, 0.35);",
    "  border-radius: 16px;",
    "  box-shadow: 0 4px 16px rgba(255, 152, 0, 0.10), 0 1px 3px rgba(0,0,0,0.04);",
    "  animation: moatFadeIn 0.4s ease-out;",
    "  position: relative; overflow: hidden;",
    "}",
    /* 顶部装饰条 */
    ".moat-card::before {",
    "  content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;",
    "  background: linear-gradient(90deg, #ff9800, #ffb74d, #ffcc80);",
    "}",
    /* ===== 头部 ===== */
    ".moat-card-header {",
    "  display: flex; justify-content: space-between; align-items: center;",
    "  margin-bottom: 16px; flex-wrap: wrap; gap: 10px;",
    "}",
    ".moat-card-title-wrap { display: flex; align-items: center; gap: 10px; }",
    ".moat-card-icon-badge {",
    "  display: inline-flex; align-items: center; justify-content: center;",
    "  width: 30px; height: 30px; border-radius: 9px;",
    "  background: linear-gradient(135deg, #ffb74d, #ff9800);",
    "  font-size: 16px; box-shadow: 0 2px 6px rgba(255,152,0,0.3);",
    "}",
    ".moat-card-title { font-size: 15px; font-weight: 700; color: #4e342e; letter-spacing: 0.2px; }",
    /* 综合评分徽章 */
    ".moat-card-overall-badge {",
    "  display: inline-flex; align-items: center; gap: 8px;",
    "  padding: 6px 14px; border-radius: 20px; border: 1px solid;",
    "  font-weight: 700; backdrop-filter: blur(4px);",
    "}",
    ".moat-card-overall-label { font-size: 13px; }",
    ".moat-card-overall-score { font-size: 12px; opacity: 0.85; font-weight: 600; }",
    /* ===== 维度行 ===== */
    ".moat-card-body { display: flex; flex-direction: column; gap: 12px; }",
    ".moat-dim-row { display: flex; align-items: center; gap: 12px; }",
    ".moat-dim-label {",
    "  width: 88px; display: flex; align-items: center; gap: 6px; flex-shrink: 0;",
    "}",
    ".moat-dim-icon {",
    "  display: inline-flex; align-items: center; justify-content: center;",
    "  width: 22px; height: 22px; border-radius: 6px;",
    "  background: rgba(255,255,255,0.7); font-size: 13px;",
    "  box-shadow: 0 1px 2px rgba(0,0,0,0.06);",
    "}",
    ".moat-dim-name { font-size: 12.5px; font-weight: 600; color: #5d4037; }",
    /* 进度条轨道 */
    ".moat-dim-bar {",
    "  flex: 1; height: 22px; position: relative;",
    "  background: rgba(0,0,0,0.04); border-radius: 11px;",
    "  border: 1px solid rgba(0,0,0,0.05);",
    "  overflow: hidden;",
    "}",
    /* 刻度点 */
    ".moat-dim-tick {",
    "  position: absolute; top: 50%; transform: translate(-50%, -50%);",
    "  width: 3px; height: 10px; border-radius: 2px;",
    "  background: rgba(255,255,255,0.6); z-index: 2;",
    "  transition: background 0.3s;",
    "}",
    ".moat-dim-tick.active { background: rgba(255,255,255,0.95); }",
    /* 进度条填充 */
    ".moat-dim-bar-fill {",
    "  height: 100%; border-radius: 11px;",
    "  transition: width 0.8s cubic-bezier(0.22, 1, 0.36, 1);",
    "  position: relative; z-index: 1;",
    "  box-shadow: 0 0 8px rgba(0,0,0,0.1);",
    "}",
    /* 填充光泽动画 */
    ".moat-dim-bar-fill::after {",
    "  content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0;",
    "  background: linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.35) 50%, transparent 100%);",
    "  animation: moatShimmer 2s ease-in-out infinite;",
    "}",
    /* 评分徽章 */
    ".moat-dim-score-badge {",
    "  min-width: 64px; padding: 4px 10px; border-radius: 8px;",
    "  border: 1px solid; display: inline-flex; flex-direction: column;",
    "  align-items: center; justify-content: center; flex-shrink: 0;",
    "  line-height: 1.2;",
    "}",
    ".moat-dim-score-badge > :first-child { font-size: 13px; font-weight: 700; }",
    ".moat-dim-score-label { font-size: 10px; opacity: 0.8; font-weight: 500; }",
    /* ===== 动画 ===== */
    "@keyframes moatFadeIn {",
    "  from { opacity: 0; transform: translateY(8px); }",
    "  to { opacity: 1; transform: translateY(0); }",
    "}",
    "@keyframes moatShimmer {",
    "  0% { transform: translateX(-100%); }",
    "  100% { transform: translateX(100%); }",
    "}"
  ].join("\\n");

  var styleEl = document.createElement("style");
  styleEl.textContent = STYLE;
  document.head.appendChild(styleEl);

  // 挂载（应用JS会重建#chat，需用MutationObserver保护挂载点）
  var mount = document.createElement("div");
  mount.id = "react-moat-card-root";

  function ensureMoatMounted() {
    if (!document.getElementById("react-moat-card-root")) {
      var chat = document.getElementById("chat");
      if (chat) {
        var monitorRoot = document.getElementById("react-agent-monitor-root");
        if (monitorRoot && monitorRoot.parentNode === chat) {
          chat.insertBefore(mount, monitorRoot.nextSibling);
        } else {
          chat.insertBefore(mount, chat.firstChild);
        }
      } else {
        document.body.appendChild(mount);
      }
    }
  }
  ensureMoatMounted();

  var root = ReactDOM.createRoot(mount);
  root.render(h(MoatDetector));

  var chatEl = document.getElementById("chat");
  if (chatEl) {
    var observer = new MutationObserver(function () {
      if (!document.getElementById("react-moat-card-root")) {
        ensureMoatMounted();
      }
    });
    observer.observe(chatEl, { childList: true });
  }
})();
