/**
 * react_moat_card.js — 护城河五维度评估卡（React 18，createElement 写法）
 *
 * 功能：当助手输出护城河分析时，自动识别五维度（品牌/技术/成本/网络效应/转换成本）
 *       的评分，渲染为可视化进度条卡片。
 *
 * 面试展示点：
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
    return h("div", { className: "moat-dim-row" },
      h("div", { className: "moat-dim-label" },
        h("span", { className: "moat-dim-icon" }, dim.icon),
        h("span", { className: "moat-dim-name" }, dim.name)
      ),
      h("div", { className: "moat-dim-bar" },
        h("div", {
          className: "moat-dim-bar-fill",
          style: { width: pct + "%", background: scoreColor(score) }
        })
      ),
      h("div", { className: "moat-dim-score", style: { color: scoreColor(score) } },
        score ? score + "/5 " + scoreLabel(score) : '—'
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
        h("span", { className: "moat-card-title" }, "🛡 护城河五维度评估" + (stockName ? " · " + stockName : "")),
        h("span", { className: "moat-card-overall", style: { color: overallColor } },
          overall + "（综合 " + avg + "/5）"
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
    ".moat-card { margin: 12px 0; padding: 16px; background: linear-gradient(135deg, #fff9f0 0%, #fff5e6 100%); border: 1px solid #ffe0b2; border-radius: 12px; box-shadow: 0 2px 8px rgba(255,152,0,0.08); }",
    ".moat-card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; flex-wrap: wrap; gap: 8px; }",
    ".moat-card-title { font-size: 14px; font-weight: 700; color: #e65100; }",
    ".moat-card-overall { font-size: 13px; font-weight: 700; }",
    ".moat-card-body { display: flex; flex-direction: column; gap: 10px; }",
    ".moat-dim-row { display: flex; align-items: center; gap: 10px; }",
    ".moat-dim-label { width: 90px; display: flex; align-items: center; gap: 5px; flex-shrink: 0; }",
    ".moat-dim-icon { font-size: 14px; }",
    ".moat-dim-name { font-size: 12px; font-weight: 600; color: #5d4037; }",
    ".moat-dim-bar { flex: 1; height: 18px; background: #f5f5f5; border-radius: 9px; overflow: hidden; }",
    ".moat-dim-bar-fill { height: 100%; border-radius: 9px; transition: width 0.6s ease; }",
    ".moat-dim-score { width: 80px; text-align: right; font-size: 11px; font-weight: 600; flex-shrink: 0; }"
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
