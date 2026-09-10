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
  // 支持格式：
  //   1. 数字评分："品牌：5/5"、"品牌(4分)"、"品牌 5"
  //   2. 文字评级："品牌：极强"、"品牌(强)"
  //   3. 段落描述："技术维度（国产第一梯队，但对国际巨头存在代差）"
  function parseMoatScores(text) {
    if (!text) return null;
    const scores = {};

    // 维度关键词映射（中文词 → 维度key），长词优先
    const dimMap = [
      { words: ['品牌', '客户与品牌', '客户集中度'], key: 'brand' },
      { words: ['技术', '研发', '专利', '创新', '芯片架构', '制程'], key: 'technology' },
      { words: ['成本', '毛利率', '规模效应', '供应链'], key: 'cost' },
      { words: ['网络效应', '网络', '平台效应', '生态', '软件生态', '开发者生态'], key: 'network' },
      { words: ['转换成本', '转换', '用户粘性', '客户锁定', '忠诚度', '粘性'], key: 'switching' },
    ];

    // 文字评级 → 分数映射（长词优先）
    const scoreWordMap = {
      '极宽': 5, '极强': 5, '极高': 5, '非常强': 5, '显著': 5, '深厚': 5, '强大': 5, '第一梯队': 5,
      '宽': 4, '强': 4, '高': 4, '较高': 4, '较强': 4, '明显': 4, '突出': 4, '领先': 4, '优势': 4,
      '中等': 3, '中': 3, '一般': 3, '适中': 3, '尚可': 3, '追赶': 3,
      '窄': 2, '弱': 2, '低': 2, '较低': 2, '较弱': 2, '有限': 2, '不明显': 2, '差距悬殊': 2, '受限': 2,
      '无': 0, '极低': 0, '没有': 0, '缺失': 0, '无优势': 0, '薄弱': 1, '劣势': 1,
    };
    const scoreWords = Object.keys(scoreWordMap).sort(function(a,b){return b.length-a.length;});

    // 正面/负面关键词（用于段落描述推断评分）
    const positiveWords = ['领先', '优势', '极强', '强大', '显著', '深厚', '第一梯队', '高', '强', '突出', '明显', '领先', '国产维度领先'];
    const negativeWords = ['劣势', '受限', '差距悬殊', '代差', '暴跌', '断供', '依赖', '无优势', '薄弱', '缺失', '低', '弱', '有限', '不明显', '差距'];

    function inferScoreFromSegment(segment) {
      // 先找评级词，但要处理否定形式（如"无优势"、"没有优势"）
      for (var i = 0; i < scoreWords.length; i++) {
        var w = scoreWords[i];
        var idx = segment.indexOf(w);
        if (idx === -1) continue;
        // 检查前面2个字符内是否有否定词
        var before = segment.substring(Math.max(0, idx - 3), idx);
        var hasNegation = before.indexOf('无') !== -1 || before.indexOf('没有') !== -1 ||
                          before.indexOf('不') !== -1 || before.indexOf('缺乏') !== -1;
        if (hasNegation) {
          // 否定形式：高分词变低分
          var orig = scoreWordMap[w];
          return orig >= 4 ? 1 : (orig >= 3 ? 2 : orig);
        }
        return scoreWordMap[w];
      }
      // 再用正面/负面关键词推断
      var posCount = 0, negCount = 0;
      positiveWords.forEach(function(w) {
        // 同样检查否定形式
        var idx = segment.indexOf(w);
        if (idx === -1) return;
        var before = segment.substring(Math.max(0, idx - 3), idx);
        var hasNeg = before.indexOf('无') !== -1 || before.indexOf('没有') !== -1 || before.indexOf('不') !== -1;
        if (hasNeg) negCount++; else posCount++;
      });
      negativeWords.forEach(function(w) { if (segment.indexOf(w) !== -1) negCount++; });
      if (posCount > 0 && negCount === 0) return 4;
      if (posCount > 0 && negCount > 0) return 3;
      if (posCount === 0 && negCount > 0) return 1;
      return -1; // 无法推断
    }

    function extractSegment(text, dimWords, currentKey) {
      // 找到维度关键词后面的段落（到换行或下一个明确的维度标题）
      for (var i = 0; i < dimWords.length; i++) {
        var word = dimWords[i];
        var idx = text.indexOf(word);
        if (idx === -1) continue;
        var start = idx + word.length;
        var segment = text.substring(start, start + 200);
        // 截到换行
        var nlIdx = segment.indexOf('\\n');
        if (nlIdx > 0) segment = segment.substring(0, nlIdx);
        // 截到下一个维度标题（要求前面有换行或冒号，避免"迁移成本"误匹配）
        var nextDimIdx = segment.length;
        dimMap.forEach(function(d) {
          if (d.key === currentKey) return;
          d.words.forEach(function(w) {
            if (w.length < 2) return; // 跳过单字词如"成本"、"技术"
            var p = segment.indexOf(w);
            // 只在词前面是换行、冒号、或段首时才当作维度标题
            if (p > 0) {
              var before = segment.charAt(p - 1);
              if (before === '\\n' || before === '：' || before === ':' || before === ' ' || before === '（' || before === '(') {
                if (p < nextDimIdx) nextDimIdx = p;
              }
            }
          });
        });
        // 截到句号
        var periodIdx = segment.indexOf('。');
        if (periodIdx > 0 && periodIdx < nextDimIdx) nextDimIdx = Math.min(nextDimIdx, periodIdx + 1);
        return segment.substring(0, nextDimIdx);
      }
      return '';
    }

    var found = false;
    dimMap.forEach(function(dim) {
      if (scores[dim.key] !== undefined) return;

      // 1) 先尝试数字评分：品牌：4/5、品牌(4分)
      for (var i = 0; i < dim.words.length; i++) {
        var word = dim.words[i];
        var numRegex = new RegExp(word + '[^0-9]{0,8}([0-5](?:\\.\\d)?)(?:\\s*[/／]\\s*5)?\\s*分?', 'i');
        var numMatch = text.match(numRegex);
        if (numMatch) {
          scores[dim.key] = Math.round(parseFloat(numMatch[1]));
          found = true;
          return;
        }
      }

      // 2) 提取该维度的描述段落，从中推断评分
      var segment = extractSegment(text, dim.words, dim.key);
      if (segment) {
        var score = inferScoreFromSegment(segment);
        if (score >= 0) {
          scores[dim.key] = score;
          found = true;
        }
      }
    });

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
    const bestRef = useRef(null); // 保留解析维度最多的结果

    useEffect(function () {
      function scanMessages() {
        // 助手消息结构：.msg-wrap.ai-msg > .msg.assistant
        var wraps = document.querySelectorAll('.msg-wrap.ai-msg');
        var bestScores = null;
        var bestCount = 0;
        for (var i = 0; i < wraps.length; i++) {
          var wrap = wraps[i];
          var inner = wrap.querySelector('.msg.assistant');
          if (!inner) continue;
          var text = inner.textContent || '';
          // 检查是否包含护城河关键词
          if (text.indexOf('护城河') !== -1 ||
              (text.indexOf('品牌') !== -1 && text.indexOf('转换成本') !== -1)) {
            var scores = parseMoatScores(text);
            if (scores) {
              var count = Object.keys(scores).length;
              // 保留维度数最多的解析结果（流式过程中逐步完善）
              if (count > bestCount) {
                bestCount = count;
                bestScores = scores;
              }
            }
          }
        }
        if (bestScores && (!bestRef.current || Object.keys(bestScores).length > Object.keys(bestRef.current.scores).length)) {
          bestRef.current = { scores: bestScores, stockName: '' };
          setMoatData(bestRef.current);
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
  // 挂载在 #chat 末尾，确保护城河卡片出现在所有消息下方
  var mount = document.createElement("div");
  mount.id = "react-moat-card-root";

  function ensureMoatMounted() {
    var chat = document.getElementById("chat");
    if (!chat) {
      if (!document.getElementById("react-moat-card-root")) {
        document.body.appendChild(mount);
      }
      return;
    }
    // 若挂载点不在 chat 中，追加到末尾
    if (mount.parentNode !== chat) {
      chat.appendChild(mount);
    } else {
      // 若已在 chat 中但不是最后一个元素，移到末尾
      if (chat.lastChild !== mount) {
        chat.appendChild(mount);
      }
    }
  }
  ensureMoatMounted();

  var root = ReactDOM.createRoot(mount);
  root.render(h(MoatDetector));

  var chatEl = document.getElementById("chat");
  if (chatEl) {
    var observer = new MutationObserver(function () {
      ensureMoatMounted();
    });
    observer.observe(chatEl, { childList: true });
  }
})();
