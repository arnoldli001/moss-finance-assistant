// load.js — k6 阶梯负载测试：1 → 5 → 10 → 20 VU 逐步加压，断言 HTTP SLO
//
// 设计决策：
//   * 压测目标为「轻量只读链路」：/health（无鉴权探针）、/api/users/{uid}/sessions
//     （JWT + 行级校验 + SQLite 读）、低频 /api/auth/login（bcrypt CPU 密集代理）。
//   * 不压 /api/task：每次请求会启动真实 Agent 任务（LLM token 消耗），仅冒烟验证契约。
//   * 429 是按角色限流（user=60 QPM）生效的正常表现，单独计数不污染错误率；
//     需要测真实延迟时先调高服务端 QPM（见 benchmarks/k6/README.md）。
//
// 运行：k6 run benchmarks/k6/load.js
// 输出：k6 run --summary-export benchmarks/results/k6_load_summary.json benchmarks/k6/load.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

// 把 429 视为"预期内"响应，避免限流污染 http_req_failed 指标
http.setResponseCallback(http.expectedStatuses(200, 201, 429));

const rate_limited = new Counter('rate_limited_429');

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 5 },  // 预热
        { duration: '1m', target: 10 },  // 轻载
        { duration: '1m', target: 20 },  // 目标负载
        { duration: '30s', target: 20 }, // 稳态观察
        { duration: '20s', target: 0 },  // 降压
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],                   // 全局错误率 < 1%（429 已豁免）
    http_req_duration: ['p(95)<500'],                 // 全局 p95 ≤ 500ms
    'http_req_duration{name:health}': ['p(95)<100'],  // 健康检查 p95 ≤ 100ms
    'http_req_duration{name:sessions}': ['p(95)<300'],// 鉴权读接口 p95 ≤ 300ms
    checks: ['rate>0.99'],
  },
  tags: { suite: 'load' },
};

export function setup() {
  const uid = `k6_load_${Date.now()}`;
  const reg = http.post(
    `${BASE_URL}/api/auth/register`,
    JSON.stringify({ user_id: uid, password: 'k6Load12345' }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (reg.status !== 200) {
    throw new Error(`setup 注册失败: ${reg.status} ${reg.body}`);
  }
  return { token: reg.json('access_token'), user_id: uid };
}

export default function (data) {
  const auth = {
    headers: { Authorization: `Bearer ${data.token}` },
  };

  // 主流量 1：健康检查（无鉴权，最轻链路）
  const h = http.get(`${BASE_URL}/health`, { tags: { name: 'health' } });
  check(h, { 'health 200/429': (res) => res.status === 200 || res.status === 429 });
  if (h.status === 429) rate_limited.add(1);

  // 主流量 2：鉴权 + 行级校验 + SQLite 读
  const s = http.get(`${BASE_URL}/api/users/${data.user_id}/sessions`,
    Object.assign({}, auth, { tags: { name: 'sessions' } }));
  check(s, { 'sessions 200/429': (res) => res.status === 200 || res.status === 429 });
  if (s.status === 429) rate_limited.add(1);

  // 低频流量：登录（bcrypt 密码哈希，CPU 密集代理，每 5 次迭代 1 次）
  if (__ITER % 5 === 0) {
    const l = http.post(
      `${BASE_URL}/api/auth/login`,
      JSON.stringify({ user_id: data.user_id, password: 'k6Load12345' }),
      Object.assign({ headers: { 'Content-Type': 'application/json' } }, { tags: { name: 'login' } }),
    );
    check(l, { 'login 200/429': (res) => res.status === 200 || res.status === 429 });
    if (l.status === 429) rate_limited.add(1);
  }

  sleep(1);
}

export function handleSummary(data) {
  // 控制台简要结论（详细数据用 --summary-export 导出 JSON）
  const m = data.metrics;
  const pct = (v) => `${(v * 100).toFixed(2)}%`;
  const lines = [
    '\n========== k6 阶梯负载 汇总 ==========',
    `总请求           : ${m.http_reqs.values.count}`,
    `错误率(不含429)  : ${pct(m.http_req_failed.values.rate)}`,
    `429 限流次数     : ${m.rate_limited_429 ? m.rate_limited_429.values.count : 0}`,
    `p95 延迟         : ${m.http_req_duration.values['p(95)'].toFixed(1)} ms`,
    `断言通过率       : ${pct(m.checks.values.rate)}`,
    '======================================\n',
  ];
  return { stdout: lines.join('\n') };
}
