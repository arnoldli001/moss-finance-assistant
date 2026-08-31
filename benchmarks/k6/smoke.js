// smoke.js — k6 冒烟测试：验证核心 API 链路可用性 + HTTP SLO 基线
// 运行：k6 run benchmarks/k6/smoke.js        （BASE_URL 可用 -e 覆盖，默认 http://localhost:8000）
import http from 'k6/http';
import { check, group, sleep } from 'k6';
import { Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

// /api/task 启动耗时（异步端点，仅测"受理"延迟，不测 Agent 执行时长）
const task_start_latency = new Trend('task_start_latency_ms', true);
// /api/task/stop 优雅取消耗时（含中断运行中的 LLM 调用）
const task_stop_latency = new Trend('task_stop_latency_ms', true);

// 401/403/404 是安全探测的"预期拒绝"，不计入 http_req_failed
http.setResponseCallback(http.expectedStatuses(200, 201, 401, 403, 404));

export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    http_req_failed: ['rate==0'],
    // 纯 HTTP 链路严格 SLO
    'http_req_duration{name:health}': ['p(95)<100'],
    'http_req_duration{name:register}': ['p(95)<500'],
    'http_req_duration{name:auth_read}': ['p(95)<300'],
    'http_req_duration{name:rbac}': ['p(95)<300'],
    // Agent 链路：受理含语义缓存嵌入 + PTD 路由决策（受本地 Ollama 冷启动影响），
    // 优雅取消含中断 LLM 调用——放宽到 30s，与任务 150s 双硬上限保持量级一致
    task_start_latency_ms: ['p(95)<30000'],
    task_stop_latency_ms: ['p(95)<30000'],
    checks: ['rate==1'], // 冒烟阶段所有断言必须通过
  },
  tags: { suite: 'smoke' },
};

const JSON_HEADERS = { 'Content-Type': 'application/json' };

export default function () {
  // 1. 健康检查（白名单端点，无鉴权）
  group('健康检查', () => {
    const r = http.get(`${BASE_URL}/health`, { tags: { name: 'health' } });
    check(r, {
      'health: status 200': (res) => res.status === 200,
      'health: status=ok': (res) => res.json('status') === 'ok',
      'X-Powered-By 头注入': (res) =>
        (res.headers['X-Powered-By'] || '').indexOf('MOSS-Finance-Assistant') >= 0,
    });
  });

  // 2. 注册 → 行级权限（只能读自己）
  let token, uid;
  group('注册 + 行级权限校验', () => {
    uid = `k6_smoke_${Date.now()}`;
    const reg = http.post(
      `${BASE_URL}/api/auth/register`,
      JSON.stringify({ user_id: uid, password: 'k6Smoke123', display_name: 'k6 冒烟' }),
      { headers: JSON_HEADERS, tags: { name: 'register' } },
    );
    check(reg, {
      'register: status 200': (res) => res.status === 200,
      'register: 签发 access_token': (res) => !!res.json('access_token'),
    });
    token = reg.json('access_token');
    const auth = { headers: { Authorization: `Bearer ${token}` }, tags: { name: 'auth_read' } };

    const me = http.get(`${BASE_URL}/api/users/${uid}`, auth);
    check(me, {
      '行级校验: 读自己 200': (res) => res.status === 200,
      '行级校验: user_id 匹配': (res) => res.json('user.user_id') === uid,
    });

    // 水平越权探测：读他人资料应被拒（401/403 显式拒绝，404 不泄露存在性）
    const other = http.get(`${BASE_URL}/api/users/another_user_000`, auth);
    check(other, {
      '行级校验: 读他人被拒(401/403/404)': (res) =>
        [401, 403, 404].indexOf(res.status) >= 0,
    });
  });

  // 3. 任务受理 + 主动停止（避免冒烟触发真实 Agent 烧 LLM token）
  group('任务启停', () => {
    const auth = { headers: { Authorization: `Bearer ${token}` }, tags: { name: 'task' } };
    const t0 = Date.now();
    const start = http.post(
      `${BASE_URL}/api/task`,
      JSON.stringify({ query: 'k6 冒烟测试任务，请直接回复完成', thread_id: `k6_smoke_${Date.now()}` }),
      auth,
    );
    task_start_latency.add(Date.now() - t0);
    check(start, {
      'task: status 200': (res) => res.status === 200,
      'task: 受理状态 started': (res) => res.json('status') === 'started',
      'task: 返回 thread_id': (res) => !!res.json('thread_id'),
    });

    const stop = http.post(
      `${BASE_URL}/api/task/stop`,
      JSON.stringify({ thread_id: start.json('thread_id') }),
      Object.assign({}, auth, { tags: { name: 'task_stop' } }),
    );
    task_stop_latency.add(stop.timings.duration);
    check(stop, {
      'task stop: 已停止或无任务': (res) =>
        ['stopped', 'not_found'].indexOf(res.json('status')) >= 0,
    });
  });

  // 4. RBAC：普通用户访问 owner/admin 专属端点应 403
  group('RBAC 权限隔离', () => {
    const r = http.get(`${BASE_URL}/api/slo/status`, {
      headers: { Authorization: `Bearer ${token}` },
      tags: { name: 'rbac' },
    });
    check(r, { 'RBAC: 普通用户访问 SLO 端点 403': (res) => res.status === 403 });
  });

  sleep(1);
}
