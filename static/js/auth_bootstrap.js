/**
 * auth_bootstrap.js — MOSS Finance Assistant 前端登录鉴权入口（A5）
 *
 * 加载时机：<script src=constants.js> → <script src=auth_bootstrap.js> → 主代码
 * 依赖：window.APP_CONSTANTS（constants.js 注入）
 *
 * 能力：
 *   1) 全局 window.fetch 打补丁 —— 自动注入 Authorization: Bearer <access>；
 *      遇 401 先尝试 refresh 刷新 access_token，再失败就清 token + 弹出登录遮罩；
 *      遇 429（限流）toast 出 message + Retry-After；403 toast 出 Forbidden 描述。
 *   2) 全局 window.WebSocket 打补丁 —— 构造函数第一个参数 url 自动追加 ?token=<access>，
 *      避免业务端 16 处 WebSocket URL 忘记带 token 导致 /ws/{thread_id} 被关 4401。
 *   3) 暴露全局 window.MossAuth：
 *        .currentUser() / .accessToken() / .decodeJwtClaims(token) / .isTokenExpired(token)
 *        .setTokens(access, refresh, userInfo) / .clearTokens() / .refreshAccessToken()
 *        .showLoginScreen(msg?) / .hideLoginScreen()
 *        .doLogin(user_id, password) / .doRegister(user_id, password, display_name?) / .doGuest()
 *        .doLogout()
 *   4) 初始化：AUTH_MODE_ENABLED=true 且 access_token 有效则保留，否则展示登录遮罩。
 *   5) 错误区 div#login-error 渲染：后端 detail.code + detail.message 友好展示。
 */
(function () {
    'use strict';

    // ======================================================================
    // 0. 常量引用（constants.js 注入）
    // ======================================================================
    const C = window.APP_CONSTANTS || {};
    const ENABLED = !!C.AUTH_MODE_ENABLED;
    const LS_ACCESS = C.AUTH_LS_ACCESS_TOKEN_KEY || 'moss_auth_access_token';
    const LS_REFRESH = C.AUTH_LS_REFRESH_TOKEN_KEY || 'moss_auth_refresh_token';
    const LS_USER = C.AUTH_LS_USER_INFO_KEY || 'moss_auth_user_info';
    const API = {
        REGISTER: C.AUTH_API_REGISTER || '/api/auth/register',
        LOGIN: C.AUTH_API_LOGIN || '/api/auth/login',
        REFRESH: C.AUTH_API_REFRESH || '/api/auth/refresh',
        GUEST: C.AUTH_API_GUEST || '/api/auth/guest',
        CHANGE_PASSWORD: C.AUTH_API_CHANGE_PASSWORD || '/api/auth/change-password',
    };
    const UNAUTH_CODES = new Set(C.AUTH_UNAUTH_CODES || [
        'TOKEN_MISSING','TOKEN_EXPIRED','TOKEN_INVALID','UNAUTHENTICATED',
        'USER_NOT_FOUND','PASSWORD_MISMATCH','NO_PASSWORD_SET'
    ]);

    // 业务级白名单 URL：这些 fetch 调用**不**自动插 Authorization（但如果 token 已存在也不拦）
    // —— 注册/登录/刷新/游客/静态资源
    const NO_INJECT_PREFIX = Object.freeze([
        API.REGISTER, API.LOGIN, API.REFRESH, API.GUEST,
        '/static/', '/health', '/docs', '/openapi.json', '/redoc', '/favicon.ico', '/'
    ]);

    // ======================================================================
    // 1. 工具：localStorage 包装（隐私模式抛错兜底）
    // ======================================================================
    const LS = {
        get(k, d = null) { try { const v = localStorage.getItem(k); return v === null ? d : v; } catch (_) { return d; } },
        set(k, v) { try { localStorage.setItem(k, v); return true; } catch (_) { return false; } },
        del(k) { try { localStorage.removeItem(k); return true; } catch (_) { return false; } }
    };

    // ======================================================================
    // 2. 工具：URL safe Base64 解码 JWT payload（纯原生，无第三方依赖）
    // ======================================================================
    function _b64UrlDecode(seg) {
        seg = String(seg).replace(/-/g, '+').replace(/_/g, '/');
        const pad = (4 - (seg.length % 4)) % 4;
        seg += '='.repeat(pad);
        try {
            const bin = atob(seg);
            const bytes = new Uint8Array(bin.length);
            for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
            return JSON.parse(new TextDecoder('utf-8').decode(bytes));
        } catch (_) { return null; }
    }
    function decodeJwtClaims(token) {
        if (!token || typeof token !== 'string') return null;
        const parts = token.split('.');
        if (parts.length < 2) return null;
        return _b64UrlDecode(parts[1]);
    }
    function isTokenExpired(token, aheadSeconds = 10) {
        const c = decodeJwtClaims(token);
        if (!c || typeof c.exp !== 'number') return true;
        return c.exp * 1000 <= Date.now() + aheadSeconds * 1000;
    }

    // ======================================================================
    // 3. 工具：轻量 toast（复用 #toast，若不存在则 fallback alert）
    // ======================================================================
    function toast(msg, ms) {
        ms = ms || (C.TOAST_DEFAULT_DURATION_MS || 2000);
        try {
            const el = document.getElementById('toast');
            if (!el) { alert(msg); return; }
            el.textContent = msg;
            el.classList.add('show');
            clearTimeout(toast._t);
            toast._t = setTimeout(() => el.classList.remove('show'), ms);
        } catch (_) { try { alert(msg); } catch (_2) {} }
    }

    // ======================================================================
    // 4. 工具：URL query 追加 token（用于 WebSocket url）
    // ======================================================================
    function _appendTokenToUrl(url, token) {
        if (!url || !token) return url;
        try {
            const u = new URL(url, (location && location.origin) || 'http://x');
            u.searchParams.set('token', token);
            // 还原相对/绝对：原是绝对 → href；是相对 → 去掉 origin
            if (/^[a-z][a-z0-9+.-]*:/i.test(String(url)) || /^\/\//.test(String(url))) {
                return u.href;
            }
            return u.pathname + u.search + u.hash;
        } catch (_) {
            // 构造 URL 失败（极罕见），fallback 字符串拼接
            return String(url) + (String(url).indexOf('?') >= 0 ? '&' : '?') + 'token=' + encodeURIComponent(token);
        }
    }

    // ======================================================================
    // 5. MossAuth 对象内部实现
    // ======================================================================
    let _refreshPromise = null;   // 并发 refresh 合并（令牌风暴防抖）

    function getAccessToken() { return LS.get(LS_ACCESS); }
    function getRefreshToken() { return LS.get(LS_REFRESH); }
    function getUserInfo() {
        try { return JSON.parse(LS.get(LS_USER, 'null')); } catch (_) { return null; }
    }

    function setTokens(access, refresh, userInfo) {
        if (access) LS.set(LS_ACCESS, access);
        if (refresh) LS.set(LS_REFRESH, refresh);
        if (userInfo && typeof userInfo === 'object') {
            LS.set(LS_USER, JSON.stringify({
                user_id: userInfo.user_id || userInfo.userId || '',
                role: userInfo.role || 'user',
                display_name: userInfo.display_name || userInfo.displayName || userInfo.name || userInfo.user_id || '',
                is_guest: !!userInfo.is_guest
            }));
        }
        _renderAuthInfoBar();
    }

    function clearTokens() {
        LS.del(LS_ACCESS); LS.del(LS_REFRESH); LS.del(LS_USER);
        _renderAuthInfoBar();
    }

    function getCurrentUser() {
        if (!ENABLED) return null;
        const info = getUserInfo();
        const access = getAccessToken();
        if (!access || !info) return null;
        // JWT claims 优先级最高（比 localStorage user_info 新且可信）
        const claims = decodeJwtClaims(access);
        return {
            user_id: (claims && claims.sub) || info.user_id || '',
            role: (claims && claims.role) || info.role || (claims && claims.guest ? 'guest' : 'user'),
            display_name: (claims && claims.name) || info.display_name || info.user_id || '',
            is_guest: !!(claims ? claims.guest : info.is_guest),
            expires_at: (claims && claims.exp) ? (claims.exp * 1000) : 0
        };
    }

    function isLoggedIn() {
        if (!ENABLED) return true;    // 鉴权关闭模式：永远认为已登录
        const a = getAccessToken();
        return !!a && !isTokenExpired(a, 5);
    }

    // ======================================================================
    // 6. 登录/注册/游客：三个 auth 端点统一调用（success=hideLoginScreen，fail=填错误区）
    // ======================================================================
    async function _callAuth(endpoint, bodyObj) {
        try {
            const r = await _origFetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(bodyObj || {})
            });
            let data = null;
            try { data = await r.json(); } catch (_) { data = null; }
            return { ok: r.ok, status: r.status, data };
        } catch (e) {
            return { ok: false, status: 0, data: null, err: e };
        }
    }

    function _setLoginError(msg, code) {
        const box = document.getElementById('login-error');
        if (!box) return;
        box.style.display = msg ? 'block' : 'none';
        box.textContent = code ? `[${code}] ${msg}` : (msg || '');
    }

    function _getLoginForm() {
        const u = document.getElementById('login-user');
        const p = document.getElementById('login-password');
        const d = document.getElementById('login-display');
        return {
            user_id: u ? u.value.trim() : '',
            password: p ? p.value : '',
            display_name: d ? d.value.trim() : ''
        };
    }

    async function doLogin(_user, _pw) {
        const f = _getLoginForm();
        const user_id = _user || f.user_id;
        const password = _pw || f.password;
        _setLoginError('');
        if (!user_id) { _setLoginError('请输入用户名 / User ID'); return false; }
        if (!password) { _setLoginError('请输入密码'); return false; }
        const r = await _callAuth(API.LOGIN, { user_id, password });
        if (!r.ok) {
            const msg = r.data && r.data.detail ? (r.data.detail.message || '登录失败') : ('登录失败（HTTP ' + r.status + '）');
            const code = r.data && r.data.detail ? r.data.detail.code : '';
            // 细粒度提示
            const hint = (code === 'USER_NOT_FOUND') ? '用户不存在，请先注册' :
                         (code === 'PASSWORD_MISMATCH') ? '密码错误' :
                         (code === 'NO_PASSWORD_SET') ? '该账号未设置密码（旧游客账号），请登录后修改密码或重新注册' : '';
            _setLoginError(hint ? `${msg}：${hint}` : msg, code);
            return false;
        }
        if (r.data && r.data.access_token) {
            setTokens(r.data.access_token, r.data.refresh_token, r.data.user);
            hideLoginScreen();
            toast('登录成功', 1500);
            // 通知主代码：当前用户变更
            try { window.dispatchEvent(new CustomEvent('moss:user-changed', { detail: getCurrentUser() })); } catch (_) {}
            return true;
        }
        _setLoginError('登录成功但响应缺失 token，请刷新重试');
        return false;
    }

    async function doRegister() {
        const f = _getLoginForm();
        _setLoginError('');
        if (!f.user_id) { _setLoginError('请输入用户名（User ID，3~64 字符，不可 guest_ 开头）'); return false; }
        if (!f.password || f.password.length < 6) { _setLoginError('密码至少 6 位'); return false; }
        if (f.password.length > 128) { _setLoginError('密码过长（≤128）'); return false; }
        const payload = { user_id: f.user_id, password: f.password };
        if (f.display_name) payload.display_name = f.display_name;
        const r = await _callAuth(API.REGISTER, payload);
        if (!r.ok) {
            const msg = r.data && r.data.detail ? (r.data.detail.message || '注册失败') : ('注册失败（HTTP ' + r.status + '）');
            const code = r.data && r.data.detail ? r.data.detail.code : '';
            const hint = (code === 'RESERVED_USER_PREFIX') ? '用户名不能以 guest_ 开头（guest_ 为游客前缀）' :
                         (code === 'USER_ID_TOO_SHORT') ? '用户名至少 3 个字符' :
                         (code === 'USER_ID_TOO_LONG') ? '用户名最多 64 个字符' :
                         (code === 'BAD_PASSWORD') ? '密码需 6~128 位' :
                         (code === 'USER_ALREADY_EXISTS') ? '用户名已存在，请直接登录或换 ID' : '';
            _setLoginError(hint ? `${msg}：${hint}` : msg, code);
            return false;
        }
        if (r.data && r.data.access_token) {
            setTokens(r.data.access_token, r.data.refresh_token, r.data.user);
            hideLoginScreen();
            toast('注册成功，欢迎加入', 1800);
            try { window.dispatchEvent(new CustomEvent('moss:user-changed', { detail: getCurrentUser() })); } catch (_) {}
            return true;
        }
        _setLoginError('注册成功但响应缺失 token，请刷新重试');
        return false;
    }

    async function doGuest() {
        _setLoginError('');
        const r = await _callAuth(API.GUEST, {});
        if (!r.ok) {
            const msg = r.data && r.data.detail ? (r.data.detail.message || '游客登录失败') : ('游客登录失败（HTTP ' + r.status + '）');
            _setLoginError(msg + '，建议注册账号长期保存会话');
            return false;
        }
        if (r.data && r.data.access_token) {
            setTokens(r.data.access_token, r.data.refresh_token, r.data.user);
            hideLoginScreen();
            toast('游客身份已创建（会话保留 24h，建议注册后使用）', 2400);
            try { window.dispatchEvent(new CustomEvent('moss:user-changed', { detail: getCurrentUser() })); } catch (_) {}
            return true;
        }
        _setLoginError('游客登录成功但响应缺失 token，请刷新重试');
        return false;
    }

    function doLogout(showMsg = true) {
        clearTokens();
        showLoginScreen(showMsg ? '已退出登录，请重新登录' : '');
        try { window.dispatchEvent(new CustomEvent('moss:user-changed', { detail: null })); } catch (_) {}
    }

    // ======================================================================
    // 7. refresh access token（并发合并：令牌风暴防抖）
    // ======================================================================
    async function refreshAccessToken() {
        if (_refreshPromise) return _refreshPromise;
        _refreshPromise = (async () => {
            try {
                const refresh = getRefreshToken();
                if (!refresh) return false;
                if (isTokenExpired(refresh, 60)) return false;   // refresh 本身也快过期，重登
                const r = await _callAuth(API.REFRESH, { refresh_token: refresh });
                if (!r.ok || !r.data || !r.data.access_token) return false;
                LS.set(LS_ACCESS, r.data.access_token);
                if (r.data.refresh_token) LS.set(LS_REFRESH, r.data.refresh_token);
                if (r.data.user) LS.set(LS_USER, JSON.stringify(r.data.user));
                _renderAuthInfoBar();
                return true;
            } finally {
                _refreshPromise = null;
            }
        })();
        return _refreshPromise;
    }

    // ======================================================================
    // 8. DOM 操作：登录遮罩显示/隐藏 + auth-info-bar 渲染（header 最右）
    // ======================================================================
    function showLoginScreen(msg) {
        const m = document.getElementById('login-screen');
        if (!m) return;
        m.style.display = 'flex';
        if (msg) _setLoginError(msg);
    }

    function hideLoginScreen() {
        const m = document.getElementById('login-screen');
        if (!m) return;
        m.style.display = 'none';
        _setLoginError('');
        // 同步 user-id-input（侧边栏）
        const u = getCurrentUser();
        if (u) {
            const input = document.getElementById('user-id-input');
            if (input) { input.value = u.user_id; input.setAttribute('readonly', 'readonly'); input.title = 'JWT 登录态：不可手动编辑'; }
        }
    }

    function _renderAuthInfoBar() {
        const bar = document.getElementById('auth-info-bar');
        if (!bar) return;
        const u = getCurrentUser();
        if (!u) { bar.innerHTML = ''; return; }
        const roleColor = (u.role === 'owner') ? '#af52de' :
                          (u.role === 'admin') ? '#ff9500' :
                          (u.role === 'user')  ? '#0a84ff' : '#8e8e93';
        const roleCN = (u.role === 'owner') ? 'Owner' :
                       (u.role === 'admin') ? '管理员' :
                       (u.role === 'user')  ? '用户' : '游客';
        const display = u.display_name || u.user_id;
        bar.innerHTML =
            `<div style="display:flex;align-items:center;gap:10px;font-size:13px;">` +
            `<span title="User ID: ${u.user_id}">${escapeHTML(display.length > 16 ? display.slice(0,16)+'…' : display)}</span>` +
            `<span id="auth-role-pill" style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;color:#fff;background:${roleColor};">${roleCN}</span>` +
            `<button id="auth-logout-btn" type="button" ` +
              `style="padding:4px 10px;border:1px solid #d2d2d7;border-radius:6px;background:#f5f5f7;color:#1d1d1f;cursor:pointer;font-size:12px;" ` +
              `onmouseover="this.style.borderColor='#0a84ff';this.style.color='#0a84ff';" ` +
              `onmouseout="this.style.borderColor='#d2d2d7';this.style.color='#1d1d1f';" >退出</button>` +
            `</div>`;
        const btn = document.getElementById('auth-logout-btn');
        if (btn) btn.addEventListener('click', () => doLogout(true));
    }

    function escapeHTML(s) {
        return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }

    // ======================================================================
    // 9. 打补丁：window.fetch
    // ======================================================================
    const _origFetch = (typeof window !== 'undefined' && window.fetch) ? window.fetch.bind(window) : null;

    function _shouldInjectAuth(urlLike) {
        let s = '';
        if (typeof urlLike === 'string') s = urlLike;
        else if (urlLike && typeof urlLike.url === 'string') s = urlLike.url;   // Request
        else return true;  // 未知类型默认插（安全侧）
        for (const p of NO_INJECT_PREFIX) {
            if (!p) continue;
            if (s === p) return false;
            if (s.length > p.length && s.startsWith(p) && (s[p.length] === '/' || s[p.length] === '?' || s[p.length] === '#')) return false;
        }
        return true;
    }

    async function patchedFetch(input, init) {
        init = init || {};
        const headers = new Headers(init.headers || {});
        const needAuth = _shouldInjectAuth(input);

        if (needAuth && ENABLED) {
            // access 快过期就先 refresh（避免无意义 401 往返）
            let access = getAccessToken();
            if (access && isTokenExpired(access, 30)) {
                try { await refreshAccessToken(); access = getAccessToken(); } catch (_) { access = getAccessToken(); }
            }
            if (access && !headers.has('Authorization')) {
                headers.set('Authorization', 'Bearer ' + access);
            }
        }
        init.headers = headers;

        const resp = await _origFetch(input, init);

        // ============ 429 限流：toast Retry-After 秒 ============
        if (resp.status === 429) {
            const retryAfter = resp.headers.get('Retry-After') || resp.headers.get('retry-after') || '60';
            let body = null;
            try { body = await resp.clone().json(); } catch (_) {}
            const qpm = body && body.detail && body.detail.qpm ? body.detail.qpm : null;
            const role = body && body.detail && body.detail.role ? body.detail.role : null;
            toast(`请求过于频繁，请 ${retryAfter} 秒后再试${qpm ? ('（' + (role ? role + ' 角色' : '') + ' QPM=' + qpm + '）') : ''}`,
                  Math.min(parseInt(retryAfter || '5', 10) * 1000 + 500, 10000));
            return resp;
        }

        // ============ 403 Forbidden：toast 原因 ============
        if (resp.status === 403) {
            let body = null;
            try { body = await resp.clone().json(); } catch (_) {}
            const code = body && body.detail ? body.detail.code : '';
            const msg = body && body.detail ? (body.detail.message || '无权限执行该操作') : '无权限执行该操作（403）';
            const hint = (code === 'FORBIDDEN_USER_MISMATCH') ? '：该会话属于其他用户' :
                         (code === 'FORBIDDEN_CREATE_OTHERS') ? '：不能操作其他账号' :
                         (code === 'GUEST_CANNOT_CHANGE_PASSWORD') ? '：游客账号无法修改密码，请先注册' :
                         (code === 'ROLE_REQUIRED_ADMIN_OR_OWNER') ? '：仅管理员/Owner 可访问' : '';
            toast('⚠️ 403 ' + msg + hint, 2600);
            return resp;
        }

        // ============ 401 Unauthorized：尝试 refresh → 再不行 show 登录屏 ============
        if (resp.status === 401 && ENABLED) {
            let body = null;
            try { body = await resp.clone().json(); } catch (_) {}
            const code = body && body.detail ? body.detail.code : '';
            // 401 细分错误码匹配后端 UNAUTH_CODES
            if (!code || UNAUTH_CODES.has(code)) {
                const alreadyRefreshed = init.__auth_refreshed;
                if (!alreadyRefreshed) {
                    const refreshed = await refreshAccessToken();
                    if (refreshed) {
                        init.__auth_refreshed = true;
                        return patchedFetch(input, init);   // 重试一次（带新 access）
                    }
                }
                // refresh 失败 / 不可用 → 清 token 并弹登录屏
                clearTokens();
                const msg = body && body.detail ? (body.detail.message || '登录已过期，请重新登录') : '登录已过期，请重新登录';
                const hint = (code === 'TOKEN_MISSING') ? '：请先登录' :
                             (code === 'TOKEN_EXPIRED') ? '：Token 已过期' :
                             (code === 'TOKEN_INVALID') ? '：Token 签名无效' :
                             (code === 'PASSWORD_MISMATCH') ? '' : '';
                showLoginScreen(msg + hint);
            }
            return resp;
        }

        return resp;
    }

    // ======================================================================
    // 10. 打补丁：window.WebSocket（构造 url 自动追加 ?token=）
    //     —— WebSocket 在旧浏览器不可 extend，使用"包装工厂函数 + 覆盖 window.WebSocket"
    // ======================================================================
    const _OrigWebSocket = (typeof window !== 'undefined' && window.WebSocket) ? window.WebSocket : null;

    // 用 function 声明（非箭头）保持构造函数语义；new PatchedWS(url,protocols) 时可正常 new
    function PatchedWebSocket(url, protocols) {
        if (!(this instanceof PatchedWebSocket)) {
            // 某些浏览器会走"直接调用 WebSocket()"而非 new 的怪逻辑，兜底
            return PatchedWebSocket._create(url, protocols);
        }
        return PatchedWebSocket._create(url, protocols);
    }
    PatchedWebSocket._create = function (url, protocols) {
        let finalUrl = url;
        if (ENABLED) {
            const token = getAccessToken();
            if (token) finalUrl = _appendTokenToUrl(url, token);
        }
        // new 原生 WebSocket：protocols 可 array / string / undefined
        let ws;
        if (protocols === undefined) ws = new _OrigWebSocket(finalUrl);
        else ws = new _OrigWebSocket(finalUrl, protocols);
        return ws;
    };
    // 原型 & 静态属性对齐（浏览器 close code 等常量需要）
    if (_OrigWebSocket) {
        for (const k of Object.keys(_OrigWebSocket)) {
            if (Object.prototype.hasOwnProperty.call(_OrigWebSocket, k)) {
                try { PatchedWebSocket[k] = _OrigWebSocket[k]; } catch (_) {}
            }
        }
        try { PatchedWebSocket.prototype = Object.create(_OrigWebSocket.prototype); PatchedWebSocket.prototype.constructor = PatchedWebSocket; } catch (_) {}
    }

    // ======================================================================
    // 11. 全局 MossAuth 对象导出 + 打补丁生效
    // ======================================================================
    const MossAuth = Object.freeze({
        get enabled() { return ENABLED; },
        get accessToken() { return getAccessToken; },
        accessToken() { return getAccessToken(); },
        refreshToken() { return getRefreshToken(); },
        userInfo() { return getUserInfo(); },
        currentUser() { return getCurrentUser(); },
        isLoggedIn() { return isLoggedIn(); },
        decodeJwtClaims, isTokenExpired,
        setTokens, clearTokens, refreshAccessToken,
        showLoginScreen, hideLoginScreen,
        doLogin, doRegister, doGuest, doLogout,
        _renderAuthInfoBar
    });
    window.MossAuth = MossAuth;

    if (ENABLED && _origFetch) {
        window.fetch = patchedFetch;
    }
    if (ENABLED && _OrigWebSocket) {
        window.WebSocket = PatchedWebSocket;
    }

    // ======================================================================
    // 12. DOM 就绪后：绑定 3 按钮（login/register/guest）+ 渲染 info-bar + 决定是否 show 登录屏
    // ======================================================================
    function _onDomReady() {
        // 3 按钮绑定
        const btnLogin = document.getElementById('login-submit');
        const btnReg = document.getElementById('register-submit');
        const btnGuest = document.getElementById('guest-submit');
        if (btnLogin) btnLogin.addEventListener('click', doLogin);
        if (btnReg) btnReg.addEventListener('click', doRegister);
        if (btnGuest) btnGuest.addEventListener('click', doGuest);
        // 回车 → 登录
        const pwEl = document.getElementById('login-password');
        if (pwEl) pwEl.addEventListener('keydown', e => { if (e.key === 'Enter') { e.preventDefault(); doLogin(); } });

        _renderAuthInfoBar();

        if (ENABLED && !isLoggedIn()) {
            // 未登录：展示登录遮罩（确保 login-screen DOM 存在）
            setTimeout(() => showLoginScreen(''), 0);
        } else if (ENABLED) {
            // 已登录：保证隐藏 + user-id-input readonly
            setTimeout(() => hideLoginScreen(), 0);
        }
    }

    if (typeof document !== 'undefined') {
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', _onDomReady);
        } else {
            _onDomReady();
        }
    }
})();
