/**
 * 前端集中常量文件 —— 所有"魔鬼数字"统一在这里定义
 * 与后端 config/constants.py 中的 MONITOR 分组保持一致
 * 修改此文件后刷新页面即可生效，不用散处改动 JS 代码
 *
 * 命名规范：与 Python 端保持同名（去掉前后缀一致），便于上下联动
 * 单位规范：毫秒单位统一加 _MS 后缀；秒不加；像素统一加 _PX；比例不加后缀
 */
(function (global) {
    'use strict';

    // ======================================================================
    // 1. WebSocket 心跳与断线重连
    // ======================================================================

    // WebSocket 心跳间隔（毫秒）：前端定时 ping 后端保活
    const WS_HEARTBEAT_INTERVAL_MS = 30000;

    // WS 最大重连次数（超过后提示用户手动刷新）
    const WS_MAX_RECONNECT = 3;

    // WS 断线重连基础延迟（毫秒），后续每次翻倍直到上限
    const WS_RECONNECT_BASE_DELAY_MS = 1000;

    // WS 断线重连最大延迟（毫秒）
    const WS_RECONNECT_MAX_DELAY_MS = 10000;

    // WS 重连恢复成功后，"连接已恢复"提示展示多少毫秒后消失
    const WS_RECONNECT_RECOVERY_HINT_MS = 2000;

    // 发送前等待 WebSocket OPEN 状态的最大等待毫秒
    const WS_WAIT_OPEN_TIMEOUT_MS = 3000;

    // 等待 WebSocket OPEN 状态时，每次轮询的间隔（毫秒）
    const WS_WAIT_OPEN_POLL_STEP_MS = 200;

    // 运行任务硬超时保护（毫秒）：5 分钟仍没结束就强制重置等待态
    const RUNNING_TIMEOUT_MS = 300000;

    // ======================================================================
    // 2. 前端监控/更新计时器
    // ======================================================================

    // 思考气泡计时：每多少毫秒更新一次"思考中 N 秒"
    const THINKING_TIMER_TICK_MS = 1000;

    // 监控进度条：每多少毫秒更新一次显示
    const PROGRESS_UPDATE_INTERVAL_MS = 5000;

    // ======================================================================
    // 3. 删除/确认/通知交互
    // ======================================================================

    // 删除操作二次确认窗口期（毫秒）：3 秒内点第二次才真正删除
    const DELETE_CONFIRM_WINDOW_MS = 3000;

    // Toast 通知：默认展示时长（毫秒）
    const TOAST_DEFAULT_DURATION_MS = 3000;

    // 复制按钮高亮展示"已复制"态多少毫秒后恢复
    const COPY_BTN_HIGHLIGHT_MS = 1500;

    // ======================================================================
    // 4. 右键菜单 / 长按交互
    // ======================================================================

    // 长按触发菜单延迟（毫秒）
    const LONG_PRESS_TRIGGER_MS = 500;

    // 长按按压反馈：按压效果多少毫秒后撤消
    const LONG_PRESS_PRESSING_RELEASE_MS = 200;

    // 长按移动容忍像素（超过则判定为拖动而不是长按，取消长按触发）
    const LONG_PRESS_MOVE_TOLERANCE_PX = 10;

    // ======================================================================
    // 5. 快捷按钮 / 删除会话等交互
    // ======================================================================

    // 点击快捷按钮后等待多少毫秒再刷新（给后端启动任务留时间）
    const SHORTCUT_BUTTON_POST_CLICK_WAIT_MS = 200;

    // ======================================================================
    // 6. 文本净化 & 渲染
    // ======================================================================

    // 消息空行合并阈值（与后端 SANITIZE_BLANK_LINE_MERGE_THRESHOLD 对应）
    const MSG_BLANK_LINE_MERGE_THRESHOLD = 3;

    // ======================================================================
    // 7. Canvas 渲染参数（聊天列表 canvas 绘制模式，防超高分屏）
    // ======================================================================

    // 设备像素比上限（防止超高分屏下 canvas 过大导致 GPU 内存不足）
    const DEVICE_PIXEL_RATIO_UPPER_LIMIT = 2;

    // 对话气泡最大宽度占聊天区宽度的比例（0.8 即 80%，留出左右边距和头像）
    const CHAT_BUBBLE_MAX_WIDTH_RATIO = 0.8;

    // 消息之间的纵向间距像素（canvas 绘制用）
    const CHAT_MESSAGE_GAP_PX = 16;

    // 头像尺寸 + 气泡额外纵向空间（canvas 绘制行距）
    const CHAT_AVATAR_ROW_EXTRA_H_PX = 8;

    // 子菜单左弹出判断：至少需要多少像素左侧空间，否则改为右弹出
    const CONTEXT_MENU_MIN_LEFT_SPACE_PX = 220;

    // ======================================================================
    // 9. 鉴权 Auth（A5 前端登录屏）
    // ======================================================================

    // 是否启用 JWT 登录态模式：true = 启用 auth_bootstrap 登录遮罩 / token 注入 / 401 拦截
    //                          false = 关闭，走老逻辑（明文 user_id 自助 create_or_login_user）
    const AUTH_MODE_ENABLED = true;

    // localStorage 存储 access_token / refresh_token 的 key 名（与后端 JWT iss 一致）
    const AUTH_LS_ACCESS_TOKEN_KEY = 'moss_auth_access_token';
    const AUTH_LS_REFRESH_TOKEN_KEY = 'moss_auth_refresh_token';
    const AUTH_LS_USER_INFO_KEY = 'moss_auth_user_info';   // {user_id, role, display_name, is_guest}

    // 后端 5 个 auth 端点（与 interfaces/api/server.py 路由契约严格一致）
    const AUTH_API_REGISTER = '/api/auth/register';
    const AUTH_API_LOGIN = '/api/auth/login';
    const AUTH_API_REFRESH = '/api/auth/refresh';
    const AUTH_API_GUEST = '/api/auth/guest';
    const AUTH_API_CHANGE_PASSWORD = '/api/auth/change-password';

    // 401 拦截时识别的 detail.code 集合（与后端 shared/utils/auth.py 细分错误码严格一致）
    const AUTH_UNAUTH_CODES = Object.freeze([
        'TOKEN_MISSING', 'TOKEN_EXPIRED', 'TOKEN_INVALID', 'UNAUTHENTICATED',
        'USER_NOT_FOUND', 'PASSWORD_MISMATCH', 'NO_PASSWORD_SET'
    ]);

    // ======================================================================
    // 10. 调试辅助（开发时使用）
    // ======================================================================

    // 是否启用常量注入检查（页面加载后在控制台打印 "CONSTANTS.js LOADED"）
    const CONSTANTS_DEBUG_BOOT_LOG = false;

    // ---- 注入到全局 window.APP_CONSTANTS ----
    const APP_CONSTANTS = Object.freeze({
        WS_HEARTBEAT_INTERVAL_MS,
        WS_MAX_RECONNECT,
        WS_RECONNECT_BASE_DELAY_MS,
        WS_RECONNECT_MAX_DELAY_MS,
        WS_RECONNECT_RECOVERY_HINT_MS,
        WS_WAIT_OPEN_TIMEOUT_MS,
        WS_WAIT_OPEN_POLL_STEP_MS,
        RUNNING_TIMEOUT_MS,
        THINKING_TIMER_TICK_MS,
        PROGRESS_UPDATE_INTERVAL_MS,
        DELETE_CONFIRM_WINDOW_MS,
        TOAST_DEFAULT_DURATION_MS,
        COPY_BTN_HIGHLIGHT_MS,
        LONG_PRESS_TRIGGER_MS,
        LONG_PRESS_PRESSING_RELEASE_MS,
        LONG_PRESS_MOVE_TOLERANCE_PX,
        SHORTCUT_BUTTON_POST_CLICK_WAIT_MS,
        MSG_BLANK_LINE_MERGE_THRESHOLD,
        DEVICE_PIXEL_RATIO_UPPER_LIMIT,
        CHAT_BUBBLE_MAX_WIDTH_RATIO,
        CHAT_MESSAGE_GAP_PX,
        CHAT_AVATAR_ROW_EXTRA_H_PX,
        CONTEXT_MENU_MIN_LEFT_SPACE_PX,
        CONSTANTS_DEBUG_BOOT_LOG,
        AUTH_MODE_ENABLED,
        AUTH_LS_ACCESS_TOKEN_KEY,
        AUTH_LS_REFRESH_TOKEN_KEY,
        AUTH_LS_USER_INFO_KEY,
        AUTH_API_REGISTER,
        AUTH_API_LOGIN,
        AUTH_API_REFRESH,
        AUTH_API_GUEST,
        AUTH_API_CHANGE_PASSWORD,
        AUTH_UNAUTH_CODES,
    });

    if (APP_CONSTANTS.CONSTANTS_DEBUG_BOOT_LOG) {
        console.log('[CONSTANTS.js LOADED] APP_CONSTANTS =', APP_CONSTANTS);
    }

    global.APP_CONSTANTS = APP_CONSTANTS;

})(typeof window !== 'undefined' ? window : globalThis);
