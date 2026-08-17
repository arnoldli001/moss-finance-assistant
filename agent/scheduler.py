"""
Layer 4 - Loop Engineering: 自动化调度模块。

定时触发盘前任务：
- 工作日 9:13 触发"盘前小作文热度"按钮（调用 zsxq 分析）
- 工作日 9:15 触发"盘前新闻"按钮（调用主智能体盘前问询）

调度器以 30 秒为轮询单位，使用中国时区（UTC+8）。
可通过环境变量 SCHEDULER_ENABLED 关闭、SCHEDULER_TZ_OFFSET 调整时区。
"""
from __future__ import annotations

import asyncio
import datetime
import os
import json
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

from config.constants import SCHEDULER_LOOK_AHEAD_DAYS_MAX, SCHEDULER_PRE_MARKET_DEFAULT_MINUTE, SCHEDULER_AFTER_MARKET_DEFAULT_MINUTE

# ======================================================================
# 配置项（可通过 .env 覆盖）
# ======================================================================
SCHEDULER_ENABLED = os.getenv("SCHEDULER_ENABLED", "1").strip() not in ("0", "false")
# 中国时区 UTC+8（可通过环境变量调整）
SCHEDULER_TZ_OFFSET = int(os.getenv("SCHEDULER_TZ_OFFSET", "8"))

# 中国时区对象
_CHINA_TZ = datetime.timezone(datetime.timedelta(hours=SCHEDULER_TZ_OFFSET))


def _now_cn() -> datetime.datetime:
    """返回当前中国时区时间。"""
    return datetime.datetime.now(_CHINA_TZ)


@dataclass
class ScheduledTask:
    """一个定时任务定义。"""
    name: str
    hour: int
    minute: int
    weekday_only: bool
    callback: Optional[Callable]
    last_run: Optional[str] = None
    enabled: bool = True


class TaskScheduler:
    """
    定时任务调度器。

    使用方式：
        scheduler = get_scheduler()
        setup_preset_tasks(scheduler, zsxq_cb, news_cb)
        asyncio.create_task(scheduler.start())
    """

    def __init__(self):
        self._tasks: List[ScheduledTask] = []
        self._running: bool = False

    def add_task(self, name: str, hour: int, minute: int, callback: Callable,
                 weekday_only: bool = True) -> None:
        """添加一个定时任务。同名任务允许重复添加（按注册顺序执行）。"""
        task = ScheduledTask(
            name=name,
            hour=hour,
            minute=minute,
            weekday_only=weekday_only,
            callback=callback,
            last_run=None,
            enabled=True,
        )
        self._tasks.append(task)

    async def start(self) -> None:
        """
        启动调度器循环。
        每 30 秒检查一次是否有任务需要执行。
        使用中国时区（UTC+8），weekday_only 任务在周末（周六/周日）跳过。
        """
        if not SCHEDULER_ENABLED:
            print("[Scheduler] 调度器已通过 SCHEDULER_ENABLED 关闭，不启动")
            return
        self._running = True
        print(f"[Scheduler] 调度器已启动，时区 UTC+{SCHEDULER_TZ_OFFSET}，"
              f"任务数 {len(self._tasks)}")
        try:
            while self._running:
                now = _now_cn()
                for task in list(self._tasks):
                    if not task.enabled:
                        continue
                    if self._should_run(task, now):
                        await self._run_task(task)
                # 每 30 秒轮询一次
                await asyncio.sleep(30)
        except asyncio.CancelledError:
            print("[Scheduler] 调度器循环被取消")
            raise
        finally:
            self._running = False

    async def _run_task(self, task: ScheduledTask) -> None:
        """执行单个任务回调并更新 last_run。回调可为同步或异步函数。"""
        print(f"[Scheduler] 触发任务: {task.name} @ {_now_cn().isoformat(timespec='seconds')}")
        task.last_run = _now_cn().date().isoformat()
        callback = task.callback
        if callback is None:
            print(f"[Scheduler] 任务 {task.name} 无回调，跳过执行")
            return
        try:
            ret = callback()
            if asyncio.iscoroutine(ret):
                await ret
        except Exception as e:
            print(f"[Scheduler] 任务 {task.name} 执行失败: {e}")

    async def stop(self) -> None:
        """停止调度器。"""
        self._running = False
        print("[Scheduler] 调度器已停止")

    def _should_run(self, task: ScheduledTask, now: datetime.datetime) -> bool:
        """
        检查任务是否应在当前时间执行：
        1. 当前小时:分钟匹配任务设定
        2. weekday_only 任务在周末（周六=5 / 周日=6）跳过
        3. 当天尚未执行过（last_run != 今天日期），避免一分钟内重复触发
        """
        if now.hour != task.hour or now.minute != task.minute:
            return False
        if task.weekday_only and now.weekday() >= 5:  # 5=周六, 6=周日
            return False
        today = now.date().isoformat()
        if task.last_run == today:
            return False
        return True

    def get_next_run_times(self) -> Dict[str, str]:
        """
        返回各任务下一次预计执行时间字符串（ISO 格式，精确到分钟）。
        用于前端展示"下次触发"信息。
        """
        now = _now_cn()
        result: Dict[str, str] = {}
        for task in self._tasks:
            if not task.enabled:
                result[task.name] = "已禁用"
                continue
            next_dt = self._compute_next_run(task, now)
            result[task.name] = next_dt.isoformat(timespec="minutes") if next_dt else "无可用时间"
        return result

    def _compute_next_run(self, task: ScheduledTask,
                          now: datetime.datetime) -> Optional[datetime.datetime]:
        """从当前时间起向前推算任务下一次执行时间。"""
        today_target = now.replace(hour=task.hour, minute=task.minute,
                                   second=0, microsecond=0)
        if now <= today_target:
            candidate = today_target
        else:
            # 今天设定时间已过，从明天开始找
            candidate = today_target + datetime.timedelta(days=1)
        # 最多向前查找 SCHEDULER_LOOK_AHEAD_DAYS_MAX 天，避免 weekday_only 配置错误导致死循环
        for _ in range(SCHEDULER_LOOK_AHEAD_DAYS_MAX):
            if not task.weekday_only or candidate.weekday() < 5:
                return candidate
            candidate += datetime.timedelta(days=1)
        return None


# ======================================================================
# 预设任务：盘前触发
# ======================================================================
PRESET_PRE_MARKET_HEAT = ScheduledTask(
    name="盘前小作文热度",
    hour=9,
    minute=SCHEDULER_PRE_MARKET_DEFAULT_MINUTE,
    weekday_only=True,
    callback=None,
)

PRESET_PRE_MARKET_NEWS = ScheduledTask(
    name="盘前新闻",
    hour=9,
    minute=SCHEDULER_AFTER_MARKET_DEFAULT_MINUTE,
    weekday_only=True,
    callback=None,
)


def setup_preset_tasks(scheduler: TaskScheduler, zsxq_callback: Callable,
                       news_callback: Callable) -> None:
    """
    将预设盘前任务注册到调度器：
    - 9:13 盘前小作文热度 → zsxq_callback（调用知识星球分析）
    - 9:15 盘前新闻 → news_callback（调用主智能体盘前问询）
    """
    scheduler.add_task(
        name=PRESET_PRE_MARKET_HEAT.name,
        hour=PRESET_PRE_MARKET_HEAT.hour,
        minute=PRESET_PRE_MARKET_HEAT.minute,
        callback=zsxq_callback,
        weekday_only=PRESET_PRE_MARKET_HEAT.weekday_only,
    )
    scheduler.add_task(
        name=PRESET_PRE_MARKET_NEWS.name,
        hour=PRESET_PRE_MARKET_NEWS.hour,
        minute=PRESET_PRE_MARKET_NEWS.minute,
        callback=news_callback,
        weekday_only=PRESET_PRE_MARKET_NEWS.weekday_only,
    )
    print(f"[Scheduler] 已注册预设盘前任务: "
          f"{PRESET_PRE_MARKET_HEAT.name}(9:13), {PRESET_PRE_MARKET_NEWS.name}(9:15)")


# ======================================================================
# 全局单例
# ======================================================================
_scheduler: Optional[TaskScheduler] = None


def get_scheduler() -> TaskScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = TaskScheduler()
    return _scheduler
