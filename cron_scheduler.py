"""定时任务调度器。

纯调度层：管理 Job CRUD 和 tick 循环，不包含任何执行/投递逻辑。
执行逻辑通过 set_executor(callback) 注入，实现调度与执行解耦。
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Awaitable, Callable

logger = logging.getLogger("cron-scheduler")

try:
    from croniter import croniter

    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False


# ─── 调度格式解析 ──────────────────────────────────────────────────────────────


def parse_duration(raw: str) -> int:
    """解析时长字符串，返回分钟数。如 '30m' → 30, '2h' → 120, '1d' → 1440。"""
    raw = raw.strip().lower()
    match = re.match(
        r"^(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$", raw
    )
    if not match:
        raise ValueError(f"无效的时长: '{raw}', 请使用 30m / 2h / 1d 格式")
    value = int(match.group(1))
    unit = match.group(2)[0]
    return value * {"m": 1, "h": 60, "d": 1440}[unit]


def parse_schedule(schedule: str) -> dict:
    """解析调度字符串，返回结构化 dict。

    支持格式:
    - '30m'             → 一次性，30 分钟后
    - 'every 2h'        → 循环，每 2 小时
    - '0 9 * * *'       → cron 表达式
    - '2026-03-15T09:00' → 一次性，指定时间
    """
    schedule = schedule.strip()

    # every X → 循环间隔
    if schedule.lower().startswith("every "):
        minutes = parse_duration(schedule[6:].strip())
        return {"kind": "interval", "minutes": minutes, "display": f"every {minutes}m"}

    # cron 表达式（5 段）
    parts = schedule.split()
    if len(parts) >= 5 and all(re.match(r"^[\d*\-,/]+$", p) for p in parts[:5]):
        if not HAS_CRONITER:
            raise ValueError("Cron 表达式需要安装 croniter: pip install croniter")
        try:
            croniter(schedule)
        except Exception as exc:
            raise ValueError(f"无效的 cron 表达式 '{schedule}': {exc}") from exc
        return {"kind": "cron", "expr": schedule, "display": schedule}

    # ISO 时间戳
    if "T" in schedule or re.match(r"^\d{4}-\d{2}-\d{2}", schedule):
        try:
            dt = datetime.fromisoformat(schedule.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.astimezone()
            return {
                "kind": "once",
                "run_at": dt.isoformat(),
                "display": f"once at {dt.strftime('%Y-%m-%d %H:%M')}",
            }
        except ValueError as exc:
            raise ValueError(f"无效的时间戳 '{schedule}': {exc}") from exc

    # 纯时长 → 一次性延时
    try:
        minutes = parse_duration(schedule)
        run_at = datetime.now().astimezone() + timedelta(minutes=minutes)
        return {
            "kind": "once",
            "run_at": run_at.isoformat(),
            "display": f"once in {schedule}",
        }
    except ValueError:
        pass

    raise ValueError(
        f"无效的调度格式 '{schedule}'。支持:\n"
        "  - 延时: '30m', '2h', '1d' (一次性)\n"
        "  - 循环: 'every 30m', 'every 2h'\n"
        "  - Cron: '0 9 * * *'\n"
        "  - 时间戳: '2026-03-15T09:00:00'"
    )


def compute_next_run(schedule: dict, last_run_at: str | None = None) -> str | None:
    """根据调度配置计算下一次执行时间，返回 ISO 字符串。"""
    now = datetime.now().astimezone()

    if schedule["kind"] == "once":
        if last_run_at:
            return None  # 一次性任务已执行
        return schedule.get("run_at")

    if schedule["kind"] == "interval":
        minutes = schedule["minutes"]
        if last_run_at:
            base = datetime.fromisoformat(last_run_at)
            return (base + timedelta(minutes=minutes)).isoformat()
        return (now + timedelta(minutes=minutes)).isoformat()

    if schedule["kind"] == "cron" and HAS_CRONITER:
        base = datetime.fromisoformat(last_run_at) if last_run_at else now
        cron = croniter(schedule["expr"], base)
        return cron.get_next(datetime).isoformat()

    return None


# ─── Cron 指令解析（从 Claude 输出中提取） ──────────────────────────────────────

_CRON_BLOCK_RE = re.compile(r"```json:cron\s*\n(.*?)\n```", re.DOTALL)


def extract_cron_commands(text: str) -> tuple[str, list[dict]]:
    """从 Claude 输出中提取 json:cron 块。

    Returns:
        (清理后的文本, cron 指令列表)
    """
    commands: list[dict] = []
    for match in _CRON_BLOCK_RE.finditer(text):
        try:
            commands.append(json.loads(match.group(1)))
        except json.JSONDecodeError:
            logger.warning("json:cron block parse failed: %s", match.group(1)[:200])
    cleaned = _CRON_BLOCK_RE.sub("", text).strip()
    return cleaned, commands


# ─── 调度器 ────────────────────────────────────────────────────────────────────


class CronScheduler:
    """轻量级定时任务调度器，后台 tick 驱动。

    纯调度职责：Job CRUD + tick 循环。
    执行逻辑通过 set_executor() 注入，调度器本身不依赖 CLI / 钉钉 / 卡片。
    """

    # executor 签名: async (job: dict, scheduler: CronScheduler) -> None
    JobExecutor = Callable[["dict", "CronScheduler"], Awaitable[None]]

    def __init__(self, jobs_dir: str | Path) -> None:
        self.jobs_dir = Path(jobs_dir)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.jobs_file = self.jobs_dir / "jobs.json"

        self._executor: CronScheduler.JobExecutor | None = None
        # 保护 jobs.json 读写的线程锁（tick 线程 vs 用户命令线程）
        self._jobs_lock = threading.Lock()

    def set_executor(self, executor: CronScheduler.JobExecutor) -> None:
        """注入 job 执行器。executor 签名: async (job, scheduler) -> None"""
        self._executor = executor

    # ── Job CRUD ──

    def _load_jobs(self) -> list[dict]:
        if not self.jobs_file.exists():
            return []
        try:
            data = json.loads(self.jobs_file.read_text(encoding="utf-8"))
            return data.get("jobs", [])
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("load jobs failed: %s", exc)
            return []

    def _save_jobs(self, jobs: list[dict]) -> None:
        tmp = self.jobs_file.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(
                {"jobs": jobs, "updated_at": datetime.now().isoformat()},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        tmp.replace(self.jobs_file)

    def create_job(
        self,
        prompt: str,
        schedule: str,
        conversation_id: str,
        staff_id: str,
        conversation_type: str = "1",
        name: str | None = None,
        repeat: int | None = None,
    ) -> dict:
        """创建定时任务。"""
        parsed = parse_schedule(schedule)

        # 一次性任务默认 repeat=1
        if parsed["kind"] == "once" and repeat is None:
            repeat = 1

        job = {
            "id": uuid.uuid4().hex[:12],
            "name": name or prompt[:40].strip(),
            "prompt": prompt,
            "schedule": parsed,
            "schedule_display": parsed.get("display", schedule),
            "conversation_id": conversation_id,
            "conversation_type": conversation_type,
            "staff_id": staff_id,
            "repeat": {"times": repeat, "completed": 0},
            "enabled": True,
            "created_at": datetime.now().isoformat(),
            "next_run_at": compute_next_run(parsed),
            "last_run_at": None,
            "last_status": None,
            "last_error": None,
        }

        with self._jobs_lock:
            jobs = self._load_jobs()
            jobs.append(job)
            self._save_jobs(jobs)
        logger.info("cron job created: id=%s schedule=%s prompt=%s", job["id"], schedule, prompt[:60])
        return job

    def list_jobs(self, staff_id: str | None = None) -> list[dict]:
        """列出任务，可按 staff_id 过滤。"""
        with self._jobs_lock:
            jobs = self._load_jobs()
        if staff_id:
            jobs = [j for j in jobs if j.get("staff_id") == staff_id]
        return [j for j in jobs if j.get("enabled", True)]

    def remove_job(self, job_id: str, staff_id: str | None = None) -> bool:
        """删除任务。支持 id 精确匹配或 name 模糊匹配。"""
        with self._jobs_lock:
            jobs = self._load_jobs()
            original_len = len(jobs)

            # 先尝试 id 精确匹配
            remaining = [j for j in jobs if j["id"] != job_id]

            # 没匹配到时尝试 name 模糊匹配
            if len(remaining) == original_len:
                job_id_lower = job_id.lower()
                remaining = [
                    j
                    for j in jobs
                    if not (
                        job_id_lower in (j.get("name") or "").lower()
                        and (staff_id is None or j.get("staff_id") == staff_id)
                    )
                ]

            if len(remaining) < original_len:
                self._save_jobs(remaining)
                logger.info("cron job removed: ref=%s", job_id)
                return True
        return False

    def get_due_jobs(self) -> list[dict]:
        """获取当前应该执行的任务。"""
        now = datetime.now().astimezone()
        with self._jobs_lock:
            jobs = self._load_jobs()

        due: list[dict] = []
        for job in jobs:
            if not job.get("enabled", True):
                continue
            next_run = job.get("next_run_at")
            if not next_run:
                continue
            next_dt = datetime.fromisoformat(next_run)
            if next_dt.tzinfo is None:
                next_dt = next_dt.astimezone()
            if next_dt <= now:
                due.append(job)

        return due

    def mark_job_run(self, job_id: str, success: bool, error: str | None = None) -> None:
        """标记任务已执行，更新状态和下次运行时间。"""
        with self._jobs_lock:
            jobs = self._load_jobs()
            for job in jobs:
                if job["id"] != job_id:
                    continue
                now_iso = datetime.now().isoformat()
                job["last_run_at"] = now_iso
                job["last_status"] = "ok" if success else "error"
                job["last_error"] = error if not success else None

                if job.get("repeat"):
                    job["repeat"]["completed"] = job["repeat"].get("completed", 0) + 1
                    times = job["repeat"].get("times")
                    if times is not None and times > 0 and job["repeat"]["completed"] >= times:
                        job["enabled"] = False
                        logger.info("cron job completed (repeat limit): id=%s", job_id)

                if job.get("enabled", True):
                    job["next_run_at"] = compute_next_run(job["schedule"], now_iso)
                    if not job["next_run_at"]:
                        job["enabled"] = False
                break

            self._save_jobs(jobs)

    # ── Tick 循环 ──

    async def tick(self) -> int:
        """检查并执行到期任务，返回执行数量。"""
        due_jobs = self.get_due_jobs()
        if not due_jobs:
            return 0

        logger.info("cron tick: %d job(s) due", len(due_jobs))
        executor = self._executor
        if not executor:
            logger.warning("cron tick: no executor set, skipping %d job(s)", len(due_jobs))
            return 0

        executed = 0
        for job in due_jobs:
            try:
                await executor(job, self)
            except Exception as exc:
                logger.exception("cron executor failed for job %s: %s", job["id"], exc)
                self.mark_job_run(job["id"], success=False, error=str(exc))
            executed += 1

        return executed

    async def start(self, interval: int = 5) -> None:
        """后台 tick 循环，每 interval 秒检查一次。"""
        logger.info("cron scheduler started (interval=%ds)", interval)
        while True:
            try:
                await self.tick()
            except Exception as exc:
                logger.exception("cron tick failed: %s", exc)
            await asyncio.sleep(interval)

    # ── 格式化 ──

    @staticmethod
    def format_job_list(jobs: list[dict]) -> str:
        """格式化任务列表为用户可读文本。"""
        if not jobs:
            return "当前没有定时任务。"

        lines = ["📋 **定时任务列表**\n"]
        for job in jobs:
            status = "✅" if job.get("enabled", True) else "⏸️"
            next_run = job.get("next_run_at", "")
            if next_run:
                try:
                    dt = datetime.fromisoformat(next_run)
                    next_run = dt.strftime("%m-%d %H:%M")
                except ValueError:
                    pass
            lines.append(
                f"- {status} **{job.get('name', job['id'])}**\n"
                f"  ID: `{job['id']}` | 调度: {job.get('schedule_display', '?')} | 下次: {next_run}"
            )
        return "\n".join(lines)
