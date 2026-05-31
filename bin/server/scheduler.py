#!/usr/bin/env python3
"""
server/scheduler.py - APScheduler setup for daily auto-refresh

Call start() once at app startup.  The scheduler reads/writes DB prefs
for the configured refresh time, so the DB must be accessible.
"""

import os

from server.config import cfg
from server.logging_setup import log
from server.refresh import run_refresh

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    _AVAILABLE = True
except ImportError:
    BackgroundScheduler = None
    _AVAILABLE = False

_scheduler = None
_job = None


def _get_scheduler():
    return _scheduler


def start(auto_refresh_settings: dict | None = None) -> None:
    """Start APScheduler and schedule the auto-refresh job if enabled."""
    global _scheduler, _job

    if not _AVAILABLE:
        log("APScheduler not installed; auto-refresh disabled", "ERROR")
        return

    try:
        _scheduler = BackgroundScheduler(timezone=os.getenv("TZ", "America/New_York"))
        _scheduler.start()
        log("APScheduler started", "INFO")
        if auto_refresh_settings:
            schedule(auto_refresh_settings)
    except Exception as e:
        log(f"Error starting APScheduler: {e}", "ERROR")


def schedule(settings: dict) -> None:
    """Apply (or remove) the auto-refresh cron job from `settings`."""
    global _job

    if not _scheduler:
        return

    enabled = bool(settings.get("enabled"))
    time_str = settings.get("time", "02:30")

    if not enabled:
        if _job:
            try:
                _scheduler.remove_job("daily_auto_refresh")
            except Exception:
                pass
            _job = None
        return

    try:
        hour, minute = [int(x) for x in time_str.split(":", 1)]
    except Exception:
        log(f"Invalid auto-refresh time: {time_str!r}", "ERROR")
        return

    _job = _scheduler.add_job(
        func=lambda: run_refresh(skip_scrape=False, source="auto"),
        trigger="cron",
        hour=hour,
        minute=minute,
        id="daily_auto_refresh",
        replace_existing=True,
        misfire_grace_time=2700,
        max_instances=1,
    )
    log(f"Auto-refresh scheduled daily at {hour:02d}:{minute:02d}", "INFO")


def next_run_iso() -> str | None:
    """Return the next scheduled run as an ISO string, or None."""
    if _job:
        try:
            return _job.next_run_time.isoformat()
        except Exception:
            pass
    return None


def stop() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        except Exception:
            pass
        _scheduler = None
