#!/usr/bin/env python3
"""
server/refresh.py - Refresh state and pipeline runners

Owns:
  - refresh_status dict (shared mutable state, thread-safe via GIL for reads)
  - run_refresh()       - invokes daily_refresh.py as subprocess
  - run_apply_filters() - re-runs export scripts only (no scraping)
"""

import re
import json
import subprocess
import threading
from datetime import datetime, timezone
from typing import Optional

from server.config import cfg
from server.logging_setup import append_log_line, log

REFRESH_PROGRESS_PREFIX = "__FDL_PROGRESS__"


# ---- Shared state ----
refresh_status: dict = {
    "running": False,
    "last_run": None,
    "last_status": None,
    "current_step": None,
    "last_run_manual": None,
    "last_status_manual": None,
    "last_run_auto": None,
    "last_status_auto": None,
    "progress": None,
}


def _new_progress() -> dict:
    return {
        "phase": "idle",
        "started_at": None,
        "finished_at": None,
        "version": None,
        "skip_scrape": False,
        "total_steps": None,
        "current_step_key": None,
        "current_step_label": None,
        "current_step_status": None,
        "current_detail": None,
        "completed_steps": [],
    }


refresh_status["progress"] = _new_progress()


def _trim_completed_steps(steps: list, limit: int = 8) -> None:
    if len(steps) > limit:
        del steps[:-limit]


def _consume_progress_marker(line: str) -> bool:
    if not line.startswith(REFRESH_PROGRESS_PREFIX):
        return False
    try:
        payload = json.loads(line[len(REFRESH_PROGRESS_PREFIX):])
    except Exception:
        return False

    progress = refresh_status.setdefault("progress", _new_progress())
    event = payload.get("event")

    if event == "refresh_start":
        refresh_status["progress"] = _new_progress()
        progress = refresh_status["progress"]
        progress["phase"] = "running"
        progress["started_at"] = payload.get("started_at")
        progress["version"] = payload.get("version")
        progress["skip_scrape"] = bool(payload.get("skip_scrape"))
        progress["total_steps"] = payload.get("total_steps")
        refresh_status["current_step"] = "Starting refresh..."

    elif event == "step_start":
        progress["phase"] = "running"
        progress["current_step_key"] = payload.get("step")
        progress["current_step_label"] = payload.get("description")
        progress["current_step_status"] = "running"
        progress["current_detail"] = None
        progress["total_steps"] = payload.get("total_steps") or progress.get("total_steps")
        refresh_status["current_step"] = (
            f"[{payload.get('step')}/{progress.get('total_steps')}] {payload.get('description')}"
        )

    elif event == "step_done":
        record = {
            "step": payload.get("step"),
            "label": payload.get("description"),
            "status": payload.get("status"),
        }
        if payload.get("exit_code") is not None:
            record["exit_code"] = payload.get("exit_code")
        progress["completed_steps"].append(record)
        _trim_completed_steps(progress["completed_steps"])
        progress["current_step_status"] = payload.get("status")
        progress["current_detail"] = None

    elif event == "refresh_done":
        progress["phase"] = payload.get("status", "success")
        progress["finished_at"] = payload.get("finished_at")
        progress["duration_seconds"] = payload.get("duration_seconds")
        progress["current_step_status"] = payload.get("status")

    return True


def _update_progress_detail(line: str) -> None:
    progress = refresh_status.get("progress")
    if not progress or progress.get("phase") != "running":
        return
    stripped = (line or "").strip()
    if not stripped or stripped.startswith("=") or stripped == "FruitDeepLinks Daily Refresh":
        return
    if stripped.startswith("[OK] Step "):
        return
    progress["current_detail"] = stripped


def run_refresh(skip_scrape: bool = False, source: str = "manual") -> None:
    """Run daily_refresh.py in a thread, streaming output into the log buffer."""
    if refresh_status["running"]:
        log("Refresh requested but one is already running; skipping", "WARNING")
        return

    refresh_status["running"] = True
    refresh_status["current_step"] = "Starting refresh..."
    refresh_status["progress"] = _new_progress()
    refresh_status["progress"]["phase"] = "starting"

    label = "Auto" if source == "auto" else "Manual"
    log(f"{label} refresh triggered (skip_scrape={skip_scrape})", "INFO")

    outcome = "error"
    try:
        cmd = ["python3", "-u", str(cfg.BIN_DIR / "daily_refresh.py")]
        if skip_scrape:
            cmd.append("--skip-scrape")

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
        )
        for line in process.stdout:
            line = line.rstrip("\n")
            if not line:
                continue
            if _consume_progress_marker(line):
                continue
            append_log_line(line)
            if re.match(r"^\[[^\]]+/\d+\]\s+", line.strip()):
                refresh_status["current_step"] = line.strip()
            _update_progress_detail(line)

        process.wait()
        if process.returncode == 0:
            outcome = "success"
            refresh_status["progress"]["phase"] = "success"
            log(f"{label} refresh completed successfully", "INFO")
        else:
            outcome = "failed"
            refresh_status["progress"]["phase"] = "failed"
            log(f"{label} refresh failed with code {process.returncode}", "ERROR")

    except Exception as e:
        outcome = "error"
        refresh_status["progress"]["phase"] = "error"
        log(f"{label} refresh error: {e}", "ERROR")
    finally:
        refresh_status["running"] = False
        refresh_status["current_step"] = None
        refresh_status["progress"]["finished_at"] = datetime.now(timezone.utc).isoformat()
        now_iso = datetime.now(timezone.utc).isoformat()
        refresh_status["last_run"] = now_iso
        refresh_status["last_status"] = outcome
        if source == "auto":
            refresh_status["last_run_auto"] = now_iso
            refresh_status["last_status_auto"] = outcome
        else:
            refresh_status["last_run_manual"] = now_iso
            refresh_status["last_status_manual"] = outcome


def run_apply_filters() -> None:
    """Re-run export scripts only (no scraping). Used by Apply Filters Now."""
    if refresh_status["running"]:
        log("Apply filters requested but refresh already running", "WARNING")
        return

    refresh_status["running"] = True
    refresh_status["current_step"] = "Applying filters..."
    log("Applying filter settings (regenerating exports)", "INFO")

    # Read live DB settings so settings-page changes take effect without restart.
    server_url = cfg.SERVER_URL
    num_lanes = cfg.NUM_LANES
    try:
        from db.connection import get_conn, db_exists
        from db.preferences import get_setting
        if db_exists():
            with get_conn() as _conn:
                server_url = get_setting(_conn, "server_url") or server_url
                num_lanes = get_setting(_conn, "num_lanes") or num_lanes
    except Exception:
        pass

    scripts = [
        (
            "fruit_build_lanes.py",
            ["python3", "-u", str(cfg.BIN_DIR / "fruit_build_lanes.py"),
             "--db", str(cfg.DB_PATH),
             "--lanes", str(num_lanes)],
        ),
        (
            "fruit_export_hybrid.py",
            ["python3", "-u", str(cfg.BIN_DIR / "fruit_export_hybrid.py"),
             "--db", str(cfg.DB_PATH)],
        ),
        (
            "fruit_export_lanes.py",
            ["python3", "-u", str(cfg.BIN_DIR / "fruit_export_lanes.py"),
             "--db", str(cfg.DB_PATH),
             "--server-url", server_url],
        ),
        (
            "fruit_build_adb_lanes.py",
            ["python3", "-u", str(cfg.BIN_DIR / "fruit_build_adb_lanes.py"),
             "--db", str(cfg.DB_PATH)],
        ),
        (
            "fruit_export_adb_lanes.py",
            ["python3", "-u", str(cfg.BIN_DIR / "fruit_export_adb_lanes.py"),
             "--db", str(cfg.DB_PATH),
             "--server-url", server_url],
        ),
    ]

    try:
        for script_name, cmd in scripts:
            refresh_status["current_step"] = f"Running {script_name}..."
            log(f"Running {script_name}", "INFO")
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
            )
            for line in process.stdout:
                append_log_line(line.strip())
            process.wait()
            if process.returncode != 0:
                raise Exception(f"{script_name} failed with code {process.returncode}")
        log("Filters applied successfully!", "INFO")
        refresh_status["last_status"] = "success"
    except Exception as e:
        log(f"Apply filters error: {e}", "ERROR")
        refresh_status["last_status"] = "error"
    finally:
        refresh_status["running"] = False
        refresh_status["current_step"] = None
        refresh_status["last_run"] = datetime.now(timezone.utc).isoformat()


def start_refresh_thread(skip_scrape: bool = False, source: str = "manual") -> None:
    """Launch run_refresh() in a daemon thread."""
    threading.Thread(
        target=lambda: run_refresh(skip_scrape=skip_scrape, source=source),
        daemon=True,
    ).start()


def start_apply_filters_thread() -> None:
    """Launch run_apply_filters() in a daemon thread."""
    threading.Thread(target=run_apply_filters, daemon=True).start()
