#!/usr/bin/env python3
"""
server/logging_setup.py - In-memory log buffer, per-run log files, werkzeug filter

Provides:
  - append_log_line(line, echo) -> int  (returns sequence number)
  - log(message, level)
  - get_recent_logs(count, after_seq) -> list[(seq, line)]
  - get_recent_entries(count, after_seq) -> list[dict(seq, ts, line)]
  - start_run_log(kind, detail) / end_run_log(status)
  - list_run_logs() / run_log_path(name)
  - configure_werkzeug()
  - BOOT_ID  (changes every server start; lets log clients detect a restart)

Every line lands in the 5000-line ring buffer (web viewer).  Lines also go to
stdout (docker logs) and, while a refresh/apply-filters run is active, to
LOG_DIR/<kind>-YYYYMMDD-HHMMSS.log so runs survive container restarts.
"""

import logging
import re
import threading
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional

from server.config import cfg

# ---- Internal state ----
_log_lock = threading.Lock()
_log_seq = 0
_log_buffer: deque = deque(maxlen=5000)
_LOG_LEVEL_NUM = getattr(logging, cfg.LOG_LEVEL, logging.INFO)

BOOT_ID = uuid.uuid4().hex[:12]

# Per-run log files
RUN_LOG_KEEP = 20
_RUN_LOG_RE = re.compile(r"^(refresh|apply-filters)-(\d{8})-(\d{6})\.log$")
_STATUS_RE = re.compile(r"^# status: (\w+)", re.M)
_run_file = None
_run_name: Optional[str] = None


class _SuppressNoisyAccessLogs(logging.Filter):
    """Keep access logs that matter; drop routine web-UI browsing.

    Kept: errors (4xx/5xx), anything that changes state (POST/PUT/DELETE...),
    and integration traffic -- M3U/XMLTV fetches and tune-time deeplink lookups
    from Channels DVR / ADBTuner / CC4C.  Dropped: successful page loads,
    static assets, the UI's own API polling, and health checks.
    """

    _REQUEST_RE = re.compile(r'"(\w+) (\S+) [^"]*" (\d{3})')
    _ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
    _INTEGRATION = ("/m3u/", "/xmltv/", "/out/", "/whatson/", "/api/lane/", "/api/adb/lanes/")
    # Integration files polled often enough to drown everything else.
    _POLLED = ("/out/blank.m3u", "/out/multisource_lanes.m3u")

    def filter(self, record):
        m = self._REQUEST_RE.search(self._ANSI_RE.sub("", record.getMessage()))
        if not m:
            return True
        method, path, status = m.group(1), m.group(2), int(m.group(3))
        if status >= 400 or method not in ("GET", "HEAD"):
            return True
        path = path.split("?", 1)[0]
        if path.startswith(self._POLLED):
            return False
        return path.startswith(self._INTEGRATION)


def configure_werkzeug() -> None:
    logging.getLogger("werkzeug").addFilter(_SuppressNoisyAccessLogs())


def _should_emit(level: str) -> bool:
    return getattr(logging, str(level).upper(), logging.INFO) >= _LOG_LEVEL_NUM


def append_log_line(line: str, echo: bool = True) -> int:
    """Add a line to the ring buffer (and the active run file; stdout if echo).

    Returns the sequence number assigned.
    """
    global _log_seq
    if line is None:
        return _log_seq
    line = str(line).rstrip("\n")
    now = datetime.now()
    with _log_lock:
        _log_seq += 1
        _log_buffer.append((_log_seq, now.strftime("%H:%M:%S"), line))
        seq = _log_seq
        if _run_file is not None:
            try:
                _run_file.write(f"{now.strftime('%H:%M:%S')} {line}\n")
                _run_file.flush()
            except Exception:
                pass
    if echo:
        print(line, flush=True)
    return seq


def log(message: str, level: str = "INFO") -> None:
    """Format and append a server-generated log line."""
    if not _should_emit(level):
        return
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    append_log_line(f"[{ts}] [{level}] {message}")


def get_recent_entries(count: int = 200, after_seq: int = 0) -> list:
    """Return up to `count` entries {seq, ts, line} with seq > after_seq."""
    with _log_lock:
        snapshot = list(_log_buffer)
    filtered = [e for e in snapshot if e[0] > after_seq]
    return [{"seq": s, "ts": t, "line": l} for s, t, l in filtered[-count:]]


def get_recent_logs(count: int = 200, after_seq: int = 0) -> list:
    """Return up to `count` log lines with seq > after_seq."""
    return [(e["seq"], e["line"]) for e in get_recent_entries(count, after_seq)]


# ---- Per-run log files ----

def _log_dir() -> Path:
    return Path(cfg.LOG_DIR)


def start_run_log(kind: str, detail: str = "") -> Optional[str]:
    """Open LOG_DIR/<kind>-<timestamp>.log; subsequent lines are copied into it."""
    global _run_file, _run_name
    end_run_log("interrupted")
    try:
        d = _log_dir()
        d.mkdir(parents=True, exist_ok=True)
        now = datetime.now()
        name = f"{kind}-{now.strftime('%Y%m%d-%H%M%S')}.log"
        fh = open(d / name, "w", encoding="utf-8")
        fh.write(f"# FruitDeepLinks {kind} {detail}\n# started: {now.isoformat(timespec='seconds')}\n")
        fh.flush()
        with _log_lock:
            _run_file, _run_name = fh, name
        _prune_run_logs(d)
        return name
    except Exception as e:
        print(f"[logging] could not open run log in {_log_dir()}: {e}", flush=True)
        return None


def end_run_log(status: str) -> None:
    global _run_file, _run_name
    with _log_lock:
        fh, _run_file, _run_name = _run_file, None, None
    if fh is None:
        return
    try:
        fh.write(f"# finished: {datetime.now().isoformat(timespec='seconds')}\n# status: {status}\n")
        fh.close()
    except Exception:
        pass


def _prune_run_logs(d: Path) -> None:
    stamped = []
    for p in d.iterdir():
        m = _RUN_LOG_RE.match(p.name)
        if m:
            stamped.append((m.group(2) + m.group(3), p))
    stamped.sort(reverse=True)
    for _, p in stamped[RUN_LOG_KEEP:]:
        try:
            p.unlink()
        except Exception:
            pass


def run_log_path(name: str) -> Optional[Path]:
    """Return the path for a run log name, or None if the name isn't a run log."""
    if not _RUN_LOG_RE.match(name or ""):
        return None
    p = _log_dir() / name
    return p if p.is_file() else None


def list_run_logs() -> list:
    """Newest-first list of run logs with kind, start time, size and final status."""
    d = _log_dir()
    if not d.is_dir():
        return []
    out = []
    for p in d.iterdir():
        m = _RUN_LOG_RE.match(p.name)
        if not m:
            continue
        kind, day, tod = m.groups()
        status = "running" if p.name == _run_name else "interrupted"
        try:
            size = p.stat().st_size
            with open(p, "rb") as fh:
                fh.seek(max(0, size - 200))
                sm = _STATUS_RE.search(fh.read().decode("utf-8", "replace"))
            if sm:
                status = sm.group(1)
        except Exception:
            size = 0
        started = datetime.strptime(day + tod, "%Y%m%d%H%M%S").isoformat()
        out.append({"name": p.name, "kind": kind, "started": started, "size": size, "status": status})
    out.sort(key=lambda r: r["started"], reverse=True)
    return out
