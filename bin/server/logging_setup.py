#!/usr/bin/env python3
"""
server/logging_setup.py - In-memory log buffer and werkzeug filter

Provides:
  - append_log_line(line) -> int  (returns sequence number)
  - log(message, level)
  - get_recent_logs(count, after_seq) -> list[(seq, line)]
  - configure_werkzeug()
"""

import logging
import threading
from collections import deque
from datetime import datetime, timezone

from server.config import cfg

# ---- Internal state ----
_log_lock = threading.Lock()
_log_seq = 0
_log_buffer: deque = deque(maxlen=5000)
_LOG_LEVEL_NUM = getattr(logging, cfg.LOG_LEVEL, logging.INFO)


class _SuppressNoisyAccessLogs(logging.Filter):
    """Filter successful access logs for high-frequency polling endpoints."""

    _NOISY = (
        "GET /out/blank.m3u ",
        "GET /out/multisource_lanes.m3u ",
        "GET /api/status ",
        "GET /api/logs?count=",
        "GET /api/logs/stream?",
    )

    def filter(self, record):
        msg = record.getMessage()
        if " 200 -" not in msg:
            return True
        return not any(p in msg for p in self._NOISY)


def configure_werkzeug() -> None:
    logging.getLogger("werkzeug").addFilter(_SuppressNoisyAccessLogs())


def _should_emit(level: str) -> bool:
    return getattr(logging, str(level).upper(), logging.INFO) >= _LOG_LEVEL_NUM


def append_log_line(line: str) -> int:
    """Add a line to the ring buffer. Returns the sequence number assigned."""
    global _log_seq
    if line is None:
        return _log_seq
    line = str(line).rstrip("\n")
    with _log_lock:
        _log_seq += 1
        _log_buffer.append((_log_seq, line))
        return _log_seq


def log(message: str, level: str = "INFO") -> None:
    """Format and append a server-generated log line."""
    if not _should_emit(level):
        return
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    append_log_line(f"[{ts}] [{level}] {message}")
    print(f"[{ts}] [{level}] {message}", flush=True)


def get_recent_logs(count: int = 200, after_seq: int = 0) -> list:
    """Return up to `count` log lines with seq > after_seq."""
    with _log_lock:
        snapshot = list(_log_buffer)
    filtered = [(s, l) for s, l in snapshot if s > after_seq]
    return filtered[-count:]
