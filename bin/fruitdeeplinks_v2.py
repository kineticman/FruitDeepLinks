#!/usr/bin/env python3
"""
fruitdeeplinks_v2.py - Entrypoint for the v2 refactored server

Replaces the monolithic fruitdeeplinks_server.py.
Run directly:  python3 fruitdeeplinks_v2.py
Or via gunicorn: gunicorn 'fruitdeeplinks_v2:app'
"""

import os
import sys
from pathlib import Path

# Ensure bin/ is on sys.path for local helper imports
_BIN_DIR = str(Path(__file__).parent)
if _BIN_DIR not in sys.path:
    sys.path.insert(0, _BIN_DIR)

from server.app import create_app
from server.config import cfg
from server.logging_setup import log
from server.services.filters import get_auto_refresh
import server.scheduler as sched

app = create_app()

if __name__ == "__main__":
    try:
        from version_info import get_version
        version = get_version()
    except ImportError:
        version = "unknown"

    log(f"FruitDeepLinks v{version} starting (v2 server)", "INFO")

    port = int(os.getenv("PORT", 6655))
    host = os.getenv("HOST", "0.0.0.0")
    log(f"Listening on http://{host}:{port}", "INFO")

    # Start auto-refresh scheduler
    try:
        settings = get_auto_refresh()
        sched.start(settings)
    except Exception as e:
        log(f"Scheduler startup error: {e}", "WARNING")

    try:
        app.run(host=host, port=port, debug=False, threaded=True)
    finally:
        sched.stop()
