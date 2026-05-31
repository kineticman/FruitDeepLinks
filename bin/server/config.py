#!/usr/bin/env python3
"""
server/config.py - Centralised environment variable loading

All os.getenv() calls for the server live here.  Import `cfg` elsewhere:

    from server.config import cfg
    print(cfg.DB_PATH)
"""

import os
from pathlib import Path


class _Config:
    # Database / filesystem paths
    DB_PATH = Path(
        os.getenv("FRUIT_DB_PATH")
        or os.getenv("PEACOCK_DB_PATH")
        or "/app/data/fruit_events.db"
    )
    OUT_DIR = Path(os.getenv("OUT_DIR", "/app/out"))
    BIN_DIR = Path(os.getenv("BIN_DIR", "/app/bin"))
    LOG_DIR = Path(os.getenv("LOG_DIR", "/app/logs"))

    # Lane config
    NUM_LANES = int(os.getenv("FRUIT_LANES", "50"))
    LANE_START_CH = int(os.getenv("FRUIT_LANE_START_CH", "9000"))
    DIRECT_START_CH = int(os.getenv("FRUIT_DIRECT_START_CH", "5000"))
    DAYS_AHEAD = int(os.getenv("FRUIT_DAYS_AHEAD", "7"))
    PADDING_MINUTES = int(os.getenv("FRUIT_PADDING_MINUTES", "45"))
    PLACEHOLDER_BLOCK_MINUTES = int(os.getenv("FRUIT_PLACEHOLDER_BLOCK_MINUTES", "60"))

    # Channels DVR integration
    CDVR_SERVER_IP = os.getenv("CHANNELS_DVR_IP", "192.168.86.72")
    CDVR_SERVER_PORT = int(os.getenv("CDVR_SERVER_PORT", "8089"))
    CDVR_API_PORT = int(os.getenv("CDVR_API_PORT", "57000"))
    CDVR_DVR_PATH = os.getenv("CDVR_DVR_PATH", "")

    # Integration servers
    SERVER_URL = os.getenv("SERVER_URL", "http://192.168.86.80:6655")
    CC_SERVER = os.getenv("CC_SERVER", "localhost")
    CC_PORT = int(os.getenv("CC_PORT", "8080"))
    CH4C_SERVER = os.getenv("CH4C_SERVER", "localhost")
    CH4C_PORT = int(os.getenv("CH4C_PORT", "8080"))
    PCAST_SERVER = os.getenv("PCAST_SERVER", "localhost")
    PCAST_PORT = int(os.getenv("PCAST_PORT", "5589"))

    # CDVR detector
    DETECTOR_ENABLED: bool = bool(CDVR_DVR_PATH and CDVR_DVR_PATH.strip())
    STREAMLINK_DIR: Path = (
        Path("/mnt/dvr") / "Imports" / "Videos" / "FruitDeepLinks"
        if DETECTOR_ENABLED
        else None
    )
    DETECT_DEBOUNCE_SECONDS = float(os.getenv("DETECT_DEBOUNCE_SECONDS", "3"))

    # Auto-refresh defaults (can be overridden by DB prefs)
    AUTO_REFRESH_ENABLED = os.getenv("AUTO_REFRESH_ENABLED", "1").lower() not in (
        "0",
        "false",
        "no",
    )
    AUTO_REFRESH_TIME = os.getenv("AUTO_REFRESH_TIME", "02:30")

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    # Scraper toggles (read by daily_refresh.py; referenced here for completeness)
    HEADLESS = os.getenv("HEADLESS", "true").lower() not in ("0", "false", "no")
    NO_NETWORK = os.getenv("NO_NETWORK", "false").lower() in ("1", "true", "yes")


cfg = _Config()
