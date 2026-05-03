#!/usr/bin/env python3
"""
db/preferences.py - User preference CRUD

Reads and writes the user_preferences key/value table.
All callers should use get_conn() from db.connection.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict

# Aliases: legacy service codes that get normalised on save
_SAVE_ALIASES = {
    "aiv_fox": "aiv_fox_one",
}

_DEFAULTS: Dict[str, Any] = {
    "enabled_services": [],
    "disabled_sports": [],
    "disabled_leagues": [],
    "service_priorities": {},
    "amazon_penalty": True,
    "amazon_master_enabled": True,
    "language_preference": "en",
}


def load(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    Return user preferences merged with defaults.
    Never raises — returns defaults if the table is missing or on error.
    """
    result = dict(_DEFAULTS)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='user_preferences'"
        )
        if not cur.fetchone():
            return result

        cur.execute("SELECT key, value FROM user_preferences")
        raw = {k: v for k, v in cur.fetchall()}
        cur.close()
    except Exception:
        return result

    def _bool(key: str, default: bool) -> bool:
        v = raw.get(key)
        if v is None:
            return default
        try:
            return bool(json.loads(v) if isinstance(v, str) else v)
        except Exception:
            return default

    def _json(key: str, default):
        v = raw.get(key)
        if v is None:
            return default
        try:
            parsed = json.loads(v) if isinstance(v, str) else v
            return parsed if parsed is not None else default
        except Exception:
            return default

    def _list(key: str) -> list:
        v = _json(key, [])
        return v if isinstance(v, list) else []

    result["enabled_services"] = _list("enabled_services")
    result["disabled_sports"] = _list("disabled_sports")
    result["disabled_leagues"] = _list("disabled_leagues")
    result["service_priorities"] = _json("service_priorities", {})
    result["amazon_penalty"] = _bool("amazon_penalty", True)
    result["amazon_master_enabled"] = _bool("amazon_master_enabled", True)

    lang = raw.get("language_preference", "en")
    if isinstance(lang, str) and lang.startswith('"'):
        try:
            lang = json.loads(lang)
        except Exception:
            lang = "en"
    result["language_preference"] = lang if lang in ("en", "es", "both") else "en"

    return result


def save(conn: sqlite3.Connection, prefs: Dict[str, Any]) -> bool:
    """
    Persist a preferences dict.  Each key becomes a row in user_preferences.
    Returns True on success.
    """
    # Normalise legacy service code aliases
    if prefs.get("enabled_services"):
        prefs = dict(prefs)
        prefs["enabled_services"] = [
            _SAVE_ALIASES.get(s, s) for s in prefs["enabled_services"]
        ]

    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_preferences (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_utc TEXT
            )
            """
        )
        now = datetime.now(timezone.utc).isoformat()
        for key, value in prefs.items():
            cur.execute(
                "INSERT OR REPLACE INTO user_preferences (key, value, updated_utc) VALUES (?, ?, ?)",
                (key, json.dumps(value), now),
            )
        conn.commit()
        return True
    except Exception:
        return False


def load_auto_refresh(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Return auto-refresh settings (enabled + time) with env/DB fallback."""
    settings = {
        "enabled": os.getenv("AUTO_REFRESH_ENABLED", "1").lower() not in ("0", "false", "no"),
        "time": os.getenv("AUTO_REFRESH_TIME", "02:30"),
    }
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='user_preferences'"
        )
        if not cur.fetchone():
            return settings
        cur.execute(
            "SELECT key, value FROM user_preferences WHERE key IN ('auto_refresh_enabled', 'auto_refresh_time')"
        )
        for key, value in cur.fetchall():
            if key == "auto_refresh_enabled":
                try:
                    settings["enabled"] = bool(json.loads(value))
                except Exception:
                    pass
            elif key == "auto_refresh_time":
                try:
                    settings["time"] = json.loads(value)
                except Exception:
                    settings["time"] = value
    except Exception:
        pass
    return settings


# ---- App settings (replaces env vars for operational config) ----
# Each entry: key -> (env_var_name, type_hint, default_value, label, description)
# type_hint: 'str' | 'int' | 'bool'
# Settings are stored in user_preferences with the key prefix "setting:"

SETTINGS_DEFS: Dict[str, tuple] = {
    "server_url": (
        "SERVER_URL", "str", "http://192.168.86.80:6655",
        "Server URL",
        "Base URL that ADBTuner and Channels DVR use to reach this server (M3U/XMLTV links).",
    ),
    "channels_dvr_ip": (
        "CHANNELS_DVR_IP", "str", "192.168.86.72",
        "Channels DVR IP",
        "IP address of your Channels DVR server.",
    ),
    "channels_source_name": (
        "CHANNELS_SOURCE_NAME", "str", "FruitDeepLinks",
        "Channels Source Name",
        "Name shown in Channels DVR for the FruitDeepLinks source.",
    ),
    "num_lanes": (
        "FRUIT_LANES", "int", 50,
        "Number of Lanes",
        "How many virtual channels (lanes) to generate.",
    ),
    "lane_start_ch": (
        "FRUIT_LANE_START_CH", "int", 9000,
        "Lane Start Channel",
        "Channel number of the first virtual lane.",
    ),
    "direct_start_ch": (
        "FRUIT_DIRECT_START_CH", "int", 5000,
        "Direct Start Channel",
        "Channel number of the first direct-provider channel.",
    ),
    "padding_minutes": (
        "FRUIT_PADDING_MINUTES", "int", 45,
        "Padding Minutes",
        "How long (minutes) to keep showing a just-ended event's deeplink.",
    ),
    "days_ahead": (
        "FRUIT_DAYS_AHEAD", "int", 7,
        "Days Ahead",
        "How far into the future to fetch events.",
    ),
    "headless": (
        "HEADLESS", "bool", True,
        "Headless Browser",
        "Run scrapers in headless mode (disable for debugging).",
    ),
    "log_level": (
        "LOG_LEVEL", "str", "INFO",
        "Log Level",
        "Logging verbosity: DEBUG, INFO, WARNING, ERROR.",
    ),
    "auto_refresh_enabled": (
        "AUTO_REFRESH_ENABLED", "bool", True,
        "Auto-Refresh Enabled",
        "Automatically refresh guide data on a daily schedule.",
    ),
    "auto_refresh_time": (
        "AUTO_REFRESH_TIME", "str", "02:30",
        "Auto-Refresh Time (HH:MM)",
        "Daily refresh time in 24-hour format (local server time).",
    ),
    # Scraper toggles — env var is a hard override; DB setting is the UI control
    "kayo_enabled": (
        "KAYO_ENABLED", "bool", True,
        "Kayo Sports",
        "Scrape Kayo Sports (Australian sports — Cricket, AFL, NRL).",
    ),
    "fanatiz_enabled": (
        "FANATIZ_ENABLED", "bool", True,
        "Fanatiz Soccer",
        "Scrape Fanatiz (Latin American soccer leagues, ~1,300 future events).",
    ),
    "bein_enabled": (
        "BEIN_ENABLED", "bool", True,
        "beIN Sports",
        "Scrape beIN Sports (international soccer, rugby, motorsports).",
    ),
    "nesn_enabled": (
        "NESN_ENABLED", "bool", True,
        "NESN",
        "Scrape NESN (New England Sports Network — Red Sox, Bruins).",
    ),
    "victory_enabled": (
        "VICTORY_ENABLED", "bool", True,
        "Victory+",
        "Scrape Victory+ (WHL, LOVB, niche sports).",
    ),
    "gotham_enabled": (
        "GOTHAM_ENABLED", "bool", True,
        "Gotham Sports (MSG/YES)",
        "Scrape Gotham Sports (NYC: Knicks, Rangers, Islanders, Devils, Yankees, Nets).",
    ),
    "espn_enabled": (
        "ESPN_ENABLED", "bool", True,
        "ESPN Watch Graph",
        "Scrape ESPN Watch Graph API for Fire TV deeplink enrichment.",
    ),
}

_SETTING_KEY_PREFIX = "setting:"


def _cast_setting(value: str, type_hint: str):
    """Cast a raw DB string to the correct Python type."""
    if type_hint == "bool":
        try:
            return bool(json.loads(value))
        except Exception:
            return value.lower() not in ("0", "false", "no", "")
    if type_hint == "int":
        try:
            return int(json.loads(value))
        except Exception:
            return int(value)
    return value  # str


def get_setting(conn: sqlite3.Connection, key: str, fallback=None):
    """Return the setting value: DB wins, then env var, then hardcoded default.

    Never raises.
    """
    defn = SETTINGS_DEFS.get(key)

    # Try DB first
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT value FROM user_preferences WHERE key = ?",
            (f"{_SETTING_KEY_PREFIX}{key}",),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            type_hint = defn[1] if defn else "str"
            return _cast_setting(row[0], type_hint)
    except Exception:
        pass

    if defn:
        env_var, type_hint, default, *_ = defn
        env_val = os.getenv(env_var)
        if env_val is not None:
            try:
                return _cast_setting(env_val, type_hint)
            except Exception:
                pass
        return default

    return fallback


def load_all_settings(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Return all settings as a dict, preferring DB over env vars."""
    result = {}
    for key in SETTINGS_DEFS:
        result[key] = get_setting(conn, key)
    return result


def save_settings(conn: sqlite3.Connection, updates: Dict[str, Any]) -> bool:
    """Persist one or more settings to the DB.  Silently ignores unknown keys."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_preferences (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_utc TEXT
            )
            """
        )
        now = datetime.now(timezone.utc).isoformat()
        for key, value in updates.items():
            if key not in SETTINGS_DEFS:
                continue
            cur.execute(
                "INSERT OR REPLACE INTO user_preferences (key, value, updated_utc) VALUES (?, ?, ?)",
                (f"{_SETTING_KEY_PREFIX}{key}", json.dumps(value), now),
            )
        conn.commit()
        return True
    except Exception:
        return False


def get_settings_schema() -> list[dict]:
    """Return settings definitions as a list suitable for rendering a settings form."""
    return [
        {
            "key": key,
            "label": label,
            "description": desc,
            "type": type_hint,
            "default": default,
            "env_var": env_var,
        }
        for key, (env_var, type_hint, default, label, desc) in SETTINGS_DEFS.items()
    ]


def save_auto_refresh(conn: sqlite3.Connection, settings: Dict[str, Any]) -> bool:
    """Persist auto-refresh settings."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_preferences (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_utc TEXT
            )
            """
        )
        now = datetime.now(timezone.utc).isoformat()
        cur.execute(
            "INSERT OR REPLACE INTO user_preferences (key, value, updated_utc) VALUES (?, ?, ?)",
            ("auto_refresh_enabled", json.dumps(bool(settings.get("enabled", False))), now),
        )
        cur.execute(
            "INSERT OR REPLACE INTO user_preferences (key, value, updated_utc) VALUES (?, ?, ?)",
            ("auto_refresh_time", json.dumps(settings.get("time", "02:30")), now),
        )
        conn.commit()
        return True
    except Exception:
        return False
