#!/usr/bin/env python3
"""
server/utils.py - Shared helpers used across route handlers

All functions are pure / stateless — no Flask or DB imports here.
"""

import json
import sqlite3


def row_to_dict(row) -> dict | None:
    """Convert a sqlite3.Row to a plain dict."""
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def pretty_json(val) -> str | None:
    """Pretty-print a JSON string, dict, or list. Returns None for empty/None."""
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        try:
            return json.dumps(val, indent=2, ensure_ascii=False, sort_keys=False)
        except Exception:
            return str(val)
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return json.dumps(json.loads(s), indent=2, ensure_ascii=False, sort_keys=False)
        except Exception:
            return val
    try:
        return json.dumps(val, indent=2, ensure_ascii=False, sort_keys=False)
    except Exception:
        return str(val)


def db_has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    """Return True if `col` exists in `table`."""
    try:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        return col in [r[1] for r in cur.fetchall()]
    except Exception:
        return False


def parse_int_arg(name: str, default: int, min_v: int = None, max_v: int = None) -> int:
    """Parse an integer query-string parameter with bounds.  Requires Flask request context."""
    from flask import request
    try:
        v = int(request.args.get(name, default))
    except Exception:
        v = default
    if min_v is not None:
        v = max(min_v, v)
    if max_v is not None:
        v = min(max_v, v)
    return v
