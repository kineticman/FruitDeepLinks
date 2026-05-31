#!/usr/bin/env python3
"""
db/stats.py - Database statistics queries

Returns summary counts and file metadata used by the dashboard.
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from db.connection import resolve_db_path


def get_db_stats(db_path: Path = None) -> Dict[str, Any]:
    """Return dashboard statistics: event counts, lane counts, file info."""
    path = db_path or resolve_db_path()
    if not path.exists():
        return {"error": "Database not found"}

    try:
        stat = path.stat()
        stats: Dict[str, Any] = {
            "db_path": str(path),
            "db_size": stat.st_size,
            "db_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        }

        conn = sqlite3.connect(str(path))
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM events")
        stats["total_events"] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM events WHERE end_utc > datetime('now')")
        stats["future_events"] = cur.fetchone()[0]

        cur.execute(
            """
            SELECT channel_name, COUNT(*) AS count
            FROM events
            WHERE end_utc > datetime('now')
            GROUP BY channel_name
            ORDER BY count DESC
            LIMIT 10
            """
        )
        stats["top_providers"] = [
            {"name": row[0], "count": row[1]} for row in cur.fetchall()
        ]

        # Lane tables (optional — may not exist on fresh installs)
        for table, key in (
            ("lanes", "lane_count"),
            ("adb_lanes", "adb_lane_count"),
        ):
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                stats[key] = cur.fetchone()[0]
            except Exception:
                stats[key] = 0

        for (table, col, is_null), key in (
            (("lane_events", "is_placeholder", False), "scheduled_events"),
            (("lane_events", "is_placeholder", True), "placeholders"),
        ):
            try:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {col} = ?",
                    (1 if is_null else 0,),
                )
                stats[key] = cur.fetchone()[0]
            except Exception:
                stats[key] = 0

        conn.close()
        return stats

    except Exception as e:
        return {"error": str(e)}
