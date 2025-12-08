#!/usr/bin/env python
"""
Migration: add adb_lanes table for provider-based ADB lane scheduling.

This creates a simple adb_lanes table that other scripts can populate.
Safe to run multiple times; will only create the table if it does not exist.
"""

import argparse
import logging
import sqlite3
from pathlib import Path
from typing import Optional

DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "fruit_events.db"


def get_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger("migrate_add_adb_lanes")


def ensure_adb_lanes_table(conn: sqlite3.Connection, log: logging.Logger) -> bool:
    """
    Ensure the adb_lanes table exists.

    Returns True if the table was newly created, False if it already existed.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='adb_lanes';"
    )
    if cur.fetchone():
        log.info("Table adb_lanes already exists; nothing to create.")
        return False

    log.info("Creating table adb_lanes ...")
    cur.execute(
        """
        CREATE TABLE adb_lanes (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_code TEXT NOT NULL,
            lane_number   INTEGER NOT NULL,
            channel_id    TEXT NOT NULL,
            event_id      TEXT NOT NULL,
            start_utc     TEXT NOT NULL,
            stop_utc      TEXT NOT NULL
        );
        """
    )
    # Helpful indexes for lookups by channel/time
    cur.execute(
        "CREATE INDEX idx_adb_lanes_channel_time ON adb_lanes(channel_id, start_utc);"
    )
    cur.execute(
        "CREATE INDEX idx_adb_lanes_provider_lane ON adb_lanes(provider_code, lane_number, start_utc);"
    )
    conn.commit()
    log.info("Table adb_lanes created with supporting indexes.")
    return True


def migrate(db_path: Path) -> None:
    log = get_logger()
    log.info("Using database: %s", db_path)

    if not db_path.exists():
        log.error(
            "Database file does not exist at %s. "
            "Run your bootstrap/ingest pipeline first.",
            db_path,
        )
        raise SystemExit(1)

    conn = sqlite3.connect(str(db_path))
    try:
        ensure_adb_lanes_table(conn, log)
    finally:
        conn.close()
        log.info("Migration complete.")


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add adb_lanes table for provider-based ADB scheduling."
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite DB (default: {DEFAULT_DB_PATH})",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    migrate(args.db_path)
