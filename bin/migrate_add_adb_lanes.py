#!/usr/bin/env python
"""
Migration: add adb_lanes table and http_deeplink_url column.

This migration handles:
1. adb_lanes table for provider-based ADB lane scheduling
2. http_deeplink_url column in playables for Android/Fire TV compatibility

Safe to run multiple times; will only create tables/columns if they don't exist.
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


def ensure_http_deeplink_column(conn: sqlite3.Connection, log: logging.Logger) -> bool:
    """
    Add http_deeplink_url column to playables table for Android/Fire TV compatibility.
    
    Returns True if column was added, False if it already existed.
    """
    cur = conn.cursor()
    
    # Check if column already exists
    cur.execute("PRAGMA table_info(playables)")
    columns = [row[1] for row in cur.fetchall()]
    
    if "http_deeplink_url" in columns:
        log.info("Column playables.http_deeplink_url already exists; nothing to create.")
        return False
    
    log.info("Adding http_deeplink_url column to playables table...")
    cur.execute("ALTER TABLE playables ADD COLUMN http_deeplink_url TEXT")
    conn.commit()
    log.info("Column playables.http_deeplink_url added.")
    return True


def populate_http_deeplinks(conn: sqlite3.Connection, log: logging.Logger) -> None:
    """
    OPTIONAL PRE-POPULATION:
      Fill playables.http_deeplink_url for rows that have a deeplink but no HTTP version yet.

    Notes:
      - Source column is deeplink_play (primary), with fallbacks:
          deeplink_open, playable_url
      - playables PK is composite: (event_id, playable_id)
      - Pass playable_id to converter (required for ESPN playChannel case)
      - Safe/idempotent: only fills blank http_deeplink_url
    """
    cur = conn.cursor()

    # Inspect schema
    cur.execute("PRAGMA table_info(playables)")
    cols = [r[1] for r in cur.fetchall()]

    if "http_deeplink_url" not in cols:
        log.info("Column playables.http_deeplink_url not found; skipping prefill.")
        return

    # Determine available source columns in priority order
    src_cols = [c for c in ("deeplink_play", "deeplink_open", "playable_url", "deeplink_url") if c in cols]
    if not src_cols:
        log.info("No deeplink columns found in playables; skipping prefill.")
        return

    if "event_id" not in cols or "playable_id" not in cols:
        log.info("playables missing event_id/playable_id; skipping prefill.")
        return

    # Import converter (supports new signature; fall back to old if needed)
    try:
        from deeplink_converter import generate_http_deeplink
    except Exception as e:
        log.info(f"deeplink_converter not available; skipping prefill (runtime conversion will be used). ({e})")
        return

    primary_col = "deeplink_play" if "deeplink_play" in cols else src_cols[0]
    log.info(f"Prefilling http_deeplink_url from {primary_col} (fallbacks: {', '.join(src_cols)})")

    # Pull rows needing HTTP; limit to keep migration snappy
    query = f"""
        SELECT event_id, playable_id, provider,
               {', '.join(src_cols)}
        FROM playables
        WHERE (http_deeplink_url IS NULL OR http_deeplink_url = '')
          AND ({primary_col} IS NOT NULL AND {primary_col} != '')
        LIMIT 20000
    """
    cur.execute(query)
    rows = cur.fetchall()

    if not rows:
        log.info("No playables need HTTP deeplink generation.")
        return

    log.info(f"Generating HTTP deeplinks for {len(rows)} playables...")
    updated = 0

    for row in rows:
        event_id, playable_id, provider, *candidates = row

        # pick first non-empty candidate in our priority order
        deeplink = next((d for d in candidates if d), None)
        if not deeplink:
            continue

        try:
            http_url = generate_http_deeplink(deeplink, provider=provider, playable_id=playable_id)
        except TypeError:
            # Back-compat: generate_http_deeplink(url, provider)
            http_url = generate_http_deeplink(deeplink, provider)

        if not http_url:
            continue

        cur.execute(
            "UPDATE playables SET http_deeplink_url = ? WHERE event_id = ? AND playable_id = ?",
            (http_url, event_id, playable_id),
        )
        updated += cur.rowcount

    if updated:
        conn.commit()
    log.info(f"HTTP deeplink generation complete. Updated {updated} rows.")


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
        # Ensure adb_lanes table
        ensure_adb_lanes_table(conn, log)
        
        # Ensure http_deeplink_url column in playables
        ensure_http_deeplink_column(conn, log)
        
        # Optionally pre-populate HTTP deeplinks (safe/idempotent)
        populate_http_deeplinks(conn, log)
            
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
