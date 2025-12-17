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
    Generate HTTP deeplinks for existing playables using deeplink_converter.
    
    This is optional - deeplinks are converted at runtime if not pre-stored.
    """
    try:
        # Import converter (optional, system works without pre-population)
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent))
        from deeplink_converter import generate_http_deeplink
    except ImportError:
        log.info("deeplink_converter not available; skipping HTTP deeplink population (will convert at runtime)")
        return
    
    cur = conn.cursor()
    
    # Check if playables table exists and has the required columns
    try:
        cur.execute("PRAGMA table_info(playables)")
        columns = [row[1] for row in cur.fetchall()]
        
        if 'playables' not in ['playables']:  # Table check
            log.info("playables table not found; skipping HTTP deeplink population")
            return
            
        # Find the primary key column
        pk_col = None
        if 'id' in columns:
            pk_col = 'id'
        elif 'playable_id' in columns:
            pk_col = 'playable_id'
        else:
            # Try to find any column with 'id' in it
            for col in columns:
                if 'id' in col.lower():
                    pk_col = col
                    break
        
        if not pk_col:
            log.info("Could not find primary key column in playables; skipping HTTP deeplink population")
            return
            
        if 'deeplink_url' not in columns:
            log.info("deeplink_url column not found in playables; skipping HTTP deeplink population")
            return
            
        log.info(f"Using primary key column: {pk_col}")
        
    except Exception as e:
        log.info(f"Could not check playables schema: {e}; skipping HTTP deeplink population")
        return
    
    # Get playables that need HTTP versions
    try:
        query = f"""
            SELECT {pk_col}, deeplink_url, provider 
            FROM playables 
            WHERE deeplink_url IS NOT NULL 
              AND (http_deeplink_url IS NULL OR http_deeplink_url = '')
            LIMIT 1000
        """
        cur.execute(query)
        
        playables = cur.fetchall()
        if not playables:
            log.info("No playables need HTTP deeplink generation.")
            return
        
        log.info(f"Generating HTTP deeplinks for {len(playables)} playables...")
        updated = 0
        
        for row in playables:
            playable_id, original_deeplink, provider = row
            
            # Generate HTTP version
            http_deeplink = generate_http_deeplink(original_deeplink, provider)
            
            if http_deeplink and http_deeplink != original_deeplink:
                cur.execute(
                    f"UPDATE playables SET http_deeplink_url = ? WHERE {pk_col} = ?",
                    (http_deeplink, playable_id)
                )
                updated += 1
        
        if updated > 0:
            conn.commit()
            log.info(f"Generated HTTP deeplinks for {updated} playables.")
        else:
            log.info("No playables had convertible deeplinks (this is OK, conversion happens at runtime).")
            
    except Exception as e:
        log.info(f"Error during HTTP deeplink population: {e}; skipping (conversion happens at runtime anyway)")
        return


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
        http_col_added = ensure_http_deeplink_column(conn, log)
        
        # Optionally populate HTTP deeplinks (works without this, converts at runtime)
        if http_col_added:
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
