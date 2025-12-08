#!/usr/bin/env python
"""
Migration: add provider_lanes table for ADB lane configuration.

Safe to run multiple times; will only create the table if it does not exist,
and will lightly bootstrap rows from playables.* provider columns if present.
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
    return logging.getLogger("migrate_add_provider_lanes")


def ensure_provider_lanes_table(conn: sqlite3.Connection, log: logging.Logger) -> bool:
    """
    Ensure the provider_lanes table exists.

    Returns True if the table was newly created, False if it already existed.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='provider_lanes';"
    )
    if cur.fetchone():
        log.info("Table provider_lanes already exists; nothing to create.")
        return False

    log.info("Creating table provider_lanes ...")
    cur.execute(
        """
        CREATE TABLE provider_lanes (
            provider_code   TEXT PRIMARY KEY,
            adb_enabled     INTEGER NOT NULL DEFAULT 0,
            adb_lane_count  INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    conn.commit()
    log.info("Table provider_lanes created.")
    return True


def bootstrap_from_playables(conn: sqlite3.Connection, log: logging.Logger) -> None:
    """
    Best-effort bootstrap provider_lanes rows from existing playables/events tables.

    This is intentionally defensive: if the expected tables/columns are missing,
    we just log and continue.
    """
    cur = conn.cursor()

    # Try a few likely spots for provider identifiers.
    provider_sources = [
        ("playables", "provider_code"),
        ("playables", "provider"),
        ("events", "provider_code"),
        ("events", "provider"),
    ]

    seen_codes = set()
    for table, column in provider_sources:
        try:
            cur.execute(
                f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL;"
            )
        except sqlite3.OperationalError as exc:
            log.debug(
                "Skipping %s.%s while bootstrapping provider_lanes: %s",
                table,
                column,
                exc,
            )
            continue

        rows = [r[0] for r in cur.fetchall() if r[0]]
        if not rows:
            continue

        for code in rows:
            if code in seen_codes:
                continue
            seen_codes.add(code)
            cur.execute(
                """
                INSERT OR IGNORE INTO provider_lanes (provider_code)
                VALUES (?);
                """,
                (code,),
            )

    if seen_codes:
        conn.commit()
        log.info(
            "Bootstrapped %d provider_lanes rows from existing tables.",
            len(seen_codes),
        )
    else:
        log.info(
            "No provider codes found to bootstrap into provider_lanes "
            "(this is OK if you're running early in the project)."
        )


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
        created = ensure_provider_lanes_table(conn, log)
        if created:
            bootstrap_from_playables(conn, log)
        else:
            # Even if the table already existed, we can still try to back-fill
            # any missing providers if the table is currently empty.
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM provider_lanes;")
            count = cur.fetchone()[0] or 0
            if count == 0:
                log.info(
                    "provider_lanes exists but is empty; attempting bootstrap from playables/events."
                )
                bootstrap_from_playables(conn, log)
            else:
                log.info(
                    "provider_lanes already has %d row(s); leaving as-is.", count
                )
    finally:
        conn.close()
        log.info("Migration complete.")


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add provider_lanes table for ADB lane configuration."
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

