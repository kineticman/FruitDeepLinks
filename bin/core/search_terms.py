#!/usr/bin/env python3
"""
core/search_terms.py - Apple TV search terms + per-term discovery stats

Single source of truth for the search terms the Apple TV scraper queries.
Users can override the list on the Search Terms page; the override is stored
in fruit_events.db user_preferences under PREF_KEY (JSON list).  An empty or
missing value means "use DEFAULT_TERMS".

Stats are written by apple_scraper_db.py into apple_events.db:
  search_term_runs   one row per scrape run
  search_term_stats  one row per (run, term): counts + duration
  search_term_hits   (term, event_id) pairs from the most recent run only,
                     used for "found only by this term" and drill-down
"""

import json
import sqlite3
from typing import Iterable, List

# "all" is always searched first by the scraper and is not user-editable.
ALWAYS_TERM = "all"

DEFAULT_TERMS: List[str] = [
    "soccer", "nba", "nhl", "mlb", "nfl", "mls",
    "champions league", "europa league", "conference league", "ligue 1", "formula 1", "cricket",
    "espn", "cbs sports", "fox", "fox one", "paramount+", "prime video", "peacock", "dazn",
    "women's college basketball", "men's college basketball",
]

PREF_KEY = "apple_search_terms"

# Number of runs of history to keep in search_term_stats / search_term_runs.
KEEP_RUNS = 60


def normalize_terms(terms: Iterable[str]) -> List[str]:
    """Strip, drop blanks/commas/"all", de-duplicate case-insensitively, keep order."""
    out: List[str] = []
    seen = set()
    for t in terms:
        if not isinstance(t, str):
            continue
        # Terms travel to the scraper as a comma-separated --terms arg.
        t = " ".join(t.replace(",", " ").split())
        key = t.lower()
        if not t or key == ALWAYS_TERM or key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out


def load_terms(conn) -> List[str]:
    """Return the configured terms from user_preferences, or DEFAULT_TERMS. Never raises."""
    if conn is not None:
        try:
            row = conn.execute(
                "SELECT value FROM user_preferences WHERE key = ?", (PREF_KEY,)
            ).fetchone()
            if row and row[0]:
                terms = normalize_terms(json.loads(row[0]))
                if terms:
                    return terms
        except Exception:
            pass
    return list(DEFAULT_TERMS)


def is_customized(conn) -> bool:
    return load_terms(conn) != DEFAULT_TERMS


# ---------------------------------------------------------------- stats schema

def init_stats_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS search_term_runs (
            run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at  TEXT NOT NULL,
            finished_at TEXT,
            term_count  INTEGER,
            total_found INTEGER          -- distinct events across all terms
        );
        CREATE TABLE IF NOT EXISTS search_term_stats (
            run_id      INTEGER NOT NULL,
            term        TEXT NOT NULL,
            position    INTEGER,
            seed_count  INTEGER,         -- event IDs on the search results page
            found_count INTEGER,         -- distinct seeds + related-game shelf events
            unique_count INTEGER,        -- found by no other term in this run
            new_full    INTEGER,
            refreshed   INTEGER,
            new_shelf   INTEGER,
            errors      INTEGER,
            duration_s  REAL,
            PRIMARY KEY (run_id, term)
        );
        CREATE TABLE IF NOT EXISTS search_term_hits (
            term     TEXT NOT NULL,
            event_id TEXT NOT NULL,
            run_id   INTEGER NOT NULL,
            PRIMARY KEY (term, event_id)
        );
        CREATE INDEX IF NOT EXISTS idx_sth_event ON search_term_hits(event_id);
        """
    )
