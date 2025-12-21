#!/usr/bin/env python
"""
fruit_build_adb_lanes.py

Builds provider-based ADB lanes using provider_lanes configuration.

This script reads:
  - provider_lanes: which providers are ADB-enabled and how many lanes they get
  - events + playables: which events belong to which providers (via provider_code)

Then writes:
  - adb_lanes: per-provider lane assignments with channel_id, event_id, and times

ASSUMED SCHEMA (fail-fast with clear errors if mismatched):

  provider_lanes(
      provider_code   TEXT PRIMARY KEY,
      adb_enabled     INTEGER NOT NULL,
      adb_lane_count  INTEGER NOT NULL
  )

  events(
      id         TEXT PRIMARY KEY,
      start_utc  TEXT,
      stop_utc   TEXT,
      ... other columns ignored here ...
  )

  playables(
      event_id      TEXT,
      provider_code TEXT,
      ... other columns ignored here ...
  )

If your events/playables schema differs, the script will log a clear error and exit;
you can then adjust the SQL queries near the top of build_adb_lanes().
"""

import argparse
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "fruit_events.db"



# Optional: integrate user filter preferences (user_preferences table)
try:
    from filter_integration import load_user_preferences, should_include_event
    FILTERS_AVAILABLE = True
except Exception:
    load_user_preferences = None
    should_include_event = None
    FILTERS_AVAILABLE = False

def get_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger("fruit_build_adb_lanes")


def table_has_columns(conn: sqlite3.Connection, table: str, required: List[str]) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    cols = {row[1] for row in cur.fetchall()}
    missing = [c for c in required if c not in cols]
    return not missing


def load_enabled_providers(conn: sqlite3.Connection, log: logging.Logger) -> Dict[str, int]:
    """
    Return mapping provider_code -> adb_lane_count for enabled providers.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT provider_code, adb_lane_count
          FROM provider_lanes
         WHERE adb_enabled = 1
           AND adb_lane_count > 0
         ORDER BY provider_code;
        """
    )
    rows = cur.fetchall()
    providers = {code: int(count) for (code, count) in rows}
    if not providers:
        log.warning("No ADB-enabled providers found in provider_lanes (adb_enabled=1, adb_lane_count>0).")
    else:
        log.info(
            "Loaded %d ADB-enabled provider(s): %s",
            len(providers),
            ", ".join(f"{code} ({count} lanes)" for code, count in providers.items()),
        )
    return providers


def load_events_for_provider(
    conn: sqlite3.Connection,
    provider_code: str,
    log: logging.Logger,
) -> List[Tuple[str, str, str]]:
    """
    Load events (event_id, start_utc, stop_utc) for a given provider_code.

    We try to be flexible about actual column names. For events we will look for:
      - primary key: one of ["id", "event_id"]
      - start time: one of ["start_utc", "start_time_utc", "start_time", "start"]
      - stop  time: one of ["stop_utc", "end_utc", "end_time_utc", "end_time", "stop"]

    For playables we will look for:
      - event FK: "event_id" or the chosen events PK column
      - provider: "provider_code" or "provider"

    If we cannot resolve an unambiguous mapping, we raise a RuntimeError with details.
    """
    # Introspect events schema
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(events);")
    event_cols = {row[1] for row in cur.fetchall()}
    if not event_cols:
        raise RuntimeError("Table 'events' not found; adjust load_events_for_provider() to your schema.")

    # Decide event PK column
    pk_candidates = ["id", "event_id"]
    event_pk_col = next((c for c in pk_candidates if c in event_cols), None)
    if not event_pk_col:
        raise RuntimeError(
            f"Could not find an event primary key column in events table; "
            f"looked for {pk_candidates}, found {sorted(event_cols)}"
        )

    # Decide start/stop time columns
    start_candidates = ["start_utc", "start_time_utc", "start_time", "start"]
    stop_candidates = ["stop_utc", "end_utc", "end_time_utc", "end_time", "stop"]

    start_col = next((c for c in start_candidates if c in event_cols), None)
    stop_col = next((c for c in stop_candidates if c in event_cols), None)

    if not start_col or not stop_col:
        raise RuntimeError(
            "Could not find start/stop time columns on events table. "
            f"Have columns: {sorted(event_cols)}; looked for start in {start_candidates}, stop in {stop_candidates}"
        )

    # Introspect playables schema
    cur.execute("PRAGMA table_info(playables);")
    playable_cols = {row[1] for row in cur.fetchall()}
    if not playable_cols:
        raise RuntimeError("Table 'playables' not found; adjust load_events_for_provider() to your schema.")

    # Provider column on playables
    if "provider_code" in playable_cols:
        provider_col = "provider_code"
    elif "provider" in playable_cols:
        provider_col = "provider"
    else:
        raise RuntimeError(
            f"Expected 'provider_code' or 'provider' column on playables table; have columns {sorted(playable_cols)}"
        )

    # Event FK column on playables
    if "event_id" in playable_cols:
        playable_event_fk = "event_id"
    elif event_pk_col in playable_cols:
        playable_event_fk = event_pk_col
    else:
        raise RuntimeError(
            "Could not find event foreign key on playables table. "
            f"Expected 'event_id' or '{event_pk_col}' in columns {sorted(playable_cols)}"
        )

    # Build and execute query dynamically
    sql = f"""
        SELECT e.{event_pk_col} AS event_id, e.{start_col} AS start_utc, e.{stop_col} AS stop_utc
          FROM events e
          JOIN playables p
            ON p.{playable_event_fk} = e.{event_pk_col}
         WHERE p.{provider_col} = ?
         GROUP BY e.{event_pk_col}, e.{start_col}, e.{stop_col}
         ORDER BY e.{start_col};
    """
    cur.execute(sql, (provider_code,))
    rows = [(r[0], r[1], r[2]) for r in cur.fetchall()]
    log.info(
        "Provider %s: loaded %d events from events/playables "
        "(events.%s, events.%s, events.%s, playables.%s, playables.%s).",
        provider_code,
        len(rows),
        event_pk_col,
        start_col,
        stop_col,
        playable_event_fk,
        provider_col,
    )
    return rows


def clear_adb_lanes(conn: sqlite3.Connection, log: logging.Logger, provider_code: Optional[str] = None) -> None:
    cur = conn.cursor()
    if provider_code is None:
        log.info("Clearing ALL rows from adb_lanes ...")
        cur.execute("DELETE FROM adb_lanes;")
    else:
        log.info("Clearing existing adb_lanes rows for provider %s ...", provider_code)
        cur.execute("DELETE FROM adb_lanes WHERE provider_code = ?;", (provider_code,))
    conn.commit()




def build_lanes_for_provider(
    conn: sqlite3.Connection,
    provider_code: str,
    lane_count: int,
    log: logging.Logger,
    prefs: Optional[dict] = None,
) -> int:
    """Build ADB lanes for a single provider with *non-overlapping* lanes.

    Strategy:

      - Load all events for this provider (event_id, start_utc, stop_utc).
      - Sort by (start time, stop time).
      - Maintain an "available from" end-time cursor per lane.
      - For each event, try to place it into the first lane whose end time
        is <= event.start. If none exist (concurrency > lane_count), the
        event is *skipped* for ADB lanes and logged as a dropped event.

    This guarantees that, for each (provider_code, lane_number), there are
    no overlapping intervals in adb_lanes. If you see many dropped events
    in the logs, increase adb_lane_count for that provider.
    """
    if lane_count <= 0:
        log.warning(
            "Provider %s has non-positive lane_count=%d; skipping.",
            provider_code,
            lane_count,
        )
        return 0

    events = load_events_for_provider(conn, provider_code, log)
    if not events:
        log.warning(
            "Provider %s has no events; skipping lane assignment.",
            provider_code,
        )
        return 0

    # Filter out events with null timestamps
    valid_events = [
        (event_id, start_utc, stop_utc)
        for (event_id, start_utc, stop_utc) in events
        if start_utc is not None and stop_utc is not None
    ]
    
    if len(valid_events) < len(events):
        null_count = len(events) - len(valid_events)
        log.warning(
            "Provider %s: filtered out %d events with null timestamps (keeping %d valid events)",
            provider_code,
            null_count,
            len(valid_events),
        )
    
    if not valid_events:
        log.warning(
            "Provider %s has no events with valid timestamps; skipping lane assignment.",
            provider_code,
        )
        return 0

    # Local ISO8601 parser that can handle the typical Apple-style timestamps.
    def _parse_iso(ts: str) -> datetime:
        """Parse ISO8601 into a *naive* datetime (UTC-like).

        We always strip timezone info so that all comparisons are between
        offset-naive datetimes. The source timestamps are UTC-ish
        (e.g. 2025-12-06T00:00:00+00:00), so dropping tzinfo is fine.
        """
        # Handle None/null timestamps
        if ts is None or not isinstance(ts, str):
            log.warning(
                "Provider %s: encountered null/invalid timestamp (type=%s), treating as datetime.min",
                provider_code,
                type(ts).__name__,
            )
            return datetime.min
        
        try:
            dt = datetime.fromisoformat(ts)
        except Exception:
            ts2 = ts.replace("Z", "")
            if "+" in ts2:
                ts2 = ts2.split("+", 1)[0]
            try:
                dt = datetime.fromisoformat(ts2)
            except Exception:
                return datetime.min

        # Force naive so we never mix aware/naive in comparisons.
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt

    # Attach parsed datetimes and sort.
    # Apply user content filters (disabled sports/leagues) for ADB lanes, if available.
    if prefs and FILTERS_AVAILABLE and should_include_event:
        disabled_sports = prefs.get("disabled_sports") or []
        disabled_leagues = prefs.get("disabled_leagues") or []
        if disabled_sports or disabled_leagues:
            # Figure out events PK column for lookups (usually 'id')
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(events)")
            ev_cols = cur.fetchall()
            pk_col = None
            for col in ev_cols:
                # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
                if len(col) >= 6 and col[5] == 1:
                    pk_col = col[1]
                    break
            if not pk_col:
                pk_col = "id"
    
            col_names = {c[1] for c in ev_cols}
            can_filter = ("genres_json" in col_names) or ("classification_json" in col_names)
    
            if can_filter:
                before_n = len(valid_events)
                filtered = []
                for (eid, s, t) in valid_events:
                    try:
                        cur.execute(
                            f"SELECT genres_json, classification_json FROM events WHERE {pk_col} = ? LIMIT 1",
                            (eid,),
                        )
                        row = cur.fetchone()
                        ev = {"genres_json": None, "classification_json": None}
                        if row:
                            ev["genres_json"] = row[0]
                            ev["classification_json"] = row[1]
                        if should_include_event(ev, prefs):
                            filtered.append((eid, s, t))
                    except Exception:
                        # Fail-open for any parsing/lookup issues
                        filtered.append((eid, s, t))
    
                valid_events = filtered
                removed = before_n - len(valid_events)
                if removed:
                    log.info(
                        "Provider %s: filtered out %d events due to disabled_sports/leagues.",
                        provider_code,
                        removed,
                    )

    events_with_dt = [
        (event_id, start_utc, stop_utc, _parse_iso(start_utc), _parse_iso(stop_utc))
        for (event_id, start_utc, stop_utc) in valid_events
    ]
    events_with_dt.sort(key=lambda row: (row[3], row[4]))  # (start_dt, stop_dt)

    clear_adb_lanes(conn, log, provider_code=provider_code)

    cur = conn.cursor()

    # lane_end_times[i] = datetime when lane i+1 becomes free.
    lane_end_times = [datetime.min for _ in range(lane_count)]

    inserted = 0
    dropped = 0

    for event_id, start_utc, stop_utc, start_dt, stop_dt in events_with_dt:
        # Find the first lane that is free by this event's start.
        chosen_index = None
        for i in range(lane_count):
            if lane_end_times[i] <= start_dt:
                chosen_index = i
                break

        if chosen_index is None:
            # All lanes are still busy; skip this event for ADB view.
            dropped += 1
            continue

        lane_number = chosen_index + 1
        channel_id = f"{provider_code}{lane_number:02d}"

        cur.execute(
            """
            INSERT INTO adb_lanes (provider_code, lane_number, channel_id, event_id, start_utc, stop_utc)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (provider_code, lane_number, channel_id, event_id, start_utc, stop_utc),
        )
        inserted += 1

        # Advance that lane's cursor.
        if stop_dt <= start_dt:
            lane_end_times[chosen_index] = start_dt
        else:
            lane_end_times[chosen_index] = stop_dt

    conn.commit()
    log.info(
        "Provider %s: assigned %d events into %d lane(s) (adb_lanes); dropped %d events due to concurrency > %d lanes.",
        provider_code,
        inserted,
        lane_count,
        dropped,
        lane_count,
    )
    return inserted



def build_adb_lanes(db_path: Path, provider_filter: Optional[str] = None) -> None:
    log = get_logger()
    log.info("Using database: %s", db_path)

    if not db_path.exists():
        log.error(
            "Database file does not exist at %s. Run your ingest/refresh pipeline first.",
            db_path,
        )
        raise SystemExit(1)

    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        # Load user preferences (filters) if available.
        prefs = None
        enabled_services = []
        if FILTERS_AVAILABLE and load_user_preferences:
            try:
                prefs = load_user_preferences(conn)
                enabled_services = prefs.get("enabled_services") or []
                log.info(
                    "ADB filters loaded: enabled_services=%d disabled_sports=%d disabled_leagues=%d",
                    len(enabled_services),
                    len(prefs.get("disabled_sports") or []),
                    len(prefs.get("disabled_leagues") or []),
                )
            except Exception as e:
                log.warning(
                    "Could not load user preferences for ADB lanes; proceeding without filters: %s",
                    e,
                )
                prefs = None
                enabled_services = []


        # Sanity-check adb_lanes table exists
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='adb_lanes';"
        )
        if not cur.fetchone():
            raise RuntimeError(
                "Table adb_lanes does not exist. Run migrate_add_adb_lanes.py first."
            )

        providers = load_enabled_providers(conn, log)
        if provider_filter:
            providers = {
                code: count
                for code, count in providers.items()
                if code == provider_filter
            }
            if not providers:
                log.warning(
                    "Provider filter %s did not match any enabled providers; nothing to do.",
                    provider_filter,
                )
                return

        if not providers:
            log.warning("No ADB-enabled providers to build lanes for; exiting.")
            return

        # If building all providers, clear all existing adb_lanes rows first.
        if provider_filter is None:
            clear_adb_lanes(conn, log, provider_code=None)

        total_inserted = 0
        for code, lane_count in providers.items():
            # Always clear existing rows for this provider so config/filter changes take effect.
            clear_adb_lanes(conn, log, provider_code=code)
        
            # If the user has explicitly limited enabled_services, respect it for ADB lanes too:
            # providers not in enabled_services are treated as "disabled" for ADB outputs.
            if enabled_services and code not in enabled_services:
                log.info(
                    "Skipping provider %s because it is not in enabled_services filters.",
                    code,
                )
                continue
        
            inserted = build_lanes_for_provider(conn, code, lane_count, log, prefs=prefs)
            total_inserted += inserted


        log.info(
            "ADB lane build complete. Total adb_lanes rows inserted: %d",
            total_inserted,
        )
    finally:
        conn.close()


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build provider-based ADB lanes into adb_lanes table."
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite DB (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--provider",
        dest="provider_filter",
        default=None,
        help="Optional provider_code to restrict lane build (default: all enabled providers)",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    build_adb_lanes(args.db_path, provider_filter=args.provider_filter)
