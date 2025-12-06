#!/usr/bin/env python3
"""debug_missing_deeplinks.py

Inspect direct (N-hour) events the same way peacock_export_hybrid's Direct M3U
exporter does, but instead of writing an M3U, emit a JSON report of the
events that *failed* to get a suitable deeplink.

Usage (inside container):

  python /app/bin/debug_missing_deeplinks.py \
      --db /app/data/fruit_events.db \
      --out /app/out/missing_direct_deeplinks.json

Defaults:
  - DB:  /app/data/fruit_events.db   (or $FRUIT_DB_PATH)
  - Out: /app/out/missing_direct_deeplinks.json
  - Window: 24 hours
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Debug direct events that had no suitable deeplink in Direct M3U"
    )
    parser.add_argument(
        "--db",
        default=os.getenv("FRUIT_DB_PATH", "/app/data/fruit_events.db"),
        help="Path to fruit_events.db (default: /app/data/fruit_events.db)",
    )
    parser.add_argument(
        "--hours-window",
        type=int,
        default=24,
        help="Hours window for direct events (same as M3U exporter, default=24)",
    )
    parser.add_argument(
        "--out",
        default="/app/out/missing_direct_deeplinks.json",
        help=(
            "Output JSON file for missing deeplink report "
            "(default: /app/out/missing_direct_deeplinks.json)"
        ),
    )
    parser.add_argument(
        "--no-filters",
        action="store_true",
        help="Ignore user filters and inspect all direct events (apply_filters=False)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional limit on number of missing events to record (0 = no limit)",
    )
    return parser.parse_args()


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def main() -> int:
    args = get_args()
    db_path = args.db
    apply_filters = not args.no_filters
    out_path = Path(args.out)

    print("=" * 70)
    print("Debugging Direct M3U deeplink *skips*")
    print(f"DB:      {db_path}")
    print(f"Out:     {out_path}")
    print(f"Window:  {args.hours_window}h")
    print(f"Filters: {'ENABLED' if apply_filters else 'DISABLED (no-filters)'}")
    print("=" * 70)

    # Ensure we can import peacock_export_hybrid from the same dir as this script
    bin_dir = Path(__file__).parent
    if str(bin_dir) not in sys.path:
        sys.path.insert(0, str(bin_dir))

    try:
        import peacock_export_hybrid as pe
    except ImportError as e:
        print(f"ERROR: Could not import peacock_export_hybrid: {e}")
        return 1

    conn = connect_db(db_path)

    # Pull events using the *same* helper as the real exporter
    events = pe.get_direct_events(
        conn, hours_window=args.hours_window, apply_filters=apply_filters
    )
    total_events = len(events)
    print(f"Direct events in window (post-filter): {total_events}")

    # Load user preferences / enabled services the same way the exporter does
    if getattr(pe, "FILTERING_AVAILABLE", False):
        prefs = pe.load_user_preferences(conn)
        enabled_services = prefs.get("enabled_services", [])
        filter_flag = "ENABLED"
    else:
        prefs = {"enabled_services": [], "disabled_sports": [], "disabled_leagues": []}
        enabled_services = []
        filter_flag = "DISABLED (filter_integration missing)"

    print(f"Filtering module: {filter_flag}")
    print(f"Enabled services (for best_deeplink): {enabled_services or 'ALL'}")

    cur = conn.cursor()
    missing: List[Dict[str, Any]] = []
    have_deeplink = 0

    for idx, event in enumerate(events, start=1):
        event = dict(event)  # plain dict for JSON
        event_id = str(event.get("id", ""))
        pvid = event.get("pvid")

        if not pvid:
            # Matches Direct M3U logic: skip if no pvid
            continue

        steps: List[str] = []
        deeplink_url: Optional[str] = None

        # --- Step 1: best playable based on user services (same as Direct M3U)
        if getattr(pe, "FILTERING_AVAILABLE", False):
            dl1 = pe.get_best_deeplink_for_event(conn, event_id, enabled_services)
            if dl1:
                deeplink_url = dl1
                steps.append("best_deeplink:OK")
            else:
                steps.append("best_deeplink:None")

            if not deeplink_url:
                dl2 = pe.get_fallback_deeplink(event)
                if dl2:
                    deeplink_url = dl2
                    steps.append("fallback_raw_attributes:OK")
                else:
                    steps.append("fallback_raw_attributes:None")
        else:
            steps.append("filtering_disabled")

        # --- Step 2: Peacock PVID deeplink (non-Apple events)
        if not deeplink_url and not event_id.startswith("appletv-"):
            payload = {"pvid": pvid, "type": "PROGRAMME", "action": "PLAY"}
            import urllib.parse
            import json as _json

            deeplink_url = (
                "https://www.peacocktv.com/deeplink?deeplinkData="
                + urllib.parse.quote(
                    _json.dumps(
                        payload,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                    safe="",
                )
            )
            steps.append("peacock_pvid_fallback:OK")

        # --- Step 3: Apple / web playable_url fallback (playables table)
        if not deeplink_url:
            try:
                cur.execute(
                    """
                    SELECT provider, playable_url, deeplink_play, deeplink_open
                    FROM playables
                    WHERE event_id = ? AND playable_url IS NOT NULL
                    ORDER BY priority ASC
                    """,
                    (event_id,),
                )
                rows = cur.fetchall()
            except sqlite3.Error as e:
                steps.append(f"playables_query_error:{e}")
                rows = []

            if rows:
                # Matches Direct M3U logic: first playable_url wins
                deeplink_url = rows[0]["playable_url"]
                steps.append(f"playables_fallback:OK ({len(rows)} rows)")
            else:
                steps.append("playables_fallback:None")

        # If we found a deeplink, this event would *not* be skipped for M3U
        if deeplink_url:
            have_deeplink += 1
            continue

        # ------------------------------------------------------------------
        # At this point, this event is exactly one of the
        # "Skipped N events with no suitable deeplinks" from the M3U exporter
        # ------------------------------------------------------------------
        record: Dict[str, Any] = {
            "event_id": event_id,
            "pvid": pvid,
            "title": event.get("title"),
            "channel_name": event.get("channel_name"),
            "start_utc": event.get("start_utc"),
            "end_utc": event.get("end_utc"),
            "steps": steps,
        }

        # Parse genres / classifications for context
        def _safe_json_load(val: Optional[str]):
            if not val:
                return None
            try:
                return json.loads(val)
            except Exception:
                return None

        record["genres"] = _safe_json_load(event.get("genres_json"))
        record["classifications"] = _safe_json_load(event.get("classification_json"))

        # Summarize playables for this event
        try:
            cur.execute(
                """
                SELECT provider, playable_url, deeplink_play, deeplink_open
                FROM playables
                WHERE event_id = ?
                """,
                (event_id,),
            )
            p_rows = cur.fetchall()
        except sqlite3.Error as e:
            p_rows = []
            record["playables_error"] = str(e)

        record["playables_count"] = len(p_rows)

        if p_rows:
            # Derive logical services for each playable (apple_mls, peacock_web, etc.)
            logical_summary: Dict[str, int] = {}
            playable_samples: List[Dict[str, Any]] = []

            try:
                from logical_service_mapper import (
                    get_logical_service_for_playable,
                    get_service_display_name,
                )
            except ImportError:
                get_logical_service_for_playable = None
                get_service_display_name = None

            for r in p_rows:
                pd: Dict[str, Any] = {
                    "provider": r["provider"],
                    "playable_url": r["playable_url"],
                    "deeplink_play": r["deeplink_play"],
                    "deeplink_open": r["deeplink_open"],
                }

                if get_logical_service_for_playable is not None:
                    try:
                        logical_code = get_logical_service_for_playable(
                            provider=r["provider"],
                            deeplink_play=r["deeplink_play"],
                            deeplink_open=r["deeplink_open"],
                            playable_url=r["playable_url"],
                            event_id=event_id,
                            conn=conn,
                        )
                        pd["logical_service"] = logical_code
                        if get_service_display_name:
                            pd["logical_service_name"] = get_service_display_name(
                                logical_code
                            )
                        logical_summary[logical_code] = (
                            logical_summary.get(logical_code, 0) + 1
                        )
                    except Exception as e:
                        pd["logical_service_error"] = str(e)

                playable_samples.append(pd)

            record["playables"] = playable_samples
            record["playables_logical_summary"] = logical_summary

        # raw_attributes_json quick peek (helps debug fallback_raw_attributes)
        raw_attrs = event.get("raw_attributes_json")
        if raw_attrs:
            record["raw_attributes_has_url"] = (
                "http" in raw_attrs or "videos://" in raw_attrs
            )
            record["raw_attributes_snippet"] = raw_attrs[:512]

        missing.append(record)

        if args.limit and len(missing) >= args.limit:
            print(f"Hit --limit={args.limit}, stopping collection early…")
            break

    total_missing = len(missing)
    print("-" * 70)
    print(f"Total direct events considered: {total_events}")
    print(f"Events with deeplink:          {have_deeplink}")
    print(f"Events missing deeplink:       {total_missing}")
    print("-" * 70)

    # Ensure output directory exists
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": db_path,
        "hours_window": args.hours_window,
        "apply_filters": apply_filters,
        "enabled_services": enabled_services,
        "total_events": total_events,
        "have_deeplink": have_deeplink,
        "missing_count": total_missing,
        "missing_events": missing,
    }
    out_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Wrote missing deeplink report to: {out_path}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

