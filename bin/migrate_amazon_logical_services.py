#!/usr/bin/env python3
"""
migrate_amazon_logical_services.py

Purpose
-------
Normalize Amazon (AIV) playables in fruit_events.db so that playables.logical_service
reflects the correct Amazon sub-channel (e.g., aiv_peacock, aiv_fox_one, aiv_nba_league_pass).

Key idea
--------
The reliable join key is the broadcast GTI embedded in the deeplink:
  broadcast=amzn1.dv.gti.<uuid>
That broadcast GTI maps directly to amazon_channels.gti (populated by the Amazon scraper).

This script:
1) Extracts broadcast GTIs from playables.deeplink_play / deeplink_open for provider='aiv'
2) Looks up amazon_channels rows for those GTIs (preferring non-stale rows)
3) Normalizes amazon_channels.channel_id / channel_name into canonical aiv_* logical services
4) Updates playables.logical_service accordingly; leaves unmapped as aiv_aggregator

Notes
-----
- Does NOT try to infer sports/league (e.g., NHL). It only maps based on Amazon channel metadata.
- Designed to work with a 48-hour scrape window: if a GTI isn't in amazon_channels, it stays aggregator.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import sqlite3
import sys
from typing import Dict, Optional, Tuple


BROADCAST_RX = re.compile(r"broadcast=(amzn1\.dv\.gti\.[^&\s]+)", re.IGNORECASE)


def utcnow_iso() -> str:
    return _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def extract_broadcast_gti(deeplink_play: Optional[str], deeplink_open: Optional[str]) -> Optional[str]:
    for s in (deeplink_play, deeplink_open):
        if not s:
            continue
        m = BROADCAST_RX.search(s)
        if m:
            return m.group(1)
    return None


def normalize_service(channel_id: Optional[str], channel_name: Optional[str]) -> Optional[str]:
    """
    Convert a raw amazon_channels.channel_id + channel_name into canonical aiv_* logical_service.
    Returns None if unknown (caller should keep as aiv_aggregator).
    """
    cid = (channel_id or "").strip()
    cname = (channel_name or "").strip()

    # Some scrapes yield Amazon internal identifiers like:
    # - amzn1.dv.channel.<uuid>
    # - amzn1.dv.spid.<uuid>
    # In those cases, the channel_name is usually the best signal.
    # Some scrapes yield benefit ids like peacockus, daznus, vixplusus, maxliveeventsus, etc.

    # First: direct known benefit ids / canonical ids
    cid_l = cid.lower()

    direct_map = {
        "aiv_nba_league_pass": "aiv_nba_league_pass",
        "aiv_wnba_league_pass": "aiv_wnba_league_pass",
        "aiv_fox_one": "aiv_fox_one",
        "aiv_peacock": "aiv_peacock",
        "aiv_max": "aiv_max",
        "aiv_dazn": "aiv_dazn",
        "aiv_vix_premium": "aiv_vix_premium",
        "aiv_vix_gratis": "aiv_vix",  # if you use aiv_vix lane
        "aiv_fanduel": "aiv_fanduel",
        "aiv_willow": "aiv_willow",
        "aiv_prime": "aiv_prime",
        "prime_included": "aiv_prime",  # normalize
        "aiv_prime_included": "aiv_prime",
        "aiv_prime_free": "aiv_free",
        "aiv_join_prime": "aiv_prime",
        "vixplusus": "aiv_vix_premium",
        "peacockus": "aiv_peacock",
        "daznus": "aiv_dazn",
        "maxliveeventsus": "aiv_max",
    }
    if cid_l in direct_map:
        return direct_map[cid_l]

    # Channel name normalization (covers amzn1.dv.channel.*, amzn1.dv.spid.*, free trials, etc.)
    name_l = cname.lower()

    # Treat "Free trial of X" as X
    if "nba league pass" in name_l:
        return "aiv_nba_league_pass"
    if "wnba league pass" in name_l:
        return "aiv_wnba_league_pass"
    if "fox one" in name_l:
        return "aiv_fox_one"
    if "peacock" in name_l:
        # "Peacock Premium Plus" etc.
        return "aiv_peacock"
    if name_l == "max" or "max" in name_l:
        # handle "Subscribe for $18.49/month" -> this is Max live events benefitId sometimes
        # Prefer Max if channel_id hinted it, otherwise only if name mentions Max
        if "subscribe for" in name_l and "max" not in name_l:
            # this is ambiguous; only map if cid was maxliveeventsus (handled above)
            return None
        return "aiv_max"
    if "dazn" in name_l:
        return "aiv_dazn"
    if "vix premium" in name_l:
        return "aiv_vix_premium"
    if name_l == "vix" or "vix" in name_l:
        return "aiv_vix"
    if "fanduel sports network" in name_l:
        return "aiv_fanduel"
    if "willow" in name_l:
        return "aiv_willow"
    if "prime" in name_l and "join" not in name_l:
        # "Prime Included" / "Prime Exclusive"
        return "aiv_prime"

    return None


def _table_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    """Return set of column names for `table`."""
    cur.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    cols = set()
    for r in rows:
        # row: (cid, name, type, notnull, dflt_value, pk) or sqlite3.Row
        name = r["name"] if isinstance(r, sqlite3.Row) else r[1]
        cols.add(str(name))
    return cols


def ensure_amazon_channels_schema(conn: sqlite3.Connection) -> None:
    """
    Make this script tolerant of older DBs by adding missing columns we rely on.
    Idempotent / safe to run repeatedly.
    """
    cur = conn.cursor()

    cols = _table_columns(cur, "amazon_channels")

    # Older DBs may not have is_stale; default to 0 (not stale).
    if "is_stale" not in cols:
        cur.execute("ALTER TABLE amazon_channels ADD COLUMN is_stale INTEGER DEFAULT 0")

    conn.commit()






def migrate(db_path: str) -> int:
    print("=" * 80)
    print("MIGRATING AMAZON PLAYABLES TO CORRECT LOGICAL SERVICES (broadcast GTI join)")
    print("=" * 80)
    print()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Ensure amazon_channels exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='amazon_channels'")
    if not cur.fetchone():
        raise SystemExit("amazon_channels table not found. Run the Amazon scraper first.")

    # Ensure schema is compatible across mixed-version DBs
    ensure_amazon_channels_schema(conn)

    # Load amazon_channels mapping (prefer non-stale)
    cur.execute("""
        SELECT gti, channel_id, channel_name, is_stale
        FROM amazon_channels
    """)
    ac_rows = cur.fetchall()

    # Build mapping gti -> best row
    by_gti: Dict[str, sqlite3.Row] = {}
    for r in ac_rows:
        gti = (r["gti"] or "").strip()
        if not gti:
            continue
        prev = by_gti.get(gti)
        if prev is None:
            by_gti[gti] = r
            continue
        # Prefer non-stale
        prev_stale = int(prev["is_stale"] or 0)
        r_stale = int(r["is_stale"] or 0)
        if prev_stale == 1 and r_stale == 0:
            by_gti[gti] = r

    # Pull candidate playables
    cur.execute("""
        SELECT rowid, event_id, provider, logical_service, deeplink_play, deeplink_open
        FROM playables
        WHERE provider='aiv'
    """)
    plays = cur.fetchall()
    print(f"Found {len(plays)} Amazon playables")

    updated = 0
    already = 0
    no_broadcast = 0
    no_match = 0
    unmapped = 0

    # Update in a transaction
    now = utcnow_iso()
    conn.execute("BEGIN")

    for r in plays:
        rowid = r["rowid"]
        current_ls = (r["logical_service"] or "").strip() or "aiv_aggregator"
        bgti = extract_broadcast_gti(r["deeplink_play"], r["deeplink_open"])
        if not bgti:
            no_broadcast += 1
            continue

        ac = by_gti.get(bgti)
        if not ac:
            no_match += 1
            continue

        new_ls = normalize_service(ac["channel_id"], ac["channel_name"])
        if not new_ls:
            unmapped += 1
            continue

        if new_ls == current_ls:
            already += 1
            continue

        conn.execute(
            "UPDATE playables SET logical_service=? WHERE rowid=?",
            (new_ls, rowid),
        )
        updated += 1

    conn.commit()

    print()
    print(f"Updated: {updated}")
    print(f"Already correct: {already}")
    print(f"No broadcast GTI: {no_broadcast}")
    print(f"Broadcast GTI not in amazon_channels: {no_match}")
    print(f"Unmapped channel metadata: {unmapped}")
    print()

    # Breakdown after migration
    print("Logical service breakdown after migration:")
    print("-" * 80)
    cur.execute("""
        SELECT COALESCE(NULLIF(TRIM(logical_service),''),'aiv_aggregator') AS ls,
               COUNT(*) AS playables
        FROM playables
        WHERE provider='aiv'
        GROUP BY 1
        ORDER BY playables DESC
    """)
    for row in cur.fetchall():
        print(f"  {row['ls']:<30s} {row['playables']} playables")

    print()
    print("=" * 80)
    print("MIGRATION COMPLETE")
    print("=" * 80)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Migrate Amazon AIV playables to correct logical_service")
    ap.add_argument("db", help="Path to fruit_events.db (e.g. /app/data/fruit_events.db)")
    args = ap.parse_args()
    return migrate(args.db)


if __name__ == "__main__":
    raise SystemExit(main())
