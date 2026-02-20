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

Fallback: if the broadcast GTI is stale/empty in amazon_channels, fall back to the
content GTI (gti= param). This handles cases where Amazon 404s the broadcast GTI page
but the content GTI has valid channel metadata (e.g., Tennis Channel per-match feeds).

This script:
1) Extracts broadcast + content GTIs from playables.deeplink_play / deeplink_open for provider='aiv'
2) Looks up amazon_channels rows, preferring non-stale broadcast GTI, falling back to content GTI
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
from typing import Dict, List, Optional, Tuple


BROADCAST_RX = re.compile(r"broadcast=(amzn1\.dv\.gti\.[^&\s]+)", re.IGNORECASE)
CONTENT_GTI_RX = re.compile(r"[?&]gti=(amzn1\.dv\.gti\.[a-f0-9-]{36})", re.IGNORECASE)
GTI_RX = re.compile(r"(amzn1\.dv\.gti\.[a-f0-9-]{36})", re.IGNORECASE)


def utcnow_iso() -> str:
    return _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def extract_gtis(deeplink_play: Optional[str], deeplink_open: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract (broadcast_gti, content_gti) from deeplink URLs.

    broadcast_gti: from broadcast= param (most reliable for multi-feed events)
    content_gti:   from gti= param (the content/event GTI)

    Both may be None if not found.
    """
    broadcast_gti: Optional[str] = None
    content_gti: Optional[str] = None

    for s in (deeplink_play, deeplink_open):
        if not s:
            continue
        if not broadcast_gti:
            m = BROADCAST_RX.search(s)
            if m:
                broadcast_gti = m.group(1)
        if not content_gti:
            m = CONTENT_GTI_RX.search(s)
            if m:
                content_gti = m.group(1)

    return broadcast_gti, content_gti


def _has_channel_data(ac: sqlite3.Row) -> bool:
    """Return True if this amazon_channels row has usable channel metadata."""
    return bool((ac["channel_id"] or "").strip() or (ac["channel_name"] or "").strip())


def resolve_channel(
    by_gti: Dict[str, sqlite3.Row],
    broadcast_gti: Optional[str],
    content_gti: Optional[str],
) -> Optional[sqlite3.Row]:
    """
    Resolve the best amazon_channels row for a playable.

    Priority:
      1. Non-stale broadcast GTI with channel data
      2. Non-stale content GTI with channel data
      3. Stale broadcast GTI with channel data (last resort)
      4. Stale content GTI with channel data (last resort)

    Returns None if no usable channel data found in either GTI.
    """
    candidates: List[Tuple[int, int, sqlite3.Row]] = []  # (is_stale, is_content, row)

    for gti, is_content in ((broadcast_gti, 0), (content_gti, 1)):
        if not gti:
            continue
        row = by_gti.get(gti)
        if row and _has_channel_data(row):
            is_stale = int(row["is_stale"] or 0)
            candidates.append((is_stale, is_content, row))

    if not candidates:
        return None

    # Sort: prefer non-stale (is_stale=0) then broadcast (is_content=0)
    candidates.sort(key=lambda x: (x[0], x[1]))
    return candidates[0][2]


def normalize_service(channel_id: Optional[str], channel_name: Optional[str]) -> Optional[str]:
    """
    Convert a raw amazon_channels.channel_id + channel_name into canonical aiv_* logical_service.
    Returns None if unknown (caller should keep as aiv_aggregator).
    """
    cid = (channel_id or "").strip()
    cname = (channel_name or "").strip()

    # If amazon_channels already provides a canonical aiv_* id, trust it (future-proof)
    cid_l = cid.lower()
    if cid_l.startswith("aiv_") and cid_l not in {"aiv_aggregator"}:
        return cid_l

    # Some scrapes yield Amazon internal identifiers like:
    # - amzn1.dv.channel.<uuid>
    # - amzn1.dv.spid.<uuid>
    # In those cases, the channel_name is usually the best signal.
    # Some scrapes yield benefit ids like peacockus, daznus, vixplusus, maxliveeventsus, etc.

    # First: direct known benefit ids / canonical ids (for backward compatibility)
    direct_map = {
        "aiv_nba_league_pass": "aiv_nba_league_pass",
        "aiv_wnba_league_pass": "aiv_wnba_league_pass",
        "aiv_fox_one": "aiv_fox_one",
        "aiv_peacock": "aiv_peacock",
        "aiv_max": "aiv_max",
        "aiv_dazn": "aiv_dazn",
        "aiv_vix_premium": "aiv_vix_premium",
        "aiv_vix_gratis": "aiv_vix",
        "aiv_fanduel": "aiv_fanduel",
        "aiv_willow": "aiv_willow",
        "aiv_prime": "aiv_prime",
        "prime_included": "aiv_prime",
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

    if "nba league pass" in name_l:
        return "aiv_nba_league_pass"
    if "wnba league pass" in name_l:
        return "aiv_wnba_league_pass"
    if "fox one" in name_l:
        return "aiv_fox_one"
    if "peacock" in name_l:
        return "aiv_peacock"
    if name_l == "max" or "max" in name_l:
        if "subscribe for" in name_l and "max" not in name_l:
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
        return "aiv_prime"

    return None


def _table_columns(cur: sqlite3.Cursor, table: str) -> set:
    """Return set of column names for `table`."""
    cur.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    cols = set()
    for r in rows:
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

    # Build mapping gti -> best row (prefer non-stale when there are duplicates)
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
    content_gti_fallbacks = 0

    # Update in a transaction
    conn.execute("BEGIN")

    for r in plays:
        rowid = r["rowid"]
        current_ls = (r["logical_service"] or "").strip() or "aiv_aggregator"

        broadcast_gti, content_gti = extract_gtis(r["deeplink_play"], r["deeplink_open"])

        if not broadcast_gti and not content_gti:
            no_broadcast += 1
            continue

        ac = resolve_channel(by_gti, broadcast_gti, content_gti)

        if ac is None:
            # Check if we at least found the GTI but it had no channel data
            found_any = any(
                gti and gti in by_gti
                for gti in (broadcast_gti, content_gti)
            )
            if found_any:
                unmapped += 1
            else:
                no_match += 1
            continue

        # Track when we had to fall back to content GTI
        if broadcast_gti and ac["gti"] != broadcast_gti:
            content_gti_fallbacks += 1

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
    print(f"Content GTI fallbacks used: {content_gti_fallbacks}")
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
