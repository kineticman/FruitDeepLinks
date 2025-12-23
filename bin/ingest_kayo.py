#!/usr/bin/env python3
"""ingest_kayo.py

Ingest adapter for Kayo Sports feed.

Loads kayo_raw.json and upserts into fruit_events.db

Usage:
    python /app/bin/ingest_kayo.py \
        --db /app/data/fruit_events.db \
        --kayo-json /app/out/kayo_raw.json
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

# Import logical service mapper
try:
    from logical_service_mapper import get_logical_service_for_playable
    LOGICAL_SERVICE_AVAILABLE = True
except ImportError:
    LOGICAL_SERVICE_AVAILABLE = False
    print("[KAYO] Warning: logical_service_mapper not available, logical_service will be NULL")


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Kayo feed into FruitDeepLinks DB")
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Path to fruit_events.db",
    )
    parser.add_argument(
        "--kayo-json",
        type=Path,
        required=True,
        help="Path to kayo_raw.json produced by kayo_scrape.py",
    )
    return parser.parse_args()


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def ensure_columns(conn: sqlite3.Connection) -> None:
    """Sanity-check that required columns exist."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(events)")
    cols = {row["name"] for row in cur.fetchall()}
    required = {"id", "title", "channel_provider_id", "genres_json", "start_utc", "end_utc"}
    missing = required - cols
    if missing:
        raise RuntimeError(f"events table is missing required columns: {sorted(missing)}")

    cur.execute("PRAGMA table_info(playables)")
    pcols = {row["name"] for row in cur.fetchall()}
    preq = {"event_id", "playable_id", "provider", "priority", "created_utc"}
    pmissing = preq - pcols
    if pmissing:
        raise RuntimeError(f"playables table is missing required columns: {sorted(pmissing)}")


def normalize_kayo_event(raw_event: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Convert one Kayo event JSON dict into (event_row, playable_rows).
    
    Maps to existing FruitDeepLinks schema:
- channel_name = "Kayo Sports" (human-friendly network label)
- channel_provider_id = Kayo linear provider code when available (e.g., "fsa505")
- genres_json = ["Sport Name"]
- classification_json includes sport + league for quick inspection
    """
    external_id = raw_event.get("external_id")
    if not external_id:
        raise ValueError("Kayo event missing external_id")

    event_id = f"kayo-{external_id}"
    title = raw_event.get("title") or external_id
    sport_raw = raw_event.get("sport") or "Sports"
    league = raw_event.get("league") or sport_raw
    # Pull additional metadata from the stored raw Kayo content payload (if present)
    raw_payload = raw_event.get("raw") or {}
    raw_data = raw_payload.get("data") or {}
    clickthrough = raw_data.get("clickthrough") or {}
    content_display = raw_data.get("contentDisplay") or {}

    # Kayo often has a "linear provider" / channel code like "fsa505"
    channel_code = raw_event.get("channel_code") or clickthrough.get("channel") or content_display.get("linearProvider")

    # Prefer a league/competition label when available (e.g., "Super Smash")
    # NOTE: This is NOT used as channel_name; it is stored in classification_json for debugging.
    info_line = content_display.get("infoLine") or []
    series_name = None
    for item in info_line:
        if isinstance(item, dict) and item.get("type") == "series":
            series_name = item.get("value")
            break
    league_best = league
    if series_name:
        league_best = series_name
    elif content_display.get("header"):
        league_best = str(content_display.get("header"))

    # Pick a "genre" bucket used by the UI's Sports filter (genres_json).
    # Kayo sometimes reports sportName="Other Sport" even when a more specific series/header exists
    # (e.g., "Ultimate Pool"). In that case, using the series/header yields a nicer filter bucket.
    genre_sport = sport_raw
    if isinstance(sport_raw, str) and sport_raw.strip().lower() in ("other sport", "othersport"):
        if isinstance(league_best, str) and league_best.strip() and league_best.strip().lower() != sport_raw.strip().lower():
            genre_sport = league_best.strip()
    start_utc = raw_event.get("start_utc")
    end_utc = raw_event.get("end_utc")
    venue = raw_event.get("venue")
    hero_image = raw_event.get("hero_image")
    
    # Calculate runtime in seconds if we have both start and end
    runtime_secs = None
    start_ms = None
    end_ms = None
    if start_utc and end_utc:
        try:
            start_dt = datetime.fromisoformat(start_utc.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_utc.replace('Z', '+00:00'))
            runtime_secs = int((end_dt - start_dt).total_seconds())
            start_ms = int(start_dt.timestamp() * 1000)
            end_ms = int(end_dt.timestamp() * 1000)
        except (ValueError, AttributeError):
            pass

    # Map to existing schema
    event_row: Dict[str, Any] = {
        "id": event_id,
        "pvid": external_id,  # Use asset ID as provider ID
        "slug": None,
        "title": title,
        "title_brief": None,
        "synopsis": (content_display.get("synopsis") if isinstance(content_display, dict) else None),
        "synopsis_brief": None,
        "channel_name": "Kayo Sports",  # Human-friendly network label (avoid sport/league here)
        "channel_provider_id": (channel_code or "kayo"),  # Kayo linear provider code when available
        "airing_type": None,
        "classification_json": json.dumps([
            {"type": "sport", "value": sport_raw},
            {"type": "league", "value": league_best},
        ], ensure_ascii=False),
        "genres_json": json.dumps([genre_sport], ensure_ascii=False),  # UI Sports filter bucket
        "content_segments_json": None,
        "is_free": 0,
        "is_premium": 1,  # Kayo is a paid service
        "runtime_secs": runtime_secs,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "start_utc": start_utc,
        "end_utc": end_utc,
        "created_ms": None,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "hero_image_url": hero_image,
        "last_seen_utc": datetime.now(timezone.utc).isoformat(),
        "raw_attributes_json": json.dumps(raw_event.get("raw") or {}, ensure_ascii=False),
    }

    playable_rows: List[Dict[str, Any]] = []
    for idx, p in enumerate(raw_event.get("playables") or []):
        playable_id = p.get("playable_id") or f"{event_id}-playable-{idx}"
        provider = p.get("provider") or "kayo"
        deeplink_play = p.get("deeplink_play")
        deeplink_open = p.get("deeplink_open")
        playable_url = p.get("playable_url")
        
        # Determine logical_service using mapper
        logical_service = None
        if LOGICAL_SERVICE_AVAILABLE:
            try:
                logical_service = get_logical_service_for_playable(
                    provider=provider,
                    deeplink_play=deeplink_play,
                    deeplink_open=deeplink_open,
                    playable_url=playable_url,
                    event_id=event_id,
                    conn=None  # Don't need conn for Kayo
                )
            except Exception as e:
                print(f"[KAYO] Warning: Could not map logical_service for {playable_id}: {e}")
                # Fallback: Kayo provider maps to kayo_web
                logical_service = "kayo_web" if provider == "kayo" else provider
        else:
            # Fallback: Kayo provider maps to kayo_web
            logical_service = "kayo_web" if provider == "kayo" else provider
        
        playable_rows.append(
            {
                "event_id": event_id,
                "playable_id": playable_id,
                "provider": provider,
                "logical_service": logical_service,
                "playable_url": playable_url,
                "deeplink_play": deeplink_play,
                "deeplink_open": deeplink_open,
                "priority": p.get("priority", 10),
                "created_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

    return event_row, playable_rows


def upsert_event(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    cur = conn.cursor()
    columns = list(row.keys())
    placeholders = ", ".join(["?"] * len(columns))
    updates = ", ".join([f"{c}=excluded.{c}" for c in columns if c != "id"])
    sql = f"""
        INSERT INTO events ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET
        {updates}
    """
    cur.execute(sql, [row[c] for c in columns])


def upsert_playables(conn: sqlite3.Connection, rows: Iterable[Dict[str, Any]]) -> None:
    cur = conn.cursor()
    for row in rows:
        # Check if playable already exists
        cur.execute("SELECT 1 FROM playables WHERE playable_id = ?", (row["playable_id"],))
        exists = cur.fetchone() is not None
        
        if exists:
            # Update existing
            columns = [c for c in row.keys() if c != "playable_id"]
            updates = ", ".join([f"{c}=?" for c in columns])
            sql = f"UPDATE playables SET {updates} WHERE playable_id = ?"
            cur.execute(sql, [row[c] for c in columns] + [row["playable_id"]])
        else:
            # Insert new
            columns = list(row.keys())
            placeholders = ", ".join(["?"] * len(columns))
            sql = f"INSERT INTO playables ({', '.join(columns)}) VALUES ({placeholders})"
            cur.execute(sql, [row[c] for c in columns])


def ingest_kayo_events(conn: sqlite3.Connection, path: Path) -> int:
    if not path.exists():
        print(f"[KAYO] No file found at {path}, skipping.")
        return 0

    data = json.loads(path.read_text(encoding="utf-8"))
    events = data.get("events") or []
    # Kayo sometimes returns duplicate external_id rows (often identical copies).
    # Since our DB primary key is based on external_id, dedupe here so "last one wins"
    # doesn't produce confusing counts.
    def _to_int(v) -> int:
        try:
            return int(v)
        except Exception:
            return 0

    def _score(ev: dict) -> tuple[int, int, int]:
        # Prefer freshest/most relevant record if duplicates differ.
        return (
            _to_int(ev.get("updated_ms") or ev.get("updatedMs") or ev.get("updated_at_ms")),
            _to_int(ev.get("start_ms") or ev.get("startMs")),
            _to_int(ev.get("end_ms") or ev.get("endMs")),
        )

    best_by_id: dict[str, dict] = {}
    dup_counts: dict[str, int] = {}
    missing_id = 0

    for ev in events:
        if not isinstance(ev, dict):
            continue
        eid = ev.get("external_id")
        if eid is None or eid == "":
            missing_id += 1
            continue
        k = str(eid)
        dup_counts[k] = dup_counts.get(k, 0) + 1
        if k not in best_by_id or _score(ev) > _score(best_by_id[k]):
            best_by_id[k] = ev

    if best_by_id:
        deduped = list(best_by_id.values())
        collapsed = len(events) - len(deduped)
        if collapsed > 0:
            dup_id_count = sum(1 for _, n in dup_counts.items() if n > 1)
            top = sorted(((k, n) for k, n in dup_counts.items() if n > 1), key=lambda x: x[1], reverse=True)[:10]
            print(f"[KAYO] Deduped events by external_id: {len(events)} -> {len(deduped)} (collapsed {collapsed}; dup_ids={dup_id_count})")
            if top:
                print(f"[KAYO] Top duplicate external_id counts: {top}")
        if missing_id:
            print(f"[KAYO] Skipped {missing_id} event(s) missing external_id during dedupe.")
        events = deduped
    if not events:
        print(f"[KAYO] File at {path} has no events[]")
        return 0

    print(f"[KAYO] Ingesting {len(events)} events from {path}")
    inserted = 0
    for raw_event in events:
        try:
            event_row, playable_rows = normalize_kayo_event(raw_event)
            upsert_event(conn, event_row)
            upsert_playables(conn, playable_rows)
            inserted += 1
        except Exception as e:
            print(f"[KAYO] Error processing event: {e}")
            continue

    conn.commit()
    print(f"[KAYO] Upserted {inserted} events into DB.")
    return inserted


def main() -> int:
    args = get_args()
    conn = connect_db(args.db)
    ensure_columns(conn)

    total_inserted = ingest_kayo_events(conn, args.kayo_json)

    if total_inserted == 0:
        print("No Kayo events ingested.")
    else:
        print(f"Total Kayo events ingested: {total_inserted}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
