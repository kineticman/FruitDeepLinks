#!/usr/bin/env python3
"""peacock_build_lanes.py - Build virtual lanes from events"""
import os, argparse, json, sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PADDING_MINUTES = int(os.getenv("PEACOCK_PADDING_MINUTES", "45"))
PLACEHOLDER_BLOCK_MINUTES = int(os.getenv("PEACOCK_PLACEHOLDER_BLOCK_MINUTES", "60"))
PLACEHOLDER_EXTRA_DAYS = int(os.getenv("PEACOCK_PLACEHOLDER_EXTRA_DAYS", "5"))
LANE_START_CH_DEFAULT = int(os.getenv("PEACOCK_LANE_START_CH", "9000"))
LANE_COUNT_DEFAULT = int(os.getenv("PEACOCK_LANES", "10"))
FAKE_CHANNELS = {"NBC Sports NOW", "NFL Channel", "Telemundo Deportes Ahora"}

@dataclass
class Event:
    event_id: str
    pvid: Optional[str]
    slug: Optional[str]
    title: str
    channel_name: Optional[str]
    start: datetime
    end_padded: datetime

def ms_to_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)

def derive_times_from_attrs(attrs: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    start_ms, end_ms, runtime_secs = attrs.get("displayStartTime"), attrs.get("displayEndTime"), attrs.get("runtime")
    runtime_seconds = None
    if isinstance(runtime_secs, (int, float)):
        runtime_seconds = int(runtime_secs)
    elif isinstance(runtime_secs, str):
        try:
            parts = runtime_secs.split(":")
            if len(parts) == 3:
                h, m, s = map(int, parts)
                runtime_seconds = h*3600 + m*60 + s
        except:
            pass
    if start_ms and not end_ms and runtime_seconds:
        end_ms = start_ms + runtime_seconds*1000
    formats = attrs.get("formats") or {}
    if isinstance(formats, dict):
        for fmt_data in formats.values():
            avail = (fmt_data or {}).get("availability") or {}
            if not start_ms:
                start_ms = avail.get("offerStartTs")
            if not end_ms and not runtime_seconds:
                end_ms = avail.get("offerEndTs")
    return start_ms, end_ms, runtime_seconds

def load_future_events(conn: sqlite3.Connection, days_ahead: int) -> List[Event]:
    now, cutoff = datetime.now(timezone.utc), datetime.now(timezone.utc) + timedelta(days=days_ahead)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, pvid, slug, title, channel_name, start_utc, end_utc, raw_attributes_json 
        FROM events 
        WHERE pvid IS NOT NULL 
          AND start_utc IS NOT NULL
          AND end_utc IS NOT NULL
    """)
    
    events: List[Event] = []
    for row in cur.fetchall():
        event_id, pvid, slug, title, channel_name, start_utc, end_utc, raw_json = row
        if channel_name in FAKE_CHANNELS:
            continue
        
        # Use the database columns directly (works for both Peacock and Apple TV)
        try:
            start_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00"))
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            else:
                start_dt = start_dt.astimezone(timezone.utc)
            
            end_dt = datetime.fromisoformat(end_utc.replace("Z", "+00:00"))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            else:
                end_dt = end_dt.astimezone(timezone.utc)
        except Exception as e:
            # Fallback: try to parse from raw_attributes_json (Peacock-style)
            try:
                attrs = json.loads(raw_json) if raw_json else {}
            except:
                continue
            
            start_ms, end_ms, runtime_secs = derive_times_from_attrs(attrs)
            if not start_ms:
                continue
            start_dt = ms_to_dt(start_ms)
            
            if end_ms:
                end_dt = ms_to_dt(end_ms)
            else:
                end_dt = start_dt + timedelta(seconds=runtime_secs if runtime_secs else 7200)
        
        # Filter by time window
        if start_dt < now or start_dt > cutoff:
            continue
        
        end_padded = end_dt + timedelta(minutes=PADDING_MINUTES)
        events.append(Event(event_id, pvid, slug, title, channel_name, start_dt, end_padded))
    
    events.sort(key=lambda e: e.start)
    return events

def ensure_lane_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS lanes (
        lane_id INTEGER PRIMARY KEY,
        name TEXT,
        logical_number INTEGER)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS lane_events (
        lane_id INTEGER,
        event_id TEXT,
        is_placeholder INTEGER,
        start_utc TEXT,
        end_utc TEXT,
        title TEXT,
        PRIMARY KEY (lane_id, event_id, start_utc))""")
    conn.commit()

def reset_lanes(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("DELETE FROM lane_events")
    cur.execute("DELETE FROM lanes")
    conn.commit()

def create_lanes(conn: sqlite3.Connection, lane_count: int):
    cur = conn.cursor()
    for lane_id in range(1, lane_count + 1):
        logical_number = LANE_START_CH_DEFAULT + (lane_id - 1)
        cur.execute(
            "INSERT INTO lanes VALUES (?, ?, ?)",
            (lane_id, f"Multi-Source Sports {lane_id}", logical_number),
        )
    conn.commit()

def build_lanes_with_placeholders(conn: sqlite3.Connection, events: List[Event], lane_count: int):
    cur = conn.cursor()
    if not events:
        print("No future events")
        return
    
    now = datetime.now(timezone.utc)
    earliest_start = min(e.start for e in events)
    latest_end = max(e.end_padded for e in events)
    
    now_floored = now.replace(minute=0, second=0, microsecond=0)
    placeholder_start_global = now_floored - timedelta(hours=1)
    earliest_floored = earliest_start.replace(minute=0, second=0, microsecond=0)
    if earliest_floored < placeholder_start_global:
        placeholder_start_global = earliest_floored - timedelta(hours=1)
    
    placeholder_end_global = (latest_end + timedelta(days=PLACEHOLDER_EXTRA_DAYS)).replace(
        minute=0, second=0, microsecond=0
    )
    
    lane_ends = [placeholder_start_global for _ in range(lane_count)]
    lane_events: List[List[Event]] = [[] for _ in range(lane_count)]
    dropped: List[Event] = []
    
    for ev in events:
        placed = False
        for idx in range(lane_count):
            if lane_ends[idx] <= ev.start:
                lane_events[idx].append(ev)
                lane_ends[idx] = ev.end_padded
                placed = True
                break
        if not placed:
            dropped.append(ev)
    
    def add_placeholder(lane_id: int, start: datetime, end: datetime):
        cur.execute(
            "INSERT OR REPLACE INTO lane_events VALUES (?, ?, ?, ?, ?, ?)",
            (
                lane_id,
                f"placeholder-{lane_id}-{start.isoformat()}",
                1,
                start.isoformat(timespec="seconds"),
                end.isoformat(timespec="seconds"),
                "Nothing Scheduled",
            ),
        )
    
    placeholder_count = 0
    for lane_id in range(1, lane_count + 1):
        blocks = lane_events[lane_id - 1]
        current = placeholder_start_global
        for ev in blocks:
            if current < ev.start:
                gap_start = current
                while gap_start < ev.start:
                    gap_end = min(
                        gap_start + timedelta(minutes=PLACEHOLDER_BLOCK_MINUTES),
                        ev.start,
                    )
                    if gap_end > gap_start:
                        add_placeholder(lane_id, gap_start, gap_end)
                        placeholder_count += 1
                    gap_start = gap_end
            cur.execute(
                "INSERT OR REPLACE INTO lane_events VALUES (?, ?, ?, ?, ?, ?)",
                (
                    lane_id,
                    ev.event_id,
                    0,
                    ev.start.isoformat(timespec="seconds"),
                    ev.end_padded.isoformat(timespec="seconds"),
                    ev.title,
                ),
            )
            current = ev.end_padded
        
        while current < placeholder_end_global:
            gap_end = min(
                current + timedelta(minutes=PLACEHOLDER_BLOCK_MINUTES),
                placeholder_end_global,
            )
            if gap_end > current:
                add_placeholder(lane_id, current, gap_end)
                placeholder_count += 1
            current = gap_end
    
    conn.commit()
    print(f"Created {placeholder_count} placeholders")
    print(f"Dropped {len(dropped)} events")

def main():
    script_dir = Path(__file__).resolve().parent
    default_db = str(script_dir.parent / 'data' / 'peacock_events.db') if script_dir.name == 'bin' else "peacock_events.db"

    env_db = os.getenv("PEACOCK_DB_PATH")
    env_lanes = os.getenv("PEACOCK_LANES")
    env_days = os.getenv("PEACOCK_DAYS_AHEAD")

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=env_db or default_db)
    ap.add_argument("--lanes", type=int, default=int(env_lanes) if env_lanes else LANE_COUNT_DEFAULT)
    ap.add_argument("--days-ahead", type=int, default=int(env_days) if env_days else 7)
    args = ap.parse_args()
    conn = sqlite3.connect(args.db)
    ensure_lane_schema(conn)
    reset_lanes(conn)
    create_lanes(conn, args.lanes)
    
    events = load_future_events(conn, args.days_ahead)
    print(f"Loaded {len(events)} future events")
    build_lanes_with_placeholders(conn, events, args.lanes)
    
    conn.close()
    print("Lane planning complete")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
