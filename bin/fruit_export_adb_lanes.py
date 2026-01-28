#!/usr/bin/env python
"""
fruit_export_adb_lanes.py

Export provider-based ADB lanes (adb_lanes table) to a single XMLTV file:

  out/adb_lanes.xml

- One <channel> per (provider_code, lane_number) using channel_id from adb_lanes.
- One <programme> per adb_lanes row, joined to events for title/description.

Assumptions:
  - adb_lanes(provider_code, lane_number, channel_id, event_id, start_utc, stop_utc)
  - events(id, start_utc, end_utc, title, synopsis, title_brief, synopsis_brief, ...)

If your schema differs, adjust the SQL queries below.
"""

import argparse
import logging
import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
from typing import Optional, Dict
import json

# Import shared XMLTV helpers
try:
    from xmltv_helpers import (
        get_provider_display_name,
        add_categories_and_tags,
    )
except ImportError:
    # Fallback if not in path
    def get_provider_display_name(provider_id: str) -> str:
        return provider_id.title() if provider_id else None
    
    def add_categories_and_tags(prog_el, event, provider_name=None, is_placeholder=False):
        pass

DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "fruit_events.db"
DEFAULT_OUT_DIR = Path(__file__).resolve().parents[1] / "out"
DEFAULT_SERVER_URL = os.getenv("FRUIT_SERVER_URL") or "http://localhost:6655"


def get_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger("fruit_export_adb_lanes")


def iso_to_xmltv(ts: str) -> str:
    """
    Convert ISO-8601 UTC string (e.g. '2025-12-06T00:00:00+00:00')
    into XMLTV time format 'YYYYMMDDHHMMSS +0000'.
    """
    if not ts:
        # Fallback: now
        dt = datetime.now(timezone.utc)
    else:
        try:
            dt = datetime.fromisoformat(ts)
        except Exception:
            # Try stripping Z or offset if weird
            try:
                if ts.endswith("Z"):
                    dt = datetime.fromisoformat(ts[:-1]).replace(tzinfo=timezone.utc)
                else:
                    dt = datetime.fromisoformat(ts)
            except Exception:
                dt = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%d%H%M%S +0000")


def snap_to_quarter(dt: datetime) -> datetime:
    """Snap a datetime down to the nearest 15-minute boundary."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    minute_block = (dt.minute // 15) * 15
    return dt.replace(minute=minute_block, second=0, microsecond=0)


def snap_up_to_quarter(dt: datetime) -> datetime:
    """Snap a datetime up to the next 15-minute boundary (or keep if already aligned)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if dt.minute % 15 == 0 and dt.second == 0 and dt.microsecond == 0:
        return dt
    remainder = dt.minute % 15
    delta_min = 15 - remainder
    return (dt + timedelta(minutes=delta_min)).replace(second=0, microsecond=0)


def get_event_image_url(event: Dict) -> Optional[str]:
    """Simply return hero_image_url from events table (pre-selected during import)."""
    return event.get("hero_image_url")


def _add_placeholder_blocks(
    tv: ET.Element,
    channel_id: str,
    provider_label: str,
    gap_start: datetime,
    gap_end: datetime,
) -> None:
    """Fill a [gap_start, gap_end) interval with <=1h placeholder blocks."""
    if gap_end <= gap_start:
        return

    cur = snap_up_to_quarter(gap_start)
    while cur < gap_end:
        block_end = min(cur + timedelta(hours=1), gap_end)
        if (block_end - cur).total_seconds() < 60:
            # Skip tiny slivers
            break

        prog_el = ET.SubElement(
            tv,
            "programme",
            channel=str(channel_id),
            start=iso_to_xmltv(cur.isoformat()),
            stop=iso_to_xmltv(block_end.isoformat()),
        )
        title_el = ET.SubElement(prog_el, "title")
        title_el.text = "Idle"

        desc_el = ET.SubElement(prog_el, "desc")
        desc_el.text = f"No active event on {provider_label}."

        
        cur = block_end



def cleanup_disabled_adb_files(conn: sqlite3.Connection, out_dir: Path, log: logging.Logger) -> None:
    """Remove M3U/XML files for disabled ADB providers."""
    # Get list of enabled providers from provider_lanes table
    cur = conn.cursor()
    cur.execute("""
        SELECT provider_code 
        FROM provider_lanes 
        WHERE adb_enabled = 1
    """)
    enabled_providers = {row[0] for row in cur.fetchall()}
    
    if not out_dir.exists():
        return
    
    # Map of provider to file suffixes (without extension)
    provider_files = {
        'aiv': ['adb_lanes_aiv', 'adb_lanes_aiv_exclusive'],
        'gametime': ['adb_lanes_gametime'],
        'max': ['adb_lanes_max'],
        'pplus': ['adb_lanes_pplus'],
        'sportscenter': ['adb_lanes_sportscenter']
    }
    
    # Remove files for disabled providers
    for provider_code, file_prefixes in provider_files.items():
        if provider_code not in enabled_providers:
            for prefix in file_prefixes:
                for ext in ['.m3u', '.xml']:
                    filepath = out_dir / f"{prefix}{ext}"
                    if filepath.exists():
                        filepath.unlink()
                        log.info("Removed %s (provider '%s' is disabled)", filepath.name, provider_code)
    
    # Also clean up main adb_lanes files if NO providers are enabled
    if not enabled_providers:
        for filename in ['adb_lanes.m3u', 'adb_lanes.xml']:
            filepath = out_dir / filename
            if filepath.exists():
                filepath.unlink()
                log.info("Removed %s (all ADB providers disabled)", filename)


def export_adb_lanes(db_path: Path, out_dir: Path, server_url: str) -> Path:
    log = get_logger()
    log.info("Using database: %s", db_path)
    log.info("Output directory: %s", out_dir)
    log.info("Server URL for ADB M3U: %s", server_url)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "adb_lanes.xml"
    m3u_path = out_dir / "adb_lanes.m3u"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Clean up files for disabled services FIRST
        cleanup_disabled_adb_files(conn, out_dir, log)
        cur = conn.cursor()

        # Ensure adb_lanes exists and has rows
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='adb_lanes';"
        )
        if not cur.fetchone():
            raise RuntimeError(
                "adb_lanes table not found. Run migrate_add_adb_lanes.py and fruit_build_adb_lanes.py first."
            )

        cur.execute("SELECT COUNT(*) FROM adb_lanes;")
        total_rows = cur.fetchone()[0]
        if total_rows == 0:
            log.warning("adb_lanes table is empty; exporting an XMLTV skeleton.")
        else:
            log.info("adb_lanes has %d row(s).", total_rows)

        # Build channels: distinct provider_code, lane_number, channel_id
        cur.execute(
            """
            SELECT provider_code, lane_number, channel_id
              FROM adb_lanes
             GROUP BY provider_code, lane_number, channel_id
             ORDER BY provider_code, lane_number;
            """
        )
        channels = cur.fetchall()
        log.info("Found %d distinct ADB channels.", len(channels))

        # Build programmes: join adb_lanes -> events for titles / synopses
        # We assume events.id, events.start_utc, events.end_utc, events.title, events.synopsis
        cur.execute(
            """
            SELECT
                   a.channel_id,
                   a.provider_code,
                   a.start_utc,
                   a.stop_utc,
                   a.event_id,
                   e.*
              FROM adb_lanes a
              JOIN events e
                ON e.id = a.event_id
             ORDER BY a.channel_id, a.start_utc;
            """
        )
        programmes = cur.fetchall()
        log.info("Will export %d programme entries.", len(programmes))

        # Build XMLTV tree
        tv = ET.Element("tv")
        tv.set("source-info-name", "FruitDeepLinks ADB View")
        tv.set("generator-info-name", "fruit_export_adb_lanes.py")

        # Channels
        for row in channels:
            provider_code = row["provider_code"]
            lane_number = row["lane_number"]
            channel_id = row["channel_id"]

            ch_el = ET.SubElement(tv, "channel", id=str(channel_id))

            # Display name: Friendly provider name + lane number
            # (e.g., "ESPN+ 01", "Amazon Exclusives 01")
            provider_display = get_provider_display_name(provider_code) or provider_code.upper()
            display_name = f"{provider_display} {int(lane_number):02d}"
            dn_el = ET.SubElement(ch_el, "display-name")
            dn_el.text = display_name

        # Programmes with placeholders and rich categories
        # Group rows by channel so we can fill gaps per-lane.
        now = datetime.now(timezone.utc)
        pre_start = snap_to_quarter(now) - timedelta(hours=1)

        by_channel: dict[str, list[sqlite3.Row]] = {}
        max_end_by_channel: dict[str, datetime] = {}

        for row in programmes:
            ch_id = row["channel_id"]
            by_channel.setdefault(ch_id, []).append(row)
            stop_iso = row["stop_utc"]
            if stop_iso:
                try:
                    dt_stop = datetime.fromisoformat(stop_iso)
                except Exception:
                    continue
                if dt_stop.tzinfo is None:
                    dt_stop = dt_stop.replace(tzinfo=timezone.utc)
                else:
                    dt_stop = dt_stop.astimezone(timezone.utc)
                prev = max_end_by_channel.get(ch_id)
                if prev is None or dt_stop > prev:
                    max_end_by_channel[ch_id] = dt_stop

        for channel_id, events in by_channel.items():
            events_sorted = sorted(
                events,
                key=lambda r: r["start_utc"] or "",
            )
            if not events_sorted:
                continue

            provider_code = events_sorted[0]["provider_code"]
            provider_label = get_provider_display_name(provider_code) or (provider_code or "").upper()

            # Determine per-channel post window
            last_end = max_end_by_channel.get(channel_id, now)
            post_end = last_end + timedelta(hours=24)

            cursor = pre_start

            for row in events_sorted:
                start_iso = row["start_utc"]
                stop_iso = row["stop_utc"]
                if not start_iso or not stop_iso:
                    continue

                try:
                    start_dt = datetime.fromisoformat(start_iso)
                    stop_dt = datetime.fromisoformat(stop_iso)
                except Exception:
                    continue

                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                else:
                    start_dt = start_dt.astimezone(timezone.utc)
                if stop_dt.tzinfo is None:
                    stop_dt = stop_dt.replace(tzinfo=timezone.utc)
                else:
                    stop_dt = stop_dt.astimezone(timezone.utc)

                # Fill gap from cursor -> event start (bounded by pre_start/post_end)
                gap_start = max(cursor, pre_start)
                gap_end = min(start_dt, post_end)
                if gap_end > gap_start:
                    _add_placeholder_blocks(tv, channel_id, provider_label, gap_start, gap_end)

                # Emit real programme
                start_attr = iso_to_xmltv(start_dt.isoformat())
                stop_attr = iso_to_xmltv(stop_dt.isoformat())

                title = row["title"] or row["title_brief"] or "Event"
                synopsis = row["synopsis"] or row["synopsis_brief"] or ""

                prog_el = ET.SubElement(
                    tv,
                    "programme",
                    channel=str(channel_id),
                    start=start_attr,
                    stop=stop_attr,
                )
                title_el = ET.SubElement(prog_el, "title")
                title_el.text = title

                if synopsis:
                    desc_el = ET.SubElement(prog_el, "desc")
                    desc_el.text = synopsis

                # Use shared helper for categories and tags
                provider_display = get_provider_display_name(provider_code) or provider_label
                add_categories_and_tags(
                    prog_el,
                    event=dict(row),
                    provider_name=provider_display,
                    is_placeholder=False,
                )

                # Add image icon from hero_image_url (pre-selected during import)
                image_url = get_event_image_url(dict(row))
                if image_url:
                    icon_el = ET.SubElement(prog_el, "icon")
                    icon_el.set("src", image_url)

                cursor = max(cursor, stop_dt)

            # Tail placeholders out to post_end
            if cursor < post_end:
                gap_start = max(cursor, pre_start)
                gap_end = post_end
                if gap_end > gap_start:
                    _add_placeholder_blocks(tv, channel_id, provider_label, gap_start, gap_end)

        tree = ET.ElementTree(tv)
        tree.write(out_path, encoding="utf-8", xml_declaration=True)
        log.info("Wrote XMLTV file: %s", out_path)
        # Also build an ADB-specific M3U playlist that matches these channel IDs.
        build_adb_m3u(conn, m3u_path, server_url, log)
        return out_path
    finally:
        conn.close()



def build_adb_m3u(
    conn: sqlite3.Connection,
    m3u_path: Path,
    server_url: str,
    log: logging.Logger,
) -> None:
    """Build M3U playlists for ADB lanes.

    - Writes a *global* playlist at ``m3u_path`` that contains one entry
      per (provider_code, lane_number).
    - Also writes a *provider-specific* playlist for each provider at
      ``adb_lanes_<provider_code>.m3u`` in the same directory.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT provider_code,
               lane_number,
               channel_id
          FROM adb_lanes
         GROUP BY provider_code, lane_number, channel_id
         ORDER BY provider_code, lane_number;
        """
    )
    rows = cur.fetchall()
    if not rows:
        log.warning("No rows in adb_lanes; skipping M3U export.")
        return

    # Group rows by provider_code so we can write per-provider M3Us.
    by_provider: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        by_provider.setdefault(row["provider_code"], []).append(row)

    # 1) Global M3U with ALL providers.
    log.info("ADB M3U (global): %d virtual channels", len(rows))
    m3u_path.parent.mkdir(parents=True, exist_ok=True)
    with m3u_path.open("w", encoding="utf-8") as f:
        f.write("#EXTM3U\n\n")
        for row in rows:
            provider_code = row["provider_code"]
            lane_number = int(row["lane_number"])
            channel_id = row["channel_id"] or f"{provider_code}{lane_number:02d}"

            provider_display = get_provider_display_name(provider_code) or provider_code

            name = f"{provider_display} {lane_number:02d}"
            chno = lane_number

            stream_url = (
                server_url.rstrip("/")
                + f"/api/adb/lanes/{provider_code}/{lane_number}/deeplink?format=text"
            )

            f.write(
                f'#EXTINF:-1 tvg-id="{channel_id}" tvg-chno="{chno}" '
                f'group-title="ADB {provider_display}",{name}\n'
            )
            f.write(stream_url + "\n\n")

    log.info("Wrote global ADB lanes M3U: %s", m3u_path)

    # 2) Per-provider M3Us: adb_lanes_<provider_code>.m3u
    base_dir = m3u_path.parent
    for provider_code, provider_rows in sorted(by_provider.items()):
        provider_file = base_dir / f"adb_lanes_{provider_code}.m3u"
        log.info(
            "ADB M3U (%s): %d virtual channels",
            provider_code,
            len(provider_rows),
        )
        with provider_file.open("w", encoding="utf-8") as f:
            f.write("#EXTM3U\n\n")
            for row in provider_rows:
                lane_number = int(row["lane_number"])
                channel_id = row["channel_id"] or f"{provider_code}{lane_number:02d}"

                provider_display = get_provider_display_name(provider_code) or provider_code

                name = f"{provider_display} {lane_number:02d}"
                chno = lane_number

                stream_url = (
                    server_url.rstrip("/")
                    + f"/api/adb/lanes/{provider_code}/{lane_number}/deeplink?format=text"
                )

                f.write(
                    f'#EXTINF:-1 tvg-id="{channel_id}" tvg-chno="{chno}" '
                    f'group-title="ADB {provider_display}",{name}\n'
                )
                f.write(stream_url + "\n\n")

        log.info("Wrote provider ADB lanes M3U for %s: %s", provider_code, provider_file)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export ADB lanes (adb_lanes) to XMLTV (adb_lanes.xml)."
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite DB (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--out-dir",
        dest="out_dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUT_DIR})",
    )
    parser.add_argument(
        "--server-url",
        dest="server_url",
        default=DEFAULT_SERVER_URL,
        help=(
            "Base server URL used in M3U entries "
            f"(default: {DEFAULT_SERVER_URL})"
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    export_adb_lanes(args.db_path, args.out_dir, args.server_url)
