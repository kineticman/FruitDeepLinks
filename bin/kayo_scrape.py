#!/usr/bin/env python3
"""kayo_scrape.py

Scraper for Kayo Sports Australia that writes normalized JSON.

Usage:
    python /app/bin/kayo_scrape.py --out /app/out/kayo_raw.json --days 7
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.kayosports.com.au/v3/content/types/landing/names/fixtures"



class KayoForbidden(Exception):
    """Raised when Kayo API denies access (HTTP 403)."""

    def __init__(self, message: str, status_code: int = 403, body_snippet: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.body_snippet = body_snippet


# Use a single session so headers + TLS settings remain consistent across requests.
_SESSION = requests.Session()

KAYO_HEADERS = {
    # Keep this intentionally "browser-ish" to avoid edge/bot blocking.
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-AU,en;q=0.9",
    "Origin": "https://kayosports.com.au",
    "Referer": "https://kayosports.com.au/fixtures",
}
_SESSION.headers.update(KAYO_HEADERS)

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kayo Sports scraper")
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Path to write kayo_raw.json",
    )
    parser.add_argument(
        "--start",
        help="Start date in YYYY-MM-DD (default: today, local).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to fetch starting from --start (inclusive). Default: 7.",
    )
    parser.add_argument(
        "--live",
        dest="with_live",
        action="store_true",
        help="Request fixtures withLive=true (default).",
    )
    parser.add_argument(
        "--no-live",
        dest="with_live",
        action="store_false",
        help="Request fixtures withLive=false.",
    )
    parser.set_defaults(with_live=True)
    
    parser.add_argument(
        "--sport",
        help="Optional sport filter (e.g. 'cricket'). If omitted, all sports.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.",
    )
    return parser.parse_args()


def build_from_param(day: datetime) -> str:
    """Build the Kayo `from=` parameter for a given day."""
    return f"{day.date().isoformat()}T05:00Z"


def fetch_fixtures_json(
    day: datetime,
    with_live: bool = True,
    sport: str | None = None,
    timeout: int = 15,
) -> Dict[str, Any]:
    """Call the Kayo fixtures endpoint for a single day and return raw JSON."""
    from_param = build_from_param(day)

    params = {
        "withLive": "true" if with_live else "false",
        "withOdds": "false",
        "evaluate": "3",
        "from": from_param,
    }
    if sport:
        params["sport"] = sport

    logger.debug("Fetching Kayo fixtures: params=%s", params)
    resp = _SESSION.get(BASE_URL, params=params, timeout=timeout)
    if resp.status_code == 403:
        snippet = (resp.text or "")[:300]
        raise KayoForbidden(
            "Kayo fixtures request blocked (403). Likely edge/bot blocking; ensure browser-like headers.",
            status_code=403,
            body_snippet=snippet,
        )

    resp.raise_for_status()

    data = resp.json()
    logger.debug(
        "Received fixtures payload: meta.info=%s",
        data.get("meta", {}).get("info", {}),
    )
    return data


def normalize_kayo_event(content: Dict[str, Any], sport_from_panel: str) -> Dict[str, Any] | None:
    """Convert one Kayo content item into normalized event format.
    
    Args:
        content: The content item from a panel
        sport_from_panel: The sport name from the panel title
    
    Returns None if the content should be skipped.
    """
    # Extract the data section
    data = content.get("data", {})
    if not data:
        logger.warning("Skipping content with no data")
        return None
    
    # Get the asset ID (this is our unique identifier)
    asset_id = data.get("id")
    if not asset_id:
        logger.warning("Skipping content with no id")
        return None
    
    event_id = f"kayo-{asset_id}"
    
    # Extract title from contentDisplay or heroDisplay
    content_display = data.get("contentDisplay", {})
    title_obj = content_display.get("title", {})
    title = title_obj.get("value", "") if isinstance(title_obj, dict) else str(title_obj)
    
    if not title:
        title = asset_id
    
    # Extract clickthrough data for more details
    clickthrough = data.get("clickthrough", {})

    # Kayo sometimes provides a linear channel/provider code (e.g., "fsa505")
    channel_code = clickthrough.get("channel") or content_display.get("linearProvider")
    
    # Sport and league
    sport_name = clickthrough.get("sportName", sport_from_panel)
    # Normalize capitalization: "football" -> "Football"
    if sport_name:
        sport_name = sport_name.title()
    league_name = (
        clickthrough.get("seriesName")
        or clickthrough.get("roundName")
        or next((i.get("value") for i in (content_display.get("infoLine") or [])
                 if isinstance(i, dict) and i.get("type") == "series"), None)
        or content_display.get("header")
        or sport_name
    )
    
    # Extract times - transmissionTime is the start time
    start_time = clickthrough.get("transmissionTime")
    if not start_time:
        # Try from values
        values = content_display.get("values", {})
        start_time = values.get("startTime")
    
    if not start_time:
        logger.warning(f"Skipping fixture {asset_id} - no start time")
        return None
    
    # Calculate end time from duration if available
    end_time = None
    duration_str = content_display.get("duration", "")
    
    # First try: explicit duration field (format: "5:40:00")
    if duration_str and start_time:
        try:
            parts = duration_str.split(':')
            if len(parts) == 3:
                hours, minutes, seconds = map(int, parts)
                duration_delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                end_dt = start_dt + duration_delta
                end_time = end_dt.isoformat().replace('+00:00', 'Z')
        except (ValueError, AttributeError) as e:
            logger.debug(f"Could not parse duration {duration_str}: {e}")
    
    # Second try: Extract from infoLine (format: "4h 10m", "3h", "54m")
    if not end_time and start_time:
        info_line = content_display.get("infoLine", [])
        length_str = None
        
        for item in info_line:
            if isinstance(item, dict) and item.get("type") == "length":
                length_str = item.get("value", "")
                break
        
        if length_str:
            try:
                import re
                
                # Parse strings like "4h 10m", "3h", "54m"
                hours = 0
                minutes = 0
                
                # Extract hours
                h_match = re.search(r'(\d+)h', length_str)
                if h_match:
                    hours = int(h_match.group(1))
                
                # Extract minutes
                m_match = re.search(r'(\d+)m', length_str)
                if m_match:
                    minutes = int(m_match.group(1))
                
                if hours > 0 or minutes > 0:
                    start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    end_dt = start_dt + timedelta(hours=hours, minutes=minutes)
                    end_time = end_dt.isoformat().replace('+00:00', 'Z')
                    logger.debug(f"Parsed length '{length_str}' -> {hours}h {minutes}m")
            except Exception as e:
                logger.debug(f"Could not parse length {length_str}: {e}")
    
    # Third try: Sport-specific duration estimates as fallback
    if not end_time and start_time:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        
        # Sport-specific duration estimates (in hours)
        sport_durations = {
            'Cricket': 6,
            'Gridiron': 3.5,
            'Basketball': 2.5,
            'Ice Hockey': 2.5,
            'Football': 2,
            'Boxing': 3,
            'Darts': 3,
            'Wrestling': 3,
            'Golf': 4,
        }
        
        duration_hours = sport_durations.get(sport_name, 3)
        end_dt = start_dt + timedelta(hours=duration_hours)
        end_time = end_dt.isoformat().replace('+00:00', 'Z')
        logger.debug(f"Estimated end time for {sport_name}: {duration_hours}h")
    
    # Venue - not directly available in this structure
    venue_name = None
    
    # Extract images
    images = content_display.get("images", {})
    hero_image = images.get("hero", "") or images.get("tile", "")
    # Remove the width parameter placeholder
    if hero_image and "${WIDTH}" in hero_image:
        hero_image = hero_image.replace("imwidth=${WIDTH}", "imwidth=1920")
    
    # Build playable URL from clickthrough.url (CANONICAL method)
    # Follow the guide: use clickthrough.url, not /sport/watch/{asset_id}
    KAYO_BASE = "https://kayosports.com.au"
    
    clickthrough_url = clickthrough.get("url")
    fixture_url = None
    
    if clickthrough_url:
        clickthrough_url = clickthrough_url.strip()
        
        # Normalize to full URL
        if clickthrough_url.startswith("http://") or clickthrough_url.startswith("https://"):
            fixture_url = clickthrough_url
        elif clickthrough_url.startswith("/"):
            fixture_url = KAYO_BASE + clickthrough_url
        else:
            fixture_url = KAYO_BASE + "/" + clickthrough_url
    
    # Validate URL has expected path prefix
    if fixture_url:
        valid_prefixes = ("/fixture/", "/event/", "/event-centre/")
        path_part = fixture_url.replace(KAYO_BASE, "")
        if not any(path_part.startswith(prefix) for prefix in valid_prefixes):
            logger.warning(f"Unexpected Kayo URL path: {fixture_url}")
    
    # Fallback only if no clickthrough found (rare)
    if not fixture_url:
        logger.warning(f"No clickthrough URL for asset {asset_id}, using fallback")
        fixture_url = KAYO_BASE
    
    playables = [{
        "playable_id": f"{event_id}-0",
        "provider": "kayo",
        "playable_url": fixture_url,
        "deeplink_play": fixture_url,
        "deeplink_open": fixture_url,
        "priority": 10,
    }]
    
    return {
        "external_id": asset_id,
        "title": title,
        "sport": sport_name,
        "league": league_name,
        "channel_code": channel_code,
        "start_utc": start_time,
        "end_utc": end_time,
        "venue": venue_name,
        "hero_image": hero_image,
        "raw": content,  # Store entire content for debugging
        "playables": playables,
    }


def fetch_kayo_schedule(
    start_date: datetime,
    days: int,
    with_live: bool = True,
    sport: str | None = None,
) -> List[Dict[str, Any]]:
    """Fetch Kayo fixtures over a date range and normalize them."""
    all_events = []
    
    for offset in range(days):
        day = start_date + timedelta(days=offset)
        logger.info(f"Fetching Kayo fixtures for {day.date().isoformat()}")
        
        try:
            payload = fetch_fixtures_json(
                day=day,
                with_live=with_live,
                sport=sport,
            )
            
            # Extract fixtures from panels structure
            # Response structure: { "panels": [ { "title": "Cricket", "contents": [...] } ] }
            panels = payload.get("panels", [])
            
            fixtures_found = 0
            for panel in panels:
                panel_title = panel.get("title", "")
                panel_type = panel.get("panelType", "")
                contents = panel.get("contents", [])
                
                # Skip nav menus and date selectors
                if panel_type in ("nav-menu-sticky", "date-selector"):
                    continue
                
                # Panel title is usually the sport name (Cricket, Basketball, etc.)
                sport_from_panel = panel_title
                
                logger.debug(f"  Processing panel: {panel_title} ({len(contents)} items)")
                
                for content in contents:
                    # Only process asset content types
                    content_type = content.get("contentType")
                    if content_type != "video":
                        # Could be "video" or sometimes just in data
                        data_content_type = content.get("data", {}).get("contentType")
                        if data_content_type != "asset":
                            continue
                    
                    normalized = normalize_kayo_event(content, sport_from_panel)
                    if normalized:
                        all_events.append(normalized)
                        fixtures_found += 1
            
            logger.info(f"  Found {fixtures_found} fixtures for {day.date().isoformat()}")
                    
        except KayoForbidden as e:
            # If Kayo blocks us for one day, it will almost always block the whole range.
            msg = (
                f"Error fetching fixtures for {day.date().isoformat()}: {e} "
                f"(status={getattr(e, 'status_code', None)})"
            )
            logger.error(msg)
            if getattr(e, "body_snippet", None):
                logger.debug("Kayo 403 body (first 300 chars): %s", e.body_snippet)
            logger.error("Stopping Kayo scrape for this run to avoid 403 spam.")
            break
        except Exception as e:
            logger.error(f"Error fetching fixtures for {day.date().isoformat()}: {e}")
            continue
    
    logger.info(f"Total normalized events: {len(all_events)}")
    return all_events


def main() -> int:
    args = get_args()
    
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    
    # Parse start date
    if args.start:
        try:
            start_date = datetime.strptime(args.start, "%Y-%m-%d")
        except ValueError:
            logger.error("--start must be in YYYY-MM-DD format")
            return 1
    else:
        start_date = datetime.now()
    
    if args.days < 1:
        logger.error("--days must be >= 1")
        return 1
    
    # Fetch events
    events = fetch_kayo_schedule(
        start_date=start_date,
        days=args.days,
        with_live=args.with_live,
        sport=args.sport,
    )
    
    # Build output payload
    payload = {
        "source": "kayo",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "events": events,
    }
    
    # Write output
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    
    logger.info(f"Wrote {len(events)} Kayo events to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
