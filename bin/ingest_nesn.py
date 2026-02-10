#!/usr/bin/env python3
"""
ingest_nesn.py - Ingest adapter for NESN data into FruitDeepLinks

Reads normalized NESN JSON from scraper and inserts into fruit_events.db
with proper schema mapping, deeplinks, and playables.

Usage:
    python /app/bin/ingest_nesn.py --db /app/data/fruit_events.db --nesn-json /app/out/nesn_raw.json
"""

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NesinIngestAdapter:
    """Adapter to ingest NESN data into FruitDeepLinks database"""
    
    # NESN service configuration
    PROVIDER = "nesn"
    SERVICE_NAME = "NESN 360"
    IS_PREMIUM = 1  # NESN requires subscription
    PRIORITY = 20   # Regional service priority
    
    def __init__(self, db_path: str):
        """Initialize database connection"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.now_utc = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
        self.now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    
    def connect(self):
        """Connect to database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logger.info(f"✓ Connected to database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"✗ Failed to connect to database: {e}")
            sys.exit(1)
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            logger.info("✓ Database connection closed")
    
    def verify_schema(self):
        """Verify required tables exist"""
        try:
            # Check events table
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
            if not self.cursor.fetchone():
                logger.error("✗ 'events' table not found")
                sys.exit(1)
            
            # Check playables table
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='playables'")
            if not self.cursor.fetchone():
                logger.error("✗ 'playables' table not found")
                sys.exit(1)
            
            logger.info("✓ Schema verified")
        except sqlite3.Error as e:
            logger.error(f"✗ Schema verification failed: {e}")
            sys.exit(1)
    
    def generate_deeplinks(self, event_id: str) -> Dict[str, str]:
        """
        Generate NESN deeplinks for an event.
        
        Args:
            event_id: External event ID (e.g., "tvschedule-1707440400000-BEC")
        
        Returns:
            Dictionary with android_deeplink and web_url
        """
        android_deeplink = f"com.nesn.nesnplayer://play/{event_id}"
        web_url = f"https://watch.nesn.com/play/{event_id}"
        
        return {
            "android_deeplink": android_deeplink,
            "web_url": web_url
        }
    
    def normalize_event(self, raw_event: Dict) -> Dict:
        """
        Normalize raw NESN event to database schema.
        
        CRITICAL FIELDS:
        - id: "{external_id}"
        - pvid: "{external_id}" (REQUIRED for M3U export!)
        - channel_provider_id: "nesn"
        - channel_name: "NESN" or "NESN+"
        - genres_json: '["NESN"]'
        - start_utc, end_utc: ISO8601 with Z
        - start_ms, end_ms: Unix milliseconds
        - runtime_secs: Duration in seconds
        - is_premium: 1 (subscription required)
        """
        external_id = raw_event.get("external_id", "")
        if not external_id:
            logger.warning("Event missing external_id")
            return None
        
        # Use airing_type from scraper (replay/premiere/live)
        airing_type = raw_event.get("airing_type", "premiere")
        
        normalized = {
            "id": external_id,
            "pvid": external_id,  # CRITICAL: Provider ID for M3U export
            "slug": external_id.lower().replace(" ", "-"),
            "title": raw_event.get("title", "Untitled"),
            "title_brief": raw_event.get("title", "")[:50],  # Shortened title
            "synopsis": raw_event.get("description", ""),
            "synopsis_brief": raw_event.get("description", "")[:100],
            "channel_name": raw_event.get("channel", "NESN"),
            "channel_provider_id": self.PROVIDER,
            "airing_type": airing_type,  # "premiere", "replay", or "live"
            "classification_json": '[]',
            "genres_json": json.dumps(self._get_filtered_genres(raw_event.get("categories", []))),
            "content_segments_json": '[]',
            "is_free": 0,
            "is_premium": self.IS_PREMIUM,
            "runtime_secs": raw_event.get("duration_secs", 0),
            "start_ms": raw_event.get("start_ms", 0),
            "end_ms": raw_event.get("end_ms", 0),
            "start_utc": raw_event.get("start_utc", ""),
            "end_utc": raw_event.get("end_utc", ""),
            "created_ms": self.now_ms,
            "created_utc": self.now_utc,
            "hero_image_url": raw_event.get("image_url", ""),
            "last_seen_utc": self.now_utc,
            "raw_attributes_json": json.dumps({
                "programme_id": raw_event.get("programme_id", ""),
                "show_code": raw_event.get("show_code", ""),
                "channel_uuid": raw_event.get("channel_uuid", ""),
                "xmltv_id": raw_event.get("xmltv_id", ""),
                "sub_type": raw_event.get("sub_type"),  # Original XMLTV value: "(R)", "NEW", "LIVE"
                "is_replay": raw_event.get("replay", False),
            })
        }
        
        return normalized
    
    def _get_filtered_genres(self, categories: list) -> list:
        """
        Filter categories to only include content/sports genres.
        Skip channel names and generic categories.
        
        Args:
            categories: List of categories from scraper
        
        Returns:
            Filtered list suitable for genres_json
        """
        if not categories:
            return []
        
        # Skip these - they're channel/provider info, not content genres
        skip_categories = {'NESN', 'NESN+', 'Sports', 'Sports Network', 'Sports Channel'}
        
        # Keep only content/sports categories
        filtered = [
            c for c in categories 
            if c and c not in skip_categories
        ]
        
        return filtered
    
    def insert_or_update_event(self, normalized: Dict) -> bool:
        """
        Insert or update event in database.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if event exists
            self.cursor.execute(
                "SELECT id FROM events WHERE id = ?",
                (normalized["id"],)
            )
            exists = self.cursor.fetchone() is not None
            
            if exists:
                # Update existing event
                update_fields = [
                    "title = ?", "synopsis = ?", "channel_name = ?",
                    "genres_json = ?", "hero_image_url = ?",
                    "last_seen_utc = ?"
                ]
                update_values = [
                    normalized["title"],
                    normalized["synopsis"],
                    normalized["channel_name"],
                    normalized["genres_json"],
                    normalized["hero_image_url"],
                    normalized["last_seen_utc"],
                    normalized["id"]
                ]
                
                self.cursor.execute(
                    f"UPDATE events SET {', '.join(update_fields)} WHERE id = ?",
                    update_values
                )
                logger.debug(f"Updated event: {normalized['id']}")
            else:
                # Insert new event
                columns = ", ".join(normalized.keys())
                placeholders = ", ".join(["?" for _ in normalized])
                
                self.cursor.execute(
                    f"INSERT INTO events ({columns}) VALUES ({placeholders})",
                    tuple(normalized.values())
                )
                logger.debug(f"Inserted event: {normalized['id']}")
            
            return True
        
        except sqlite3.Error as e:
            logger.error(f"✗ Failed to insert/update event {normalized['id']}: {e}")
            return False
    
    def insert_playables(self, event_id: str, deeplinks: Dict) -> bool:
        """
        Create playables for NESN with both Android and Web deeplinks.
        
        Priority order:
        1. Android deeplink (app) - highest priority
        2. Web URL (fallback) - lower priority
        
        Args:
            event_id: Event ID
            deeplinks: Dictionary with android_deeplink and web_url
        
        Returns:
            True if successful, False otherwise
        """
        try:
            playables = [
                # Android deeplink (highest priority)
                {
                    "event_id": event_id,
                    "playable_id": f"{event_id}-android",
                    "provider": self.PROVIDER,
                    "logical_service": "nesn",
                    "deeplink_play": deeplinks["android_deeplink"],
                    "deeplink_open": deeplinks["android_deeplink"],
                    "playable_url": deeplinks["android_deeplink"],
                    "http_deeplink_url": deeplinks["web_url"],
                    "title": "NESN",
                    "content_id": event_id,
                    "priority": self.PRIORITY,
                    "created_utc": self.now_utc,
                    "service_name": self.SERVICE_NAME,
                    "locale": "en_US"
                },
                # Web deeplink (fallback)
                {
                    "event_id": event_id,
                    "playable_id": f"{event_id}-web",
                    "provider": self.PROVIDER,
                    "logical_service": "nesn",
                    "deeplink_play": deeplinks["web_url"],
                    "deeplink_open": deeplinks["web_url"],
                    "playable_url": deeplinks["web_url"],
                    "http_deeplink_url": deeplinks["web_url"],
                    "title": "NESN",
                    "content_id": event_id,
                    "priority": self.PRIORITY + 1,  # Lower priority than Android
                    "created_utc": self.now_utc,
                    "service_name": self.SERVICE_NAME,
                    "locale": "en_US"
                }
            ]
            
            for playable in playables:
                # Check if playable exists
                self.cursor.execute(
                    "SELECT playable_id FROM playables WHERE event_id = ? AND playable_id = ?",
                    (playable["event_id"], playable["playable_id"])
                )
                exists = self.cursor.fetchone() is not None
                
                if exists:
                    # Update existing playable
                    update_fields = ["deeplink_play = ?", "playable_url = ?", "priority = ?"]
                    update_values = [
                        playable["deeplink_play"],
                        playable["playable_url"],
                        playable["priority"],
                        playable["event_id"],
                        playable["playable_id"]
                    ]
                    
                    self.cursor.execute(
                        f"UPDATE playables SET {', '.join(update_fields)} WHERE event_id = ? AND playable_id = ?",
                        update_values
                    )
                else:
                    # Insert new playable
                    columns = ", ".join(playable.keys())
                    placeholders = ", ".join(["?" for _ in playable])
                    
                    self.cursor.execute(
                        f"INSERT INTO playables ({columns}) VALUES ({placeholders})",
                        tuple(playable.values())
                    )
            
            logger.debug(f"Created {len(playables)} playables for {event_id}")
            return True
        
        except sqlite3.Error as e:
            logger.error(f"✗ Failed to insert playables for {event_id}: {e}")
            return False
    
    def ingest(self, nesn_json_path: str) -> Tuple[int, int]:
        """
        Ingest NESN JSON data into database.
        
        Args:
            nesn_json_path: Path to nesn_raw.json from scraper
        
        Returns:
            Tuple of (events_inserted, playables_inserted)
        """
        logger.info(f"=== NESN Ingest Starting ===")
        logger.info(f"Reading from: {nesn_json_path}")
        
        # Read scraped data
        try:
            with open(nesn_json_path, 'r', encoding='utf-8') as f:
                scraped_data = json.load(f)
        except Exception as e:
            logger.error(f"✗ Failed to read {nesn_json_path}: {e}")
            sys.exit(1)
        
        events = scraped_data.get("events", [])
        logger.info(f"Read {len(events)} events from scraper")
        
        events_inserted = 0
        playables_inserted = 0
        
        for raw_event in events:
            try:
                # Normalize event
                normalized = self.normalize_event(raw_event)
                if not normalized:
                    continue
                
                # Validate critical fields
                if not normalized.get("end_utc"):
                    logger.warning(f"Skipping {normalized['id']}: missing end_utc")
                    continue
                
                if not normalized.get("pvid"):
                    logger.warning(f"Skipping {normalized['id']}: missing pvid")
                    continue
                
                # Insert/update event
                if not self.insert_or_update_event(normalized):
                    continue
                
                # Generate and insert deeplinks
                deeplinks = self.generate_deeplinks(normalized["id"])
                if not self.insert_playables(normalized["id"], deeplinks):
                    continue
                
                events_inserted += 1
                playables_inserted += 2  # 2 playables per event
            
            except Exception as e:
                logger.warning(f"Failed to process event: {e}")
                continue
        
        # Commit changes
        try:
            self.conn.commit()
            logger.info(f"✓ Database committed")
        except sqlite3.Error as e:
            logger.error(f"✗ Failed to commit: {e}")
            sys.exit(1)
        
        logger.info(f"\n=== Ingest Summary ===")
        logger.info(f"Events inserted/updated: {events_inserted}")
        logger.info(f"Playables created: {playables_inserted}")
        
        return (events_inserted, playables_inserted)


def main():
    parser = argparse.ArgumentParser(
        description="Ingest NESN data into FruitDeepLinks"
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to fruit_events.db"
    )
    parser.add_argument(
        "--nesn-json",
        required=True,
        help="Path to nesn_raw.json from scraper"
    )
    
    args = parser.parse_args()
    
    # Create adapter and ingest
    adapter = NesinIngestAdapter(args.db)
    adapter.connect()
    adapter.verify_schema()
    
    try:
        adapter.ingest(args.nesn_json)
    finally:
        adapter.close()
    
    logger.info(f"\n=== Ingest Complete ===")


if __name__ == "__main__":
    main()
