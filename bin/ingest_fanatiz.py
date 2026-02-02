#!/usr/bin/env python3
"""
ingest_fanatiz.py - Ingest Fanatiz events into fruit_events.db

Reads fanatiz_raw.json and populates events, playables, and event_images tables.

Usage:
    python /app/bin/ingest_fanatiz.py --db /app/data/fruit_events.db --fanatiz-json /app/out/fanatiz_raw.json
    
Database Schema:
    - events: Core event metadata
    - playables: Deeplink/playable entries
    - event_images: Image URLs by type
"""

import argparse
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FanatizIngestor:
    """Ingest Fanatiz events into FruitDeepLinks database"""
    
    PROVIDER = "fanatiz"
    LOGICAL_SERVICE = "fanatiz_web"  # Will be mapped by logical_service_mapper
    PRIORITY = 20  # Specialty soccer service
    DEFAULT_FAVICON = "https://watch.fanatiz.com/favicon.ico"
    
    def __init__(self, db_path: str):
        """Initialize with database connection"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        
        self.stats = {
            'events_processed': 0,
            'events_inserted': 0,
            'events_updated': 0,
            'playables_inserted': 0,
            'images_inserted': 0,
            'errors': 0
        }
    
    def ingest_from_file(self, json_path: str):
        """
        Ingest events from JSON file
        
        Args:
            json_path: Path to fanatiz_raw.json
        """
        logger.info(f"Loading events from {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        events = data.get('events', [])
        logger.info(f"Found {len(events)} events to process")
        
        # Process in transaction for performance
        try:
            self.conn.execute('BEGIN')
            
            for event in events:
                try:
                    self._ingest_event(event)
                    self.stats['events_processed'] += 1
                except Exception as e:
                    logger.error(f"Error ingesting event {event.get('external_id')}: {e}")
                    self.stats['errors'] += 1
            
            self.conn.commit()
            logger.info("Transaction committed successfully")
            
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            self.conn.rollback()
            raise
        
        self._log_stats()
    
    def _ingest_event(self, event: Dict):
        """
        Ingest a single event with playables and images
        
        Args:
            event: Normalized event dict from scraper
        """
        external_id = event['external_id']
        event_id = f"{self.PROVIDER}-{external_id}"
        
        # Build genres_json (sport + league)
        sport = event.get('sport', 'Soccer')
        league = event.get('league', sport)
        tournament = event.get('tournament', league)
        
        genres = [sport]
        if league and league != sport:
            genres.append(league)
        if tournament and tournament not in genres:
            genres.append(tournament)
        
        genres_json = json.dumps(genres)
        
        # Parse timestamps
        start_utc = event['start_utc']
        end_utc = event['end_utc']
        
        start_ms = self._utc_to_ms(start_utc)
        end_ms = self._utc_to_ms(end_utc)
        runtime_secs = int((end_ms - start_ms) / 1000) if start_ms and end_ms else 7200
        
        # Current timestamp
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        # Build title using full team names from metadata if available
        metadata = event.get('metadata', {})
        hero_image_url = (event.get('hero_image_url') or event.get('hero_image') or '').strip()

        home_team_full = metadata.get('home_team')
        away_team_full = metadata.get('away_team')
        
        if home_team_full and away_team_full:
            title = f"{home_team_full} vs {away_team_full}"
        else:
            title = event['title']  # Fallback to short names
        
        # Build synopsis from metadata
        synopsis_parts = []
        
        # Add full team names if available (most important info)
        home_team = metadata.get('home_team')
        away_team = metadata.get('away_team')
        if home_team and away_team:
            synopsis_parts.append(f"{home_team} vs {away_team}")
        
        # Add event status if interesting
        event_status = metadata.get('event_status')
        if event_status and event_status not in ['Pending', 'Scheduled']:
            synopsis_parts.append(event_status)
        
        # Add week/round if available
        if metadata.get('week'):
            synopsis_parts.append(f"Week {metadata['week']}")
        
        synopsis = ' • '.join(synopsis_parts) if synopsis_parts else 'Soccer Match'
        
        # Build channel_name - use "Fanatiz" for now since we don't have tournament mapping
        # The tournament name could be enhanced later if needed
        channel_name = "Fanatiz Soccer"
        
        # Check if event exists
        existing = self.conn.execute(
            'SELECT id FROM events WHERE id = ?',
            (event_id,)
        ).fetchone()
        
        if existing:
            # Update existing event
            self.conn.execute('''
                UPDATE events SET
                    title = ?,
                    title_brief = ?,
                    synopsis = ?,
                    synopsis_brief = ?,
                    channel_name = ?,
                    genres_json = ?,
                    start_utc = ?,
                    end_utc = ?,
                    start_ms = ?,
                    end_ms = ?,
                    runtime_secs = ?,
                    hero_image_url = COALESCE(?, hero_image_url),
                    last_seen_utc = ?,
                    raw_attributes_json = ?
                WHERE id = ?
            ''', (
                title,
                title[:50] if len(title) > 50 else title,
                synopsis,
                synopsis[:100] if len(synopsis) > 100 else synopsis,
                channel_name,
                genres_json,
                start_utc,
                end_utc,
                start_ms,
                end_ms,
                runtime_secs,
                (hero_image_url or None),
                now_utc,
                json.dumps(metadata),
                event_id
            ))
            self.stats['events_updated'] += 1
            
        else:
            # Insert new event
            self.conn.execute('''
                INSERT INTO events (
                    id, pvid, title, title_brief, synopsis, synopsis_brief,
                    channel_name, channel_provider_id,
                    genres_json, is_premium,
                    runtime_secs, start_ms, end_ms, start_utc, end_utc,
                    created_ms, created_utc, hero_image_url, last_seen_utc,
                    raw_attributes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event_id,
                external_id,  # CRITICAL: pvid required for M3U export
                title,
                title[:50] if len(title) > 50 else title,
                synopsis,
                synopsis[:100] if len(synopsis) > 100 else synopsis,
                channel_name,
                self.PROVIDER,  # channel_provider_id
                genres_json,
                1,  # is_premium (Fanatiz requires subscription)
                runtime_secs,
                start_ms,
                end_ms,
                start_utc,
                end_utc,
                now_ms,
                now_utc,
                (hero_image_url or None),
                now_utc,
                json.dumps(metadata)
            ))
            self.stats['events_inserted'] += 1
        
        # Ingest playables
        playables = event.get('playables', [])
        if playables:
            self._ingest_playables(event_id, external_id, playables)
        else:
            # Create stub playable for events without video entries
            self._create_stub_playable(event_id, external_id)
        
        # Ingest images
        if hero_image_url:
            self._ingest_image(event_id, 'hero', hero_image_url)

        imgs = event.get('images')
        if isinstance(imgs, list):
            for im in imgs:
                if not isinstance(im, dict):
                    continue
                itype = (im.get('type') or '').strip()
                url = im.get('url') or im.get('src') or ''
                if itype and url:
                    self._ingest_image(event_id, itype, url)
        elif isinstance(imgs, dict):
            # dict style: {type: url}
            for itype, url in imgs.items():
                if itype and url:
                    self._ingest_image(event_id, str(itype), str(url))
    
    def _promote_hero_image_url(self, event_id: str) -> str:
        """Backfill events.hero_image_url from event_images (or favicon fallback).

        Preference order: hero -> team_home -> team_away -> DEFAULT_FAVICON.
        Only updates the events row if hero_image_url is currently NULL/empty.
        """
        row = self.conn.execute(
            """
            SELECT url
            FROM event_images
            WHERE event_id = ?
              AND img_type IN ('hero','team_home','team_away')
            ORDER BY CASE img_type
                WHEN 'hero' THEN 1
                WHEN 'team_home' THEN 2
                WHEN 'team_away' THEN 3
                ELSE 9
            END
            LIMIT 1
            """,
            (event_id,),
        ).fetchone()

        url = (row['url'] if row else '') or self.DEFAULT_FAVICON

        self.conn.execute(
            """
            UPDATE events
               SET hero_image_url = ?
             WHERE id = ?
               AND (hero_image_url IS NULL OR hero_image_url = '')
            """,
            (url, event_id),
        )
        return url

    def _ingest_playables(self, event_id: str, external_id: str, playables: List[Dict]):
        """
        Ingest playables for an event
        
        Args:
            event_id: FDL event ID (fanatiz-{external_id})
            external_id: Fanatiz event ID
            playables: List of playable dicts
        """
        # Delete existing playables for this event
        self.conn.execute('DELETE FROM playables WHERE event_id = ?', (event_id,))
        
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        for idx, playable in enumerate(playables):
            playable_id = playable.get('playable_id', f"{external_id}-{idx}")
            
            # Build deeplink URL
            deeplink_url = playable.get('deeplink_url')
            
            # Build title
            title = playable.get('title', 'Main Feed')
            locale = playable.get('locale')
            if locale:
                title = f"{title} ({locale.upper()})"
            
            # Insert playable
            self.conn.execute('''
                INSERT INTO playables (
                    event_id, playable_id, provider, logical_service,
                    deeplink_play, playable_url, http_deeplink_url,
                    title, locale, priority, created_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event_id,
                playable_id,
                self.PROVIDER,
                self.LOGICAL_SERVICE,
                deeplink_url,
                deeplink_url,
                deeplink_url,
                title,
                locale,
                self.PRIORITY + idx,  # Increment priority for variants
                now_utc
            ))
            
            self.stats['playables_inserted'] += 1
    
    def _create_stub_playable(self, event_id: str, external_id: str):
        """
        Create a stub playable for events without video entries
        
        This allows events to appear in the guide even without playables.
        Actual playback will require authentication/app launch.
        
        Args:
            event_id: FDL event ID
            external_id: Fanatiz event ID
        """
        # Delete existing playables
        self.conn.execute('DELETE FROM playables WHERE event_id = ?', (event_id,))
        
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Create stub with actual Fanatiz watch URL
        stub_url = f"https://watch.fanatiz.com/event-detail?id={external_id}"
        
        self.conn.execute('''
            INSERT INTO playables (
                event_id, playable_id, provider, logical_service,
                deeplink_play, playable_url, http_deeplink_url,
                title, priority, created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            event_id,
            f"{external_id}-stub",
            self.PROVIDER,
            self.LOGICAL_SERVICE,
            stub_url,
            stub_url,
            stub_url,
            'Fanatiz Stream',
            self.PRIORITY,
            now_utc
        ))
        
        self.stats['playables_inserted'] += 1
    
    def _ingest_image(self, event_id: str, img_type: str, url: str):
        """
        Ingest an image for an event
        
        Args:
            event_id: FDL event ID
            img_type: Image type (hero, keyart, boxart)
            url: Image URL
        """
        if not url:
            return
        
        # Insert or ignore (avoid duplicates)
        self.conn.execute('''
            INSERT OR IGNORE INTO event_images (event_id, img_type, url)
            VALUES (?, ?, ?)
        ''', (event_id, img_type, url))
        
        if self.conn.total_changes > 0:
            self.stats['images_inserted'] += 1
    
    def _utc_to_ms(self, utc_str: str) -> Optional[int]:
        """Convert ISO8601 UTC string to milliseconds since epoch"""
        try:
            dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except Exception as e:
            logger.error(f"Error parsing timestamp {utc_str}: {e}")
            return None
    
    def _log_stats(self):
        """Log ingestion statistics"""
        logger.info("Ingestion complete:")
        logger.info(f"  Events processed: {self.stats['events_processed']}")
        logger.info(f"  Events inserted: {self.stats['events_inserted']}")
        logger.info(f"  Events updated: {self.stats['events_updated']}")
        logger.info(f"  Playables inserted: {self.stats['playables_inserted']}")
        logger.info(f"  Images inserted: {self.stats['images_inserted']}")
        logger.info(f"  Errors: {self.stats['errors']}")
    
    def close(self):
        """Close database connection"""
        self.conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Ingest Fanatiz events into FruitDeepLinks database'
    )
    parser.add_argument(
        '--db',
        required=True,
        help='Path to fruit_events.db'
    )
    parser.add_argument(
        '--fanatiz-json',
        required=True,
        help='Path to fanatiz_raw.json'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Verify files exist
    if not Path(args.db).exists():
        logger.error(f"Database not found: {args.db}")
        return 1
    
    if not Path(args.fanatiz_json).exists():
        logger.error(f"JSON file not found: {args.fanatiz_json}")
        return 1
    
    try:
        # Create ingestor
        ingestor = FanatizIngestor(args.db)
        
        # Ingest events
        ingestor.ingest_from_file(args.fanatiz_json)
        
        # Cleanup
        ingestor.close()
        
        logger.info("Ingest completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Ingest failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())
