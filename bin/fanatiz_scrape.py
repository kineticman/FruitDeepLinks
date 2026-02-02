#!/usr/bin/env python3
"""
fanatiz_scrape.py - Fanatiz sports schedule scraper with date filtering

Scrapes Fanatiz's sports events API with proper filtering:
- Uses filterBy-statusCategory=future to get only upcoming events
- Optionally filters by date range (client-side after fetching)
- Limits pages for testing

Usage:
    # Get all future events (~1,300 events)
    python fanatiz_scrape.py --out fanatiz_raw.json
    
    # Get only next 14 days
    python fanatiz_scrape.py --out fanatiz_raw.json --days 14
    
    # Test with first 5 pages only
    python fanatiz_scrape.py --out fanatiz_raw.json --max-pages 5
"""

import argparse
import json
import logging
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FanatizScraper:
    """Scraper for Fanatiz sports schedule API"""
    
    BASE_URL = "https://www.fanatiz.com/api/sports"
    
    def __init__(self, days: Optional[int] = None, max_pages: Optional[int] = None, event_type: str = 'football', status_category: str = 'future'):
        """
        Initialize scraper
        
        Args:
            days: Number of days ahead to fetch (None = all future events)
            max_pages: Maximum pages to fetch (None = all pages)
        """
        self.days = days
        self.max_pages = max_pages
        self.event_type = event_type
        self.status_category = status_category
        # Fanatiz API seems to paginate more consistently with sections=false and explicit sort order
        self.sections = 'false'
        self.sort_by_asc = 'startDate'
        self.session = self._create_session()
        self.events = []
        self.stats = {
            'total_fetched': 0,
            'total_kept': 0,
            'total_filtered': 0,
            'total_pages': 0,
            'events_with_playables': 0,
            'events_without_playables': 0,
            'errors': 0
        }
        
        # Calculate cutoff date if days specified
        self.cutoff_date = None
        if days:
            self.cutoff_date = datetime.now(timezone.utc) + timedelta(days=days)
            logger.info(f"Will filter events to {days} days (until {self.cutoff_date.strftime('%Y-%m-%d')})")
        
    def _create_session(self) -> requests.Session:
        """Create requests session with retry logic"""
        session = requests.Session()
        
        # Retry strategy
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Default headers
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://www.fanatiz.com',
            'Referer': 'https://www.fanatiz.com/'
        })
        
        return session
    
    def fetch_events_page(self, page: int = 1, items_per_page: int = 50) -> Dict:
        """
        Fetch a single page of events from Fanatiz API
        
        Args:
            page: Page number (1-indexed)
            items_per_page: Results per page
            
        Returns:
            API response dict
        """
        url = f"{self.BASE_URL}/events"
        
        params = {
            'page': page,
            'itemsPerPage': items_per_page,
            'sections': self.sections,
            'sortByAsc': self.sort_by_asc,
            'filterBy-type': self.event_type,
            'filterBy-statusCategory': self.status_category,
        }
            
        logger.info(f"Fetching page {page} ({items_per_page} items/page)")
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Log pagination info
            if 'data' in data and 'pagination' in data['data']:
                pagination = data['data']['pagination']
                logger.info(f"  → Page {pagination.get('page')}/{pagination.get('pages')} "
                          f"({pagination.get('totalItems')} total future events)")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page}: {e}")
            self.stats['errors'] += 1
            return {}
    
    def fetch_all_events(self) -> List[Dict]:
        """
        Fetch all events using pagination
        
        Returns:
            List of normalized event dicts
        """
        logger.info("Starting Fanatiz scrape")
        logger.info(f"  Filter: Future events only")
        if self.days:
            logger.info(f"  Date range: Next {self.days} days")
        if self.max_pages:
            logger.info(f"  Page limit: {self.max_pages} pages (for testing)")
        
        page = 1
        has_more = True
        
        while has_more:
            # Check max pages limit
            if self.max_pages and page > self.max_pages:
                logger.info(f"Reached max pages limit ({self.max_pages})")
                break
            
            data = self.fetch_events_page(page=page, items_per_page=50)
            
            if not data or 'data' not in data:
                logger.warning(f"No data returned for page {page}")
                break
            
            # Extract events from data.items[]
            items = data.get('data', {}).get('items', [])
            if not items:
                logger.info(f"No more events on page {page}")
                break
            
            logger.info(f"Processing {len(items)} events from page {page}")
            
            for raw_event in items:
                try:
                    normalized = self.normalize_event(raw_event)
                    if normalized:
                        self.stats['total_fetched'] += 1
                        
                        # Apply date filter if specified
                        if self._should_keep_event(normalized):
                            self.events.append(normalized)
                            self.stats['total_kept'] += 1
                            
                            if normalized.get('playables'):
                                self.stats['events_with_playables'] += 1
                            else:
                                self.stats['events_without_playables'] += 1
                        else:
                            self.stats['total_filtered'] += 1
                            
                except Exception as e:
                    logger.error(f"Error normalizing event: {e}")
                    logger.debug(f"  Raw event: {json.dumps(raw_event, indent=2)[:500]}")
                    self.stats['errors'] += 1
            
            self.stats['total_pages'] += 1
            
            # Check pagination
            pagination = data.get('data', {}).get('pagination', {})
            total_pages = pagination.get('pages', 0)
            current_page = pagination.get('page', page)
            
            if current_page >= total_pages:
                has_more = False
                logger.info(f"Reached last page ({current_page}/{total_pages})")
            else:
                page += 1
                time.sleep(1.2)  # Rate limiting with added delay to avoid potential limits
        
        logger.info(f"\nScrape complete:")
        logger.info(f"  Events fetched: {self.stats['total_fetched']}")
        logger.info(f"  Events kept: {self.stats['total_kept']}")
        if self.days:
            logger.info(f"  Events filtered (outside date range): {self.stats['total_filtered']}")
        logger.info(f"  With playables: {self.stats['events_with_playables']}")
        logger.info(f"  Without playables: {self.stats['events_without_playables']}")
        logger.info(f"  Pages fetched: {self.stats['total_pages']}")
        logger.info(f"  Errors: {self.stats['errors']}")
        
        return self.events
    
    def _should_keep_event(self, event: Dict) -> bool:
        """
        Check if event should be kept based on date filter
        
        Args:
            event: Normalized event dict
            
        Returns:
            True if event should be kept
        """
        if not self.cutoff_date:
            return True
        
        start_utc = event.get('start_utc')
        if not start_utc:
            return True
        
        try:
            event_date = datetime.fromisoformat(start_utc.replace('Z', '+00:00'))
            return event_date <= self.cutoff_date
        except:
            return True
    
    def normalize_event(self, raw: Dict) -> Optional[Dict]:
        """
        Normalize Fanatiz API event to FDL format
        
        Args:
            raw: Raw event dict from Fanatiz API
            
        Returns:
            Normalized event dict or None if invalid
        """
        try:
            # Event ID (use 'id' field, fallback to '_id')
            event_id = raw.get('id') or raw.get('_id')
            if not event_id:
                logger.warning("Event missing ID, skipping")
                return None
            
            # Start time
            start_utc = raw.get('startDate')
            if not start_utc:
                logger.warning(f"Event {event_id} missing start date")
                return None
            
            # Ensure ISO8601 format with Z
            if not start_utc.endswith('Z'):
                start_utc = f"{start_utc}Z"
            
            # Calculate end time (2.5 hours for soccer)
            end_utc = self._calculate_end_time(start_utc, 150)
            
            # Extract team information
            home_team_obj = raw.get('homeTeam', {})
            away_team_obj = raw.get('awayTeam', {})
            
            home_team = home_team_obj.get('shortName') or home_team_obj.get('name', 'Home')
            away_team = away_team_obj.get('shortName') or away_team_obj.get('name', 'Away')
            
            # Build title
            title = f"{home_team} vs {away_team}"
            
            # Extract tournament info
            # Tournament IDs are in __belong.Tournament[]
            tournament_ids = raw.get('__belong', {}).get('Tournament', [])
            tournament_name = 'Soccer'  # Default
            
            # Sport type
            sport_type = raw.get('type', 'football')
            logger.debug(f"Sport type: {sport_type}")
            sport = self._normalize_sport(sport_type)
            
            # Event status
            event_status = raw.get('eventStatus', {})
            status_category = raw.get('statusCategory', 'future')
            
            # Images
            # Fanatiz provides team image IDs (often under homeTeam.flag / awayTeam.flag).
            # We can derive a publicly fetchable URL via the Next.js optimizer endpoint.
            hero_image = None  # original hero artwork isn't present in this API response

            home_image_id = self._extract_team_image_id(home_team_obj)
            away_image_id = self._extract_team_image_id(away_team_obj)

            images = []
            hero_image_url = None

            if home_image_id:
                u = self._build_next_image_url(home_image_id, scale=75, w=256, q=75)
                images.append({'type': 'team_home', 'url': u, 'image_id': home_image_id})
                hero_image_url = hero_image_url or u

            if away_image_id:
                u = self._build_next_image_url(away_image_id, scale=75, w=256, q=75)
                images.append({'type': 'team_away', 'url': u, 'image_id': away_image_id})
                hero_image_url = hero_image_url or u
            
            # Extract playables from videos[] array
            playables = self._extract_playables(raw, event_id)
            
            # Build metadata
            metadata = {
                'opta_id': raw.get('_externalId'),
                'opta_provider': raw.get('_externalProvider'),
                'home_team': home_team_obj.get('name'),
                'away_team': away_team_obj.get('name'),
                'status': status_category,
                'event_status': event_status.get('name', {}).get('original'),
                'type': sport_type,
                'week': raw.get('week'),
                'home_score': raw.get('homeScore'),
                'away_score': raw.get('awayScore'),
                'home_image_id': home_image_id,
                'away_image_id': away_image_id,
            }
            
            # Build normalized event
            event = {
                'external_id': str(event_id),
                'title': title,
                'subtitle': tournament_name,
                'sport': sport,
                'league': tournament_name,
                'tournament': tournament_name,
                'start_utc': start_utc,
                'end_utc': end_utc,
                'hero_image': hero_image,
                'hero_image_url': hero_image_url,
                'images': images,
                'playables': playables,
                'metadata': metadata
            }
            
            return event
            
        except Exception as e:
            logger.error(f"Error normalizing event: {e}")
            return None
    
    def _normalize_sport(self, sport_type: str) -> str:
        """Normalize sport name to title case"""
        sport_map = {
            'football': 'Soccer',
            'soccer': 'Soccer',
            'basketball': 'Basketball',
            'rugby': 'Rugby',
        }
        
        normalized = sport_type.lower()
        return sport_map.get(normalized, sport_type.title())
    
    def _extract_team_image_id(self, team_obj: Dict) -> Optional[str]:
        """Best-effort extraction of a Fanatiz image ID from a team object."""
        if not isinstance(team_obj, dict):
            return None
        for k in ("flag", "logo", "image", "imageId", "flagId"):
            v = team_obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None
    
    def _build_next_image_url(self, image_id: str, *, cut: str = "square", scale: int = 75, w: int = 256, q: int = 75) -> str:
        """Build a Channels-friendly image URL via Fanatiz's Next.js image optimizer."""
        base = f"https://watch.fanatiz.com/api/images?id={image_id}&cut={cut}&scale={scale}"
        enc = urllib.parse.quote(base, safe="")
        return f"https://watch.fanatiz.com/_next/image?url={enc}&w={w}&q={q}"
    
    def _extract_playables(self, raw: Dict, event_id: str) -> List[Dict]:
        """
        Extract playable video entries from videos[] array
        
        Args:
            raw: Raw event dict
            event_id: Event ID for reference
            
        Returns:
            List of playable dicts
        """
        playables = []
        videos = raw.get('videos', [])
        
        for idx, video in enumerate(videos):
            video_id = video.get('id') or video.get('_id')
            if not video_id:
                continue
            
            # Extract metadata
            title = video.get('title', 'Main Feed')
            locale = video.get('locale')
            
            # Build deeplink URL using actual Fanatiz watch scheme
            deeplink_url = f"https://watch.fanatiz.com/event-detail?id={event_id}"
            
            playable = {
                'playable_id': str(video_id),
                'title': title,
                'deeplink_url': deeplink_url,
                'locale': locale,
            }
            
            playables.append(playable)
        
        return playables
    
    def _calculate_end_time(self, start_utc: str, duration_mins: int) -> str:
        """
        Calculate end time from start time and duration
        
        Args:
            start_utc: ISO8601 start time
            duration_mins: Duration in minutes
            
        Returns:
            ISO8601 end time
        """
        try:
            start_dt = datetime.fromisoformat(start_utc.replace('Z', '+00:00'))
            end_dt = start_dt + timedelta(minutes=duration_mins)
            return end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            logger.error(f"Error calculating end time: {e}")
            return start_utc
    
    def save_to_file(self, output_path: str):
        """Save scraped events to JSON file"""
        output = {
            'events': self.events,
            'metadata': {
                'scraped_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'total_fetched': self.stats['total_fetched'],
                'total_kept': self.stats['total_kept'],
                'total_filtered': self.stats['total_filtered'],
                'total_pages': self.stats['total_pages'],
                'events_with_playables': self.stats['events_with_playables'],
                'events_without_playables': self.stats['events_without_playables'],
                'errors': self.stats['errors'],
                'api_version': 'v1',
                'days_requested': self.days,
                'max_pages_requested': self.max_pages,
                'filter_used': 'future',
            }
        }
        
        # Ensure output directory exists
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        file_size = Path(output_path).stat().st_size / 1024
        logger.info(f"\nSaved {len(self.events)} events to {output_path}")
        logger.info(f"File size: {file_size:.1f} KB")


def main():
    parser = argparse.ArgumentParser(
        description='Scrape Fanatiz sports schedule',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--out',
        required=True,
        help='Output JSON file path (e.g., fanatiz_raw.json)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=None,
        help='Number of days ahead to fetch (default: all future events)'
    )
    parser.add_argument(
        '--max-pages',
        type=int,
        default=None,
        help='Maximum pages to fetch for testing (default: all pages)'
    )
    parser.add_argument(
        '--event-type',
        dest='event_type',
        default='football',
        help="Fanatiz filterBy-type (default: football)"
    )
    parser.add_argument(
        '--status-category',
        dest='status_category',
        default='future',
        help="Fanatiz filterBy-statusCategory (default: future). Use 'live,future' for live + upcoming."
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Create scraper
        scraper = FanatizScraper(days=args.days, max_pages=args.max_pages, event_type=args.event_type, status_category=args.status_category)
        
        # Fetch events
        scraper.fetch_all_events()
        
        # Save to file
        scraper.save_to_file(args.out)
        
        logger.info("\nScrape completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Scrape failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())