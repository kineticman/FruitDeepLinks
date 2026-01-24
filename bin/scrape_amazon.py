#!/usr/bin/env python3
"""
Amazon Channel Scraper for FruitDeeplinks
Scrapes Amazon Prime Video channel/subscription requirements for live events.

Usage:
  python scrape_amazon.py [options]

Options:
  --db PATH           Database path (default: data/fruit_events.db)
  --workers N         Concurrent workers (default: 5)
  --retry N           Retry attempts (default: 2)
  --max N             Max GTIs to scrape (for testing)
  --refresh           Ignore cache and refresh all
  --no-cache          Disable caching entirely
  --bootstrap         Force schema creation/update
  --debug-dir PATH    Save debug artifacts

Features:
  - Auto-bootstrap schema if missing
  - Async parallel scraping (5x faster)
  - 7-day smart caching
  - Handles all Amazon page formats
  - Comprehensive error classification
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import re
import sys
import sqlite3
import logging
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException
try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False

import requests

# Configuration
DEFAULT_DB = "data/fruit_events.db"
CACHE_FILE = "data/amazon_gti_cache.pkl"
CACHE_MAX_AGE_DAYS = 7
DEBUG_DIR: Optional[str] = None

# HTTP Request configuration
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
REQUEST_TIMEOUT = 15

# Known channel mappings
CHANNEL_MAPPINGS = {
    'amzn1.dv.channel.7a36cb2b-40e6-40c7-809f-a6cf9b9f0859': 'NBA League Pass',
    'peacockus': 'Peacock Premium',
    'maxliveeventsus': 'Max',
    'daznus': 'DAZN',
    'vixplusus': 'ViX Premium',
    'vixus': 'ViX Gratis',
    'amzn1.dv.spid.8cc2a36e-cd1b-d2cb-0e3b-b9ddce868f1d': 'FOX One',
    'FSNOHIFSOH3': 'FanDuel Sports Network',
}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('amazon_scraper')

# Silence urllib3 connection pool warnings (harmless noise from Selenium)
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)


# ==============================================================================
# CHROME DRIVER SETUP (Cross-platform compatibility)
# ==============================================================================

def make_driver(headless: bool = True) -> webdriver.Chrome:
    """Create Chrome/Chromium WebDriver with cross-platform support (matches Apple scraper)"""
    
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    
    # Try different methods to initialize driver
    driver = None
    
    # Method 1: System chromedriver
    try:
        logger.debug("Attempting system chromedriver...")
        service = Service()
        driver = webdriver.Chrome(service=service, options=opts)
        logger.debug("✓ Successfully initialized with system chromedriver")
        return driver
    except Exception as e:
        logger.debug(f"System chromedriver failed: {e}")
    
    # Method 2: webdriver-manager (if available)
    if WEBDRIVER_MANAGER_AVAILABLE:
        try:
            logger.debug("Attempting webdriver-manager...")
            wm_path = Path(ChromeDriverManager().install())
            if wm_path.exists():
                service = Service(executable_path=str(wm_path))
                driver = webdriver.Chrome(service=service, options=opts)
                logger.debug("✓ Successfully initialized with webdriver-manager")
                return driver
        except Exception as e:
            logger.debug(f"webdriver-manager failed: {e}")
    
    # Method 3: Common system paths
    common_paths = [
        "/usr/bin/chromedriver",
        "/usr/local/bin/chromedriver",
        "/opt/homebrew/bin/chromedriver",
    ]
    
    for path in common_paths:
        if Path(path).exists():
            try:
                logger.debug(f"Attempting chromedriver at {path}...")
                service = Service(executable_path=path)
                driver = webdriver.Chrome(service=service, options=opts)
                logger.debug(f"✓ Successfully initialized with {path}")
                return driver
            except Exception as e:
                logger.debug(f"Failed with {path}: {e}")
    
    raise RuntimeError(
        "Unable to initialize Chrome/Chromium WebDriver. "
        "Ensure either Google Chrome or Chromium is installed with matching chromedriver."
    )


# ==============================================================================
# BOOTSTRAP / SCHEMA MANAGEMENT
# ==============================================================================

AMAZON_CHANNELS_SCHEMA = """
CREATE TABLE IF NOT EXISTS amazon_channels (
    gti TEXT PRIMARY KEY,
    gti_type TEXT,
    channel_id TEXT,
    channel_name TEXT,
    availability TEXT,
    subscription_type TEXT,
    requires_prime INTEGER DEFAULT 0,
    is_free INTEGER DEFAULT 0,
    unavailable_message TEXT,
    is_stale INTEGER DEFAULT 0,
    http_status INTEGER,
    scrape_attempt_count INTEGER DEFAULT 1,
    first_seen_utc TEXT,
    last_available_utc TEXT,
    last_scraped_utc TEXT,
    created_utc TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_amazon_channels_channel_id 
    ON amazon_channels(channel_id);
CREATE INDEX IF NOT EXISTS idx_amazon_channels_stale 
    ON amazon_channels(is_stale);
CREATE INDEX IF NOT EXISTS idx_amazon_channels_type 
    ON amazon_channels(gti_type);
CREATE INDEX IF NOT EXISTS idx_amazon_channels_last_scraped 
    ON amazon_channels(last_scraped_utc);

CREATE TABLE IF NOT EXISTS amazon_services (
    service_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    amazon_channel_id TEXT NOT NULL,
    logical_service TEXT,
    requires_prime INTEGER DEFAULT 0,
    is_free INTEGER DEFAULT 0,
    monthly_price_usd DECIMAL(5,2),
    provider_url TEXT,
    icon_url TEXT,
    description TEXT,
    is_active INTEGER DEFAULT 1,
    sort_order INTEGER DEFAULT 100,
    created_utc TEXT DEFAULT (datetime('now')),
    updated_utc TEXT DEFAULT (datetime('now')),
    UNIQUE(amazon_channel_id)
);

CREATE INDEX IF NOT EXISTS idx_amazon_services_logical 
    ON amazon_services(logical_service);
CREATE INDEX IF NOT EXISTS idx_amazon_services_active 
    ON amazon_services(is_active);

CREATE TABLE IF NOT EXISTS amazon_channel_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gti TEXT NOT NULL,
    channel_id TEXT,
    channel_name TEXT,
    availability TEXT,
    is_stale INTEGER,
    changed_at_utc TEXT DEFAULT (datetime('now')),
    change_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_history_gti 
    ON amazon_channel_history(gti);
CREATE INDEX IF NOT EXISTS idx_history_changed 
    ON amazon_channel_history(changed_at_utc);

CREATE TABLE IF NOT EXISTS amazon_playable_overrides (
    playable_id TEXT PRIMARY KEY,
    override_gti TEXT,
    override_channel_id TEXT,
    reason TEXT,
    created_by TEXT,
    created_utc TEXT DEFAULT (datetime('now'))
);
"""

AMAZON_VIEW_SCHEMA = """
DROP VIEW IF EXISTS v_amazon_playables_with_channels;

CREATE VIEW v_amazon_playables_with_channels AS
WITH playable_matches AS (
    SELECT
        p.event_id,
        p.playable_id,
        p.provider,
        p.title,
        p.deeplink_play,
        p.deeplink_open,
        p.http_deeplink_url,
        p.logical_service,
        ac.gti,
        ac.gti_type,
        ac.channel_id,
        ac.channel_name,
        ac.availability,
        ac.subscription_type,
        ac.requires_prime,
        ac.is_free,
        ac.unavailable_message,
        ac.is_stale,
        ac.last_scraped_utc,
        s.service_id,
        s.display_name as service_display_name,
        -- Prioritize broadcast GTI over main GTI
        CASE 
            WHEN p.deeplink_play LIKE '%broadcast=' || ac.gti || '%' THEN 1
            WHEN p.deeplink_open LIKE '%broadcast=' || ac.gti || '%' THEN 1
            ELSE 2
        END as match_priority
    FROM playables p
    LEFT JOIN amazon_channels ac ON (
        p.deeplink_play LIKE '%gti=' || ac.gti || '%' OR
        p.deeplink_open LIKE '%gti=' || ac.gti || '%' OR
        p.http_deeplink_url LIKE '%gti=' || ac.gti || '%' OR
        p.deeplink_play LIKE '%broadcast=' || ac.gti || '%' OR
        p.deeplink_open LIKE '%broadcast=' || ac.gti || '%'
    )
    LEFT JOIN amazon_services s ON ac.channel_id = s.amazon_channel_id
    WHERE p.provider = 'aiv'
),
ranked_matches AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id, playable_id 
            ORDER BY match_priority ASC, last_scraped_utc DESC
        ) as rn
    FROM playable_matches
)
SELECT
    event_id,
    playable_id,
    provider,
    title,
    deeplink_play,
    deeplink_open,
    http_deeplink_url,
    logical_service,
    gti,
    gti_type,
    channel_id,
    channel_name,
    service_id,
    service_display_name,
    availability,
    subscription_type,
    requires_prime,
    is_free,
    unavailable_message,
    is_stale,
    last_scraped_utc,
    CASE
        WHEN is_stale = 1 THEN 'Event no longer available on Amazon'
        WHEN availability = 'regional_restriction' THEN 'Not available in your region'
        WHEN is_free = 1 THEN 'FREE to watch (with ads)'
        WHEN channel_id = 'prime_premium' THEN 'Included with Amazon Prime ($14.99/mo)'
        WHEN channel_id = 'prime_included' THEN 'Included with your Prime membership'
        WHEN service_display_name IS NOT NULL THEN 'Requires ' || service_display_name
        ELSE 'Requires ' || COALESCE(channel_name, 'subscription')
    END as user_message
FROM ranked_matches
WHERE rn = 1;
"""

AMAZON_EVENTS_VIEW_SCHEMA = """
DROP VIEW IF EXISTS v_amazon_events_best_option;

CREATE VIEW v_amazon_events_best_option AS
SELECT 
    e.id as event_id,
    e.title,
    e.start_utc,
    e.end_utc,
    MIN(
        CASE 
            WHEN v.is_free = 1 THEN 1
            WHEN v.channel_id = 'prime_included' THEN 2
            WHEN v.channel_id = 'prime_premium' THEN 3
            ELSE 4
        END
    ) as best_tier,
    (SELECT service_display_name 
     FROM v_amazon_playables_with_channels 
     WHERE event_id = e.id AND is_stale = 0
     ORDER BY 
        CASE 
            WHEN is_free = 1 THEN 1
            WHEN channel_id = 'prime_included' THEN 2
            WHEN channel_id = 'prime_premium' THEN 3
            ELSE 4
        END,
        service_id
     LIMIT 1
    ) as best_service,
    (SELECT service_id
     FROM v_amazon_playables_with_channels 
     WHERE event_id = e.id AND is_stale = 0
     ORDER BY 
        CASE 
            WHEN is_free = 1 THEN 1
            WHEN channel_id = 'prime_included' THEN 2
            WHEN channel_id = 'prime_premium' THEN 3
            ELSE 4
        END,
        service_id
     LIMIT 1
    ) as best_service_id,
    COUNT(DISTINCT v.playable_id) as total_playables,
    COUNT(DISTINCT v.service_id) as unique_services,
    GROUP_CONCAT(DISTINCT v.service_display_name) as all_services
FROM events e
JOIN v_amazon_playables_with_channels v ON e.id = v.event_id
WHERE v.is_stale = 0 
  AND v.channel_id IS NOT NULL
  AND e.start_utc >= datetime('now')
GROUP BY e.id;
"""


def bootstrap_database(db_path: str, force: bool = False) -> None:
    """Ensure amazon_channels table and view exist"""
    
    if not os.path.exists(db_path):
        logger.error(f"Database not found: {db_path}")
        logger.error("Please ensure fruit_events.db exists first")
        sys.exit(1)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='amazon_channels'
    """)
    table_exists = cursor.fetchone() is not None
    
    if table_exists and not force:
        logger.info("✓ amazon_channels table exists")
        
        # Check if new columns exist, add them if not
        cursor.execute("PRAGMA table_info(amazon_channels)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        new_columns = {
            'gti_type': 'TEXT',
            'subscription_type': 'TEXT',
            'http_status': 'INTEGER',
            'scrape_attempt_count': 'INTEGER DEFAULT 1',
            'first_seen_utc': 'TEXT',
            'last_available_utc': 'TEXT'
        }
        
        for col_name, col_type in new_columns.items():
            if col_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE amazon_channels ADD COLUMN {col_name} {col_type}")
                    logger.info(f"✓ Added column: {col_name}")
                except Exception as e:
                    logger.warning(f"Could not add {col_name}: {e}")
        
        conn.commit()
    else:
        logger.info("Creating amazon_channels and related tables...")
        cursor.executescript(AMAZON_CHANNELS_SCHEMA)
        conn.commit()
        logger.info("✓ Created tables")
    
    # Seed amazon_services
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='amazon_services'
    """)
    if cursor.fetchone():
        cursor.execute("SELECT COUNT(*) FROM amazon_services")
        if cursor.fetchone()[0] == 0:
            logger.info("Seeding amazon_services...")
            cursor.executescript("""
                INSERT INTO amazon_services (service_id, display_name, amazon_channel_id, logical_service, requires_prime, is_free, monthly_price_usd, sort_order) VALUES
                    ('aiv_prime', 'Amazon Prime', 'prime_premium', 'aiv_prime', 0, 0, 14.99, 10),
                    ('aiv_prime_included', 'Included with Prime', 'prime_included', 'aiv_prime', 1, 0, 0.00, 5),
                    ('aiv_prime_free', 'Free with Ads', 'prime_free', 'aiv_free', 0, 1, 0.00, 1),
                    ('aiv_nba_league_pass', 'NBA League Pass', 'amzn1.dv.channel.7a36cb2b-40e6-40c7-809f-a6cf9b9f0859', 'aiv_nba_league_pass', 1, 0, 14.99, 20),
                    ('aiv_peacock', 'Peacock Premium', 'peacockus', 'aiv_peacock', 1, 0, 7.99, 30),
                    ('aiv_dazn', 'DAZN', 'daznus', 'aiv_dazn', 1, 0, 19.99, 40),
                    ('aiv_fox_one', 'FOX One', 'amzn1.dv.spid.8cc2a36e-cd1b-d2cb-0e3b-b9ddce868f1d', 'aiv_fox', 1, 0, NULL, 50),
                    ('aiv_vix_premium', 'ViX Premium', 'vixplusus', 'aiv_vix_premium', 1, 0, 6.99, 60),
                    ('aiv_vix_gratis', 'ViX Gratis', 'vixus', 'aiv_vix', 0, 1, 0.00, 61),
                    ('aiv_fanduel', 'FanDuel Sports Network', 'FSNOHIFSOH3', 'aiv_fanduel', 1, 0, NULL, 70),
                    ('aiv_max', 'Max', 'maxliveeventsus', 'aiv_max', 1, 0, NULL, 80);
            """)
            conn.commit()
            logger.info("✓ Seeded amazon_services")
    else:
        logger.info("✓ amazon_services table will be created with schema")
    
    # Always update view (safe to recreate)
    logger.info("Creating/updating views...")
    cursor.executescript(AMAZON_VIEW_SCHEMA)
    cursor.executescript(AMAZON_EVENTS_VIEW_SCHEMA)
    conn.commit()
    logger.info("✓ Created views")
    
    # Verify playables table exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='playables'
    """)
    if not cursor.fetchone():
        logger.error("ERROR: playables table not found!")
        logger.error("This script requires the main fruit_events.db schema")
        conn.close()
        sys.exit(1)
    
    # Check for Amazon playables
    cursor.execute("SELECT COUNT(*) FROM playables WHERE provider = 'aiv'")
    count = cursor.fetchone()[0]
    logger.info(f"✓ Found {count} Amazon playables in database")
    
    # Backfill gti_type for existing records
    cursor.execute("""
        UPDATE amazon_channels 
        SET gti_type = CASE 
            WHEN EXISTS (
                SELECT 1 FROM playables 
                WHERE deeplink_play LIKE '%broadcast=' || amazon_channels.gti || '%'
            ) THEN 'broadcast'
            ELSE 'main'
        END
        WHERE gti_type IS NULL
    """)
    backfilled = cursor.rowcount
    if backfilled > 0:
        conn.commit()
        logger.info(f"✓ Backfilled gti_type for {backfilled} records")
    
    conn.close()


# ==============================================================================
# CACHE MANAGEMENT
# ==============================================================================

def load_cache() -> Dict[str, Any]:
    """Load GTI cache from disk"""
    if not os.path.exists(CACHE_FILE):
        return {}
    
    try:
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
        
        # Filter expired entries
        cutoff = datetime.now(timezone.utc) - timedelta(days=CACHE_MAX_AGE_DAYS)
        fresh_cache = {}
        
        for gti, result in cache.items():
            cached_at = result.get('_cached_at')
            if cached_at and datetime.fromisoformat(cached_at) > cutoff:
                fresh_cache[gti] = result
        
        if len(fresh_cache) < len(cache):
            logger.info(f"Removed {len(cache) - len(fresh_cache)} expired cache entries")
        
        return fresh_cache
        
    except Exception as e:
        logger.warning(f"Failed to load cache: {e}")
        return {}


def save_cache(cache: Dict[str, Any]) -> None:
    """Save GTI cache to disk"""
    try:
        # Ensure directory exists
        cache_dir = os.path.dirname(CACHE_FILE)
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(cache, f)
        logger.info(f"Cache saved: {CACHE_FILE}")
    except Exception as e:
        logger.warning(f"Failed to save cache: {e}")


# ==============================================================================
# SCRAPING LOGIC
# ==============================================================================

def clean_channel_name(raw_label: str, channel_id: str) -> str:
    """Clean up channel name from label"""
    if channel_id in CHANNEL_MAPPINGS:
        return CHANNEL_MAPPINGS[channel_id]
    
    # Clean up common patterns
    cleaned = raw_label.replace('Start your free trial to ', '')
    cleaned = cleaned.replace('Start your 7-day free trial', '').strip()
    cleaned = cleaned.replace('Watch with ', '').strip()
    cleaned = cleaned.replace('{lineBreak}', ' ').strip()
    
    # Special cases
    if channel_id == 'prime_premium':
        return 'Amazon Prime'
    elif channel_id == 'prime_included':
        return 'Included with Prime'
    elif channel_id == 'prime_free':
        return 'Free with Ads'
    
    return cleaned if cleaned else 'Unknown'


def parse_amazon_json(scripts: List[str], gti: str, title: str) -> Dict[str, Any]:
    """Parse Amazon JSON from script contents"""
    
    # First try: Parse as JSON
    for script_text in scripts:
        try:
            data = json.loads(script_text)
            provider_info = extract_provider_info(data, gti)
            
            result = {
                'gti': gti,
                'title': title,
                **provider_info
            }
            
            # If we found valid provider info, return it
            if 'error' not in provider_info or provider_info.get('channel_id'):
                return result
                
        except json.JSONDecodeError:
            continue
    
    # Second try: Extract data using regex (for malformed JSON)
    combined_text = ' '.join(scripts)
    regex_result = extract_provider_info_regex(combined_text, gti, title)
    if regex_result.get('channel_id') or regex_result.get('success'):
        return regex_result
    
    # No valid data found
    return {
        'gti': gti,
        'title': title,
        'error': 'NO_JSON',
        'error_detail': 'No valid JSON found in scripts',
    }


def extract_provider_info_regex(text: str, gti: str, title: str) -> Dict[str, Any]:
    """Extract channel info using regex when JSON parsing fails"""
    
    result = {
        'gti': gti,
        'title': title,
        'channel_id': None,
        'channel_name': None,
        'subscription_type': None,
        'availability': 'unknown',
        'requires_prime': False,
        'is_free': False,
    }
    
    # Check for known channel names
    channel_patterns = {
        'NBA League Pass': 'nba_league_pass',
        'Peacock': 'peacock',
        'DAZN': 'dazn',
        'FOX One': 'fox_one',
        'FanDuel': 'fanduel',
        'ViX Premium': 'vix_premium',
        'ViX': 'vix',
        'Max': 'max',
    }
    
    for name, code in channel_patterns.items():
        if name in text or name.lower() in text.lower():
            result['channel_id'] = code
            result['channel_name'] = name
            result['subscription_type'] = 'CHANNEL'
            result['availability'] = 'requires_subscription'
            break
    
    # Check for free/entitled - but be VERY specific about "with ads"
    # Don't just search entire page text, look for specific patterns
    if 'Entitled' in text or 'entitled' in text:
        # Only mark as "Free with Ads" if we see explicit free-with-ads messaging
        # NOT just because "with ads" appears somewhere on the page
        # Look for patterns like "Watch free with ads" or "Free (with ads)"
        text_lower = text.lower()
        
        # Specific patterns that indicate truly free content
        free_with_ads_patterns = [
            'watch free with ads',
            'free (with ads)',
            'free with ads',
            'stream free with ads',
        ]
        
        is_free_with_ads = any(pattern in text_lower for pattern in free_with_ads_patterns)
        
        if is_free_with_ads:
            result['channel_id'] = 'prime_free'
            result['channel_name'] = 'Free with Ads'
            result['is_free'] = True
        else:
            # Default to "Included with Prime" for Prime exclusives
            result['channel_id'] = 'prime_included'
            result['channel_name'] = 'Included with Prime'
        result['requires_prime'] = True
        result['subscription_type'] = 'PRIME'
        result['availability'] = 'entitled'
        result['success'] = True
        return result
    
    # If we found a channel, mark as success
    if result['channel_id']:
        result['success'] = True
        return result
    
    # No channel info found
    result['error'] = 'NO_CHANNEL_REGEX'
    result['error_detail'] = 'Could not extract channel info from text'
    return result


def extract_provider_info(data: Dict[str, Any], main_gti: str) -> Dict[str, Any]:
    """Extract channel/subscription info from Amazon page JSON"""
    
    result = {
        'channel_id': None,
        'channel_name': None,
        'subscription_type': None,
        'availability': 'unknown',
        'entitlement_type': 'Unknown',
        'page_id': None,
        'requires_prime': False,
        'is_free': False,
        'unavailable_message': None,
    }
    
    try:
        action_atf = data['props']['body'][0]['props']['atf']['state']['action']['atf']
    except (KeyError, IndexError, TypeError):
        result['error'] = 'PARSE_ERROR'
        result['error_detail'] = 'Invalid JSON structure'
        return result
    
    if not isinstance(action_atf, dict):
        result['error'] = 'PARSE_ERROR'
        result['error_detail'] = 'action.atf is not a dict'
        return result
    
    available_ids = list(action_atf.keys())
    if not available_ids:
        result['error'] = 'NO_JSON'
        result['error_detail'] = 'No IDs in action.atf'
        return result
    
    # Use main_gti if available, otherwise use first ID
    page_id = available_ids[0]
    if main_gti in action_atf:
        page_id = main_gti
    
    result['page_id'] = page_id
    action = action_atf[page_id]
    
    if not isinstance(action, dict):
        result['error'] = 'NO_JSON'
        result['error_detail'] = 'Invalid action structure'
        return result
    
    messages = action.get('messages', {})
    result['entitlement_type'] = messages.get('entitlementType', 'Unknown')
    
    # Check acquisition actions for subscription requirements
    acq_actions = action.get('acquisitionActions', {})
    
    if isinstance(acq_actions, dict):
        for key in ['primaryWaysToWatch', 'moreWaysToWatch', 'alternateWaysToWatch']:
            if key not in acq_actions:
                continue
            
            watch_options = acq_actions[key]
            
            # Handle both formats:
            # Format 1 (list): [{children: [...]}]
            # Format 2 (object): {children: [...]}
            options_list = []
            if isinstance(watch_options, list):
                options_list = watch_options
            elif isinstance(watch_options, dict) and 'children' in watch_options:
                options_list = [watch_options]
            else:
                continue
            
            if not options_list:
                continue
            
            first_option = options_list[0]
            if not isinstance(first_option, dict) or 'children' not in first_option:
                continue
            
            children = first_option.get('children', [])
            if not children:
                continue
            
            # Navigate through nested children if needed
            acquisition = children[0]
            if not isinstance(acquisition, dict):
                continue
            
            # If first child also has children, go one level deeper
            if 'children' in acquisition and isinstance(acquisition.get('children'), list):
                nested_children = acquisition['children']
                if nested_children and isinstance(nested_children[0], dict):
                    acquisition = nested_children[0]
            
            benefit_id = acquisition.get('benefitId')
            raw_label = acquisition.get('label', '')
            s_type = acquisition.get('sType', 'UNKNOWN')
            
            # Determine channel
            if benefit_id:
                result['channel_id'] = benefit_id
                result['requires_prime'] = True
            elif s_type == 'PRIME':
                result['channel_id'] = 'prime_premium'
                result['requires_prime'] = True
            else:
                result['channel_id'] = 'unknown'
            
            result['channel_name'] = clean_channel_name(raw_label, result['channel_id'])
            result['subscription_type'] = s_type
            result['availability'] = f'{key}_option'
            return result
    
    # Check if entitled (already included/free)
    if result['entitlement_type'] == 'Entitled':
        focus_msg = messages.get('focusMessage', {})
        if isinstance(focus_msg, dict):
            dv_msg = focus_msg.get('dvMessage', {})
            if isinstance(dv_msg, dict):
                focus_text = dv_msg.get('string', '')
                
                if isinstance(focus_text, str) and 'with ads' in focus_text.lower():
                    result['channel_id'] = 'prime_free'
                    result['channel_name'] = 'Free with Ads'
                    result['is_free'] = True
                    result['subscription_type'] = 'PRIME'
                    result['availability'] = 'entitled'
                    return result
        
        result['channel_id'] = 'prime_included'
        result['channel_name'] = 'Included with Prime'
        result['requires_prime'] = True
        result['subscription_type'] = 'PRIME'
        result['availability'] = 'entitled'
        return result
    
    # Check for regional restrictions
    for msg_key in ['focusMessage', 'buyBoxMessage']:
        msg = messages.get(msg_key, {})
        if isinstance(msg, dict):
            dv_msg = msg.get('dvMessage', {})
            if isinstance(dv_msg, dict):
                text = dv_msg.get('string', '')
                
                if isinstance(text, str) and text.strip():
                    text_clean = text.replace('{lineBreak}', ' ').strip()
                    text_lower = text_clean.lower()
                    
                    if ('regional' in text_lower and 'restriction' in text_lower) or \
                       ('unavailable' in text_lower and 'location' in text_lower):
                        result['channel_id'] = 'prime_premium'
                        result['channel_name'] = 'Amazon Prime'
                        result['subscription_type'] = 'PRIME'
                        result['availability'] = 'regional_restriction'
                        result['unavailable_message'] = text_clean
                        return result
    
    result['error'] = 'NO_CHANNEL'
    result['error_detail'] = 'No channel information found in page data'
    return result


def scrape_single_gti_http(
    gti: str,
    url: str,
    title: str,
    timeout_sec: int = 15,
) -> Dict[str, Any]:
    """Scrape a single GTI using fast HTTP requests (no Chrome needed!)"""
    
    try:
        # Keep headers minimal - Amazon blocks requests with too many headers
        headers = {
            'User-Agent': USER_AGENT,
        }
        
        # Make HTTP request
        response = requests.get(url, headers=headers, timeout=timeout_sec)
        http_status = response.status_code
        
        # Check for actual 404
        if http_status == 404:
            return {
                'gti': gti,
                'title': title,
                'error': 'STALE_GTI_404',
                'error_detail': 'GTI no longer exists on Amazon',
                'http_status': 404,
                'success': False
            }
        
        # Check page size - stub pages are typically < 10KB
        page_size = len(response.text)
        is_stub_page = page_size < 10000
        
        # Check if page contains "404" or "not found" indicators
        page_text = response.text.lower()
        has_404_text = '404' in page_text or 'page not found' in page_text or 'page you requested cannot be found' in page_text
        
        # If it's a small stub page with 404 indicators, it's a real 404
        if is_stub_page and has_404_text:
            return {
                'gti': gti,
                'title': title,
                'error': 'STALE_GTI_404',
                'error_detail': 'GTI no longer exists on Amazon (stub page with 404)',
                'http_status': http_status,
                'success': False
            }
        
        # Extract JSON from page source
        scripts_json = extract_scripts_with_gti(response.text)
        
        if not scripts_json:
            # No JSON found - this shouldn't happen based on our test
            # Return error so we can fall back to Chrome if needed
            return {
                'gti': gti,
                'title': title,
                'error': 'NO_SCRIPT_HTTP',
                'error_detail': 'No script tags with GTI found (try Chrome)',
                'success': False,
                'http_status': http_status
            }
        
        # Parse JSON
        result = parse_amazon_json(scripts_json, gti, title)
        result['http_status'] = http_status
        result['method'] = 'http'  # Mark as HTTP scrape
        
        if 'error' not in result:
            result['success'] = True
        else:
            result['success'] = False
        
        return result
        
    except requests.Timeout:
        return {
            'gti': gti,
            'title': title,
            'error': 'TIMEOUT_HTTP',
            'error_detail': f'HTTP request timeout after {timeout_sec}s',
            'success': False
        }
    except requests.RequestException as e:
        return {
            'gti': gti,
            'title': title,
            'error': 'NETWORK_ERROR_HTTP',
            'error_detail': str(e)[:200],
            'success': False
        }
    except Exception as e:
        logger.debug(f"HTTP scrape failed for {gti}: {e}")
        return {
            'gti': gti,
            'title': title,
            'error': 'HTTP_FAILED',
            'error_detail': str(e)[:200],
            'success': False
        }


def scrape_single_gti_with_driver(
    driver: webdriver.Chrome,
    gti: str,
    url: str,
    title: str,
    timeout_sec: int = 15,
) -> Dict[str, Any]:
    """Scrape a single GTI using an existing driver instance (for pooling)"""
    
    try:
        driver.set_page_load_timeout(timeout_sec)
        
        # Navigate
        try:
            driver.get(url)
            http_status = 200  # Selenium doesn't expose HTTP status directly
        except TimeoutException:
            return {
                'gti': gti,
                'title': title,
                'error': 'TIMEOUT',
                'error_detail': f'Page load timeout after {timeout_sec}s',
                'success': False
            }
        except WebDriverException as e:
            error_msg = str(e)
            if 'net::ERR_' in error_msg or 'NS_ERROR_' in error_msg:
                return {
                    'gti': gti,
                    'title': title,
                    'error': 'NETWORK_ERROR',
                    'error_detail': error_msg[:200],
                    'success': False
                }
            raise
        
        # Check page size and 404 indicators (same logic as HTTP)
        page_source = driver.page_source
        page_size = len(page_source)
        is_stub_page = page_size < 10000
        
        page_text = page_source.lower()
        has_404_text = '404' in page_text or 'page not found' in page_text or 'page you requested cannot be found' in page_text
        
        # If it's a small stub page with 404 indicators, it's a real 404
        if is_stub_page and has_404_text:
            return {
                'gti': gti,
                'title': title,
                'error': 'STALE_GTI_404',
                'error_detail': 'GTI no longer exists on Amazon (stub page with 404)',
                'http_status': 404,
                'success': False
            }
        
        time.sleep(2)  # Wait for JS to execute
        
        # Extract JSON from page source
        page_source = driver.page_source
        scripts_json = extract_scripts_with_gti(page_source)
        
        if not scripts_json:
            return {
                'gti': gti,
                'title': title,
                'error': 'NO_SCRIPT',
                'error_detail': 'No script tags with GTI found',
                'success': False
            }
        
        # Parse JSON
        result = parse_amazon_json(scripts_json, gti, title)
        result['http_status'] = http_status
        
        if 'error' not in result:
            result['success'] = True
        else:
            result['success'] = False
        
        return result
        
    except Exception as e:
        logger.error(f"Unexpected error for {gti}: {e}")
        return {
            'gti': gti,
            'title': title,
            'error': 'UNKNOWN',
            'error_detail': str(e)[:200],
            'success': False
        }


def scrape_single_gti(
    gti: str,
    url: str,
    title: str,
    timeout_sec: int = 15,
) -> Dict[str, Any]:
    """Scrape a single GTI using Selenium (creates new driver - for backwards compatibility)"""
    
    driver = None
    
    try:
        driver = make_driver(headless=True)
        return scrape_single_gti_with_driver(driver, gti, url, title, timeout_sec)
        
    except Exception as e:
        logger.error(f"Unexpected error for {gti}: {e}")
        return {
            'gti': gti,
            'title': title,
            'error': 'UNKNOWN',
            'error_detail': str(e)[:200],
            'success': False
        }
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def extract_scripts_with_gti(page_source: str) -> List[str]:
    """Extract script contents that contain GTIs and decode HTML entities"""
    import html
    
    GTI_PATTERN = re.compile(r'amzn1\.dv\.gti\.[0-9a-fA-F-]{36}|B0[A-Z0-9]{8}')
    
    scripts = []
    script_pattern = re.compile(r'<script[^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)
    
    for match in script_pattern.finditer(page_source):
        script_content = match.group(1)
        if GTI_PATTERN.search(script_content):
            # Decode HTML entities (&#34; -> ", etc.)
            decoded_content = html.unescape(script_content)
            scripts.append(decoded_content)
    
    return scripts


def scrape_all_threaded(
    events: List[Dict[str, str]],
    workers: int = 5,
    retry: int = 2,
    http_only: bool = False,
) -> List[Dict[str, Any]]:
    """Scrape all GTIs with multi-pass HTTP retries, then Chrome fallback for stubborn failures"""
    
    completed = {'count': 0, 'lock': threading.Lock()}
    total = len(events)
    
    # Multi-pass strategy: Try HTTP 3 times, then Chrome for failures
    MAX_HTTP_PASSES = 3
    
    # Track which GTIs to process
    remaining_events = events.copy()
    all_results = {}  # gti -> result mapping
    
    # HTTP Passes 1-3
    for http_pass in range(1, MAX_HTTP_PASSES + 1):
        if not remaining_events:
            break
            
        logger.info(f"\n{'='*80}")
        logger.info(f"HTTP PASS {http_pass}/{MAX_HTTP_PASSES} - {len(remaining_events)} GTIs remaining")
        logger.info(f"{'='*80}")
        
        pass_results = scrape_pass_http(
            remaining_events, 
            workers, 
            completed, 
            total
        )
        
        # Separate successes from failures
        new_failures = []
        for result in pass_results:
            gti = result['gti']
            all_results[gti] = result
            
            # Check if this is a real failure that should be retried
            error = result.get('error')
            if not result.get('success') and error not in ['STALE_GTI_404']:
                # Only retry network/bot-detection issues (HTTP 503, timeouts, stub pages)
                # Don't retry parsing failures (NO_JSON, NO_CHANNEL_REGEX) - Chrome won't help
                if error in ['TIMEOUT_HTTP', 'NETWORK_ERROR_HTTP', 'NO_SCRIPT_HTTP', 'HTTP_FAILED']:
                    new_failures.append({
                        'gti': result['gti'],
                        'url': f"https://www.amazon.com/gp/video/detail/{result['gti']}",
                        'title': result['title']
                    })
        
        logger.info(f"Pass {http_pass} complete: {len(pass_results) - len(new_failures)} succeeded, {len(new_failures)} to retry")
        remaining_events = new_failures
        
        # Small delay between passes to avoid triggering rate limits
        if remaining_events and http_pass < MAX_HTTP_PASSES:
            logger.info("Waiting 2 seconds before next pass...")
            import time
            time.sleep(2)
    
    # Chrome fallback for remaining failures (if not in http_only mode)
    if remaining_events and not http_only:
        logger.info(f"\n{'='*80}")
        logger.info(f"CHROME FALLBACK - {len(remaining_events)} stubborn GTIs")
        logger.info(f"{'='*80}")
        
        chrome_results = scrape_pass_chrome(
            remaining_events,
            workers,
            completed,
            total
        )
        
        for result in chrome_results:
            gti = result['gti']
            all_results[gti] = result
    
    elif remaining_events and http_only:
        logger.info(f"\n{len(remaining_events)} GTIs still failed (Chrome disabled with --http-only)")
    
    # Convert results dict to list
    results = [all_results[event['gti']] for event in events]
    
    return results


def scrape_pass_http(
    events: List[Dict[str, str]],
    workers: int,
    completed: Dict,
    total: int
) -> List[Dict[str, Any]]:
    """Single pass of HTTP scraping with threading"""
    
    results = []
    pass_completed = {'count': 0, 'lock': threading.Lock()}
    pass_total = len(events)
    
    def scrape_and_log(event: Dict[str, str]) -> Dict[str, Any]:
        """Scrape a GTI with HTTP and log progress"""
        # Add 3-second delay between requests to avoid rate limiting
        import time
        time.sleep(3)
        
        result = scrape_single_gti_http(
            event['gti'],
            event['url'],
            event['title']
        )
        
        # Log progress - track both global and pass-specific
        with completed['lock']:
            completed['count'] += 1
            global_current = completed['count']
        
        with pass_completed['lock']:
            pass_completed['count'] += 1
            pass_current = pass_completed['count']
        
        # Log every 5th GTI
        status = "✓" if result.get('success') else "✗"
        channel = result.get('channel_name', result.get('error', 'Unknown'))[:30]
        title = result.get('title', 'Unknown')[:35]
        
        if pass_current % 5 == 0 or pass_current == pass_total:
            pct = 100 * global_current / total
            logger.info(f"[{global_current}/{total}] ({pct:.1f}%) {status} ⚡ {title:35} | {channel}")
        
        return result
    
    # Process with thread pool
    with ThreadPoolExecutor(max_workers=workers * 2) as executor:
        futures = [executor.submit(scrape_and_log, event) for event in events]
        
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Thread execution failed: {e}")
    
    return results


def scrape_pass_chrome(
    events: List[Dict[str, str]],
    workers: int,
    completed: Dict,
    total: int
) -> List[Dict[str, Any]]:
    """Chrome fallback pass for stubborn failures"""
    
    results = []
    pass_completed = {'count': 0, 'lock': threading.Lock()}
    pass_total = len(events)
    
    # Create driver pool
    driver_pool = []
    logger.info(f"Creating driver pool with {workers} Chrome instances...")
    
    for i in range(workers):
        try:
            logger.info(f"  Creating driver {i+1}/{workers}...")
            driver = make_driver(headless=True)
            driver_pool.append(driver)
            logger.info(f"  ✓ Driver {i+1}/{workers} ready")
        except Exception as e:
            logger.error(f"Failed to create driver {i+1}/{workers}: {e}")
    
    if not driver_pool:
        logger.error("Failed to create any Chrome drivers - skipping Chrome fallback")
        return results
    
    logger.info(f"✓ Driver pool ready with {len(driver_pool)} instances")
    
    # Semaphore and round-robin index
    pool_semaphore = threading.Semaphore(len(driver_pool))
    driver_index = {'next': 0, 'lock': threading.Lock()}
    
    def get_next_driver():
        """Get next available driver from pool (round-robin)"""
        with driver_index['lock']:
            idx = driver_index['next']
            driver_index['next'] = (idx + 1) % len(driver_pool)
            return driver_pool[idx]
    
    def scrape_and_log_chrome(event: Dict[str, str]) -> Dict[str, Any]:
        """Scrape with Chrome and log progress"""
        with pool_semaphore:
            driver = get_next_driver()
            
            result = scrape_single_gti_with_driver(
                driver,
                event['gti'],
                event['url'],
                event['title']
            )
            result['method'] = 'chrome'
        
        # Log progress - track both global and pass-specific
        with completed['lock']:
            completed['count'] += 1
            global_current = completed['count']
        
        with pass_completed['lock']:
            pass_completed['count'] += 1
            pass_current = pass_completed['count']
        
        status = "✓" if result.get('success') else "✗"
        channel = result.get('channel_name', result.get('error', 'Unknown'))[:30]
        title = result.get('title', 'Unknown')[:35]
        
        if pass_current % 5 == 0 or pass_current == pass_total:
            pct = 100 * global_current / total
            logger.info(f"[{global_current}/{total}] ({pct:.1f}%) {status} 🌐 {title:35} | {channel}")
        
        return result
    
    # Process with thread pool
    try:
        with ThreadPoolExecutor(max_workers=len(driver_pool) * 2) as executor:
            futures = [executor.submit(scrape_and_log_chrome, event) for event in events]
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Chrome scrape failed: {e}")
    finally:
        # Clean up drivers
        logger.info("Closing driver pool...")
        for i, driver in enumerate(driver_pool):
            try:
                driver.quit()
            except Exception as e:
                logger.debug(f"Error closing driver {i+1}: {e}")
    
    return results


def import_to_database(results: List[Dict[str, Any]], db_path: str) -> None:
    """Import scrape results to database"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    now = datetime.now(timezone.utc).isoformat()
    
    inserted = 0
    updated = 0
    stale = 0
    
    for result in results:
        gti = result['gti']
        gti_type = result.get('gti_type', 'unknown')
        http_status = result.get('http_status', 0)
        
        if result.get('error') == 'STALE_GTI_404':
            # Mark as stale
            cursor.execute("""
                INSERT INTO amazon_channels (
                    gti, gti_type, channel_name, is_stale, http_status, 
                    last_scraped_utc, first_seen_utc
                )
                VALUES (?, ?, 'No longer available', 1, 404, ?, ?)
                ON CONFLICT(gti) DO UPDATE SET
                    is_stale = 1,
                    channel_name = 'No longer available',
                    http_status = 404,
                    last_scraped_utc = ?,
                    scrape_attempt_count = scrape_attempt_count + 1
            """, (gti, gti_type, now, now, now))
            stale += 1
        
        elif result.get('success'):
            # Check if this is a new GTI
            cursor.execute("SELECT gti, is_stale FROM amazon_channels WHERE gti = ?", (gti,))
            existing = cursor.fetchone()
            
            is_new = existing is None
            was_stale = existing[1] if existing else False
            
            cursor.execute("""
                INSERT INTO amazon_channels (
                    gti, gti_type, channel_id, channel_name, availability,
                    subscription_type, requires_prime, is_free, unavailable_message,
                    is_stale, http_status, last_scraped_utc, first_seen_utc, 
                    last_available_utc, scrape_attempt_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, 1)
                ON CONFLICT(gti) DO UPDATE SET
                    gti_type = excluded.gti_type,
                    channel_id = excluded.channel_id,
                    channel_name = excluded.channel_name,
                    availability = excluded.availability,
                    subscription_type = excluded.subscription_type,
                    requires_prime = excluded.requires_prime,
                    is_free = excluded.is_free,
                    unavailable_message = excluded.unavailable_message,
                    is_stale = 0,
                    http_status = excluded.http_status,
                    last_scraped_utc = excluded.last_scraped_utc,
                    last_available_utc = excluded.last_available_utc,
                    scrape_attempt_count = scrape_attempt_count + 1
            """, (
                gti,
                gti_type,
                result.get('channel_id'),
                result.get('channel_name'),
                result.get('availability'),
                result.get('subscription_type'),
                1 if result.get('requires_prime') else 0,
                1 if result.get('is_free') else 0,
                result.get('unavailable_message'),
                http_status,
                now,
                now,  # first_seen_utc (will be ignored on conflict)
                now   # last_available_utc
            ))
            
            if is_new:
                inserted += 1
                # Log to history
                cursor.execute("""
                    INSERT INTO amazon_channel_history (gti, channel_id, channel_name, availability, is_stale, change_type)
                    VALUES (?, ?, ?, ?, 0, 'discovered')
                """, (gti, result.get('channel_id'), result.get('channel_name'), result.get('availability')))
            elif was_stale:
                updated += 1
                # Log restoration
                cursor.execute("""
                    INSERT INTO amazon_channel_history (gti, channel_id, channel_name, availability, is_stale, change_type)
                    VALUES (?, ?, ?, ?, 0, 'restored')
                """, (gti, result.get('channel_id'), result.get('channel_name'), result.get('availability')))
            else:
                updated += 1
    
    conn.commit()
    
    logger.info(f"✓ Imported: {inserted} new, {updated} updated, {stale} stale")
    
    # Show stats
    cursor.execute("""
        SELECT 
            channel_name,
            COUNT(*) as count
        FROM amazon_channels
        WHERE is_stale = 0
        GROUP BY channel_name
        ORDER BY count DESC
        LIMIT 10
    """)
    
    logger.info("Top channels:")
    for row in cursor.fetchall():
        logger.info(f"  {row[0]}: {row[1]} GTIs")
    
    conn.close()


# ==============================================================================
# MAIN
# ==============================================================================

def extract_gtis_from_db(db_path: str, max_gtis: Optional[int] = None) -> List[Dict[str, str]]:
    """Extract Amazon GTIs from playables table (future/live events only)
    
    Extracts all unique broadcast GTIs and main GTIs from playables.
    Note: Each event may have multiple playables with different GTIs.
    """
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get current time in UTC
    now = datetime.now(timezone.utc).isoformat()
    
    # Get ALL playables for UPCOMING events only (starts within next 24 hours, hasn't started yet)
    # Amazon typically doesn't publish event pages until ~24 hours before start time
    query = """
        SELECT 
            p.title,
            p.deeplink_play
        FROM playables p
        JOIN events e ON p.event_id = e.id
        WHERE p.provider = 'aiv' 
            AND p.deeplink_play LIKE '%gti=%'
            AND (
                e.start_utc IS NULL 
                OR (
                    datetime(e.start_utc) >= datetime('now')  -- Event hasn't started yet
                    AND datetime(e.start_utc) <= datetime('now', '+24 hours')  -- Starts within 24 hours
                )
            )
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    main_gti_pattern = re.compile(r'[?&]gti=(amzn1\.dv\.gti\.[0-9a-f-]{36})')
    broadcast_gti_pattern = re.compile(r'broadcast=(amzn1\.dv\.gti\.[0-9a-f-]{36})')
    
    events = {}
    used_broadcast = 0
    used_main = 0
    
    for title, deeplink in rows:
        # Try broadcast GTI first (for live events)
        broadcast_match = broadcast_gti_pattern.search(deeplink)
        if broadcast_match:
            gti = broadcast_match.group(1)
            if gti not in events:
                events[gti] = {
                    'gti': gti,
                    'url': f'https://www.amazon.com/gp/video/detail/{gti}',
                    'title': title,
                    'gti_type': 'broadcast'
                }
                used_broadcast += 1
                
                if max_gtis and len(events) >= max_gtis:
                    break
        
        # Also get main GTI if present
        main_match = main_gti_pattern.search(deeplink)
        if main_match:
            gti = main_match.group(1)
            if gti not in events:
                events[gti] = {
                    'gti': gti,
                    'url': f'https://www.amazon.com/gp/video/detail/{gti}',
                    'title': title,
                    'gti_type': 'main'
                }
                used_main += 1
                
                if max_gtis and len(events) >= max_gtis:
                    break
        
        if max_gtis and len(events) >= max_gtis:
            break
    
    # Count how many past events were skipped  
    cursor.execute("""
        SELECT COUNT(*)
        FROM playables p
        JOIN events e ON p.event_id = e.id
        WHERE p.provider = 'aiv' 
            AND p.deeplink_play LIKE '%gti=%'
            AND e.start_utc IS NOT NULL
            AND datetime(e.start_utc) < datetime('now')
    """)
    skipped_past = cursor.fetchone()[0]
    
    conn.close()
    
    if skipped_past > 0:
        logger.info(f"✓ Skipped {skipped_past} past playables")
    
    logger.info(f"✓ Extracted {used_broadcast} broadcast GTIs, {used_main} main GTIs")
    
    return list(events.values())


def main():
    parser = argparse.ArgumentParser(description='Amazon Channel Scraper')
    parser.add_argument('--db', default=DEFAULT_DB, help='Database path')
    parser.add_argument('--workers', type=int, default=5, help='Concurrent workers')
    parser.add_argument('--retry', type=int, default=2, help='Retry attempts')
    parser.add_argument('--max', type=int, help='Max GTIs to scrape')
    parser.add_argument('--refresh', action='store_true', help='Ignore cache')
    parser.add_argument('--no-cache', action='store_true', help='Disable cache')
    parser.add_argument('--bootstrap', action='store_true', help='Force schema update')
    parser.add_argument('--debug-dir', help='Debug output directory')
    parser.add_argument('--http-only', action='store_true', help='Use HTTP only (no Chrome fallback)')
    
    args = parser.parse_args()
    
    # Bootstrap database
    logger.info("="*80)
    logger.info("AMAZON CHANNEL SCRAPER")
    logger.info("="*80)
    bootstrap_database(args.db, force=args.bootstrap)
    
    # Extract GTIs
    logger.info(f"\nExtracting GTIs from {args.db}...")
    events = extract_gtis_from_db(args.db, args.max)
    logger.info(f"✓ Found {len(events)} unique GTIs to scrape")
    
    if not events:
        logger.error("No Amazon events found in database")
        return
    
    # Load cache
    cache = {} if args.no_cache else load_cache()
    
    # Filter cached
    if cache and not args.refresh:
        uncached = [e for e in events if e['gti'] not in cache]
        logger.info(f"✓ {len(cache)} GTIs in cache, {len(uncached)} to scrape")
        events = uncached
    
    if not events:
        logger.info("All GTIs cached, nothing to scrape")
        return
    
    # Scrape
    logger.info(f"\nScraping {len(events)} GTIs with {args.workers} workers...")
    if args.http_only:
        logger.info("Using HTTP only (Chrome fallback disabled)")
    else:
        logger.info("Using fast HTTP requests (Chrome fallback if needed)...")
    results = scrape_all_threaded(events, args.workers, args.retry, http_only=args.http_only)
    
    # Update cache
    if not args.no_cache:
        for result in results:
            result['_cached_at'] = datetime.now(timezone.utc).isoformat()
            cache[result['gti']] = result
        save_cache(cache)
    
    # Import to DB
    logger.info("\nImporting to database...")
    import_to_database(results, args.db)
    
    # Summary
    successful = sum(1 for r in results if r.get('success'))
    failed = len(results) - successful
    http_count = sum(1 for r in results if r.get('method') == 'http')
    chrome_count = sum(1 for r in results if r.get('method') == 'chrome')
    
    logger.info("\n" + "="*80)
    logger.info("SCRAPE COMPLETE")
    logger.info("="*80)
    logger.info(f"Total: {len(results)}")
    logger.info(f"Success: {successful} ({100*successful/len(results):.1f}%)")
    logger.info(f"Failed: {failed}")
    if http_count or chrome_count:
        logger.info(f"Method: ⚡ HTTP {http_count} | 🌐 Chrome {chrome_count}")
    logger.info("="*80)


if __name__ == '__main__':
    main()
