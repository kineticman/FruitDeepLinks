#!/usr/bin/env python3
"""
fruit_enrich_espn.py - Enrich Apple TV ESPN events with ESPN Watch Graph IDs

This script matches Apple TV ESPN playables with ESPN Watch Graph events using
the program.id field, then adds espn_graph_id to playables for FireTV deeplinks.

Usage:
  python fruit_enrich_espn.py
  python fruit_enrich_espn.py --fruit-db data/fruit_events.db --espn-db data/espn_graph.db
  python fruit_enrich_espn.py --dry-run
"""

import argparse
import json
import sqlite3
import sys
from typing import Dict, List, Optional


def _log(msg: str) -> None:
    print(msg, flush=True)


def ensure_espn_graph_id_column(fruit_db: str) -> None:
    """Add espn_graph_id column to playables table if it doesn't exist"""
    conn = sqlite3.connect(fruit_db)
    cursor = conn.cursor()
    
    # Check if column exists
    cursor.execute("PRAGMA table_info(playables)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if 'espn_graph_id' not in columns:
        _log("Adding espn_graph_id column to playables table...")
        cursor.execute("ALTER TABLE playables ADD COLUMN espn_graph_id TEXT")
        
        # Add index for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_playables_espn_graph ON playables(espn_graph_id)")
        
        conn.commit()
        _log("✅ Column added successfully")
    else:
        _log("✅ espn_graph_id column already exists")
    
    conn.close()


def get_apple_espn_playables(fruit_db: str) -> List[Dict]:
    """
    Get all ESPN playables from Apple TV with their externalId values.
    
    Returns list of dicts with:
      - event_id: Apple TV event ID
      - playable_id: Playable ID in database
      - external_id: ESPN's program ID (from playables JSON)
      - title: Event title for logging
      - start_utc: Event start time
    """
    conn = sqlite3.connect(fruit_db)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            e.id as event_id,
            e.title,
            e.start_utc,
            e.raw_attributes_json,
            p.playable_id,
            p.service_name
        FROM events e
        JOIN playables p ON e.id = p.event_id
        WHERE p.provider IN ('sportscenter', 'espn', 'espn+')
    """)
    
    results = []
    
    for row in cursor.fetchall():
        event_id = row[0]
        title = row[1]
        start_utc = row[2]
        raw_json = row[3]
        playable_id = row[4]
        
        # Parse raw_attributes to get all externalIds from playables
        if raw_json:
            try:
                raw_attrs = json.loads(raw_json)
                playables_dict = raw_attrs.get('playables', {})
                
                # Each playable has an externalId
                for pid, playable_data in playables_dict.items():
                    if pid == playable_id:  # Match the specific playable
                        external_id = playable_data.get('externalId')
                        if external_id:
                            results.append({
                                'event_id': event_id,
                                'playable_id': playable_id,
                                'external_id': external_id,
                                'title': title,
                                'start_utc': start_utc
                            })
                            break
            except json.JSONDecodeError:
                _log(f"Warning: Could not parse raw_attributes for event {event_id}")
                continue
    
    conn.close()
    
    _log(f"Found {len(results)} Apple TV ESPN playables")
    return results


def get_espn_graph_events(espn_db: str) -> Dict[str, Dict]:
    """
    Get ESPN Watch Graph events indexed by program_id.
    
    Returns dict where key is program_id and value is:
      - id: ESPN Watch Graph ID
      - feed_url: Primary feed URL (contains playback ID)
      - airing_id: ESPN airing ID (may be None)
      - simulcast_airing_id: ESPN simulcast ID (may be None)
      - name: Event name
    """
    try:
        conn = sqlite3.connect(espn_db)
    except sqlite3.OperationalError as e:
        _log(f"❌ Error: Could not open ESPN database: {espn_db}")
        _log(f"   {e}")
        _log("\nMake sure you've run the ESPN scraper first:")
        _log("  python fruit_ingest_espn_graph.py --db data/espn_graph.db --days 7")
        sys.exit(1)
    
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
    if not cursor.fetchone():
        _log(f"❌ Error: No 'events' table found in {espn_db}")
        _log("\nRun the ESPN scraper first to populate the database")
        conn.close()
        sys.exit(1)
    
    cursor.execute("""
        SELECT e.id, e.program_id, e.airing_id, e.simulcast_airing_id, e.title, f.url
        FROM events e
        JOIN feeds f ON e.id = f.event_id
        WHERE e.program_id IS NOT NULL
          AND (e.program_id, f.id) IN (
              SELECT e2.program_id, MIN(f2.id)
              FROM events e2
              JOIN feeds f2 ON e2.id = f2.event_id
              WHERE e2.program_id IS NOT NULL
              GROUP BY e2.program_id
          )
    """)
    
    results = {}
    for row in cursor.fetchall():
        program_id = row[1]
        results[program_id] = {
            'id': row[0],
            'airing_id': row[2],
            'simulcast_airing_id': row[3],
            'title': row[4],
            'feed_url': row[5]  # Feed URL from feeds table
        }
    
    conn.close()
    
    _log(f"Found {len(results)} ESPN Watch Graph events with program_id")
    return results


def enrich_playables(fruit_db: str, espn_db: str, dry_run: bool = False) -> None:
    """
    Match Apple TV ESPN playables with ESPN Watch Graph events.
    Updates playables.espn_graph_id for matched events.
    """
    _log("="*80)
    _log("ESPN ENRICHMENT - Matching Apple TV with ESPN Watch Graph")
    _log("="*80)
    
    # Ensure column exists
    if not dry_run:
        ensure_espn_graph_id_column(fruit_db)
    
    _log("\nStep 1: Loading Apple TV ESPN playables...")
    apple_playables = get_apple_espn_playables(fruit_db)
    
    if not apple_playables:
        _log("⚠️  No ESPN playables found in Apple TV database")
        _log("   Make sure fruit_import_appletv.py has run successfully")
        return
    
    _log("\nStep 2: Loading ESPN Watch Graph events...")
    espn_events = get_espn_graph_events(espn_db)
    
    if not espn_events:
        _log("⚠️  No ESPN events found in ESPN Watch Graph database")
        return
    
    _log("\nStep 3: Matching playables using program.id...")
    _log("-"*80)
    
    conn = sqlite3.connect(fruit_db)
    cursor = conn.cursor()
    
    matched = 0
    unmatched = 0
    updated = 0
    unmatched_details = []  # Track unmatched for debugging
    
    for playable in apple_playables:
        external_id = playable['external_id']
        
        if external_id in espn_events:
            espn_event = espn_events[external_id]
            
            # Extract playback ID from feed URL (the actual working ID!)
            # FROM: https://www.espn.com/watch/player/_/id/187c6919-eb2a-4cd8-9ec5-127b4fb41c8b
            # TO:   187c6919-eb2a-4cd8-9ec5-127b4fb41c8b
            espn_playback_id = None
            
            if espn_event.get('feed_url'):
                try:
                    # Extract playback ID from feed URL
                    feed_url = espn_event['feed_url']
                    if '/id/' in feed_url:
                        espn_playback_id = feed_url.split('/id/')[-1]
                        # Clean any query parameters
                        espn_playback_id = espn_playback_id.split('?')[0].split('#')[0]
                except Exception as e:
                    _log(f"⚠️ Warning: Could not extract playback ID from {feed_url}: {e}")
            
            # Fallback to event ID format if no feed URL (shouldn't happen, but defensive)
            if not espn_playback_id and espn_event.get('id'):
                # Extract middle UUID from espn-watch:UUID:hash format
                try:
                    parts = espn_event['id'].split(':')
                    if len(parts) >= 2:
                        espn_playback_id = parts[1]
                except Exception:
                    pass
            
            if espn_playback_id:
                # Store the PLAYBACK ID, not the event ID!
                # This is the critical fix - use feed URL playback ID
                espn_graph_id = f"espn-watch:{espn_playback_id}"
                
                if dry_run:
                    _log(f"[DRY-RUN] Would match: {playable['title'][:50]}")
                    _log(f"          Apple playable: {playable['playable_id']}")
                    _log(f"          ESPN Graph ID:  {espn_graph_id}")
                else:
                    # Update playable with ESPN Graph ID
                    cursor.execute("""
                        UPDATE playables 
                        SET espn_graph_id = ?
                        WHERE playable_id = ?
                    """, (espn_graph_id, playable['playable_id']))
                    
                    if cursor.rowcount > 0:
                        updated += 1
                
                matched += 1
                
                # Log first 5 matches for verification
                if matched <= 5:
                    _log(f"✅ Match #{matched}: {playable['title'][:60]}")
                    _log(f"   program.id:     {external_id}")
                    _log(f"   ESPN Graph ID:  {espn_graph_id}")
                    _log(f"   FireTV URL:     https://www.espn.com/watch/player/_/id/{espn_graph_id}")
            else:
                _log(f"⚠️  Match found but no usable ESPN ID: {playable['title'][:50]}")
                unmatched += 1
        else:
            unmatched += 1
            unmatched_details.append({
                'title': playable['title'],
                'program_id': external_id,
                'playable_id': playable['playable_id'],
                'start_utc': playable.get('start_utc', 'Unknown')
            })
            
            # Log first 3 unmatched for debugging
            if unmatched <= 3:
                _log(f"❌ No match: {playable['title'][:60]}")
                _log(f"   program.id: {external_id}")
    
    if not dry_run and updated > 0:
        conn.commit()
        _log(f"\n💾 Updated {updated} playables in database")
    
    conn.close()
    
    # Summary
    _log("\n" + "="*80)
    _log("ENRICHMENT SUMMARY")
    _log("="*80)
    _log(f"Total Apple TV ESPN playables: {len(apple_playables)}")
    _log(f"Total ESPN Watch Graph events: {len(espn_events)}")
    _log(f"")
    _log(f"✅ Matched:   {matched} ({matched/len(apple_playables)*100:.1f}%)")
    _log(f"❌ Unmatched: {unmatched} ({unmatched/len(apple_playables)*100:.1f}%)")
    
    if dry_run:
        _log("\n🔍 This was a DRY RUN - no changes were made")
        _log("   Run without --dry-run to update the database")
    else:
        _log(f"\n✅ Successfully enriched {updated} ESPN playables with FireTV-compatible IDs")
    
    # Write unmatched events to file for debugging
    if unmatched_details:
        debug_file = "espn_unmatched_debug.txt"
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("UNMATCHED ESPN EVENTS - DEBUG REPORT\n")
            f.write("="*80 + "\n\n")
            f.write(f"Total unmatched: {len(unmatched_details)}\n")
            f.write(f"Total ESPN Graph events available: {len(espn_events)}\n\n")
            f.write("="*80 + "\n")
            f.write("UNMATCHED EVENTS:\n")
            f.write("="*80 + "\n\n")
            
            for i, event in enumerate(unmatched_details, 1):
                f.write(f"{i}. {event['title']}\n")
                f.write(f"   Start Time: {event['start_utc']}\n")
                f.write(f"   Apple program.id: {event['program_id']}\n")
                f.write(f"   Playable ID: {event['playable_id']}\n\n")
            
            f.write("="*80 + "\n")
            f.write("DEBUGGING TIPS:\n")
            f.write("="*80 + "\n\n")
            f.write("1. Check if these events exist in ESPN Watch Graph:\n")
            f.write(f"   sqlite3 data/espn_graph.db \"SELECT title FROM events WHERE title LIKE '%[event name]%'\"\n\n")
            f.write("2. Check program_id in ESPN database:\n")
            f.write(f"   sqlite3 data/espn_graph.db \"SELECT COUNT(*) FROM events WHERE program_id = '[program_id]'\"\n\n")
            f.write("3. See what ESPN has for similar events:\n")
            f.write(f"   sqlite3 data/espn_graph.db \"SELECT title, program_id FROM events WHERE title LIKE '%football%' LIMIT 10\"\n\n")
        
        _log(f"\n📝 Wrote unmatched events to: {debug_file}")
    
    # Recommendations
    if unmatched > 0:
        _log("\n💡 Tips for improving match rate:")
        _log("   - ESPN Watch Graph might not have all events yet")
        _log("   - Some events might be on different days")
        _log("   - Try running ESPN scraper with more days: --days 14")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich Apple TV ESPN events with ESPN Watch Graph IDs for FireTV deeplinks"
    )
    parser.add_argument(
        "--fruit-db",
        default="data/fruit_events.db",
        help="Path to FruitDeepLinks database (default: data/fruit_events.db)"
    )
    parser.add_argument(
        "--espn-db",
        default="data/espn_graph.db",
        help="Path to ESPN Watch Graph database (default: data/espn_graph.db)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be matched without making changes"
    )
    
    args = parser.parse_args()
    
    enrich_playables(args.fruit_db, args.espn_db, args.dry_run)


if __name__ == "__main__":
    main()
