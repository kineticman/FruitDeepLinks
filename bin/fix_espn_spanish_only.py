#!/usr/bin/env python3
"""
fix_espn_spanish_only.py - Fix ESPN playables that only have Spanish broadcasts

For ESPN events where Apple TV only provides Spanish playables (es_MX locale),
this script updates the deeplink to use the externalId instead of the Spanish
punchoutUrl playID. This allows the ESPN app to launch the main English broadcast.

Usage:
  python fix_espn_spanish_only.py --db data/fruit_events.db
  python fix_espn_spanish_only.py --db data/fruit_events.db --dry-run
"""

import argparse
import sqlite3
from pathlib import Path
from typing import List, Tuple


def find_spanish_only_events(conn: sqlite3.Connection) -> List[Tuple]:
    """
    Find ESPN events that only have Spanish playables (no English alternatives).
    
    Returns: List of (event_id, playable_id, deeplink_play, service_name, title, espn_graph_id, raw_attributes_json) tuples
    """
    cur = conn.cursor()
    
    # Find events with ESPN playables
    # Note: external_id is stored in events.raw_attributes_json, not in playables table
    cur.execute("""
        WITH event_locales AS (
            SELECT 
                event_id,
                COUNT(CASE WHEN locale = 'es_MX' THEN 1 END) as spanish_count,
                COUNT(CASE WHEN locale = 'en_US' OR locale IS NULL THEN 1 END) as english_count
            FROM playables
            WHERE logical_service IN ('espn_plus', 'espn_linear')
            GROUP BY event_id
        )
        SELECT 
            p.event_id,
            p.playable_id,
            p.deeplink_play,
            p.service_name,
            p.title,
            p.espn_graph_id,
            e.raw_attributes_json
        FROM playables p
        JOIN event_locales el ON p.event_id = el.event_id
        JOIN events e ON p.event_id = e.id
        WHERE p.logical_service IN ('espn_plus', 'espn_linear')
          AND el.spanish_count > 0
          AND el.english_count = 0
          AND p.locale = 'es_MX'
          AND e.raw_attributes_json IS NOT NULL
        ORDER BY p.event_id, p.priority
    """)
    
    return cur.fetchall()


def fix_spanish_only_playables(
    conn: sqlite3.Connection, 
    playables: List[Tuple], 
    dry_run: bool = False
) -> int:
    """
    Update deeplinks for Spanish-only playables to use ESPN Graph ID or externalId.
    
    Priority:
    1. espn_graph_id (if enriched by fruit_enrich_espn.py)
    2. externalId from raw_attributes_json (fallback if no ESPN Graph data)
    
    Args:
        conn: Database connection
        playables: List of (event_id, playable_id, deeplink_play, service_name, title, espn_graph_id, raw_attributes_json)
        dry_run: If True, don't make changes
    
    Returns: Number of playables updated
    """
    if not playables:
        print("✅ No Spanish-only ESPN playables found")
        return 0
    
    import json
    cur = conn.cursor()
    updates = []
    
    for event_id, playable_id, deeplink_play, service_name, title, espn_graph_id, raw_json in playables:
        # Extract current playID from deeplink (if exists)
        current_playid = None
        if deeplink_play and 'playID=' in deeplink_play:
            current_playid = deeplink_play.split('playID=')[1].split('&')[0]
        
        # Extract externalId from raw_attributes_json
        external_id = None
        if raw_json:
            try:
                attrs = json.loads(raw_json)
                playables_dict = attrs.get('playables', {})
                if playables_dict:
                    # Get first playable's externalId
                    first_playable = next(iter(playables_dict.values()), {})
                    external_id = first_playable.get('externalId')
            except:
                pass
        
        if not external_id:
            # Skip if we can't find externalId
            continue
        
        # Determine best playID to use
        # Priority: ESPN Graph ID > externalId
        best_playid = None
        source = None
        
        if espn_graph_id:
            # ESPN Graph ID is in format: espn-watch:PLAYID
            # Extract just the playID
            if espn_graph_id.startswith('espn-watch:'):
                best_playid = espn_graph_id.replace('espn-watch:', '', 1)
                source = "ESPN Graph"
            else:
                # Fallback: use it as-is
                best_playid = espn_graph_id
                source = "ESPN Graph"
        
        if not best_playid:
            # Use externalId as fallback
            best_playid = external_id
            source = "externalId"
        
        # Skip if already using the best playID
        if current_playid == best_playid:
            continue
        
        # Build new deeplink
        new_deeplink = f"sportscenter://x-callback-url/showWatchStream?playID={best_playid}"
        
        if dry_run:
            print(f"\n[DRY RUN] Would update:")
            print(f"  Event: {event_id}")
            print(f"  Title: {title[:60]}...")
            print(f"  Service: {service_name}")
            print(f"  Old playID: {current_playid}")
            print(f"  New playID: {best_playid} (from {source})")
        
        updates.append((new_deeplink, event_id, playable_id))
    
    if not updates:
        print("✅ All Spanish-only playables already using best playID")
        return 0
    
    if dry_run:
        print(f"\n[DRY RUN] Would update {len(updates)} playables")
        return len(updates)
    
    # Apply updates
    cur.executemany("""
        UPDATE playables
        SET deeplink_play = ?
        WHERE event_id = ? AND playable_id = ?
    """, updates)
    conn.commit()
    
    print(f"✅ Updated {len(updates)} Spanish-only playables")
    return len(updates)


def show_statistics(conn: sqlite3.Connection) -> None:
    """Show statistics about ESPN playables by locale"""
    cur = conn.cursor()
    
    print("\n" + "="*60)
    print("ESPN Playables Statistics")
    print("="*60)
    
    # Overall locale distribution
    cur.execute("""
        SELECT 
            locale,
            COUNT(*) as count
        FROM playables
        WHERE logical_service IN ('espn_plus', 'espn_linear')
        GROUP BY locale
    """)
    
    print("\nLocale distribution:")
    for locale, count in cur.fetchall():
        locale_name = locale if locale else "NULL"
        print(f"  {locale_name}: {count} playables")
    
    # Events with Spanish-only playables
    cur.execute("""
        WITH event_locales AS (
            SELECT 
                event_id,
                COUNT(CASE WHEN locale = 'es_MX' THEN 1 END) as spanish_count,
                COUNT(CASE WHEN locale = 'en_US' OR locale IS NULL THEN 1 END) as english_count
            FROM playables
            WHERE logical_service IN ('espn_plus', 'espn_linear')
            GROUP BY event_id
        )
        SELECT COUNT(*)
        FROM event_locales
        WHERE spanish_count > 0 AND english_count = 0
    """)
    
    spanish_only_count = cur.fetchone()[0]
    print(f"\nEvents with Spanish-only playables: {spanish_only_count}")


def main():
    ap = argparse.ArgumentParser(description="Fix Spanish-only ESPN playables")
    ap.add_argument("--db", default="data/fruit_events.db", help="Path to fruit_events.db")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    ap.add_argument("--stats", action="store_true", help="Show statistics only")
    args = ap.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return 1
    
    conn = sqlite3.connect(db_path)
    
    # Show statistics if requested
    if args.stats:
        show_statistics(conn)
        conn.close()
        return 0
    
    # Find Spanish-only playables
    print("Searching for ESPN events with Spanish-only playables...")
    playables = find_spanish_only_events(conn)
    
    if not playables:
        print("✅ No Spanish-only ESPN playables found")
        show_statistics(conn)
        conn.close()
        return 0
    
    print(f"Found {len(playables)} Spanish-only ESPN playables")
    
    # Fix them
    updated_count = fix_spanish_only_playables(conn, playables, dry_run=args.dry_run)
    
    if updated_count > 0 and not args.dry_run:
        print("\n" + "="*60)
        print("NEXT STEPS:")
        print("="*60)
        print("1. Rebuild lanes to apply changes:")
        print("   python fruit_build_lanes.py --db data/fruit_events.db")
        print("\n2. Re-export to CDVR:")
        print("   python fruit_export_direct.py --db data/fruit_events.db")
    
    show_statistics(conn)
    conn.close()
    return 0


if __name__ == "__main__":
    exit(main())
