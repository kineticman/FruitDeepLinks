#!/usr/bin/env python3
"""
migrate_amazon_logical_services.py - Update existing Amazon playables with correct logical_service

This script updates all playables with provider='aiv' to have the correct logical_service
based on the amazon_channels and amazon_services mapping.
"""

import sqlite3
import sys
import re
from datetime import datetime

def extract_gti_from_deeplink(deeplink: str):
    """Extract GTI from Amazon deeplink"""
    if not deeplink:
        return None
    
    # Try broadcast GTI first
    match = re.search(r'broadcast=(amzn1\.dv\.gti\.[0-9a-f-]{36})', deeplink)
    if match:
        return match.group(1)
    
    # Fall back to main GTI
    match = re.search(r'[?&]gti=(amzn1\.dv\.gti\.[0-9a-f-]{36})', deeplink)
    if match:
        return match.group(1)
    
    return None


def migrate_amazon_playables(db_path: str):
    """Update all Amazon playables with correct logical_service"""
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    print("="*80)
    print("MIGRATING AMAZON PLAYABLES TO CORRECT LOGICAL SERVICES")
    print("="*80)
    print()
    
    # Get all Amazon playables
    cur.execute("""
        SELECT playable_id, deeplink_play, deeplink_open, logical_service
        FROM playables
        WHERE provider = 'aiv'
    """)
    
    playables = cur.fetchall()
    print(f"Found {len(playables)} Amazon playables")
    print()
    
    updated = 0
    not_found = 0
    already_correct = 0
    
    for playable_id, deeplink_play, deeplink_open, current_logical_service in playables:
        # Extract GTI
        gti = extract_gti_from_deeplink(deeplink_play or deeplink_open)
        
        if not gti:
            not_found += 1
            continue
        
        # Look up logical_service from amazon_channels + amazon_services
        cur.execute("""
            SELECT s.logical_service
            FROM amazon_channels ac
            JOIN amazon_services s ON ac.channel_id = s.amazon_channel_id
            WHERE ac.gti = ? AND ac.is_stale = 0
            LIMIT 1
        """, (gti,))
        
        row = cur.fetchone()
        
        if row and row[0]:
            new_logical_service = row[0]
            
            if new_logical_service != current_logical_service:
                # Update the playable
                cur.execute("""
                    UPDATE playables
                    SET logical_service = ?
                    WHERE playable_id = ?
                """, (new_logical_service, playable_id))
                updated += 1
            else:
                already_correct += 1
        else:
            # No mapping found - leave as aiv_aggregator
            not_found += 1
    
    conn.commit()
    
    print(f"✓ Updated: {updated} playables")
    print(f"✓ Already correct: {already_correct} playables")
    print(f"⚠ No mapping found: {not_found} playables (left as aiv_aggregator)")
    print()
    
    # Show breakdown of logical_services after migration
    cur.execute("""
        SELECT logical_service, COUNT(*) as count
        FROM playables
        WHERE provider = 'aiv'
        GROUP BY logical_service
        ORDER BY count DESC
    """)
    
    print("Logical service breakdown after migration:")
    print("-"*80)
    for logical_service, count in cur.fetchall():
        print(f"  {logical_service or '(null)':30s} {count:4d} playables")
    
    print()
    print("="*80)
    print("MIGRATION COMPLETE")
    print("="*80)
    
    conn.close()


if __name__ == '__main__':
    db_path = sys.argv[1] if len(sys.argv) > 1 else "/app/data/fruit_events.db"
    migrate_amazon_playables(db_path)
