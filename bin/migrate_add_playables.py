#!/usr/bin/env python3
"""
migrate_add_playables.py - Add playables table for multi-punchout support

This migration adds proper storage for multiple deeplinks per event,
enabling per-user service filtering.
"""

import sqlite3
import argparse
from pathlib import Path


def create_playables_table(conn: sqlite3.Connection):
    """Create playables table to store all punchout URLs per event"""
    cur = conn.cursor()
    
    # Playables table - stores multiple streaming options per event
    cur.execute("""
        CREATE TABLE IF NOT EXISTS playables (
            event_id TEXT NOT NULL,
            playable_id TEXT NOT NULL,
            provider TEXT,
            deeplink_play TEXT,
            deeplink_open TEXT,
            playable_url TEXT,
            title TEXT,
            content_id TEXT,
            priority INTEGER DEFAULT 0,
            created_utc TEXT,
            PRIMARY KEY (event_id, playable_id),
            FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
        )
    """)
    
    # Index for fast lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_playables_event_id 
        ON playables(event_id)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_playables_provider 
        ON playables(provider)
    """)
    
    conn.commit()
    print("✓ Created playables table")


def create_user_preferences_table(conn: sqlite3.Connection):
    """Create table for user service preferences"""
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_preferences (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_utc TEXT
        )
    """)
    
    conn.commit()
    print("✓ Created user_preferences table")


def add_default_preferences(conn: sqlite3.Connection):
    """Add default service preferences"""
    cur = conn.cursor()
    
    # Default: all services enabled
    default_services = {
        'enabled_services': '[]',  # Empty = all enabled
        'service_priority': 'sportsonespn,peacock,pplus,aiv,cbssportsapp,nbcsportstve,foxone,fsapp,dazn,vixapp'
    }
    
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    
    for key, value in default_services.items():
        cur.execute("""
            INSERT OR IGNORE INTO user_preferences (key, value, updated_utc)
            VALUES (?, ?, ?)
        """, (key, value, now))
    
    conn.commit()
    print("✓ Added default preferences")


def migrate_existing_data(conn: sqlite3.Connection):
    """Migrate existing pvid data to playables table if needed"""
    cur = conn.cursor()
    
    # Check if events table has raw_attributes_json
    cur.execute("PRAGMA table_info(events)")
    columns = {row[1] for row in cur.fetchall()}
    
    if 'raw_attributes_json' not in columns:
        print("⚠ No raw_attributes_json column - skipping migration")
        return
    
    # Count events with playables data
    cur.execute("""
        SELECT COUNT(*) FROM events 
        WHERE raw_attributes_json IS NOT NULL 
        AND raw_attributes_json != ''
    """)
    event_count = cur.fetchone()[0]
    
    print(f"Found {event_count} events with raw data")
    
    if event_count == 0:
        print("⚠ No events to migrate")
        return
    
    # This will be handled by the import script on next run
    print("ℹ Existing data will be migrated on next refresh")


def main():
    parser = argparse.ArgumentParser(description='Migrate database for multi-punchout support')
    parser.add_argument('--db', required=True, help='Path to database file')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    parser.add_argument('--yes', '-y', action='store_true', help='Skip confirmation prompt')
    args = parser.parse_args()
    
    db_path = Path(args.db)
    
    if not db_path.exists():
        print(f"✗ Database not found: {db_path}")
        return 1
    
    print("="*70)
    print("FruitDeepLinks Database Migration")
    print("Adding Multi-Punchout Support")
    print("="*70)
    print(f"Database: {db_path}")
    print(f"Dry run: {args.dry_run}")
    print()
    
    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
        print()
        print("Would create:")
        print("  - playables table (event_id, playable_id, provider, deeplinks...)")
        print("  - user_preferences table (key, value)")
        print("  - Indexes for fast lookups")
        print()
        return 0
    
    # Backup reminder
    if not args.yes:
        print("⚠ IMPORTANT: Back up your database before running migrations!")
        response = input("Continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Migration cancelled")
            return 0
    else:
        print("⚠ Skipping confirmation (--yes flag provided)")
    
    print()
    
    conn = sqlite3.connect(str(db_path))
    
    try:
        create_playables_table(conn)
        create_user_preferences_table(conn)
        add_default_preferences(conn)
        migrate_existing_data(conn)
        
        print()
        print("="*70)
        print("✓ Migration completed successfully!")
        print("="*70)
        print()
        print("Next steps:")
        print("1. Update appletv_to_peacock.py to populate playables table")
        print("2. Update export scripts to use service filtering")
        print("3. Add admin UI for service selection")
        
        return 0
        
    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        conn.rollback()
        return 1
    finally:
        conn.close()


if __name__ == '__main__':
    import sys
    sys.exit(main())
