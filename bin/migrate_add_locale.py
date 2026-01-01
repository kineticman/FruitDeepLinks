#!/usr/bin/env python3
"""
migrate_add_locale.py - Add locale column to playables table

This migration:
1. Adds locale column to playables table (if missing)
2. Populates locale for ESPN playables based on service_name and title
3. No-op if column already exists and is populated
"""

import argparse
import sqlite3
from pathlib import Path


def ensure_locale_column(conn: sqlite3.Connection) -> bool:
    """Add locale column to playables table if it doesn't exist"""
    cur = conn.cursor()
    
    # Check if column exists
    cur.execute("PRAGMA table_info(playables)")
    columns = [row[1] for row in cur.fetchall()]
    
    if 'locale' in columns:
        print("✅ locale column already exists")
        return False
    
    print("Adding locale column to playables table...")
    cur.execute("ALTER TABLE playables ADD COLUMN locale TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playables_locale ON playables(locale)")
    conn.commit()
    print("✅ locale column added successfully")
    return True


def populate_locale_for_espn(conn: sqlite3.Connection) -> int:
    """
    Populate locale column for ESPN playables that are missing it.
    
    Heuristics:
    - service_name contains "Deportes" -> es_MX
    - title contains "En Español" -> es_MX
    - Otherwise -> en_US (default)
    
    Returns: Number of rows updated
    """
    cur = conn.cursor()
    
    # Check if locale column exists
    cur.execute("PRAGMA table_info(playables)")
    columns = {row[1] for row in cur.fetchall()}
    
    if 'locale' not in columns:
        print("⚠️  locale column doesn't exist, skipping population")
        return 0
    
    # Check for required columns (may not exist on fresh installs)
    if 'logical_service' not in columns:
        print("⚠️  logical_service column doesn't exist yet (fresh install?), skipping population")
        return 0
    
    # Check if service_name column exists (added in recent migration)
    has_service_name = 'service_name' in columns
    
    # Build query based on available columns
    if has_service_name:
        query = """
            SELECT event_id, playable_id, service_name, title
            FROM playables
            WHERE logical_service IN ('espn_plus', 'espn_linear')
              AND (locale IS NULL OR locale = '')
        """
    else:
        # Fallback: use title only
        query = """
            SELECT event_id, playable_id, title
            FROM playables
            WHERE logical_service IN ('espn_plus', 'espn_linear')
              AND (locale IS NULL OR locale = '')
        """
    
    try:
        cur.execute(query)
        rows = cur.fetchall()
    except sqlite3.OperationalError as e:
        # Handle case where table exists but is empty or missing columns
        print(f"⚠️  Could not query playables: {e}")
        return 0
    
    if not rows:
        print("✅ All ESPN playables have locale populated (or no ESPN playables yet)")
        return 0
    
    print(f"Found {len(rows)} ESPN playables missing locale")
    
    updates = []
    for row in rows:
        event_id = row[0]
        playable_id = row[1]
        
        if has_service_name:
            service_name = row[2]
            title = row[3]
        else:
            service_name = None
            title = row[2]
        
        # Determine locale
        service_lower = (service_name or "").lower()
        title_lower = (title or "").lower()
        
        if "deportes" in service_lower or "español" in title_lower:
            locale = "es_MX"
        else:
            locale = "en_US"
        
        updates.append((locale, event_id, playable_id))
    
    # Apply updates
    cur.executemany("""
        UPDATE playables
        SET locale = ?
        WHERE event_id = ? AND playable_id = ?
    """, updates)
    conn.commit()
    
    spanish_count = sum(1 for u in updates if u[0] == "es_MX")
    english_count = len(updates) - spanish_count
    
    print(f"✅ Updated {len(updates)} playables:")
    print(f"   - {english_count} marked as English (en_US)")
    print(f"   - {spanish_count} marked as Spanish (es_MX)")
    
    return len(updates)


def main():
    ap = argparse.ArgumentParser(description="Add locale column and populate for ESPN playables")
    ap.add_argument("--db", default="data/fruit_events.db", help="Path to fruit_events.db")
    ap.add_argument("--yes", action="store_true", help="Auto-confirm without prompting")
    args = ap.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return 1
    
    conn = sqlite3.connect(db_path)
    
    # Step 1: Ensure column exists
    column_added = ensure_locale_column(conn)
    
    # Step 2: Populate locale data
    updated_count = populate_locale_for_espn(conn)
    
    conn.close()
    
    if column_added or updated_count > 0:
        print(f"\n✅ Migration complete")
    else:
        print(f"\n✅ No changes needed (already migrated)")
    
    return 0


if __name__ == "__main__":
    exit(main())
