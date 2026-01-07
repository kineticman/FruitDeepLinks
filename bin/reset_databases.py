#!/usr/bin/env python3
"""
reset_databases.py - Safely clear databases to start fresh

This script:
1. Backs up existing databases
2. Clears apple_events.db
3. Clears fruit_events.db
4. Preserves auth tokens
5. Shows what was removed
"""
import os
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

# Paths
BIN_DIR = Path(__file__).parent
ROOT_DIR = BIN_DIR.parent
DATA_DIR = ROOT_DIR / "data"
BACKUP_DIR = DATA_DIR / "backups"

APPLE_DB = DATA_DIR / "apple_events.db"
FRUIT_DB = DATA_DIR / "fruit_events.db"
ESPN_DB = DATA_DIR / "espn_graph.db"
APPLE_AUTH = DATA_DIR / "apple_uts_auth.json"

def get_db_stats(db_path: Path) -> dict:
    """Get statistics from database before deletion"""
    if not db_path.exists():
        return {"exists": False}
    
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        stats = {"exists": True, "size_mb": db_path.stat().st_size / 1024 / 1024}
        
        # Get table counts
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]
        
        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                stats[f"{table}_count"] = count
            except:
                pass
        
        conn.close()
        return stats
    except Exception as e:
        return {"exists": True, "error": str(e)}

def backup_database(db_path: Path, backup_dir: Path) -> bool:
    """Backup database before deletion"""
    if not db_path.exists():
        return True
    
    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"{db_path.stem}_{timestamp}.db"
        shutil.copy2(db_path, backup_path)
        print(f"  ✓ Backed up to: {backup_path.name}")
        return True
    except Exception as e:
        print(f"  ✗ Backup failed: {e}")
        return False

def main():
    print("\n" + "=" * 60)
    print("DATABASE RESET - Start Fresh")
    print("=" * 60)
    print()
    
    # Check what exists
    print("Current database status:")
    print("-" * 60)
    
    apple_stats = get_db_stats(APPLE_DB)
    fruit_stats = get_db_stats(FRUIT_DB)
    espn_stats = get_db_stats(ESPN_DB)
    auth_exists = APPLE_AUTH.exists()
    
    if apple_stats["exists"]:
        print(f"apple_events.db: {apple_stats.get('size_mb', 0):.1f} MB")
        if "apple_events_count" in apple_stats:
            print(f"  Events: {apple_stats['apple_events_count']}")
    else:
        print("apple_events.db: Not found")
    
    if fruit_stats["exists"]:
        print(f"fruit_events.db: {fruit_stats.get('size_mb', 0):.1f} MB")
        if "events_count" in fruit_stats:
            print(f"  Events: {fruit_stats['events_count']}")
        if "playables_count" in fruit_stats:
            print(f"  Playables: {fruit_stats['playables_count']}")
    else:
        print("fruit_events.db: Not found")
    
    if espn_stats["exists"]:
        print(f"espn_graph.db: {espn_stats.get('size_mb', 0):.1f} MB")
        if "events_count" in espn_stats:
            print(f"  Events: {espn_stats['events_count']}")
    else:
        print("espn_graph.db: Not found")
    
    print(f"apple_uts_auth.json: {'EXISTS' if auth_exists else 'Not found'}")
    
    # Confirm
    print()
    print("=" * 60)
    print("⚠️  WARNING: This will DELETE all scraped data!")
    print("=" * 60)
    print()
    print("This script will:")
    print("  1. Backup existing databases to data/backups/")
    print("  2. Delete apple_events.db (Apple TV scraped events)")
    print("  3. Delete fruit_events.db (master aggregated events)")
    print("  4. Delete espn_graph.db (ESPN Watch Graph data)")
    print("  5. PRESERVE apple_uts_auth.json (auth tokens)")
    print()
    
    response = input("Type 'YES' to proceed: ")
    
    if response.strip().upper() != "YES":
        print("\n✗ Cancelled - no changes made")
        return 0
    
    print()
    print("=" * 60)
    print("Starting database reset...")
    print("=" * 60)
    print()
    
    # Backup
    print("Step 1: Backing up databases...")
    backup_success = True
    
    if apple_stats["exists"]:
        if not backup_database(APPLE_DB, BACKUP_DIR):
            backup_success = False
    
    if fruit_stats["exists"]:
        if not backup_database(FRUIT_DB, BACKUP_DIR):
            backup_success = False
    
    if espn_stats["exists"]:
        if not backup_database(ESPN_DB, BACKUP_DIR):
            backup_success = False
    
    if not backup_success:
        print("\n✗ Backup failed - aborting reset")
        return 1
    
    print()
    
    # Delete databases
    print("Step 2: Deleting databases...")
    
    deleted_count = 0
    
    if APPLE_DB.exists():
        try:
            APPLE_DB.unlink()
            print(f"  ✓ Deleted: apple_events.db")
            deleted_count += 1
        except Exception as e:
            print(f"  ✗ Failed to delete apple_events.db: {e}")
    
    if FRUIT_DB.exists():
        try:
            FRUIT_DB.unlink()
            print(f"  ✓ Deleted: fruit_events.db")
            deleted_count += 1
        except Exception as e:
            print(f"  ✗ Failed to delete fruit_events.db: {e}")
    
    if ESPN_DB.exists():
        try:
            ESPN_DB.unlink()
            print(f"  ✓ Deleted: espn_graph.db")
            deleted_count += 1
        except Exception as e:
            print(f"  ✗ Failed to delete espn_graph.db: {e}")
    
    # Clear import stamp
    import_stamp = DATA_DIR / ".apple_import_stamp.json"
    if import_stamp.exists():
        try:
            import_stamp.unlink()
            print(f"  ✓ Deleted: .apple_import_stamp.json")
        except:
            pass
    
    print()
    
    # Preserve auth
    if auth_exists:
        print("Step 3: Preserving auth tokens...")
        print(f"  ✓ Kept: apple_uts_auth.json (tokens preserved)")
    else:
        print("Step 3: Auth tokens...")
        print(f"  ⚠ No auth tokens found - you'll need to run multi_scraper.py")
    
    print()
    print("=" * 60)
    print("✓ Database reset complete!")
    print("=" * 60)
    print()
    print(f"Databases deleted: {deleted_count}")
    print(f"Backups saved to: {BACKUP_DIR}")
    print()
    print("Ready to start fresh with hybrid scraper!")
    print()
    print("Next steps:")
    print("  1. Run: python3 daily_refresh.py")
    print("  2. Watch hybrid performance stats")
    print("  3. See 10x speedup on Step 1b (shelf upgrade)")
    print()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
