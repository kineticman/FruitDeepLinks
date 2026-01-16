#!/usr/bin/env python3
"""
daily_refresh.py - Daily refresh script for FruitDeepLinks
Orchestrates the full pipeline: scrape -> import -> plan -> export
(FIXED: Removed broken parse_events and obsolete peacock_ingest_atom)
"""

import os
import sys
import subprocess
import sqlite3
import time
import json
from pathlib import Path
from datetime import datetime

# Paths
BIN_DIR = Path(__file__).parent
ROOT_DIR = BIN_DIR.parent
OUT_DIR = ROOT_DIR / "out"
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "fruit_events.db"
APPLE_DB_PATH = DATA_DIR / "apple_events.db"
APPLE_AUTH_PATH = DATA_DIR / "apple_uts_auth.json"

# Ensure directories exist
OUT_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

# Apple import stamping (lets --skip-scrape be fast by skipping Step 6 when Apple DB unchanged)
APPLE_IMPORT_STAMP_PATH = DATA_DIR / ".apple_import_stamp.json"

def _load_json(path: Path):
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _apple_db_signature(path: Path):
    try:
        st = path.stat()
        return {"mtime_ns": getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)), "size": st.st_size}
    except Exception:
        return None

def _apple_import_is_fresh(apple_db_path: Path) -> bool:
    """Return True if Step 6 has already imported the current apple_events.db content."""
    sig = _apple_db_signature(apple_db_path)
    if not sig:
        return False
    stamp = _load_json(APPLE_IMPORT_STAMP_PATH)
    if not isinstance(stamp, dict):
        return False
    return (
        stamp.get("apple_db_size") == sig["size"]
        and stamp.get("apple_db_mtime_ns") == sig["mtime_ns"]
    )

def _write_apple_import_stamp(apple_db_path: Path):
    sig = _apple_db_signature(apple_db_path)
    if not sig:
        return
    stamp = {
        "written_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "apple_db_path": str(apple_db_path),
        "apple_db_size": sig["size"],
        "apple_db_mtime_ns": sig["mtime_ns"],
    }
    try:
        APPLE_IMPORT_STAMP_PATH.write_text(json.dumps(stamp, indent=2), encoding="utf-8")
    except Exception:
        # Non-fatal
        pass



def run_step(step_num, total_steps, description, command, allow_fail: bool = False):
    """Run a pipeline step and handle errors"""
    print(f"\n{'=' * 60}")
    print(f"[{step_num}/{total_steps}] {description}")
    print(f"{'=' * 60}")

    try:
        subprocess.run(
            command,
            check=True,
            cwd=BIN_DIR,
            capture_output=False,
        )
        print(f"[OK] Step {step_num} complete")
        return True
    except subprocess.CalledProcessError as e:
        if allow_fail:
            print(f"⚠ Step {step_num} FAILED (non-fatal) with exit code {e.returncode}")
            return False
        print(f"[ERROR] Step {step_num} FAILED with exit code {e.returncode}")
        return False


def _is_nonempty_json_object(path: Path) -> bool:
    """Return True only if path exists and contains a non-empty JSON object."""
    if not path.exists():
        return False
    try:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        return isinstance(obj, dict) and len(obj) > 0
    except Exception:
        return False


def main():
    start_time = datetime.now()
    print("\n" + "=" * 60)
    print("FruitDeepLinks Daily Refresh")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Total steps in this pipeline
    total_steps = 13

    # Check for --skip-scrape flag
    skip_scrape = "--skip-scrape" in sys.argv

    # Fresh-install defensive step: bootstrap Apple UTS auth tokens if missing/invalid.
    # Prevents brand new installs from failing with:
    #   ERROR: No cached auth tokens found ... /app/data/apple_uts_auth.json
    skip_apple = False
    apple_bootstrap_enabled = os.getenv("APPLE_AUTH_BOOTSTRAP", "true").lower() not in ("0", "false", "no")
    if (not skip_scrape) and apple_bootstrap_enabled and (not _is_nonempty_json_object(APPLE_AUTH_PATH)):
        print("\n" + "=" * 60)
        print("Apple auth tokens not found (or invalid); bootstrapping via multi_scraper.py --headless")
        print(f"Target: {APPLE_AUTH_PATH}")
        print("=" * 60)
        ok = run_step("0", total_steps, "Bootstrapping Apple UTS auth tokens", [
            "python3", "multi_scraper.py", "--headless",
        ], allow_fail=True)
        if (not ok) or (not _is_nonempty_json_object(APPLE_AUTH_PATH)):
            print("⚠ Apple auth bootstrap failed; Apple scrape will be skipped this run.")
            print("   You can run: docker exec -it fruitdeeplinks python3 /app/bin/multi_scraper.py --headless")
            skip_apple = True


    # Step 1: Scrape Apple TV Sports (into apple_events.db)
    if skip_scrape or skip_apple:
        print("\n" + "=" * 60)
        print(f"[1/{total_steps}] Scraping Apple TV Sports. SKIPPED")
        print("=" * 60)
        apple_db = DATA_DIR / "apple_events.db"
        if skip_scrape and (not apple_db.exists()):
            print(f"ERROR: --skip-scrape set but {apple_db} not found")
            return 1
    else:
        # Step 1a: Scrape all search terms (HYBRID OPTIMIZED)
        # Uses fast requests library after initial Selenium session
        if not run_step(1, total_steps, "Scraping Apple TV Sports (all terms) - HYBRID MODE", [
            "python3", "apple_scraper_db.py",
            "--headless",
            "--db", str(DATA_DIR / "apple_events.db"),
        ]):
            return 1
        
        # Step 1b: Upgrade all shelf events to full (10x SPEEDUP WITH HYBRID!)
        # This step benefits most from hybrid optimization:
        # - Before: ~500ms per event via Selenium execute_script()
        # - After: ~50ms per event via fast requests library
        # - Typical: 200-300 shelf events = 100s saved (2-3 minutes faster!)
        print("\n" + "=" * 60)
        print(f"[1b/{total_steps}] Upgrading shelf events to full (HYBRID 10x BOOST)")
        print("=" * 60)
        if not run_step("1b", total_steps, "Upgrading all shelf events", [
            "python3", "apple_scraper_db.py",
            "--headless",
            "--skip-seeds",
            "--upgrade-shelf-limit", "9999",
            "--db", str(DATA_DIR / "apple_events.db"),
        ]):
            return 1

    # Step 2: Scrape Kayo Sports
    kayo_days = os.getenv("KAYO_DAYS", "7")
    kayo_json = OUT_DIR / "kayo_raw.json"

    if skip_scrape:
        print("\n" + "=" * 60)
        print(f"[2/{total_steps}] Scraping Kayo Sports ({kayo_days} days). SKIPPED")
        print("=" * 60)
        if not kayo_json.exists():
            print(f"WARNING: --skip-scrape set but {kayo_json} not found; Kayo ingest will be skipped.")
    else:
        if not run_step(2, total_steps, f"Scraping Kayo Sports ({kayo_days} days)", [
            "python3", "kayo_scrape.py",
            "--out", str(kayo_json),
            "--days", kayo_days,
        ]):
            return 1

    # Fresh-install safety: ensure DB file exists before migrations
    if not DB_PATH.exists():
        print("\n" + "=" * 60)
        print("Database file not found; creating new empty database.")
        print("=" * 60)
        try:
            DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            sqlite3.connect(DB_PATH).close()
            print(f"[OK] Created new DB at {DB_PATH}")
        except Exception as e:
            print(f"[ERROR] Failed to create DB at {DB_PATH}: {e}")
            return 1

    # Step 3: Ensure database schema (playables table)
    if not run_step(3, total_steps, "Ensuring database schema (playables table)", [
        "python3", "migrate_add_playables.py",
        "--db", str(DB_PATH),
        "--yes",
    ]):
        return 1

    # Step 4: Ensure database schema (provider_lanes table)
    if not run_step(4, total_steps, "Ensuring database schema (provider_lanes table)", [
        "python3", "migrate_add_provider_lanes.py",
        "--db", str(DB_PATH),
    ]):
        return 1
    
    # Step 5b: Ensure espn_graph_id column exists in playables table
    # (Non-fatal migration that enables ESPN enrichment to store FireTV deeplinks)
    print("\n" + "=" * 60)
    print(f"[5b/{total_steps}] Ensuring database schema (espn_graph_id column)")
    print("=" * 60)
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(playables)")
        columns = [row[1] for row in cur.fetchall()]
        if "espn_graph_id" not in columns:
            cur.execute("ALTER TABLE playables ADD COLUMN espn_graph_id TEXT")
            conn.commit()
            print("[OK] Added espn_graph_id column to playables")
        else:
            print("[OK] espn_graph_id column already exists")
        conn.close()
        print("[OK] Step 5b complete")
    except Exception as e:
        print(f"[WARN] Step 5b failed (non-fatal): {e}")
        # Non-fatal - ESPN enrichment will just skip if column doesn't exist
    
    # Step 5c: Clean up old events (keep database fresh and improve ESPN enrichment rate)
    print("\n" + "=" * 60)
    print(f"[5c/{total_steps}] Cleaning up old events")
    print("=" * 60)
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        # Count events before cleanup
        cur.execute("SELECT COUNT(*) FROM events WHERE end_utc < datetime('now', '-1 day')")
        old_count = cur.fetchone()[0]
        
        if old_count > 0:
            # Delete events that ended before yesterday
            # This removes stale events and improves ESPN enrichment match rate
            cur.execute("DELETE FROM events WHERE end_utc < datetime('now', '-1 day')")
            deleted = cur.rowcount
            conn.commit()
            print(f"[OK] Deleted {deleted} old events (ended before yesterday)")
        else:
            print("[OK] No old events to clean up")
        
        conn.close()
        print("[OK] Step 5c complete")
    except Exception as e:
        print(f"[WARN] Step 5c failed (non-fatal): {e}")
    
    # Step 5d: Ensure locale column exists and populate for ESPN playables
    if not run_step("5d", total_steps, "Ensuring locale column and populating ESPN locales", [
        "python3", "migrate_add_locale.py",
        "--db", str(DB_PATH),
        "--yes",
    ]):
        return 1

    # Step 6: Import Apple TV events (DB-to-DB from apple_events.db)
    # NOTE: Step 6 can be slow (GZIP + JSON parse). If --skip-scrape was used and the Apple DB
    # hasn't changed since the last successful import, we skip this step to keep "Skip Scrape" fast.
    force_apple_import = "--force-apple-import" in sys.argv

    if (skip_scrape or skip_apple) and (not force_apple_import) and _apple_import_is_fresh(APPLE_DB_PATH):
        print("\n" + "=" * 60)
        print(f"[6/{total_steps}] Importing Apple TV events to master database. SKIPPED (apple_events.db unchanged)")
        print("=" * 60)
        print("Step 6 complete (skipped)")
    else:
        if not run_step(6, total_steps, "Importing Apple TV events to master database", [
            "python3", "-u", "fruit_import_appletv.py",
            "--apple-db", str(APPLE_DB_PATH),
            "--fruit-db", str(DB_PATH),
        ]):
            return 1
        _write_apple_import_stamp(APPLE_DB_PATH)
    
    # Step 6a: Clean up old Apple TV events (keep database lean)
    print("\n" + "=" * 60)
    print(f"[6a/{total_steps}] Cleaning up old Apple TV events")
    print("=" * 60)
    try:
        conn = sqlite3.connect(APPLE_DB_PATH)
        cur = conn.cursor()
        
        # Count events before cleanup
        cur.execute("SELECT COUNT(*) FROM apple_events WHERE last_updated < datetime('now', '-5 days')")
        old_count = cur.fetchone()[0]
        
        if old_count > 0:
            # Delete events not updated in 7+ days (likely ended/cancelled)
            cur.execute("DELETE FROM apple_events WHERE last_updated < datetime('now', '-5 days')")
            deleted = cur.rowcount
            conn.commit()
            print(f"Deleted {deleted} old Apple TV events (not updated in 7+ days)")
        else:
            print("No old Apple TV events to clean up")
        
        conn.close()
        print("Step 6a complete")
    except Exception as e:
        print(f"Step 6a failed (non-fatal): {e}")

    # Step 7: Import Kayo events
    kayo_json = OUT_DIR / "kayo_raw.json"
    if kayo_json.exists():
        if not run_step(7, total_steps, "Importing Kayo events to database", [
            "python3", "ingest_kayo.py",
            "--db", str(DB_PATH),
            "--kayo-json", str(kayo_json),
        ]):
            return 1
    else:
        print(f"\n[7/{total_steps}] Kayo data not found at {kayo_json}, skipping ingest")

    # Step 7b: Scrape ESPN Watch Graph (skippable, runs after Apple TV import)
    espn_days = os.getenv("ESPN_DAYS", "14")
    espn_db = DATA_DIR / "espn_graph.db"
    
    if skip_scrape:
        print("\n" + "=" * 60)
        print(f"[7b/{total_steps}] Scraping ESPN Watch Graph ({espn_days} days). SKIPPED")
        print("=" * 60)
        if not espn_db.exists():
            print(f"WARNING: --skip-scrape set but {espn_db} not found; ESPN enrichment will be skipped.")
    else:
        print("\n" + "=" * 60)
        print(f"[7b/{total_steps}] Scraping ESPN Watch Graph ({espn_days} days)")
        print("=" * 60)
        # ESPN scrape is non-fatal - don't stop pipeline if it fails
        run_step("7b", total_steps, f"Scraping ESPN Watch Graph ({espn_days} days)", [
            "python3", "fruit_ingest_espn_graph.py",
            "--db", str(espn_db),
            "--days", espn_days,
        ], allow_fail=True)

    # Step 7b-cleanup: Clean up old ESPN Graph events (after scraping, before enrichment)
    if espn_db.exists():
        print("\n" + "=" * 60)
        print(f"[7b-cleanup/{total_steps}] Cleaning up old ESPN Graph events")
        print("=" * 60)
        try:
            conn = sqlite3.connect(espn_db)
            cur = conn.cursor()
            
            # Count events before cleanup (using stop_utc for ESPN Graph)
            cur.execute("SELECT COUNT(*) FROM events WHERE stop_utc < datetime('now', '-2 days')")
            old_count = cur.fetchone()[0]
            
            if old_count > 0:
                # Delete events that ended before 2 days ago
                # CASCADE automatically removes associated feeds
                cur.execute("DELETE FROM events WHERE stop_utc < datetime('now', '-2 days')")
                deleted = cur.rowcount
                conn.commit()
                print(f"[OK] Deleted {deleted} old ESPN Graph events (and their feeds)")
            else:
                print("[OK] No old ESPN Graph events to clean up")
            
            conn.close()
            print("[OK] Step 7b-cleanup complete")
        except Exception as e:
            print(f"[WARN] Step 7b-cleanup failed (non-fatal): {e}")

    # Step 7c: Enrich ESPN playables with Watch Graph IDs (conditional based on scraping)
    if espn_db.exists():
        print("\n" + "=" * 60)
        print(f"[7c/{total_steps}] Enriching ESPN playables with FireTV deeplinks")
        print("=" * 60)
        
        # Build enrichment command
        espn_enrich_cmd = [
            "python3", "fruit_enrich_espn.py",
            "--fruit-db", str(DB_PATH),
            "--espn-db", str(espn_db),
        ]
        
        # Skip enrichment if no scraping happened (databases unchanged)
        if skip_scrape:
            espn_enrich_cmd.append("--skip-enrich")
        
        # ESPN enrichment is non-fatal - don't stop pipeline if it fails
        run_step("7c", total_steps, "Enriching ESPN playables with FireTV deeplinks", 
                espn_enrich_cmd, allow_fail=True)
    else:
        print("\n" + "=" * 60)
        print(f"[7c/{total_steps}] Enriching ESPN playables. SKIPPED")
        print("=" * 60)
        print(f"ESPN database not found at {espn_db}, skipping enrichment")
    
    # Step 7d: Fix Spanish-only ESPN playables to use externalId
    # This fixes events where Apple TV only provides Spanish broadcasts
    if not run_step("7d", total_steps, "Fixing Spanish-only ESPN playables", [
        "python3", "fix_espn_spanish_only.py",
        "--db", str(DB_PATH),
    ]):
        return 1

        # Step 8: Prefill HTTP deeplinks for any newly-imported playables
    if not run_step("8", total_steps, "Prefilling HTTP deeplinks (http_deeplink_url)", [
        "python3", "migrate_add_adb_lanes.py",
        "--db", str(DB_PATH),
    ]):
        return 1

# Step 9: Build virtual lanes (Channels-style direct lanes)
    lanes = os.getenv("FRUIT_LANES", os.getenv("PEACOCK_LANES", "40"))
    if not run_step(9, total_steps, f"Building {lanes} virtual lanes", [
        "python3", "fruit_build_lanes.py",
        "--db", str(DB_PATH),
        "--lanes", lanes,
    ]):
        return 1

    # Step 10: Export direct channels (primary XML/M3U)
    if not run_step(10, total_steps, "Exporting Direct channels", [
        "python3", "fruit_export_hybrid.py",
        "--db", str(DB_PATH),
    ]):
        return 1

    # Step 11: Export virtual lanes (existing hybrid lane view)
    server_url = os.getenv("SERVER_URL", "http://192.168.86.80:6655")
    if not run_step(11, total_steps, "Exporting Virtual Lanes", [
        "python3", "fruit_export_lanes.py",
        "--db", str(DB_PATH),
        "--server-url", server_url,
    ]):
        return 1

    # Step 12: Build ADB lanes per provider (adb_lanes table)
    if not run_step(12, total_steps, "Building ADB lanes per provider", [
        "python3", "fruit_build_adb_lanes.py",
        "--db", str(DB_PATH),
    ]):
        return 1

    # Step 13: Export ADB XMLTV + M3U playlists
    server_url = os.getenv("SERVER_URL", "http://192.168.86.80:6655")
    if not run_step(13, total_steps, "Exporting ADB lanes XMLTV and M3U", [
        "python3", "fruit_export_adb_lanes.py",
        "--db", str(DB_PATH),
        "--out-dir", str(OUT_DIR),
        "--server-url", server_url,
    ]):
        return 1

    # Force Channels DVR refresh (if configured)
    channels_ip = os.getenv("CHANNELS_DVR_IP")
    channels_source_name = os.getenv("CHANNELS_SOURCE_NAME")

    if channels_ip and channels_source_name:
        print("\n" + "=" * 60)
        print("Forcing Channels DVR refresh.")
        print("=" * 60)

        try:
            # M3U refresh
            subprocess.run([
                "curl", "-s", "-X", "POST",
                f"http://{channels_ip}:8089/providers/m3u/sources/{channels_source_name}/refresh",
                "-o", "/dev/null",
            ], check=False)
            print("  [OK] M3U playlist refreshed")

            time.sleep(2)

            # XMLTV refresh
            subprocess.run([
                "curl", "-s", "-X", "PUT",
                f"http://{channels_ip}:8089/dvr/lineups/XMLTV-{channels_source_name}",
                "-o", "/dev/null",
            ], check=False)
            print("  [OK] XMLTV guide refreshed")
        except Exception as e:
            print(f"  ⚠ Channels DVR refresh failed: {e}")
    elif channels_ip and not channels_source_name:
        print("\n" + "=" * 60)
        print("Skipping Channels DVR refresh: CHANNELS_SOURCE_NAME not set.")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("Skipping Channels DVR refresh: CHANNELS_DVR_IP not set.")
        print("=" * 60)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n" + "=" * 60)
    print("SUCCESS: Refresh complete!")
    print(f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration:.1f} seconds")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
