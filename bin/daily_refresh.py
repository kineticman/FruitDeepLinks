#!/usr/bin/env python3
"""
daily_refresh.py - Daily refresh script for FruitDeepLinks
Orchestrates the full pipeline: scrape → import → plan → export
(FIXED: Removed broken parse_events and obsolete peacock_ingest_atom)
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

# Paths
BIN_DIR = Path(__file__).parent
ROOT_DIR = BIN_DIR.parent
OUT_DIR = ROOT_DIR / "out"
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "fruit_events.db"

# Ensure directories exist
OUT_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

def run_step(step_num, total_steps, description, command):
    """Run a pipeline step and handle errors"""
    print(f"\n{'='*60}")
    print(f"[{step_num}/{total_steps}] {description}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            command,
            check=True,
            cwd=BIN_DIR,
            capture_output=False
        )
        print(f"✔ Step {step_num} complete")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Step {step_num} FAILED with exit code {e.returncode}")
        return False

def main():
    start_time = datetime.now()
    print("\n" + "="*60)
    print("FruitDeepLinks Daily Refresh")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Check for --skip-scrape flag
    skip_scrape = "--skip-scrape" in sys.argv
    
    # Step 1: Scrape Apple TV Sports
    if skip_scrape:
        print("\n[1/6] Scraping Apple TV Sports... SKIPPED")
        multi_scraped = OUT_DIR / "multi_scraped.json"
        if not multi_scraped.exists():
            print(f"ERROR: --skip-scrape set but {multi_scraped} not found")
            return 1
    else:
        if not run_step(1, 6, "Scraping Apple TV Sports", [
            "python3", "multi_scraper.py",
            "--headless",
            "--out", str(OUT_DIR / "multi_scraped.json")
        ]):
            return 1
    
    # Step 2: Ensure database schema is up to date
    if not run_step(2, 6, "Ensuring database schema (playables table)", [
        "python3", "migrate_add_playables.py",
        str(DB_PATH)
    ]):
        return 1
    
    # Step 3: Import Apple TV events (reads multi_scraped.json directly)
    if not run_step(3, 6, "Importing Apple TV events to database", [
        "python3", "appletv_to_peacock.py",
        "--apple-json", str(OUT_DIR / "multi_scraped.json"),
        "--peacock-db", str(DB_PATH)
    ]):
        return 1
    
    # Step 4: Build virtual lanes
    lanes = os.getenv("PEACOCK_LANES", "40")
    if not run_step(4, 6, f"Building {lanes} virtual lanes", [
        "python3", "peacock_build_lanes.py",
        "--db", str(DB_PATH),
        "--lanes", lanes
    ]):
        return 1
    
    # Step 5: Export direct channels
    if not run_step(5, 6, "Exporting Direct channels", [
        "python3", "peacock_export_hybrid.py",
        "--db", str(DB_PATH)
    ]):
        return 1
    
    # Step 6: Export virtual lanes
    server_url = os.getenv("SERVER_URL", "http://192.168.86.80:6655")
    if not run_step(6, 6, "Exporting Virtual Lanes", [
        "python3", "peacock_export_lanes.py",
        "--db", str(DB_PATH),
        "--server-url", server_url
    ]):
        return 1
    
    # Force Channels DVR refresh (if configured)
    channels_ip = os.getenv("CHANNELS_DVR_IP")
    if channels_ip:
        print("\n" + "="*60)
        print("Forcing Channels DVR refresh...")
        print("="*60)
        try:
            import time
            # M3U refresh
            subprocess.run([
                "curl", "-s", "-X", "POST",
                f"http://{channels_ip}:8089/providers/m3u/sources/appletvdeeper/refresh"
            ], check=False)
            print("  ✔ M3U playlist refreshed")
            
            time.sleep(2)
            
            # XMLTV refresh
            subprocess.run([
                "curl", "-s", "-X", "PUT",
                f"http://{channels_ip}:8089/dvr/lineups/XMLTV-appletvdeeper"
            ], check=False)
            print("  ✔ XMLTV guide refreshed")
        except Exception as e:
            print(f"  ⚠ Channels DVR refresh failed: {e}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "="*60)
    print("SUCCESS: Refresh complete!")
    print(f"Duration: {duration:.1f} seconds")
    print(f"Completed: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
