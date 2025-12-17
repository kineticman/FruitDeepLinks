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
        print(f"✔ Step {step_num} complete")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✖ Step {step_num} FAILED with exit code {e.returncode}")
        return False


def main():
    start_time = datetime.now()
    print("\n" + "=" * 60)
    print("FruitDeepLinks Daily Refresh")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Total steps in this pipeline
    total_steps = 12

    # Check for --skip-scrape flag
    skip_scrape = "--skip-scrape" in sys.argv

    # Step 1: Scrape Apple TV Sports
    if skip_scrape:
        print("\n" + "=" * 60)
        print(f"[1/{total_steps}] Scraping Apple TV Sports. SKIPPED")
        print("=" * 60)
        multi_scraped = OUT_DIR / "multi_scraped.json"
        if not multi_scraped.exists():
            print(f"ERROR: --skip-scrape set but {multi_scraped} not found")
            return 1
    else:
        if not run_step(1, total_steps, "Scraping Apple TV Sports", [
            "python3", "multi_scraper.py",
            "--headless",
            "--out", str(OUT_DIR / "multi_scraped.json"),
        ]):
            return 1

    # Step 2: Scrape Kayo Sports
    kayo_days = os.getenv("KAYO_DAYS", "7")
    if not run_step(2, total_steps, f"Scraping Kayo Sports ({kayo_days} days)", [
        "python3", "kayo_scrape.py",
        "--out", str(OUT_DIR / "kayo_raw.json"),
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
            print(f"✔ Created new DB at {DB_PATH}")
        except Exception as e:
            print(f"✖ Failed to create DB at {DB_PATH}: {e}")
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
    ]):
        return 1

    # Step 5: Ensure database schema (adb_lanes table)
    if not run_step(5, total_steps, "Ensuring database schema (adb_lanes table)", [
        "python3", "migrate_add_adb_lanes.py",
    ]):
        return 1

    # Step 6: Import Apple TV events (reads multi_scraped.json directly)
    if not run_step(6, total_steps, "Importing Apple TV events to database", [
        "python3", "fruit_import_appletv.py",
        "--apple-json", str(OUT_DIR / "multi_scraped.json"),
        "--fruit-db", str(DB_PATH),
    ]):
        return 1

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

    # Step 8: Build virtual lanes (Channels-style direct lanes)
    lanes = os.getenv("FRUIT_LANES", os.getenv("PEACOCK_LANES", "40"))
    if not run_step(8, total_steps, f"Building {lanes} virtual lanes", [
        "python3", "fruit_build_lanes.py",
        "--db", str(DB_PATH),
        "--lanes", lanes,
    ]):
        return 1

    # Step 9: Export direct channels (primary XML/M3U)
    if not run_step(9, total_steps, "Exporting Direct channels", [
        "python3", "fruit_export_hybrid.py",
        "--db", str(DB_PATH),
    ]):
        return 1

    # Step 10: Export virtual lanes (existing hybrid lane view)
    server_url = os.getenv("SERVER_URL", "http://192.168.86.80:6655")
    if not run_step(10, total_steps, "Exporting Virtual Lanes", [
        "python3", "fruit_export_lanes.py",
        "--db", str(DB_PATH),
        "--server-url", server_url,
    ]):
        return 1

    # Step 11: Build ADB lanes per provider (adb_lanes table)
    if not run_step(11, total_steps, "Building ADB lanes per provider", [
        "python3", "fruit_build_adb_lanes.py",
        "--db", str(DB_PATH),
    ]):
        return 1

    # Step 12: Export ADB XMLTV + M3U playlists
    server_url = os.getenv("SERVER_URL", "http://192.168.86.80:6655")
    if not run_step(12, total_steps, "Exporting ADB lanes XMLTV and M3U", [
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
            print("  ✔ M3U playlist refreshed")

            time.sleep(2)

            # XMLTV refresh
            subprocess.run([
                "curl", "-s", "-X", "PUT",
                f"http://{channels_ip}:8089/dvr/lineups/XMLTV-{channels_source_name}",
                "-o", "/dev/null",
            ], check=False)
            print("  ✔ XMLTV guide refreshed")
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

