# FruitDeepLinks Naming + Pipeline Overview

This document summarizes the cleanup from legacy *Peacock* naming to generic **FruitDeepLinks** naming, and how the main scripts connect.

---

## 1. Canonical script names

Implementation scripts (what you should use going forward):

- `daily_refresh.py`  
  Orchestrates the full pipeline: scrape → migrate DB → import Apple TV → build lanes → export direct channels → export lanes.

- `multi_scraper.py`  
  Scrapes Apple TV Sports (and Peacock where applicable) and writes `out/multi_scraped.json`.

- `fruit_import_appletv.py`  
  Imports Apple TV events from `multi_scraped.json` into the shared SQLite DB and extracts playables.

- `migrate_add_playables.py`  
  Ensures the database schema (tables + indexes) is up to date, including the `playables` table.

- `fruit_build_lanes.py`  
  Reads events from the DB and builds `lanes` + `lane_events` tables with placeholders.

- `fruit_export_hybrid.py`  
  Exports **direct channels** (non‑virtual) to `out/direct.xml` and `out/direct.m3u`, including filtering support.

- `fruit_export_lanes.py`  
  Exports **virtual lanes** to:
  - `out/multisource_lanes.xml` (XMLTV)
  - `out/multisource_lanes.m3u` (standard M3U)
  - `out/multisource_lanes_chrome.m3u` (Chrome Capture)

- `fruitdeeplinks_server.py`  
  Flask server / API that:
  - Serves the web UI
  - Exposes `/api/filters` + `/api/filters/preferences`
  - Can trigger refresh/apply‑filters flows using the scripts above.

Legacy-compatible wrapper scripts (kept so older calls still work):

- `appletv_to_peacock.py` → delegates to `fruit_import_appletv.py`
- `peacock_build_lanes.py` → delegates to `fruit_build_lanes.py`
- `peacock_export_hybrid.py` → delegates to `fruit_export_hybrid.py`
- `peacock_export_lanes.py` → delegates to `fruit_export_lanes.py`

These wrappers exist only for backward compatibility; new code should call the `fruit_*` scripts directly.

---

## 2. Env vars: FRUIT_* vs legacy PEACOCK_*

### Database path

Canonical (preferred):

- `FRUIT_DB_PATH` → path to `fruit_events.db` (or equivalent).

Fallbacks:

- `PEACOCK_DB_PATH` is still honored **only as a fallback** so existing deployments keep working.

Used in:

- `fruitdeeplinks_server.DB_PATH`
- `fruit_build_lanes.py --db` default
- `fruit_export_hybrid.py --db` default
- `fruit_export_lanes.py --db` default

### Lane planning / channels

Canonical:

- `FRUIT_LANES` → default number of lanes to build.
- `FRUIT_LANE_START_CH` → default starting channel number for lanes.
- `FRUIT_PADDING_MINUTES` → time padding around events.
- `FRUIT_PLACEHOLDER_BLOCK_MINUTES` → placeholder block size.
- `FRUIT_PLACEHOLDER_EXTRA_DAYS` → how far beyond real events to extend placeholders.
- `FRUIT_DAYS_AHEAD` → default horizon for lane planning.

Fallbacks:

- `PEACOCK_LANES`
- `PEACOCK_LANE_START_CH`
- `PEACOCK_PADDING_MINUTES`
- `PEACOCK_PLACEHOLDER_BLOCK_MINUTES`
- `PEACOCK_PLACEHOLDER_EXTRA_DAYS`
- `PEACOCK_DAYS_AHEAD`

Used in:

- `fruit_build_lanes.py` top‑level constants (via `_get_int_env([...], default)`).
- `fruit_build_lanes.py.main()` argument defaults.
- `daily_refresh.py` (lane count for step 4).
- `fruitdeeplinks_server.py` (UI defaults for lane count).

### What stayed out of `.env`

Filtering (enabled services, excluded sports/leagues) is **not** driven by env anymore. It lives entirely in the DB via:

- `filter_integration.py`
- `logical_service_mapper.py`
- `provider_utils.py`
- UI/API endpoints in `fruitdeeplinks_server.py`.

This keeps filters user‑friendly and web‑driven.

---

## 3. Pipeline wiring

### Daily batch: `daily_refresh.py`

1. **Scrape Apple TV Sports**

   - Calls: `multi_scraper.py`
   - Output: `out/multi_scraped.json`

2. **Migrate DB**

   - Calls: `migrate_add_playables.py --db <DB_PATH> --yes`
   - Ensures schema + `playables` table are correct.

3. **Import Apple events**

   - Calls:  
     `fruit_import_appletv.py --apple-json out/multi_scraped.json --fruit-db <DB_PATH>`

4. **Build virtual lanes**

   - Calls:  
     `fruit_build_lanes.py --db <DB_PATH> --lanes <FRUIT_LANES|PEACOCK_LANES|40>`

5. **Export direct channels**

   - Calls:  
     `fruit_export_hybrid.py --db <DB_PATH>`

6. **Export virtual lanes**

   - Calls:  
     `fruit_export_lanes.py --db <DB_PATH> --server-url <SERVER_URL>`

Optional: if `CHANNELS_DVR_IP` is set, `daily_refresh.py` triggers Channels DVR to reload the M3U/XMLTV sources.

### Web UI “Apply filters” path

Inside `fruitdeeplinks_server.py`, the `run_apply_filters()` helper does a **partial refresh**:

- Skips scraping + import.
- Runs only:
  - `fruit_build_lanes.py`
  - `fruit_export_hybrid.py`
  - `fruit_export_lanes.py`

This regenerates the exports based on updated filter preferences without hitting Apple again.

---

## 4. CLI + compatibility notes

### `fruit_import_appletv.py`

- New args:
  - `--apple-json` (required)
  - `--fruit-db` (preferred)
  - `--peacock-db` (deprecated, still supported)
  - `--dry-run`

Logic:

- Chooses DB path from `--fruit-db` if provided, else `--peacock-db`.
- Raises an argument error if neither is supplied.

### Legacy wrappers

Each legacy `peacock_*` script now looks like:

```python
#!/usr/bin/env python3
"""Legacy wrapper – delegates to fruit_* script."""

from fruit_build_lanes import main  # or the appropriate fruit_* module

if __name__ == "__main__":
    raise SystemExit(main())
```

So:

- Any old automation or manual invocations that reference the `peacock_*` filenames keep working.
- All new docs and code should use `fruit_*` names.

---

## 5. Mental model

- **DB is global** (`fruit_events.db`), shared by everything.
- **Scrapers** (currently Apple/Peacock via `multi_scraper.py`) populate that DB.
- **Import/normalize** happens in `fruit_import_appletv.py`.
- **Lane planning** is handled by `fruit_build_lanes.py` using FRUIT_* envs (with PEACOCK_* fallback).
- **Exports** are handled by `fruit_export_hybrid.py` + `fruit_export_lanes.py`.
- **Filtering** is entirely DB/UI‑based and reused by the exports.

When in doubt, think:

> Apple/Peacock scraping is just one *source*;  
> **FruitDeepLinks** is the unified DB + lanes + exports on top of it.
