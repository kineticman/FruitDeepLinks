# FruitDeepLinks HTTP API Guide

This document describes the HTTP API provided by the FruitDeepLinks server.
It is intended for developers building dashboards, automation, or integrations
(Channels DVR, Chrome Capture, scripts, etc.) on top of FruitDeepLinks.

Base URL examples (adjust host/port as needed):

- Docker / LAN: `http://192.168.86.80:6655`
- Local dev:     `http://localhost:6655`

All endpoints return JSON unless otherwise noted. Many support CORS so you can
call them directly from a browser-based UI.

---

## 1. Health & Status

### 1.1 `GET /health`

Lightweight health probe for container orchestrators, Portainer, etc.

**Response example:**

```json
{
  "ok": true,
  "status": "healthy",
  "database": "ok",
  "timestamp": "2025-12-07T01:40:48"
}
```

Use this for readiness / liveness checks. It does **not** trigger any work.

---

### 1.2 `GET /api/status`

Detailed status for dashboards and troubleshooting. Includes:

- Database presence and basic stats
- Output file info (XMLTV / M3U names, sizes, timestamps)
- Last manual refresh result
- Auto-refresh configuration and last run (if enabled)

**Typical fields:**

```json
{
  "ok": true,
  "database": {
    "exists": true,
    "event_count": 1234,
    "lane_event_count": 230,
    "lane_count": 15
  },
  "output": {
    "xmltv": {
      "direct": {
        "path": "out/direct.xml",
        "size_bytes": 123456,
        "mtime": "2025-12-07T00:15:00"
      },
      "lanes": {
        "path": "out/lanes.xml",
        "size_bytes": 234567,
        "mtime": "2025-12-07T00:15:01"
      }
    },
    "m3u": {
      "direct": { "...": "..." },
      "lanes":  { "...": "..." }
    }
  },
  "refresh": {
    "running": false,
    "last_run_started": "2025-12-06T23:59:00",
    "last_run_finished": "2025-12-07T00:05:32",
    "last_run_ok": true,
    "last_run_message": "Completed successfully"
  },
  "auto_refresh": {
    "enabled": true,
    "time": "02:30",
    "next_run": "2025-12-08T02:30:00"
  }
}
```

Exact keys may vary slightly by version but the structure is stable.

---

## 2. XMLTV & M3U Outputs

> Note: these endpoints serve files that are already generated on disk.
> They **do not** trigger a refresh. Use `/api/refresh` or `/api/apply-filters`
> (if exposed in this build) to regenerate outputs.

### 2.1 XMLTV

- `GET /xmltv/direct`  Primary “direct” XMLTV guide (no per-lane metadata).

- `GET /xmltv/lanes`  Lane-based XMLTV guide (each virtual channel represented as a lane).

- `GET /out/<filename>`  Raw file download from the `out/` directory (debug / ad-hoc use).

### 2.2 M3U

- `GET /m3u/direct`  Primary M3U playlist. This is typically what Channels DVR should use.

- `GET /m3u/lanes`  Lane-based M3U, where each lane is represented as a separate channel.

---

## 3. Filters & Preferences

Filters allow you to control which services, sports, and leagues are included
when building lanes / outputs. Internally, the server shares logic with the
CLI exporters (`peacock_export_hybrid.py`, `filter_integration.py`).

### 3.1 `GET /api/filters`

Returns:

- Available values:
  - `available_services` (logical providers seen in DB)
  - `available_sports`
  - `available_leagues`
- Current user preferences (enabled/disabled sets).

**Example:**

```json
{
  "ok": true,
  "available_services": ["sportscenter", "peacock_web", "aiv", "pplus"],
  "available_sports": ["Baseball", "Basketball", "Soccer"],
  "available_leagues": ["NBA", "WNBA", "MLB", "MLS"],
  "preferences": {
    "enabled_services": ["sportscenter", "peacock_web"],
    "disabled_sports": ["Tennis"],
    "disabled_leagues": ["Friendly"]
  }
}
```

---

### 3.2 `GET /api/filters/preferences`

Returns just the current preferences block:

```json
{
  "ok": true,
  "preferences": {
    "enabled_services": ["sportscenter", "peacock_web"],
    "disabled_sports": ["Tennis"],
    "disabled_leagues": ["Friendly"]
  }
}
```

---

### 3.3 `POST /api/filters/preferences`

Updates preferences. Body must be JSON.

**Request body example:**

```json
{
  "enabled_services": ["sportscenter", "peacock_web", "pplus"],
  "disabled_sports": ["Tennis"],
  "disabled_leagues": ["Friendlies"]
}
```

Response mirrors the GET shape with updated preferences.

> Changing preferences **does not automatically rebuild** XMLTV/M3U.
> Call `/api/apply-filters` or the full refresh pipeline afterwards,
> depending on how your instance is configured.

---

### 3.4 `POST /api/apply-filters`  (if available in this build)

Runs the “filters-only” pipeline:

- Reads current DB
- Applies filter rules
- Rebuilds lanes and exports
- Does **not** re-scrape Apple TV

This is a quicker way to iterate on filters without waiting for a full scrape.

Typical response:

```json
{
  "ok": true,
  "started_at": "2025-12-07T00:12:00",
  "finished_at": "2025-12-07T00:12:05",
  "message": "Filters applied and outputs rebuilt"
}
```

---

## 4. Lanes & "What’s On"

Lanes are the virtual channels the M3U/XMLTV exporters produce. This section
exposes lane-level metadata and the "current event" per lane.

Internally, the server consults:

- `lane_events` and `events` tables
- `playables` table
- Filter logic from `filter_integration`

The deeplink resolution order matches the **direct.m3u** exporter.

### 4.1 `GET /api/lanes`

Returns basic info per lane plus the current event (if any).

**Example:**

```json
[
  {
    "lane_id": 1,
    "event_count": 34,
    "current": {
      "event_id": "appletv-umc.cse.6z6unsz5b7bwx9qszdcxzyrod",
      "title": "Notre Dame Fighting Irish at Radford Highlanders",
      "channel_name": "ESPN+",
      "synopsis": "Men's College Basketball",
      "start_utc": "2025-12-07T00:00:00+00:00",
      "end_utc": "2025-12-07T02:35:00+00:00",
      "is_placeholder": 0,
      "lane_id": 1
    }
  },
  {
    "lane_id": 2,
    "event_count": 29,
    "current": null
  }
]
```

Use this to build a “grid” of lanes and see which ones are currently active.

---

### 4.2 `GET /api/lanes/<lane_id>/schedule`

Returns the future schedule of events for a specific lane (including placeholders).

**Example:**

```json
{
  "ok": true,
  "lane_id": 1,
  "events": [
    {
      "event_id": "appletv-...",
      "title": "Notre Dame Fighting Irish at Radford Highlanders",
      "start_utc": "2025-12-07T00:00:00+00:00",
      "end_utc": "2025-12-07T02:35:00+00:00",
      "is_placeholder": 0
    },
    {
      "event_id": "placeholder-...",
      "title": "Sports filler",
      "start_utc": "2025-12-07T02:35:00+00:00",
      "end_utc": "2025-12-07T03:05:00+00:00",
      "is_placeholder": 1
    }
  ]
}
```

Good for detailed per-lane EPG views.

---

## 5. Lane "What’s On" (JSON + TXT)

These endpoints are designed to power **dynamic launchers** and dashboards.
They answer questions like:

- "What is currently on lane 1 right now?"
- "What deep link should I call for lane 1?"

### 5.1 Deeplink Resolution Logic

The server uses the same strategy as the M3U exporter (direct.m3u):

1. Explicit deeplink columns on `events`, if present and non-null.
2. `filter_integration.get_best_deeplink_for_event(conn, event_id, enabled_services)`
   (respects filter preferences and provider priorities).
3. `filter_integration.get_fallback_deeplink(event_row)` based on `raw_attributes_json`.
4. Peacock web deeplink for non-Apple events with `pvid`:
   `https://www.peacocktv.com/deeplink?deeplinkData=...`
5. Apple TV `playables.playable_url` (highest priority row first).
6. Final fallback: `apple_tv_url` from `events.raw_attributes_json`.

The chosen URL is returned in:

- `deeplink_url`
- `deeplink_url_full` (explicit “full” column if present, else same as `deeplink_url`).

---

### 5.2 `GET /whatson/<lane_id>` (JSON)

Returns the current event UID and (optionally) deeplink for a single lane.

**Query parameters:**

- `include=deeplink` – include deeplink fields in JSON.
- `deeplink=1` or `true` or `yes` – synonym for `include=deeplink`.
- `dynamic=1` – also treated as a request to include deeplinks.
- `at=YYYY-MM-DDTHH:MM:SS` – optional timestamp override (defaults to "now").

**Examples:**

```bash
# Minimal JSON: what's on lane 1 (UID only)
curl "http://HOST:6655/whatson/1"

# JSON + deeplink fields
curl "http://HOST:6655/whatson/1?include=deeplink"
```

**JSON response:**

```json
{
  "ok": true,
  "lane": 1,
  "event_uid": "umc.cse.6z6unsz5b7bwx9qszdcxzyrod",
  "deeplink_url": "sportscenter://x-callback-url/showWatchStream?playID=454f6a50-5583-4fcd-b97d-23438516641e&x-source=AppleUMC",
  "deeplink_url_full": "sportscenter://x-callback-url/showWatchStream?playID=454f6a50-5583-4fcd-b97d-23438516641e&x-source=AppleUMC",
  "at": "2025-12-07T01:57:56"
}
```

If no current event exists for that lane at the given time, `event_uid` and
deeplink fields will be `null` (but the status will still be `ok:true`).

---

### 5.3 `GET /whatson/<lane_id>?format=txt` (plain text)

**TXT mode** is intended for scripts, Channels custom sources, Chrome Capture,
or anything that just needs a single string out.

**Query parameters:**

- `format=txt` – use plain text instead of JSON.
- `param=...` – which field to return:
  - `event_uid` (default)
  - `deeplink_url`
  - `deeplink_url_full`
- `at=...` – optional timestamp override, same as JSON mode.

**Examples:**

```bash
# UID in plain text (default)
curl "http://HOST:6655/whatson/1?format=txt"

# Explicit UID
curl "http://HOST:6655/whatson/1?format=txt&param=event_uid"

# Best deeplink URL for this lane
curl "http://HOST:6655/whatson/1?format=txt&param=deeplink_url"

# Full deeplink, if you distinguish it
curl "http://HOST:6655/whatson/1?format=txt&param=deeplink_url_full"
```

**Responses:**

- On success: body is the requested string (no JSON wrapper).  Example:
  ```text
  sportscenter://x-callback-url/showWatchStream?playID=454f6a50-5583-4fcd-b97d-23438516641e&x-source=AppleUMC
  ```

- If there is no current event, or the requested field is missing, the response
  body is empty (`Content-Length: 0`) with HTTP 200.

- On internal error, TXT mode also returns an empty body (to keep shell usage simple).

---

### 5.4 `GET /whatson/all` (JSON)

Returns a snapshot of all lanes at a given moment, including UIDs and optionally
deeplinks.

**Query parameters:**

- `include=deeplink` / `deeplink=1` / `dynamic=1` – include deeplink fields.
- `at=...` – optional timestamp override.

**Example:**

```bash
curl "http://HOST:6655/whatson/all?include=deeplink"
```

**Response:**

```json
{
  "ok": true,
  "at": "2025-12-07T01:40:49",
  "items": [
    {
      "lane": 1,
      "event_uid": "umc.cse.6z6unsz5b7bwx9qszdcxzyrod",
      "deeplink_url": "sportscenter://x-callback-url/showWatchStream?playID=454f6a50-5583-4fcd-b97d-23438516641e&x-source=AppleUMC",
      "deeplink_url_full": "sportscenter://x-callback-url/showWatchStream?playID=454f6a50-5583-4fcd-b97d-23438516641e&x-source=AppleUMC"
    },
    {
      "lane": 2,
      "event_uid": null,
      "deeplink_url": null,
      "deeplink_url_full": null
    }
  ]
}
```

Use this for dashboards that want to render "what’s live right now" across
all virtual channels without making many HTTP calls.

---

## 6. Logs (Optional)

If enabled in this build, the server exposes recent logs and a Server-Sent Events
(SSE) endpoint for live log streaming.

### 6.1 `GET /api/logs`

Query parameters:

- `count` – number of lines to return (default: implementation-specific).

**Example:**

```bash
curl "http://HOST:6655/api/logs?count=200"
```

Returns a JSON array of log lines or a structured object, depending on version.

---

### 6.2 `GET /api/logs/stream` (SSE)

Streams logs as they are generated using SSE. Useful for web UIs that want a
live log pane.

Example (using `curl` for demonstration):

```bash
curl "http://HOST:6655/api/logs/stream"
```

The response is a text/event-stream with chunks like:

```text
event: log
data: 2025-12-07 00:00:00 [INFO] Starting daily refresh...
```

---

## 7. Refresh & Auto-Refresh (If exposed)

Depending on configuration, your instance may expose these:

### 7.1 `POST /api/refresh`

Triggers a refresh pipeline, typically running `bin/daily_refresh.py` in a
background thread. This can do:

- Full scrape + ingest + lane build + export, or
- Filters-only rebuild, depending on parameters.

Typical request body:

```json
{
  "skip_scrape": false
}
```

Check `/api/status` and `/api/logs` for progress.

---

### 7.2 `GET /api/auto-refresh` / `POST /api/auto-refresh`

Get or set a daily auto-refresh schedule managed by APScheduler.

**Example request:**

```json
{
  "enabled": true,
  "time": "02:30"
}
```

---

## 8. Versioning & Compatibility

- This document describes the API as of December 2025, aligned with the
  FruitDeepLinks server that includes:
  - `whatson` endpoints (JSON + TXT)
  - Lane summary / schedule endpoints
  - Shared deeplink selection with `direct.m3u` exporters.
- Backwards compatibility:
  - Existing XMLTV/M3U endpoints remain unchanged.
  - New endpoints are additive and safe to call from external tools.

For changes in future versions, see the project `CHANGELOG.md`.
