# FruitDeepLinks ADB Lanes API

This document describes the **ADB Lanes API** used by FruitDeepLinks to serve
deep-links to ADBTuner / Channels DVR via provider-specific M3U files
(e.g. `adb_lanes_sportscenter.m3u`, `adb_lanes_pplus.m3u`).

The goal is simple: for a given _provider_ and _lane number_, return the
best deep-link for the event that is currently playing on that lane, in a
format that is easy for ADBTuner to consume.

---

## Overview

The ADB Lanes API exposes two main endpoints:

1. `GET /api/adb/lanes`  
   Returns the list of ADB-capable providers and their lane configuration
   (backed by the `provider_lanes` table).

2. `GET /api/adb/lanes/<group>/<lane_number>/deeplink`  
   Returns the deeplink for the “now playing” event on a specific lane.

The **`group`** parameter is the provider code (e.g. `sportscenter`,
`pplus`, `aiv`, etc.). The **`lane_number`** is the human-readable lane
number used in M3U channel names and URIs.

The server maps those two values to an internal `lane_id`, finds the current
event in `lane_events`, resolves its deeplink(s) from the events tables, and
returns a single URL string (for text mode) or a JSON document (for debug).

---

## 1. `GET /api/adb/lanes`

### Purpose

Lists all providers that have entries in the `provider_lanes` table, along
with their ADB-related configuration.

This is mainly a **diagnostic / admin** endpoint so you can quickly see
which providers are enabled for ADB lanes and how many lanes each is
configured to use.

### Request

```http
GET /api/adb/lanes
```

No query parameters.

### Response (JSON)

```json
{
  "status": "success",
  "providers": [
    {
      "provider_code": "sportscenter",
      "adb_enabled": 1,
      "adb_lane_count": 10,
      "created_at": "2025-12-09 00:20:24",
      "updated_at": "2025-12-09 00:21:19"
    },
    {
      "provider_code": "pplus",
      "adb_enabled": 1,
      "adb_lane_count": 10,
      "created_at": "2025-12-09 00:20:24",
      "updated_at": "2025-12-09 00:21:19"
    },
    {
      "provider_code": "aiv",
      "adb_enabled": 0,
      "adb_lane_count": 0,
      "created_at": "2025-12-09 00:20:24",
      "updated_at": "2025-12-09 00:21:19"
    }
    // ...
  ]
}
```

#### Fields

- `provider_code`  
  Short string identifying the provider in the database and export scripts
  (e.g. `sportscenter`, `pplus`, `aiv`, `nflctv`, `max`, etc.).

- `adb_enabled`  
  - `1` → ADB lanes are enabled for this provider.
  - `0` → Provider is not currently exposed via the ADB API. Any
    `/api/adb/lanes/<provider>/...` calls will return an empty string in
    text mode (or an error in JSON mode).

- `adb_lane_count`  
  How many lanes are exposed for this provider. This allows you to expose a
  different number of lanes for ADBTuner than you use in your normal
  lane-based exports, if desired.  
  If `adb_lane_count` is `0`, no ADB lanes will be exposed.

---

## 2. `GET /api/adb/lanes/<group>/<lane_number>/deeplink`

### Purpose

Return a **single deep-link URL string** (for ADBTuner) or a JSON object
(for debugging) for the “now playing” event on a given lane.

This endpoint is what the `adb_lanes_*.m3u` files point to.

### Path Parameters

- `group`  
  The provider code / lane “group”. Examples:
  - `sportscenter`
  - `pplus`
  - `aiv`
  - `nflctv`
  - etc.

- `lane_number`  
  The lane number within that group, as used in the M3U filenames and M3U
  channel names. It is mapped 1:1 to the internal `lane_id` (for now).

### Query Parameters

- `format` (optional, default: `text`)  
  Controls the response format.
  - `format=text` or `format=txt`  
    Returns a **plain text string** (no JSON, no quotes) suitable for
    ADBTuner:
    ```text
    sportscenter://x-callback-url/showWatchStream?playChannel=espndeportes&x-source=AppleUMC
    ```
  - `format=json`  
    Returns a JSON document with additional metadata.

- `param` (optional, default: `deeplink_url`)  
  Controls **which field** is returned in text mode. Has no effect in JSON
  mode.
  - `param=deeplink_url` (default)  
    Returns the primary deeplink URL (provider-specific).
  - `param=deeplink_url_full`  
    Returns the “full” or more verbose deeplink (if present); falls back to
    the primary deeplink.
  - `param=event_uid`  
    Returns the Apple event UMC UID (`umc.cse.*`) instead of a URL.

- `at` (optional)  
  Timestamp in ISO format used to choose the “current” event. If omitted,
  the server uses the current UTC time.

  Example:
  ```text
  at=2025-12-09T01:22:25
  ```

### Behavior

1. **Provider gating via `provider_lanes`**

   Before doing any work, the endpoint checks `provider_lanes` for the
   combination of `provider_code=<group>`:

   - If `adb_enabled` is `0` →  
     - `format=text` → respond with an empty body (`""`) and HTTP 200.  
     - `format=json` → respond with `{ "ok": false, "error": "ADB not enabled for provider" }` and HTTP 404.

   - If `adb_lane_count > 0` and `lane_number` is greater than
     `adb_lane_count` → same behavior as above (lane considered out-of-range).

2. **Lane lookup**

   The endpoint maps `lane_number` → `lane_id` 1:1. In the future, this
   could be adjusted to allow more complex mappings, but today lanes are
   aligned numerically.

3. **“Now playing” event selection**

   Using the `lane_id` and `at` timestamp, the server queries `lane_events`
   (and joins against `events`) to find a single event that is active at
   that time:

   ```sql
   SELECT
       le.event_id,
       le.start_utc,
       le.end_utc,
       le.is_placeholder,
       e.title,
       e.channel_name,
       e.synopsis
   FROM lane_events le
   LEFT JOIN events e ON le.event_id = e.id
   WHERE le.lane_id = ?
     AND datetime(le.start_utc) <= datetime(?)
     AND datetime(le.end_utc) > datetime(?)
   ORDER BY le.start_utc DESC
   LIMIT 1;
   ```

4. **Deeplink resolution**

   Once the event row is found, the server uses the existing link helpers
   (`get_event_link_columns`, `get_event_link_info`) to select the
   deeplink(s) for that event:

   - `event_uid` – Apple UMC event ID (`umc.cse.*`)
   - `deeplink_url` – preferred deeplink for ADB / native app launch
   - `deeplink_url_full` – more verbose variant (if the data source
     provides one)

5. **Response formatting**

   - **Text mode (`format=text` or `txt`)**

     Depending on `param`, one of the above fields is returned as a single
     line of text. Example:

     ```http
     GET /api/adb/lanes/sportscenter/1/deeplink?format=text
     ```

     Response:

     ```text
     sportscenter://x-callback-url/showWatchStream?playChannel=espndeportes&x-source=AppleUMC
     ```

     This is exactly what an M3U channel entry expects as its URL.

   - **JSON mode (`format=json`)**

     Returns a structured response, useful for testing:

     ```json
     {
       "ok": true,
       "group": "sportscenter",
       "lane": 1,
       "at": "2025-12-09T01:22:25",
       "event_uid": "umc.cse.3pqzfld77owtpdrgw2vsmxea9",
       "deeplink_url": "sportscenter://x-callback-url/showWatchStream?playChannel=espndeportes&x-source=AppleUMC",
       "deeplink_url_full": "sportscenter://x-callback-url/showWatchStream?playChannel=espndeportes&x-source=AppleUMC"
     }
     ```

---

## Example Usage

### A. SportsCenter ADB lane (lane 1, text mode)

```http
GET /api/adb/lanes/sportscenter/1/deeplink?format=text
```

Response:

```text
sportscenter://x-callback-url/showWatchStream?playChannel=espndeportes&x-source=AppleUMC
```

This is what `adb_lanes_sportscenter.m3u` channels will call, and ADBTuner
will use this deeplink to launch the ESPN app.

### B. SportsCenter ADB lane (JSON debug)

```http
GET /api/adb/lanes/sportscenter/1/deeplink?format=json
```

Response:

```json
{
  "ok": true,
  "group": "sportscenter",
  "lane": 1,
  "at": "2025-12-09T01:22:25",
  "event_uid": "umc.cse.3pqzfld77owtpdrgw2vsmxea9",
  "deeplink_url": "sportscenter://x-callback-url/showWatchStream?playChannel=espndeportes&x-source=AppleUMC",
  "deeplink_url_full": "sportscenter://x-callback-url/showWatchStream?playChannel=espndeportes&x-source=AppleUMC"
}
```

### C. Paramount+ ADB lane (if enabled)

Once `provider_code = "pplus"` is configured with `adb_enabled = 1` and a
non-zero `adb_lane_count`, the pattern is the same:

```http
GET /api/adb/lanes/pplus/1/deeplink?format=text
```

If there is an event currently active on `pplus` lane 1, you will receive a
Paramount+ deeplink URL (e.g. `pplus://...`). If not, the response will be
an empty string in text mode.

---

## M3U Integration

The ADB-specific M3U files (for ADBTuner / Channels DVR) should point each
channel URL at this endpoint. For example, a sportscenter lane might look
like:

```m3u
#EXTM3U
#EXTINF:-1 channel-id="ADB-sportscenter-001" tvg-name="ADB SportsCenter 1" group-title="ADB SportsCenter",ADB SportsCenter 1
http://192.168.86.80:6655/api/adb/lanes/sportscenter/1/deeplink?format=text

#EXTINF:-1 channel-id="ADB-sportscenter-002" tvg-name="ADB SportsCenter 2" group-title="ADB SportsCenter",ADB SportsCenter 2
http://192.168.86.80:6655/api/adb/lanes/sportscenter/2/deeplink?format=text
```

Each time ADBTuner requests the channel URL, the server will:

1. Look up the current event for that lane.
2. Resolve the appropriate deeplink.
3. Return it as a plain text URL.

---

## Enabling ADB Lanes for a New Provider

To turn on ADB lanes for a new provider:

1. **Ensure the provider has events and lanes**  
   The main FruitDeepLinks pipeline must already be ingesting events for
   that provider and assigning them to lanes.

2. **Insert or update a row in `provider_lanes`**  
   Set:
   - `provider_code` = the provider’s code (matches `group` in the API URL)
   - `adb_enabled` = `1`
   - `adb_lane_count` = number of lanes to expose via ADB

3. **Generate an ADB M3U for that provider**  
   Expose URLs in the pattern:
   ```text
   /api/adb/lanes/<provider_code>/<lane_number>/deeplink?format=text
   ```

4. **Add the new M3U to Channels DVR / ADBTuner**  
   Channels will treat each lane as a channel; when it tunes that channel,
   it will hit the API and get the correct deeplink for the current event.

Once those steps are complete, the provider’s lanes become first-class ADB
channels driven entirely by the FruitDeepLinks database and schedule
pipeline.
