# Apple Sports Deep-Link Behavior and Live Enrichment Pipeline

## 1. Deeplinks: non‑live vs live behavior

From our experiments with Apple’s sports stack (tv.apple.com + Apple Sports), we’ve seen two distinct phases for events:

1. **Non‑live / far in the future**
   - Schedule/search/browse responses mostly expose:
     - Web URLs (e.g. `https://sports.apple.com/...` or `https://tv.apple.com/...`)
     - Basic event metadata (teams, start time, league, images).
   - App-specific deep links (e.g. `gametime://…`, `aiv://…`) **do not always appear** in the early data we scrape.

2. **Live or near‑live window**
   - When we hit the **UTS sporting-events** endpoint for a **live-ish** event, the payload includes richer “playables”:
     - Multiple **EbsEvent** objects per event, each describing a playable stream variant.
     - Nested **“punchout” / app deep links** for partners like NBA and Prime Video.
   - Example for *Chicago Bulls at Indiana Pacers*:
     - Channels (from `channels` array):
       - `NBA` (`tvs.sbd.1000310`)
       - `Prime Video` (`tvs.sbd.12962`)
     - Playables (8 `EbsEvent` entries) contained deep-link-ish strings:
       - Several `aiv://aiv/detail?...` links (Prime Video live deep links)
       - One `gametime://game/0022500295?source=atv-search` (NBA app deep link)

**Key point:** the “rich” deep-link set (gametime / aiv) shows up reliably when querying the **sporting-events** endpoint for events in a live window, even when earlier scrapes didn’t surface those app URLs.

---

## 2. Auth + UTS API endpoints

We use Apple’s UTS (Universal Media / TV stack) APIs under `https://tv.apple.com/api/uts`.

### Auth tokens

- **`utscf`**: cookie value captured from tv.apple.com via Playwright.
- **`utsk`**: query parameter token also captured from tv.apple.com.

`multi_scraper.py` is responsible for:
- Launching a browser with Playwright.
- Having you log in / click a sports link once.
- Capturing **`utscf`** and **`utsk`** from the network traffic.
- Caching them to:

```text
data/apple_uts_auth.json
{
  "utscf": "...",
  "utsk": "..."
}
```

### Sporting-events endpoint

The live refresher script calls:

```text
GET https://tv.apple.com/api/uts/v3/sporting-events/{pvid}?caller=web&locale=en-US&pfm=web&sf=143441&v=90&utscf=...&utsk=...
```

Where:

- `{pvid}` is the Apple event id (e.g. `umc.cse.4kgedt2m66qvfj4sh9tkukv9x`).
- `caller`, `locale`, `pfm`, `sf`, `v` are fixed parameters that match what the browser uses.
- `utscf` and `utsk` are pulled from `apple_uts_auth.json`.

This endpoint returns a JSON payload with at least:

- `data.sportingEvent` or `data.event` (we treat that as `event_obj`).
- `event_obj.channels`: providers (NBA, Prime Video, ESPN, etc.).
- `event_obj.playables`: array/dict of **EbsEvent** objects with entitlement + deep-link info.

If `channels`/`playables` are missing at the top level, we fall back to `data.canvas.shelves[*].items[*]` for the same fields.

---

## 3. Data model: fruit_events.db

We store everything in `data/fruit_events.db` (SQLite). Relevant columns of the `events` table:

- `id` (TEXT): internal event id, e.g. `appletv-umc.cse.4kgedt2m66qvfj4sh9tkukv9x`
- `pvid` (TEXT): Apple’s event id (e.g. `umc.cse.4kgedt2m66qvfj4sh9tkukv9x`)
- `title` (TEXT): human-readable title (e.g. `Chicago Bulls at Indiana Pacers`)
- `start_utc`, `end_utc` (TEXT ISO8601): UTC start/end times
- `channel_name` (TEXT): provider name (NBA, Prime Video, etc.)
- `channel_provider_id` (TEXT): provider id (e.g. `tvs.sbd.1000310`)
- `raw_attributes_json` (TEXT JSON): catch‑all blob for extra attributes
- `last_seen_utc` (TEXT ISO8601): last time the row was touched/refreshed

The flow:

1. **`multi_scraper.py` + `parse_events.py`**
   - Scraper hits UTS browse/search/shelf endpoints.
   - Parser writes/updates `events` rows with core fields (pvid, title, start_utc, league, etc.) and some initial `raw_attributes_json`.

2. **`apple_live_playables_enricher.py`**
   - Reads the DB and enriches selected rows with live `channels` + `playables` from `v3/sporting-events`.

---

## 4. Live enrichment script: apple_live_playables_enricher.py

### Paths

- **Project root**: `PROJECT_ROOT = Path(__file__).resolve().parents[1]`
- **Auth cache**: `AUTH_CACHE_PATH = PROJECT_ROOT / "data" / "apple_uts_auth.json"`
- **DB path**: `DB_PATH = PROJECT_ROOT / "data" / "fruit_events.db"`

### 4.1. Loading auth

```python
data = json.loads(AUTH_CACHE_PATH.read_text("utf-8"))
utscf = data["utscf"]
utsk = data["utsk"]
```

We build a base parameter string:

```python
base_params = (
    "caller=web&locale=en-US&pfm=web&sf=143441&v=90"
    f"&utscf={utscf}&utsk={utsk}"
)
```

### 4.2. Selecting “live-ish” events

We only enrich events that are likely to be live or about to start, to keep calls cheap and focused:

```python
now = datetime.now(timezone.utc)
pre  = now - timedelta(hours=2)
post = now + timedelta(hours=4)

SELECT id, pvid, title, channel_name, start_utc, end_utc, raw_attributes_json
FROM events
WHERE pvid LIKE 'umc.cse.%'
  AND start_utc IS NOT NULL
```

For each row, we parse `start_utc` as an aware datetime and keep only events where:

```python
pre <= start_dt <= post
```

So the candidate set is “start time within [-2h, +4h] of now” and with a UMC-style pvid.

### 4.3. Fetching sporting-events payload

For each candidate event:

```python
url = f"{BASE_API}/v3/sporting-events/{pvid}?{base_params}"
r = requests.get(url, headers={"Accept": "application/json"})
payload = r.json()
```

We derive `event_obj` as:

```python
data      = payload.get("data", {})
event_obj = data.get("sportingEvent") or data.get("event") or data
```

### 4.4. Normalizing channels and playables

Apple sometimes returns `channels` / `playables` as either:

- a **list** of objects, or
- a **dict** keyed by id.

We normalize both to lists of dicts:

```python
def normalize_obj_list(obj):
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        return list(obj.values())
    return []

channels_raw  = event_obj.get("channels") or []
playables_raw = event_obj.get("playables") or []

channels  = normalize_obj_list(channels_raw)
playables = normalize_obj_list(playables_raw)
```

If both are empty, we fallback to `data["canvas"]["shelves"][*]["items"][*]` and look for items that have `channels` or `playables`, normalizing them the same way.

### 4.5. Writing back into the DB

We load the existing JSON blob:

```python
attrs = json.loads(ev["raw_json"] or "{}")
```

Then merge in the live data:

```python
if channels:
    attrs["channels"] = channels
if playables:
    attrs["playables"] = playables
```

For convenience, we also promote the **primary channel** to top-level columns:

```python
primary = channels[0] if channels else None
new_channel_name = primary.get("name") if primary else ev["channel_name"]
new_provider_id  = primary.get("id")   if primary else None
```

Finally, we update the row:

```sql
UPDATE events
SET
  channel_name        = :new_channel_name,
  channel_provider_id = :new_provider_id,
  raw_attributes_json = :attrs_json,
  last_seen_utc       = :now_iso
WHERE id = :event_id
```

So after enrichment, each live-ish event row has:

- Up-to-date `channel_name` / `channel_provider_id` reflecting the primary provider (NBA, Prime, etc.).
- An expanded `raw_attributes_json` that contains both the original scraped attributes **plus**:
  - `channels`: full provider list
  - `playables`: full list of EbsEvent objects (with embedded deep links)

---

## 5. Inspecting stored deep links

We added a helper script `bin/inspect_event_playables.py` to verify what’s stored.

It:

1. Looks up `events` by exact `title`.
2. Loads `raw_attributes_json`.
3. Prints:
   - Channels (name + id)
   - Playables (type + top-level keys)
4. Recursively scans the entire JSON for strings containing:
   - `gametime://`
   - `aiv://`
   - `sports.apple.com`

Example output for *Chicago Bulls at Indiana Pacers*:

```text
Channels: 2
  [1] NBA  raw-id=tvs.sbd.1000310
  [2] Prime Video  raw-id=tvs.sbd.12962

Playables: 8
  [Playable 1..8] type = EbsEvent, with keys including punchoutUrls, serviceId, serviceName, etc.

=== Deep-link-ish strings found ===
  aiv://aiv/detail?gti=amzn1.dv.gti....&action=watch&type=live&...
  ...
  gametime://game/0022500295?source=atv-search
```

This confirms that:

- The live enrichment process is **successfully capturing app deep links** and storing them under `raw_attributes_json` for events in the live window.
- We can mine those strings later to drive:
  - **NBA app** (via `gametime://game/...`)
  - **Prime Video** (via `aiv://aiv/detail?...`)
  - **Web/AppleTV** (via `https://sports.apple.com/...`, if present)

---

## 6. How to use this in exports

From an exporter’s perspective (XMLTV/M3U builder), the workflow is:

1. Make sure you’ve run:
   - `multi_scraper.py` (to populate/refresh the schedule)
   - `parse_events.py --input multi_scraped.json` (to update `events`)
   - `apple_live_playables_enricher.py` (near game time, to inject channels/playables)

2. When building the M3U / deeplink fields for a given event row:
   - Read `raw_attributes_json`.
   - Search `attrs["playables"]` for deep-link-ish strings, e.g.:
     - Prefer `gametime://` for NBA lanes.
     - Prefer `aiv://` for Prime Video lanes.
     - Fall back to `https://sports.apple.com/...` or `tv.apple.com` if needed.

All the raw ingredients (providers, EbsEvent metadata, app deep links) are now present in `fruit_events.db` and refreshed by the live enrichment script.
