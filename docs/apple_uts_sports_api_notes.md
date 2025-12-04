# Apple UTS Sports API – Working Notes

These are reverse‑engineered notes from probing Apple’s sports stack (tv.apple.com + sports.apple.com).

---

## 1. Auth / Session Model (UTS)

All the “real” data lives behind **tv.apple.com** UTS APIs, not sports.apple.com.

Typical UTS URL shape:

```text
https://tv.apple.com/api/uts/...?...&caller=web&locale=en-US&pfm=web&sf=143441&v=90&utscf=<cookie>&utsk=<token>
```

- `utscf` and `utsk` are per‑session auth parameters.
- We capture them from an existing `GET /api/uts/v3/sporting-events/<id>` request.
- Flow (via Selenium / Chrome performance logs):
  1. Open: `https://tv.apple.com/us/collection/sports/uts.col.search.SE?searchTerm=all`
  2. Manually click any event tile and wait for the page to fully load.
  3. Read Chrome DevTools *performance* logs, find a `/api/uts/v3/sporting-events/...` request, and extract `utscf` + `utsk` query params.
- Once we have `utscf` and `utsk`, we can reuse them for other UTS endpoints in that session.

---

## 2. High‑Level Data Model (Canvas → Shelves → Items)

Every “page” is a **canvas**:

```jsonc
{
  "data": {
    "canvas": {
      "id": "uts.cvs....",
      "shelves": [
        {
          "id": "uts.col.Sports.future" | "uts.col.SportsRelated...." | "...",
          "header": { "title": "...", "url": "..." },
          "items": [ /* things on that row */ ]
        }
      ]
    }
  }
}
```

- **Canvas** ≈ page/screen.
- **Shelves** ≈ rows/strips on the page (e.g., “Upcoming”, “Other Games”, “Replays”).
- **Items** inside shelves are typed; we care about `type: "SportingEvent"` (games).

A `SportingEvent` item usually includes:

- IDs
  - `id` (umc.cse.… – the sporting‑event ID)
  - `leagueId`, `sportId`
- Names
  - `title`, `shortTitle`
  - `leagueName`, `leagueAbbreviation`
  - `sportName`
- Timing
  - `startTime`, `startAirTime`, `endAirTime` (epoch ms)
  - `airingType` (`Live`, etc.)
- Service info
  - `playable.serviceName`
  - `playable.externalServiceId`
  - `playable.serviceLogo.url`
- Teams
  - `competitors[]` with `name`, `shortName`, `nickname`
  - `isHome`, `isFavorite`, `ranking`
  - Team logos + colors (`teamLogo`, `teamLogoDark`, `teamLogoLight`, etc.)
- Live scoreboard (if applicable)
  - `clockScore` → `clockTime`, `periodType`, `periodValue`, `scores[]`
  - `sportingEventProgressStatus` (`IN_PROGRESS`, etc.)

**Core scrape pattern:**

> “Get a list of games” = find the right canvas → pick the shelves you care about → flatten `items` where `type == "SportingEvent"`.

---

## 3. Directory‑Style Endpoints

### `/v3/sports`

```text
GET https://tv.apple.com/api/uts/v3/sports?...&utscf=&utsk=
```

- Returns a list of **sports** (Soccer, Baseball, Hockey, etc.).
- Each entry has `id` (umc.csp.…), `name`, and artwork.
- Pure catalog – **no schedules/events** here.

### `/v3/leagues`

```text
GET https://tv.apple.com/api/uts/v3/leagues?...&utscf=&utsk=
```

- Returns a small list of **leagues**.
- In our probes it returned only:
  - MLS → `umc.csl.3c9plmy5skze52ff5ce24mo4g`
  - MLB → `umc.csl.50vezwb1n14iqvdgtxwcpo2z1`
- Strong hint: this is a directory of **Apple‑owned/packaged sports products** (e.g., MLS Season Pass, MLB Friday Night Baseball), *not* every league on earth.

### `/v3/sports/{sportId}`

```text
GET https://tv.apple.com/api/uts/v3/sports/<sportId>?...
```

- Returns detailed metadata for a single sport (branding, images).
- Our probes show **no schedule data** – just identity/art.

---

## 4. League Canvases (Apple‑Owned Leagues)

### `/v2/browse/sports/group/{leagueId}`

```text
GET https://tv.apple.com/api/uts/v2/browse/sports/group/<leagueId>?...
```

- For **MLS** (`umc.csl.3c9plmy5skze52ff5ce24mo4g`):
  - `data.canvas.shelves` includes a shelf with `id: "uts.col.Sports.future"`.
  - That shelf’s `items[]` are `SportingEvent` objects representing:
    - Upcoming MLS matches
    - Pre/post‑game shows (Countdown, Wrap‑Up, etc.).
- For **MLB** (in current tests):
  - `data.canvas.shelves` came back empty → likely offseason, no upcoming content.

**Practical use:**

You can treat the MLS league canvas as a **league‑specific upcoming schedule**:

- Call `GET /v3/leagues` to get league IDs.
- For each league ID, call:
  ```text
  GET /v2/browse/sports/group/<leagueId>?...
  ```
- Flatten `data.canvas.shelves[].items` where `type == "SportingEvent"` → list of future events for that league.

We implemented this in `apple_uts_league_future_sweeper.py` (v2) and successfully got 7 upcoming MLS events; MLB returned 0 events (no shelves).

---

## 5. “Upcoming” Collections (HTML vs JSON)

Some canvases reference collection URLs like:

```text
https://tv.apple.com/us/collection/upcoming/uts.col.Sports.future?ctx_sport=<leagueId>&ctx_shelf=<shelfId>&...
```

- Hitting these URLs in our probes usually returns an **HTML page**, not a pure JSON API payload.
- They’re useful for understanding navigation / context, but not the best machine‑readable source.
- The **JSON version of the schedule is already present** in the league canvas itself (`/v2/browse/sports/group/{leagueId}`), so we treat the collection URLs as informational, not primary data.

---

## 6. Event‑Level Canvases (Including Non‑Apple Leagues)

### `/v3/sporting-events/{eventId}`

```text
GET https://tv.apple.com/api/uts/v3/sporting-events/<eventId>?...
```

- `eventId` is the `umc.cse.*` you see in:
  - tv.apple.com event pages
  - sports.apple.com iOS share links, e.g.:  
    `https://sports.apple.com/us/event/umc.cse.6z6pqbpxls1lnnqlcyxeelxv1?...` (CFB game)
- This endpoint returns an **event‑centric canvas**:
  - `data.canvas.shelves[]` includes strips like:
    - `"Other Games"` (e.g., other CFB games)
    - Potentially highlights/replays related to this event.

**Important discovery:**

- The CFB event canvas we probed shows:
  - `leagueId`: `umc.csl.7h0yrhl69b8vwdwj527eduzr9`
  - `leagueAbbreviation`: `"cfb"`
  - `leagueName`: `"College Football"`
- This CFB `leagueId` **does not appear in `/v3/leagues`**, but it’s fully modeled at the event level.

This confirms:

> Non‑MLS/MLB leagues (e.g., College Football) exist as full league objects inside UTS event canvases, even if they’re not listed in `/v3/leagues`.

**“Other Games” shelf example:**

- Event canvas has a shelf titled `"Other Games"`.
- Its `items[]` are all `SportingEvent` objects for other CFB games (LSU @ OU, Wisconsin @ Minnesota, etc.).
- Each item has full team info, start times, and deep‑link service data (ESPN/FS1).

Effectively, a single event canvas provides a **mini board** of related games for that sport/day.

---

## 7. Service / Deep‑Link Metadata (ESPN, FS1, etc.)

Inside each `SportingEvent` (in league or event canvases) we see:

- `playable.serviceName` examples:
  - `"MLS Live US"`
  - `"ESPN+ V2"`
  - `"ESPN Unlimited"`
  - `"ESPN3 V2"`
  - `"FS1"`
- `playable.externalServiceId` examples:
  - `com.apple.mls.svod.ebs.US`
  - `com.apple.atvp.mls.svod.ebs.US`
  - `com.fox.sports.live.FS1`
  - ESPN‑flavored IDs for DTC vs linear variants.
- `punchoutUrls.play` / `punchoutUrls.open` examples:
  - `sportscenter://x-callback-url/showWatchStream?playID=...&x-source=AppleUMC`

This makes UTS a **translation layer** for deep links:

- From Apple’s sporting event → to the right ESPN/FS1/MLS/whatever stream via `serviceName`, `externalServiceId`, and `punchoutUrls`.

For DeepLinks‑style projects, this is essentially an Apple→ESPN/FS1 resolver with full schedule + matchup context.

---

## 8. Practical Scrape Patterns (What Actually Works)

### 8.1 Get Session Tokens Once per Run

1. Launch Selenium Chrome with performance logging.
2. Navigate to:  
   `https://tv.apple.com/us/collection/sports/uts.col.search.SE?searchTerm=all`
3. Manually click any event tile, wait for full load.
4. Read `Network.requestWillBeSent` entries in performance logs:
   - Find a URL containing `/api/uts/v3/sporting-events/`.
   - Extract `utscf` and `utsk` from its query string.
5. Reuse `utscf` / `utsk` for subsequent UTS calls.

### 8.2 Apple‑Owned League Schedules (MLS, MLB, etc.)

1. Call `/v3/leagues` with the session params.
2. For each league ID:
   - Call `/v2/browse/sports/group/{leagueId}`.
   - Flatten `data.canvas.shelves[].items` where `type == "SportingEvent"`.
3. That flattened list = **Apple’s view of upcoming events for that league**.
   - MLS returns games + studio shows (Countdown, Wrap‑Up).
   - MLB currently returns no shelves in offseason (in tests).

### 8.3 Non‑Apple Leagues via Event Canvases (e.g., College Football)

1. Start from a **sports.apple.com share link** or tv.apple.com event URL:
   - Extract `umc.cse.*` event ID.
2. Call `/v3/sporting-events/{eventId}` with session params.
3. Flatten `data.canvas.shelves[].items` where `type == "SportingEvent"`:
   - This captures:
     - The primary event (somewhere in the canvas).
     - Related‑games strips like `"Other Games"`.
4. For each `SportingEvent` item, capture:
   - IDs: `event_id`, `leagueId`, `sportId`
   - Names: `title`, `shortTitle`, `leagueName`, `leagueAbbreviation`, `sportName`
   - Times: `startAirTime`, `endAirTime`
   - Service: `serviceName`, `externalServiceId`, `serviceLogoUrl`
   - Deep links: `punchoutUrls.play` (e.g., ESPN SportsCenter URLs)
   - Teams: `competitors[]` → home/away, logos, colors, rankings
   - Live state: `clockScore` where present

This gives you a **rich, real‑time board for that sport** even if `/v3/leagues` does not list the league.

---

## 9. Conceptual Summary

- UTS is **canvas → shelves → items**, and the interesting items are `type: "SportingEvent"`.
- `/v3/leagues` appears to be **Apple‑packaged leagues** (MLS, MLB, etc.).
- Non‑Apple leagues (e.g., College Football) still have `leagueId` + metadata, but only show up at **event canvas** level (and related shelves like “Other Games”), not in `/v3/leagues`.
- League canvases (`/v2/browse/sports/group/{leagueId}`) are a clean way to get **upcoming schedules** for Apple‑owned packages (MLS today).
- Event canvases (`/v3/sporting-events/{eventId}`) are a flexible way to:
  - Pull the main event.
  - Harvest related games for that sport/day.
  - Discover ESPN/FS1/MLS deep links and service metadata.
- For FruitDeepLinks / DeepLinks projects, the pipeline is essentially:
  1. Obtain UTS tokens (`utscf`/`utsk`) via a single Selenium auth dance.
  2. Hit league canvases and/or event canvases.
  3. Flatten shelves into normalized `SportingEvent` rows.
  4. Store in DB, XMLTV, M3U, or use directly for building deep links.
