# Amazon Exclusives (`aiv_exclusive`) — Technical Summary

## Goal

Introduce a **completely new, first-class service** called **Amazon Exclusives** (`aiv_exclusive`) that:

- Includes **only events where Amazon Prime Video (AIV) is the sole mapped streaming option**
- Does **not** modify or weaken existing `aiv` behavior
- Works across:
  - Filters UI
  - Direct export
  - Hybrid export
  - ADB lanes (build + export)
- Requires **no database schema migration**
- Requires **no manual SQL** or special user actions

---

## Canonical Definition (Locked)

An event is **Amazon Exclusive** if and only if:

```sql
EXISTS (
  SELECT 1 FROM playables
  WHERE event_id = e.id
    AND logical_service = 'aiv'
)
AND NOT EXISTS (
  SELECT 1 FROM playables
  WHERE event_id = e.id
    AND logical_service IS NOT NULL
    AND logical_service <> ''
    AND logical_service <> 'aiv'
)
```

### Verified Counts (from production DB snapshot)
- AIV events total: **641**
- Amazon Exclusive events: **81**
- Strict vs mapped-only exclusives: **81 / 81** (exact match)

---

## Architectural Decision

`aiv_exclusive` is implemented as a **synthetic / derived logical service**.

- No rows are added to `playables.logical_service`
- Exclusivity is computed dynamically at query/filter time
- AIV playables are **cloned in-memory** and relabeled as `aiv_exclusive` when needed
- This avoids schema changes and preserves backward compatibility

---

## Component-Level Changes

### 1. `logical_service_mapper.py`

Purpose: Register `aiv_exclusive` as a first-class service everywhere services are enumerated.

Changes:
- Added display name:
  ```python
  'aiv_exclusive': 'Amazon Exclusives'
  ```
- Added priority (same tier as AIV):
  ```python
  'aiv_exclusive': 27
  ```
- Added derived event count using the canonical SQL predicate

Effect:
- `/api/filters` and UI correctly show:
  ```
  Amazon Exclusives (81)
  ```

No DB writes occur here.

---

### 2. `filter_integration.py` (Core Logic)

Purpose: Make `aiv_exclusive` behave as a **unique service** during filtering and export.

Key additions:

#### A. Exclusivity detector
```python
def is_aiv_exclusive_event(conn, event_id) -> bool
```

#### B. Synthetic playable creation
When:
- `aiv_exclusive` is enabled
- AND the event qualifies as exclusive

Then:
- Clone the event’s AIV playable(s)
- Relabel:
  ```python
  logical_service = 'aiv_exclusive'
  ```
- Preserve deeplink, priority, and provider metadata

This allows:
- Normal filtering logic to work
- Correct service identity in exports
- No accidental web-url fallback

#### C. Amazon-family handling
Both `aiv` and `aiv_exclusive` are treated as Amazon-family services for penalty logic.

#### D. Fallback suppression
If **only** `aiv_exclusive` is enabled:
- Events that lose all playables **do not fall back** to `raw_attributes_json.webUrl`
- They are excluded cleanly instead

---

### 3. Direct + Hybrid Exports

Results:
- `aiv` enabled → unchanged behavior
- `aiv_exclusive` enabled → only exclusive events exported
- Hybrid fallback path respects exclusivity and service labeling

No export format changes required.

---

### 4. ADB Lanes — Builder (`fruit_build_adb_lanes.py`)

Problem:
- ADB lanes normally load events by matching `playables.logical_service`
- `aiv_exclusive` does not exist in the DB

Solution:
- Added a special-case loader:
  - If `provider_code == 'aiv_exclusive'`
  - Load events using the canonical exclusivity SQL predicate

Result:
- ADB lane planning works correctly
- No impact to existing AIV lanes

---

### 5. ADB Lanes — Export (`fruit_export_adb_lanes.py`)

Presentation improvements:
- Channel display names:
  ```
  Amazon Exclusives 01
  ```
- M3U group-title:
  ```
  ADB Amazon Exclusives
  ```

Purely cosmetic; no logic changes.

---

### 6. ADB Provider Registry (`provider_lanes` table)

Important clarification:

- **No new schema**
- **No migration**
- Uses the existing `provider_lanes` table

Fact:
> The ADB lane builder only processes providers that exist in `provider_lanes`.

#### UX Solution (No Manual SQL)

- `fruitdeeplinks_server.py` injects `aiv_exclusive` as a **synthetic provider** into:
  ```
  GET /api/provider_lanes
  ```
- The ADB config UI displays it immediately
- When the user saves settings:
  - Existing `POST /api/provider_lanes` UPSERT logic
  - Automatically creates or updates the row

Result:
- Zero extra steps for users
- First enable auto-seeds the DB row

---

## Final Behavior Matrix

| Scenario | Result |
|-------|-------|
| Enable `aiv` only | All Prime events (including exclusives) |
| Enable `aiv_exclusive` only | Only Prime-exclusive events |
| Enable both | Superset behavior; services remain distinct |
| Direct export | Works |
| Hybrid export | Works |
| ADB lanes | Works |
| DB migration | **Not required** |
| Manual SQL | **Not required** |

---

## Design Guarantees

- Backward compatible
- Deterministic selection
- No schema changes
- No user friction
- Unique service identity preserved end-to-end

---

## Files Modified

- `logical_service_mapper.py`
- `filter_integration.py`
- `fruit_export_hybrid.py`
- `fruit_build_adb_lanes.py`
- `fruit_export_adb_lanes.py`
- `adb_provider_mapper.py`
- `fruitdeeplinks_server.py`