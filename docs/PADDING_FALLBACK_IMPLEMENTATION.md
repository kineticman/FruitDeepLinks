# Padding Window Deeplink Fallback - Implementation Summary

## Overview
Implemented automatic deeplink fallback during padding windows so users can continue watching even after an event officially ends but is still shown in the XMLTV guide.

## Problem Solved
- **Before**: When an event ended but was still in its padding window (FRUIT_PADDING_MINUTES = 45), the detector would return "no deeplink found" because lane_events only had placeholders
- **After**: System automatically falls back to the most recent real event's deeplink if we're within the padding window

## Changes Made

### 1. New Helper Function: `get_fallback_event_for_lane()`
**Location**: Lines 763-824

**Purpose**: Finds the most recent non-placeholder event on a lane that ended within the padding window

**Logic**:
```python
# Calculates padding_window_start = now - FRUIT_PADDING_MINUTES
# Queries for non-placeholder events that:
#   - Are on the specified lane
#   - Ended after padding_window_start
#   - Ended before current time
# Returns most recent match
```

**Returns**:
- Event details including: event_id, title, deeplink, end_utc
- `is_fallback: True` flag for logging/tracking

### 2. Updated `/whatson/<lane_id>` Endpoint
**Location**: Lines 1904-2039

**New Behavior**:
```python
# 1. Query for current event on lane (may be placeholder)
# 2. If current event is placeholder:
#      - Call get_fallback_event_for_lane()
#      - If fallback found, use its deeplink
#      - Log: "Using FALLBACK event '<title>' (ended at <timestamp>)"
# 3. If current event is real (not placeholder):
#      - Use normal flow (unchanged)
# 4. Return deeplink with is_fallback flag in JSON
```

**Key Changes**:
- Checks `row["is_placeholder"]` after querying lane_events
- Calls fallback logic if placeholder detected
- Adds `is_fallback` field to JSON response
- Enhanced logging for debugging

### 3. Enhanced `get_deeplink_for_lane()` Function
**Location**: Lines 995-1033

**Improvements**:
- Now extracts and passes through `is_fallback` flag from API response
- Logs when fallback deeplinks are being used
- Helps detector thread understand it's using padding fallback

## How It Works - Example Scenario

**Lakers vs Warriors game ends at 10:00 PM, padding = 45 minutes:**

### 10:00 PM (Game Ends)
- `lane_events`: Switches to placeholder
- `XMLTV guide`: Still shows "Lakers vs Warriors" (extended with padding until 10:45 PM)
- **User Experience**: Seamless - stream continues working

### 10:15 PM (During Padding)
1. User tunes to Fruit Lane showing Lakers game
2. HLS endpoint triggers detector
3. Detector calls `/whatson/<lane_id>?include=deeplink`
4. API sees current slot is placeholder
5. API calls `get_fallback_event_for_lane()`
6. Finds Lakers game ended at 10:00 PM (< 45 min ago)
7. Returns Lakers deeplink with `is_fallback: True`
8. Detector launches Lakers stream successfully

### 10:46 PM (After Padding)
- Fallback window expires
- No deeplink returned (correct behavior)
- User sees placeholder content or no stream

## Logging Output

### Normal Event (No Fallback):
```
[2025-01-15 22:00:00] [INFO] /whatson/1 returning deeplink for 'Lakers vs Warriors'
[2025-01-15 22:00:00] [INFO] Detector: matched lane=1 client=192.168.86.45
```

### Fallback During Padding:
```
[2025-01-15 22:15:00] [INFO] Lane 1: Current slot is placeholder, checking for fallback event within padding window
[2025-01-15 22:15:00] [INFO] Lane 1: Using FALLBACK event 'Lakers vs Warriors' (ended at 2025-01-15T22:00:00Z)
[2025-01-15 22:15:00] [INFO] get_deeplink_for_lane: Lane 1 using FALLBACK deeplink for 'Lakers vs Warriors'
[2025-01-15 22:15:00] [INFO] Detector: matched lane=1 client=192.168.86.45
```

### No Fallback Available:
```
[2025-01-15 23:00:00] [INFO] Lane 1: Current slot is placeholder, checking for fallback event within padding window
[2025-01-15 23:00:00] [INFO] Lane 1: No fallback event found within padding window
[2025-01-15 23:00:00] [WARN] Detector: no deeplink found for lane=1
```

## Configuration

The padding window is controlled by environment variable:
```bash
FRUIT_PADDING_MINUTES=45  # Default value
```

This matches the padding used in XMLTV export scripts, ensuring consistent behavior between guide display and deeplink availability.

## Testing Recommendations

### 1. Test Normal Flow (No Padding)
- Tune to lane during active event
- Verify deeplink launches immediately
- Check logs show no "FALLBACK" messages

### 2. Test Padding Window
- Wait for event to end (or set end_utc in past)
- Tune to lane during padding window
- Verify deeplink still launches
- Check logs show "Using FALLBACK event" message

### 3. Test Expired Padding
- Wait > FRUIT_PADDING_MINUTES after event ends
- Tune to lane
- Verify no deeplink is available
- Check logs show "No fallback event found"

### 4. Test Multiple Lanes
- Verify each lane independently tracks its own fallback
- Ensure lane 1's fallback doesn't affect lane 2

## PowerShell Deployment Commands

```powershell
# 1. Copy updated file to container
docker cp /path/to/fruitdeeplinks_server.py fruitdeeplinks:/app/bin/fruitdeeplinks_server.py

# 2. Restart Flask server
docker exec fruitdeeplinks supervisorctl restart flask

# 3. Watch logs for fallback behavior
docker logs -f fruitdeeplinks

# 4. Test specific lane
Invoke-WebRequest -Uri "http://192.168.86.80:6655/whatson/1?include=deeplink" | Select-Object -ExpandProperty Content | ConvertFrom-Json
```

## Database Queries for Verification

```sql
-- Check current events and placeholders
SELECT 
    lane_id,
    event_id,
    is_placeholder,
    start_utc,
    end_utc,
    datetime(end_utc) as end_time
FROM lane_events
WHERE lane_id = 1
  AND datetime(end_utc) > datetime('now', '-60 minutes')
ORDER BY start_utc;

-- Find recent events that could serve as fallbacks
SELECT 
    le.lane_id,
    e.title,
    le.end_utc,
    le.is_placeholder,
    round((julianday('now') - julianday(le.end_utc)) * 1440) as minutes_since_end
FROM lane_events le
JOIN events e ON le.event_id = e.id
WHERE le.lane_id = 1
  AND le.is_placeholder = 0
  AND datetime(le.end_utc) >= datetime('now', '-60 minutes')
  AND datetime(le.end_utc) <= datetime('now')
ORDER BY le.end_utc DESC;
```

## Benefits

✅ **Seamless UX**: Users don't notice when events end - deeplink continues working
✅ **Smart Padding**: Leverages existing FRUIT_PADDING_MINUTES configuration
✅ **Transparent**: Logging clearly shows when fallback is active
✅ **Safe**: Only fallsback if recent event exists within padding window
✅ **Per-Lane**: Each lane independently manages its own fallback state

## Future Enhancements (Optional)

- Add `/api/lanes/status` endpoint showing which lanes are in fallback mode
- Dashboard indicator showing "🔄 Padding Fallback Active" for affected lanes
- Metrics tracking: % of deeplink launches that use fallback
- Configurable per-lane padding overrides
