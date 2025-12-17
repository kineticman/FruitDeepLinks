# HTTP Deeplink Conversion for Android/Fire TV

## Overview

Apple TV Sports API returns deeplinks in app scheme format (e.g., `aiv://`, `sportscenter://`), which work great on tvOS but often fail on Android and Fire TV. This feature converts those scheme-based deeplinks to HTTP URLs that work across all platforms.

## How It Works

1. **Automatic Platform Detection** - When a client tunes to a Fruit Lane, the detector identifies the platform (tvOS, Android TV, Fire TV)
2. **Format Selection** - Uses scheme-based deeplinks for Apple TV, HTTP deeplinks for Android/Fire TV
3. **Runtime Conversion** - Converts deeplinks on-the-fly using provider-specific converters
4. **Extensible** - Easy to add new provider conversions as we learn their schemas

## Supported Conversions

### Amazon Prime Video ✅
- **Input:** `aiv://aiv/detail?gti=amzn1.dv.gti.XXX&action=watch&type=live&...`
- **Output:** `https://app.primevideo.com/detail?gti=amzn1.dv.gti.XXX`
- **Status:** Tested on Fire TV - works!

### ESPN+ (Partial)
- **Input:** `sportscenter://x-callback-url/showWatchStream?playID=XXX`
- **Output:** `https://www.espn.com/watch/player/_/id/XXX`
- **Status:** Needs Fire TV testing

### Peacock (Partial)
- **Input:** `peacock://event/XXX`
- **Output:** `https://www.peacocktv.com/watch/playback/event/XXX`
- **Status:** Needs Fire TV testing

### Pending Research
- Paramount+
- Max (HBO Max)
- DAZN
- FOX Sports
- Hulu
- Others...

## API Usage

### For CDVR Detector (Automatic)

The detector automatically uses the right format based on client platform:
- Apple TV → scheme format (`aiv://...`)
- Fire TV → HTTP format (`https://app.primevideo.com/...`)
- Android TV → HTTP format

### For External Tools (Manual)

Get deeplinks in specific format via API:

```bash
# Default (scheme format for Apple TV)
GET /whatson/5?include=deeplink

# HTTP format for Android/Fire TV
GET /whatson/5?include=deeplink&deeplink_format=http

# Plain text for scripts
GET /whatson/5?format=txt&param=deeplink_url&deeplink_format=http
```

**Example Response:**
```json
{
  "ok": true,
  "lane": 5,
  "event_uid": "...",
  "deeplink_url": "https://app.primevideo.com/detail?gti=amzn1.dv.gti.XXX",
  "deeplink_url_full": "https://app.primevideo.com/detail?gti=amzn1.dv.gti.XXX"
}
```

## Adding New Conversions

Edit `deeplink_converter.py` and add a new converter function:

```python
def convert_SERVICE_NAME(punchout_url: str) -> Optional[str]:
    """
    Convert SERVICE deeplinks to HTTP format.
    
    Input:  scheme://path?params
    Output: https://service.com/path?params
    """
    if not punchout_url.startswith("scheme://"):
        return None
    
    # Extract required parameters using regex
    match = re.search(r'param=([^&]+)', punchout_url)
    if match:
        value = match.group(1)
        return f"https://service.com/path?param={value}"
    
    return None
```

Then add to the converters list in `generate_http_deeplink()`:

```python
converters = [
    convert_amazon_prime,
    convert_espn,
    convert_peacock,
    convert_SERVICE_NAME,  # Add here
    # ...
]
```

## Testing Conversions

Run the test suite:

```bash
python3 bin/deeplink_converter.py
```

Expected output:
```
Testing deeplink conversions:

✓ Original: aiv://aiv/detail?gti=amzn1.dv.gti.10fd272d...
  Expected: https://app.primevideo.com/detail?gti=amzn1.dv.gti.10fd272d...
  Got:      https://app.primevideo.com/detail?gti=amzn1.dv.gti.10fd272d...
```

## Migration

If you have existing data, run the migration to populate HTTP deeplinks:

```bash
docker exec fruitdeeplinks python3 /app/bin/migrate_add_http_deeplinks.py
```

This:
1. Adds `http_deeplink_url` column to `playables` table
2. Generates HTTP versions for existing playables
3. Future imports will auto-generate both formats

## Fire TV Testing Guide

To test a new conversion on Fire TV:

1. **Find the deeplink** in FruitDeepLinks:
   ```bash
   curl "http://your-ip:6655/whatson/5?include=deeplink&deeplink_format=http"
   ```

2. **Launch via ADB**:
   ```bash
   adb connect YOUR_FIRETV_IP
   adb shell am start -d "https://app.primevideo.com/detail?gti=XXX"
   ```

3. **Verify**:
   - Does the app launch?
   - Does it go to the correct event?
   - Does it auto-play or require button press?

4. **Document** the schema in `deeplink_converter.py`

## Troubleshooting

**Deeplink not converting:**
- Check logs for "deeplink_converter not available"
- Ensure `deeplink_converter.py` is in `/app/bin/`
- Verify converter function is added to converters list

**Wrong content launching:**
- Double-check parameter extraction regex
- Compare HTTP URL to what works in browser
- Test URL manually via `adb shell am start -d`

**Platform detection issues:**
- Check detector logs for platform string
- Verify platform detection in `has_api_support()`
- Test with `?deeplink_format=http` manually

## Environment Variables

```env
# Optional: Override cooldown (default 5 minutes)
LANE_COOLDOWN_MINUTES=5
```

## Contributing

When you discover a new working HTTP deeplink schema:

1. Add converter function to `deeplink_converter.py`
2. Add test case to `__main__` section
3. Document the schema with Input/Output examples
4. Submit PR with Fire TV test results

## Credits

- HTTP conversion concept: Brad + ADB developer collaboration
- Amazon Prime schema: Discovered through Fire TV testing
- Ongoing research: Community-driven

---

**Status:** BETA - Actively adding provider support as we test on Fire TV
