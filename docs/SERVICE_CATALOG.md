# FruitDeepLinks Service Catalog

**Last Updated:** December 2024  
**Database Analysis Date:** December 4, 2025

## Overview

This document catalogs all streaming services discovered in the FruitDeepLinks database, including their deeplink URL schemes, provider codes, and availability statistics.

---

## Service Summary

**Total Services Found:** 18 logical services  
**Total Playables:** 2,505 future events  
**App-Based Services:** 12  
**Web-Based Services:** 6

---

## 1. APP-BASED SERVICES (Native Deeplinks)

These services use custom URL schemes that open directly in their native mobile/TV apps.

### 1.1 Prime Video (aiv)

**Count:** 756 playables (30.2% of total)  
**Provider Code:** `aiv`, `gametime`  
**Display Name:** Prime Video, Prime Video TNF  
**Priority:** 4

**Deeplink Schemes:**
```
aiv://aiv/detail?gti={amazon_id}&action=watch&type=live&territory=US&time=live&broadcast={broadcast_id}
gametime://aiv/detail?gti={amazon_id}&action=watch&type=live&territory=US
```

**Example:**
```
aiv://aiv/detail?gti=amzn1.dv.gti.817360b4-9b20-4799-b775-fe5e1f1889e9&action=watch&type=live&territory=US&time=live&broadcast=amzn1.dv.gti.0942a12e-ea5a-4594-9385-3b689ff31ba0&refMarker=atv_dvm_liv_apl_us_bd_l_src_av
```

**Content Types:**
- Live sports (NBA, NHL, NFL Thursday Night Football)
- International cricket (The Ashes)
- Olympic events
- Documentary sports content

**Notes:**
- `aiv` = Amazon Instant Video (standard Prime Video)
- `gametime` = Thursday Night Football specific deeplinks
- Both resolve to Prime Video app
- Requires Amazon Prime membership

---

### 1.2 ESPN / ESPN+ (sportscenter / sportsonespn)

**Count:** 623 playables (24.9% of total)  
**Provider Codes:** `sportscenter`, `sportsonespn`  
**Display Names:** ESPN, ESPN+  
**Priority:** 0 (highest - preferred when available)

**Deeplink Schemes:**
```
sportscenter://x-callback-url/showArticle?id={article_id}
sportsonespn://x-callback-url/showGame?gameId={game_id}
```

**Example:**
```
sportscenter://x-callback-url/showArticle?id=123456789
sportsonespn://x-callback-url/showGame?gameId=401234567
```

**Content Types:**
- College sports (football, basketball, swimming, etc.)
- International soccer
- UFC/MMA events
- Tennis, golf, motorsports
- NHL, MLB games

**Notes:**
- `sportscenter` = General ESPN app content
- `sportsonespn` = ESPN+ subscription content
- May require ESPN+ subscription ($10.99/month)
- Often has both free and premium content

---

### 1.3 CBS Sports / CBS (cbssportsapp / cbstve)

**Count:** 391 playables (15.6% of total)  
**Provider Codes:** `cbssportsapp`, `cbstve`  
**Display Names:** CBS Sports, CBS  
**Priority:** 6-7

**Deeplink Schemes:**
```
cbssportsapp://open?url={encoded_url}
cbstve://live?channel={channel_id}
```

**Example:**
```
cbssportsapp://open?url=https%3A%2F%2Fwww.cbssports.com%2Flive%2F
cbstve://live?channel=cbs_sports
```

**Content Types:**
- NFL games
- College sports (SEC, Big Ten)
- Golf (PGA Tour)
- UEFA Champions League soccer

**Notes:**
- `cbssportsapp` = CBS Sports mobile app
- `cbstve` = CBS broadcast TV app
- Some content requires Paramount+ subscription
- Local CBS affiliate authentication may be required

---

### 1.4 Paramount+ (pplus)

**Count:** 282 playables (11.3% of total)  
**Provider Code:** `pplus`  
**Display Name:** Paramount+  
**Priority:** 3

**Deeplink Scheme:**
```
pplus://play/video/{video_id}
pplus://open?url={encoded_url}
```

**Example:**
```
pplus://play/video/abc123def456
pplus://open?url=https%3A%2F%2Fwww.paramountplus.com%2Flive-tv%2Fstream%2F
```

**Content Types:**
- UEFA Champions League
- Serie A (Italian soccer)
- NWSL (women's soccer)
- College sports (via CBS)
- Golf tournaments

**Notes:**
- Requires Paramount+ subscription ($5.99-11.99/month)
- Includes CBS Sports content
- Often overlaps with CBS Sports offerings

---

### 1.5 ViX (vixapp)

**Count:** 74 playables (3.0% of total)  
**Provider Code:** `vixapp`  
**Display Name:** ViX  
**Priority:** 19

**Deeplink Scheme:**
```
vixapp://watch/{content_id}
```

**Example:**
```
vixapp://watch/12345-sports-event
```

**Content Types:**
- Liga MX (Mexican soccer)
- International soccer tournaments
- Spanish-language sports content

**Notes:**
- Free tier available (ad-supported)
- Premium tier: ViX Premium ($6.99/month)
- Primarily Spanish-language content

---

### 1.6 DAZN (open.dazn.com)

**Count:** 49 playables (2.0% of total)  
**Provider Code:** `open.dazn.com`, `dazn`  
**Display Name:** DAZN  
**Priority:** 17

**Deeplink Scheme:**
```
dazn://watch/{event_id}
open.dazn.com://watch/{event_id}
```

**Example:**
```
dazn://watch/live/abc123
```

**Content Types:**
- Boxing
- MMA
- International soccer
- Motorsports

**Notes:**
- Requires DAZN subscription ($19.99/month)
- Combat sports specialist
- Global sports coverage

---

### 1.7 NFL+ (nflctv)

**Count:** 38 playables (1.5% of total)  
**Provider Code:** `nflctv`  
**Display Name:** NFL+  
**Priority:** 20

**Deeplink Scheme:**
```
nflctv://watch/{game_id}
```

**Example:**
```
nflctv://watch/2024120812
```

**Content Types:**
- NFL games (mobile/tablet only for live)
- NFL Network shows
- NFL RedZone

**Notes:**
- Requires NFL+ subscription ($6.99/month)
- Live games limited to mobile/tablet
- Full replays available

---

### 1.8 truTV / TNT (watchtru / watchtnt)

**Count:** 21 playables (0.8% of total)  
**Provider Codes:** `watchtru`, `watchtnt`  
**Display Names:** truTV, TNT  
**Priority:** 21-22

**Deeplink Schemes:**
```
watchtru://watch/{content_id}
watchtnt://watch/{content_id}
```

**Example:**
```
watchtru://watch/sports-event-123
watchtnt://watch/nba-game-456
```

**Content Types:**
- NCAA Tournament (March Madness)
- NBA games
- NHL games
- Soccer tournaments

**Notes:**
- Requires cable/streaming TV provider authentication
- Part of Warner Bros. Discovery network
- Often simulcast with Max

---

### 1.9 FOX Sports (foxone / fsapp)

**Count:** 12 playables (0.5% of total)  
**Provider Codes:** `foxone`, `fsapp`  
**Display Name:** FOX Sports  
**Priority:** 9-10

**Deeplink Schemes:**
```
foxone://watch/{event_id}
fsapp://watch/{event_id}
```

**Example:**
```
foxone://watch/live/nfl-game-123
```

**Content Types:**
- NFL games
- MLB games
- College football
- FIFA World Cup

**Notes:**
- May require cable/TV provider authentication
- `foxone` = FOX Sports 1 content
- `fsapp` = General FOX Sports app

---

### 1.10 NBC Sports (nbcsportstve)

**Count:** 4 playables (0.2% of total)  
**Provider Code:** `nbcsportstve`  
**Display Name:** NBC Sports  
**Priority:** 8

**Deeplink Scheme:**
```
nbcsportstve://watch/{event_id}
```

**Example:**
```
nbcsportstve://watch/premier-league-123
```

**Content Types:**
- Premier League soccer
- Olympics
- Golf (PGA Tour)
- NHL games

**Notes:**
- Requires cable/TV provider authentication
- Or Peacock Premium subscription
- Limited standalone content

---

## 2. WEB-BASED SERVICES (HTTPS URLs)

These services use standard HTTPS URLs that open in a web browser or embedded web view.

### 2.1 Peacock (Web) (peacock_web)

**Count:** 20 playables (0.8% of total)  
**Logical Service Code:** `peacock_web`  
**Display Name:** Peacock (Web)  
**Priority:** 2

**URL Pattern:**
```
https://www.peacocktv.com/deeplink?deeplinkData={json_payload}
```

**Example:**
```
https://www.peacocktv.com/deeplink?deeplinkData=%7B%22pvid%22%3A%22028cc3d6-4d07-40fb-b1a2-0ed6f4af2c21%22%2C%22type%22%3A%22PROGRAMME%22%2C%22action%22%3A%22PLAY%22%7D
```

**Decoded Payload:**
```json
{
  "pvid": "028cc3d6-4d07-40fb-b1a2-0ed6f4af2c21",
  "type": "PROGRAMME",
  "action": "PLAY"
}
```

**Content Types:**
- Swimming events
- Figure skating
- Olympic sports
- Golf
- Premier League soccer

**Notes:**
- Requires Peacock Premium subscription ($5.99/month)
- Some events require Premium Plus ($11.99/month)
- Web-only events (no native app deeplink available)
- May redirect to native app if installed

---

### 2.2 Max (max)

**Count:** 19 playables (0.8% of total)  
**Logical Service Code:** `max`  
**Display Name:** Max  
**Priority:** 5

**URL Pattern:**
```
https://play.hbomax.com/sport/{event_id}?utm_source={source}
```

**Example:**
```
https://play.hbomax.com/sport/b515f167-d19c-51f4-a296-727d42fe9d62?utm_source=generic_web_share
```

**Content Types:**
- Soccer (Liga MX, international tournaments)
- UFC/combat sports
- March Madness (NCAA Tournament)

**Notes:**
- Requires Max subscription ($9.99-19.99/month)
- Formerly HBO Max
- Sports tier may require Max Ultimate plan
- Some soccer content exclusive to Max

---

### 2.3 F1 TV (f1tv)

**Count:** 14 playables (0.6% of total)  
**Logical Service Code:** `f1tv`  
**Display Name:** F1 TV  
**Priority:** 18

**URL Pattern:**
```
https://f1tv.formula1.com/detail/{season_id}/{event_name}?referrer={referrer}
```

**Example:**
```
https://f1tv.formula1.com/detail/1000009202/2025-abu-dhabi-gp-qualifying?referrer=https%3A%2F%2Ftv.apple.com
```

**Content Types:**
- Formula 1 races (live and replays)
- F1 qualifying sessions
- Practice sessions
- F1 documentaries

**Notes:**
- Requires F1 TV Pro subscription ($9.99/month or $79.99/year)
- Live streaming available in most countries
- Blackout restrictions apply in some regions
- Multi-camera views and driver onboards

---

### 2.4 Apple MLS (apple_mls)

**Count:** 2 playables (0.1% of total)  
**Logical Service Code:** `apple_mls`  
**Display Name:** Apple MLS  
**Priority:** 11

**URL Pattern:**
```
https://tv.apple.com/us/sporting-event/{team1}-vs-{team2}/umc.cse.{event_id}
```

**Example:**
```
https://tv.apple.com/us/sporting-event/inter-miami-cf-vs-vancouver-whitecaps-fc/umc.cse.xyz123abc
```

**Content Types:**
- MLS regular season games
- MLS playoffs
- Leagues Cup
- MLS All-Star Game

**Notes:**
- Requires MLS Season Pass ($14.99/month or $99/season)
- Or included with Apple TV+ bundle
- Available globally (first time MLS widely available outside US)
- Every game, no blackouts
- Currently low count due to off-season

---

### 2.5 Apple MLB (apple_mlb)

**Count:** 0 playables (0.0% of total - OFF SEASON)  
**Logical Service Code:** `apple_mlb`  
**Display Name:** Apple MLB  
**Priority:** 12

**URL Pattern:**
```
https://tv.apple.com/us/sporting-event/{team1}-vs-{team2}/umc.cse.{event_id}
```

**Example:**
```
https://tv.apple.com/us/sporting-event/yankees-vs-red-sox/umc.cse.abc456def
```

**Content Types:**
- MLB Friday Night Baseball (exclusive games)
- Select MLB games throughout season

**Notes:**
- **FREE with Apple TV+ subscription ($9.99/month)**
- Or available à la carte during baseball season
- 2 exclusive games every Friday night
- No blackout restrictions
- Currently 0 playables (baseball off-season)

---

### 2.6 Apple TV+ Other (apple_other)

**Count:** 0 playables (0.0% of total)  
**Logical Service Code:** `apple_other`  
**Display Name:** Apple TV+  
**Priority:** 15

**URL Pattern:**
```
https://tv.apple.com/us/sporting-event/{event-name}/umc.cse.{event_id}
```

**Content Types:**
- Future: Could include NBA, NHL, or other sports
- Olympic sports
- Niche/international sports

**Notes:**
- Placeholder for future Apple sports deals
- Apple is actively pursuing sports rights
- Potential future: NBA, NFL, NHL partnerships

---

## 3. SERVICE PRIORITY RANKING

When multiple streaming options are available for a single event, FruitDeepLinks selects the "best" option based on priority:

| Priority | Service           | Reasoning                                    |
|----------|-------------------|----------------------------------------------|
| 0        | ESPN+             | Premium sports service, best quality         |
| 1        | Peacock (Native)  | Streaming-first, good quality                |
| 2        | Peacock (Web)     | Web fallback for Peacock                     |
| 3        | Paramount+        | Reliable streaming platform                  |
| 4        | Prime Video       | High quality, widely available               |
| 5        | Max               | Premium platform, good quality               |
| 6        | CBS Sports App    | Sports-focused, good UX                      |
| 7        | CBS               | Broadcast quality                            |
| 8        | NBC Sports        | Sports-focused                               |
| 9-10     | FOX Sports        | Sports-focused                               |
| 11       | Apple MLS         | Sport-specific, premium quality              |
| 12       | Apple MLB         | Sport-specific, premium quality              |
| 13-14    | Apple NBA/NHL     | Future potential                             |
| 15       | Apple TV+ Other   | General streaming                            |
| 16-17    | DAZN              | Niche sports                                 |
| 18       | F1 TV             | Very sport-specific                          |
| 19       | ViX               | International content                        |
| 20       | NFL+              | Limited availability (mobile only)           |
| 21-22    | truTV/TNT         | Requires cable auth                          |
| 30-31    | Generic Web       | Last resort fallback                         |

---

## 4. SUBSCRIPTION COST SUMMARY

**Free Tier:**
- None (all require subscription or cable authentication)

**Under $10/month:**
- **Peacock Premium:** $5.99/month
- **Paramount+:** $5.99/month (with ads)
- **ViX Premium:** $6.99/month
- **NFL+:** $6.99/month
- **Apple TV+:** $9.99/month (includes MLB)

**$10-20/month:**
- **ESPN+:** $10.99/month
- **Peacock Premium Plus:** $11.99/month
- **Paramount+ (no ads):** $11.99/month
- **Max:** $9.99-19.99/month
- **Apple MLS Season Pass:** $14.99/month
- **DAZN:** $19.99/month

**Specialized:**
- **F1 TV Pro:** $9.99/month or $79.99/year
- **Cable/TV Provider:** Required for NBC Sports, CBS (some content), FOX Sports

**Bundle Opportunities:**
- **Disney Bundle:** ESPN+, Disney+, Hulu ($14.99/month)
- **Paramount+/Showtime:** $11.99/month

---

## 5. TECHNICAL NOTES

### Deeplink URL Schemes

**Native App Schemes:**
- Custom protocol (e.g., `aiv://`, `sportsonespn://`, `pplus://`)
- Registered with iOS/Android/tvOS
- Opens app directly if installed
- Falls back to app store if not installed

**Web URLs:**
- Standard `https://` protocol
- Opens in browser or in-app web view
- May redirect to native app if detected
- Universal links (iOS) and App Links (Android) enable app handoff

### Authentication

**App-Based Services:**
- Usually handle authentication in-app
- OAuth tokens stored in keychain
- Deeplinks work if user already logged in

**Web-Based Services:**
- May require login via web browser
- Cookies store session
- May prompt for credentials

### Platform Support

Most services support:
- **iOS/iPadOS:** All native schemes work
- **Android:** Most native schemes work
- **Android TV/Fire TV:** Native apps preferred, web fallback
- **Apple TV:** Native apps strongly preferred
- **Roku:** Web-based access only (no custom URL schemes)

---

## 6. FILTERING RECOMMENDATIONS

### For Maximum Coverage
Enable all services you have subscriptions for.

### For Premium Sports Fans
Minimum:
- ESPN+ ($10.99)
- Peacock Premium ($5.99)
- Prime Video (included with Prime)

### For Budget Sports Fans
- Prime Video (already have Prime?)
- Peacock Premium ($5.99)
- ViX (free tier)

### For Specific Sports

**Soccer:**
- Paramount+ (Champions League, Serie A)
- ViX (Liga MX)
- Peacock (Premier League)
- Max (Liga MX)

**American Football:**
- Prime Video (Thursday Night Football)
- NFL+ (mobile replays)
- ESPN+ (college)

**Basketball:**
- ESPN+ (college)
- Prime Video (some games)
- Max (March Madness)

**Baseball:**
- Apple TV+ (Friday Night Baseball)
- ESPN+ (some games)

**Hockey:**
- ESPN+ (NHL)
- Prime Video (some games)

**Motorsports:**
- F1 TV (Formula 1)
- ESPN+ (various)

---

## 7. MISSING/FUTURE SERVICES

Services that exist but weren't found in current dataset:

### Not Currently in Database:
- **YouTube TV** - Has sports, no deeplinks found yet
- **Hulu + Live TV** - Sports included, no deeplinks
- **FuboTV** - Sports-focused streaming, no deeplinks
- **FloSports** - Niche sports, no integration
- **NESN** - Regional sports network, no deeplinks

### Potential Future Additions:
- **Apple NBA** - If Apple acquires NBA rights
- **Apple NHL** - If Apple acquires NHL rights
- **Netflix Sports** - If Netflix enters live sports
- **Regional Sports Networks** - Bally Sports, etc.

---

## 8. DATA FRESHNESS

**Current Snapshot:** December 4, 2025  
**Season Context:**
- ✅ NFL: Active (Week 14)
- ✅ NBA: Active (Regular season)
- ✅ NHL: Active (Regular season)
- ✅ College Basketball: Active
- ✅ College Football: Ending (bowl season)
- ❌ MLB: Off-season (explains 0 Apple MLB events)
- ❌ MLS: Off-season (playoffs ended, explains only 2 events)
- ✅ Soccer: International leagues active

---

## 9. APPENDIX: RAW SERVICE CODES

For developers and advanced users, here are all the raw provider codes found:

```
App-Based Services:
  aiv                   Prime Video (standard)
  gametime              Prime Video TNF
  sportscenter          ESPN (standard)
  sportsonespn          ESPN+ (subscription)
  cbssportsapp          CBS Sports mobile app
  cbstve                CBS broadcast TV
  pplus                 Paramount+
  vixapp                ViX
  open.dazn.com         DAZN
  dazn                  DAZN (alternate)
  nflctv                NFL+
  watchtru              truTV
  watchtnt              TNT
  foxone                FOX Sports 1
  fsapp                 FOX Sports app
  nbcsportstve          NBC Sports
  peacock               Peacock (native - not in current dataset)
  peacocktv             Peacock (alternate)

Web-Based Logical Services:
  peacock_web           Peacock web URLs
  max                   Max/HBO Max
  f1tv                  F1 TV
  apple_mls             Apple MLS Season Pass
  apple_mlb             Apple MLB (Friday Night Baseball)
  apple_nba             Apple NBA (future)
  apple_nhl             Apple NHL (future)
  apple_other           Other Apple TV+ sports
  https                 Generic web fallback
```

---

**Document Version:** 1.0  
**Generated by:** FruitDeepLinks Logical Service Mapper  
**Contact:** See GitHub repository for updates
