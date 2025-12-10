#!/usr/bin/env python3
"""
Check for events with multiple providers in the database
"""
import sqlite3

DB_PATH = "/app/data/fruit_events.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

print("EVENTS WITH MULTIPLE PROVIDERS")
print("="*80)

# Find events with multiple distinct providers
cur = conn.cursor()
cur.execute("""
    SELECT event_id, COUNT(DISTINCT provider) as provider_count
    FROM playables
    GROUP BY event_id
    HAVING COUNT(DISTINCT provider) > 1
    ORDER BY provider_count DESC
    LIMIT 10
""")

multi_provider_events = cur.fetchall()

print(f"Found {len(multi_provider_events)} events with multiple providers\n")

for event_row in multi_provider_events:
    event_id = event_row['event_id']
    provider_count = event_row['provider_count']
    
    # Get event title
    cur.execute("SELECT title FROM events WHERE id = ?", (event_id,))
    title_row = cur.fetchone()
    title = title_row['title'] if title_row else 'Unknown'
    
    print(f"Event: {title}")
    print(f"  ID: {event_id}")
    print(f"  Providers: {provider_count}")
    
    # Get all playables for this event
    cur.execute("""
        SELECT provider, deeplink_play, priority
        FROM playables
        WHERE event_id = ?
        ORDER BY priority DESC
    """, (event_id,))
    
    playables = cur.fetchall()
    for p in playables:
        deeplink_preview = (p['deeplink_play'] or '')[:60]
        print(f"    - {p['provider']:15s} (priority: {p['priority']:2d}) {deeplink_preview}...")
    
    print()

print("="*80)

# Now specifically check Brown vs Providence
print("\nBROWN VS PROVIDENCE CHECK:")
print("-"*80)

event_id = "appletv-umc.cse.6l8qjo6qhtenjiim3wllfky82"

cur.execute("SELECT title FROM events WHERE id = ?", (event_id,))
row = cur.fetchone()

if row:
    print(f"Event: {row['title']}")
    
    cur.execute("""
        SELECT provider, deeplink_play, priority
        FROM playables
        WHERE event_id = ?
        ORDER BY priority DESC
    """, (event_id,))
    
    playables = cur.fetchall()
    print(f"Playables: {len(playables)}")
    
    for p in playables:
        print(f"  - {p['provider']:15s} (priority: {p['priority']:2d})")
    
    if len(playables) == 1:
        print("\n⚠️  Only 1 playable found - need to re-import to get all services")
    elif len(playables) > 1:
        print(f"\n✓ {len(playables)} playables found - adjusting priority will help")
else:
    print("Event not found in database")

conn.close()
