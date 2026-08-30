import feedparser

print("SCRIPT STARTED - LINE 1")

feed = feedparser.parse("https://feeds.bbci.co.uk/sport/football/rss.xml")

print("Feed status:", feed.get("status", "no status field"))
print("Bozo (parse error flag):", feed.bozo)
if feed.bozo:
    print("Parse exception:", feed.bozo_exception)

print(f"Total entries: {len(feed.entries)}")
print()
if len(feed.entries) > 0:
    print(feed.entries[0].title)
    print(feed.entries[0].summary)
else:
    print("No entries found - feed may have failed to load")