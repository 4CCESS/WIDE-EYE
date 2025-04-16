import feedparser

def fetch_rss_feed(url):
    """
    Fetch and parse an RSS feed from the given URL.
    Returns a list of entries with title, link, and published date.
    """
    feed = feedparser.parse(url)
    entries = []

    for entry in feed.entries:
        item = {
            'title': entry.get('title', 'No title'),
            'link': entry.get('link', 'No link'),
            'published': entry.get('published', 'No date')
        }
        entries.append(item)
    
    return entries

def main():
    # Example RSS feed URLs — feel free to add your own
    rss_urls = [
        "https://kyivindependent.com/news-archive/rss/",
    ]

    for url in rss_urls:
        print(f"\n--- Fetching from: {url} ---")
        entries = fetch_rss_feed(url)
        print (len(entries))
        for entry in entries:  # Show only the first 5 entries
            print(f"Title: {entry['title']}")
            print(f"Link: {entry['link']}")
            print(f"Published: {entry['published']}")
            print()

if __name__ == "__main__":
    main()
