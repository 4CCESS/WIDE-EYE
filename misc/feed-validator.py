import csv
import feedparser

INPUT_FILE = 'feeds/tagged_sources.csv'
OUTPUT_FILE = 'validated_feeds.csv'
VERBOSE_ENTRIES_FILE = 'full_text_entries.txt'
TITLE_ENTRIES_FILE = 'title_entries.txt'

def validate_feed(url):
    try:
        feed = feedparser.parse(url)

        if feed.bozo:
            return {
                'status': 'Parse Error',
                'title': feed.feed.get('title', ''),
                'entries': len(feed.entries),
                'error': str(feed.bozo_exception)
            }
        elif not feed.entries:
            return {
                'status': 'No Entries',
                'title': feed.feed.get('title', ''),
                'entries': 0,
                'error': ''
            }
        else:

            with open(VERBOSE_ENTRIES_FILE, 'a', encoding='utf-8') as f:
                f.write("=== FEED START ===\n")
                f.write(f"Source: {feed.feed.get('title', '')}\n")

                for entry in feed.entries:
                    title = entry.get('title', 'No Title')
                    published = entry.get('published', 'No Date')
                    summary = entry.get('summary', 'No Summary')

                    f.write("=== ENTRY START ===\n")
                    f.write(f"Title    : {title}\n")
                    f.write(f"Published: {published}\n")
                    f.write(f"Summary  :\n{summary}\n")
                    f.write("=== ENTRY END ===\n\n")

                    with open(TITLE_ENTRIES_FILE, 'a', encoding='utf-8') as g:
                        g.write(f"{title}\n")

                

            return {
                'status': 'OK',
                'title': feed.feed.get('title', ''),
                'entries': len(feed.entries),
                'error': ''
            }

    except Exception as e:
        return {
            'status': 'Failed',
            'title': '',
            'entries': 0,
            'error': str(e)
        }

def main():
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)

    header = rows[0]
    data_rows = rows[1:]

    results = []
    for region, tags, name, url in data_rows:
        result = validate_feed(url)
        results.append({
            'Region': region,
            'Tags': tags,
            'Source': name,
            'URL': url,
            'Status': result['status'],
            'Feed Title': result['title'],
            'Entries': result['entries'],
            'Error': result['error']
        })

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Region', 'Tags', 'Source', 'URL', 'Status', 'Feed Title', 'Entries', 'Error'
        ])
        writer.writeheader()
        writer.writerows(results)

    print(f"✅ Validation complete. Results written to: {OUTPUT_FILE}")

if __name__ == '__main__':
    main()
