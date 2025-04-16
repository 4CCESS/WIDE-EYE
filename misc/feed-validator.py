import csv
import feedparser

INPUT_FILE = 'rss_feeds.csv'
OUTPUT_FILE = 'validated_feeds.csv'

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
    for region, name, url in data_rows:
        result = validate_feed(url)
        results.append({
            'Region': region,
            'Source': name,
            'URL': url,
            'Status': result['status'],
            'Feed Title': result['title'],
            'Entries': result['entries'],
            'Error': result['error']
        })

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Region', 'Source', 'URL', 'Status', 'Feed Title', 'Entries', 'Error'
        ])
        writer.writeheader()
        writer.writerows(results)

    print(f"✅ Validation complete. Results written to: {OUTPUT_FILE}")

if __name__ == '__main__':
    main()
