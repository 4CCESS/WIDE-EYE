import time
import feedparser

class Collector:
    def __init__(self):
        # Initialize any common collector state here
        self.data_source_methods = {
            1: self.collect_rss
            # Future source types like 2: self.collect_api, 3: self.collect_file, etc.
        }

    def main(self):
        print("Select a data source type:")
        print("1. RSS Feed")

        try:
            source_type = int(input("Enter number (1): ").strip())
            if source_type not in self.data_source_methods:
                print("Invalid input. Exiting.")
                return

            if source_type == 1:
                rss_url = input("Enter RSS feed URL: ").strip()
                refresh_rate = float(input("Enter refresh rate in seconds: ").strip())
                self.data_source_methods[source_type](rss_url, refresh_rate)

        except ValueError:
            print("Invalid input. Exiting.")
            return

    def collect_rss(self, url, refresh_rate):
        print(f"\nStarting RSS collection from {url} every {refresh_rate} seconds.\n")
        try:
            while True:
                feed = feedparser.parse(url)

                if feed.bozo:
                    print(f"Error parsing RSS feed: {feed.bozo_exception}")
                else:
                    print(f"Retrieved {len(feed.entries)} entries from {url}")
                    # extrapolate_rss
                    # deliver_intel
                    for entry in feed.entries[:3]:  # Print top 3 for demo
                        print(f"→ {entry.title}")
                
                print("\nWaiting for next refresh...\n")
                time.sleep(refresh_rate)
        except KeyboardInterrupt:
            print("\nCollector stopped manually.")
    
    # def extrapolate_rss
    # def deliver_intel

# Run the collector
if __name__ == "__main__":
    collector = Collector()
    collector.main()
