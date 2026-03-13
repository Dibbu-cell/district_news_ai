from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import feedparser
import requests

from config.config import REQUEST_WORKERS


RSS_FEEDS = [
    {"url": "https://www.thehindu.com/news/national/feeder/default.rss", "source": "the_hindu_national"},
    {"url": "https://www.thehindu.com/news/cities/Delhi/feeder/default.rss", "source": "the_hindu_delhi", "state_hint": "delhi", "district_hint": "new delhi"},
    {"url": "https://www.thehindu.com/news/cities/Hyderabad/feeder/default.rss", "source": "the_hindu_hyderabad", "state_hint": "telangana", "district_hint": "hyderabad"},
    {"url": "https://www.thehindu.com/news/cities/chennai/feeder/default.rss", "source": "the_hindu_chennai", "state_hint": "tamil nadu", "district_hint": "chennai"},
    {"url": "https://www.thehindu.com/news/cities/bangalore/feeder/default.rss", "source": "the_hindu_bengaluru", "state_hint": "karnataka", "district_hint": "bengaluru urban"},
    {"url": "https://indianexpress.com/section/india/feed/", "source": "indian_express_india"},
    {"url": "https://indianexpress.com/section/cities/delhi/feed/", "source": "indian_express_delhi", "state_hint": "delhi", "district_hint": "new delhi"},
    {"url": "https://indianexpress.com/section/cities/mumbai/feed/", "source": "indian_express_mumbai", "state_hint": "maharashtra", "district_hint": "mumbai"},
    {"url": "https://indianexpress.com/section/cities/chandigarh/feed/", "source": "indian_express_chandigarh", "state_hint": "chandigarh", "district_hint": "chandigarh"},
]


def _to_iso8601(struct_time_value):

    if struct_time_value is None:
        return None

    return datetime(*struct_time_value[:6]).isoformat() + "Z"


def _fetch_feed(feed_config):

    articles = []

    try:
        response = requests.get(feed_config["url"], timeout=15)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
    except requests.RequestException:
        return articles

    if getattr(feed, "bozo", False):
        return articles

    for entry in feed.entries:
        articles.append(
            {
                "title": entry.get("title"),
                "content": entry.get("summary") or entry.get("description"),
                "url": entry.get("link"),
                "source": feed_config["source"],
                "published_at": _to_iso8601(entry.get("published_parsed") or entry.get("updated_parsed")),
                "state_hint": feed_config.get("state_hint"),
                "district_hint": feed_config.get("district_hint"),
            }
        )

    return articles


def fetch_local_publishers():

    articles = []

    with ThreadPoolExecutor(max_workers=min(REQUEST_WORKERS, len(RSS_FEEDS))) as executor:
        futures = [executor.submit(_fetch_feed, feed_config) for feed_config in RSS_FEEDS]

        for future in as_completed(futures):
            try:
                articles.extend(future.result())
            except Exception:
                continue

    return articles