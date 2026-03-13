from datetime import datetime
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests

from config.config import REQUEST_WORKERS
from collectors.query_builder import build_google_news_terms


def _to_iso8601(struct_time_value):

    if struct_time_value is None:
        return None

    return datetime(*struct_time_value[:6]).isoformat() + "Z"


def fetch_google_news():

    def fetch_single_query(query):

        query_articles = []
        url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-IN&gl=IN&ceid=IN:en"

        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            feed = feedparser.parse(response.content)
        except requests.RequestException:
            return query_articles

        if getattr(feed, "bozo", False):
            return query_articles

        for entry in feed.entries:

            query_articles.append({
                "title": entry.get("title"),
                "content": entry.get("summary"),
                "url": entry.get("link"),
                "source": "google_news",
                "published_at": _to_iso8601(entry.get("published_parsed"))
            })

        return query_articles

    articles = []

    with ThreadPoolExecutor(max_workers=REQUEST_WORKERS) as executor:
        futures = [executor.submit(fetch_single_query, query) for query in build_google_news_terms()]

        for future in as_completed(futures):
            try:
                articles.extend(future.result())
            except Exception:
                continue

    return articles