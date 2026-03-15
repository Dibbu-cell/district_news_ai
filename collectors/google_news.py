from datetime import datetime, timedelta
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests

from config.config import (
    GOOGLE_DAY_SLICE_DAYS,
    GOOGLE_LOOKBACK_DAYS,
    GOOGLE_USE_DAY_SLICES,
    REQUEST_WORKERS,
)
from collectors.query_builder import build_google_news_terms


def _to_iso8601(struct_time_value):

    if struct_time_value is None:
        return None

    return datetime(*struct_time_value[:6]).isoformat() + "Z"


def _build_google_query_variants(query):

    base_query = query.strip()

    if not base_query:
        return []

    variants = []
    seen = set()

    def add_variant(value):
        cleaned = " ".join(value.split())

        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            variants.append(cleaned)

    if "when:" in base_query.lower() or "after:" in base_query.lower() or "before:" in base_query.lower():
        add_variant(base_query)
        return variants

    if GOOGLE_LOOKBACK_DAYS > 0:
        add_variant(f"{base_query} when:{GOOGLE_LOOKBACK_DAYS}d")
    else:
        add_variant(base_query)

    if GOOGLE_USE_DAY_SLICES and GOOGLE_DAY_SLICE_DAYS > 0:
        today = datetime.utcnow().date()

        for offset in range(GOOGLE_DAY_SLICE_DAYS):
            day_start = today - timedelta(days=offset + 1)
            day_end = day_start + timedelta(days=1)
            add_variant(f"{base_query} after:{day_start.isoformat()} before:{day_end.isoformat()}")

    return variants


def _fetch_query_variants(query, state_hint=None, district_hint=None):

    query_articles = []

    for effective_query in _build_google_query_variants(query):
        url = f"https://news.google.com/rss/search?q={quote_plus(effective_query)}&hl=en-IN&gl=IN&ceid=IN:en"

        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            feed = feedparser.parse(response.content)
        except requests.RequestException:
            continue

        if getattr(feed, "bozo", False):
            continue

        for entry in feed.entries:

            query_articles.append({
                "title": entry.get("title"),
                "content": entry.get("summary"),
                "url": entry.get("link"),
                "source": "google_news",
                "published_at": _to_iso8601(entry.get("published_parsed")),
                "state_hint": state_hint,
                "district_hint": district_hint,
            })

    return query_articles


def fetch_google_news_targets(targets, after_date=None, before_date=None):

    QUERY_TEMPLATES = [
        "{district} {state} news",
        "{district} district news india",
        "{district} civic issues local news",
    ]

    articles = []
    query_tasks = []

    for state_name, district_name in targets:
        for template in QUERY_TEMPLATES:
            base = template.format(district=district_name, state=state_name)
            if after_date and before_date:
                query = f"{base} after:{after_date} before:{before_date}"
            else:
                query = base
            query_tasks.append((query, state_name, district_name))

    with ThreadPoolExecutor(max_workers=REQUEST_WORKERS) as executor:
        futures = [
            executor.submit(_fetch_query_variants, q, s, d)
            for q, s, d in query_tasks
        ]

        for future in as_completed(futures):
            try:
                articles.extend(future.result())
            except Exception:
                continue

    return articles


def fetch_google_news():

    def fetch_single_query(query):

        return _fetch_query_variants(query)

    articles = []

    with ThreadPoolExecutor(max_workers=REQUEST_WORKERS) as executor:
        futures = [executor.submit(fetch_single_query, query) for query in build_google_news_terms()]

        for future in as_completed(futures):
            try:
                articles.extend(future.result())
            except Exception:
                continue

    return articles