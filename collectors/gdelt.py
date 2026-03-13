import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from config.config import GDELT_MAX_RECORDS, GDELT_WINDOW_HOURS, REQUEST_WORKERS

from collectors.query_builder import BASE_TERMS, build_district_terms, build_state_terms


def _fetch_query(query, start_time, end_time):

    url = "https://api.gdeltproject.org/api/v2/doc/doc"

    params = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": GDELT_MAX_RECORDS,
        "format": "json",
        "startdatetime": start_time.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end_time.strftime("%Y%m%d%H%M%S")
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError):
        return []

    articles = []

    for article in data.get("articles", []):

        articles.append({
            "title": article.get("title"),
            "content": article.get("title") or article.get("sourcecountry"),
            "url": article.get("url"),
            "source": "gdelt",
            "published_at": article.get("seendate")
        })

    return articles


def _build_time_windows(end_time, hours=24, window_hours=GDELT_WINDOW_HOURS):

    windows = []
    current_end = end_time
    start_limit = end_time - timedelta(hours=hours)

    while current_end > start_limit:
        current_start = max(start_limit, current_end - timedelta(hours=window_hours))
        windows.append((current_start, current_end))
        current_end = current_start

    return windows


def fetch_gdelt():

    end_time = datetime.utcnow()
    articles = []

    high_priority_terms = list(BASE_TERMS) + build_state_terms(limit=20)
    district_terms = build_district_terms(limit=25)
    futures = []

    with ThreadPoolExecutor(max_workers=REQUEST_WORKERS) as executor:
        for query in high_priority_terms:
            for window_start, window_end in _build_time_windows(end_time):
                futures.append(executor.submit(_fetch_query, query, window_start, window_end))

        full_day_start = end_time - timedelta(days=1)

        for query in district_terms:
            futures.append(executor.submit(_fetch_query, query, full_day_start, end_time))

        for future in as_completed(futures):
            try:
                articles.extend(future.result())
            except Exception:
                continue

    return articles