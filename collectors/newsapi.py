import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from config.config import NEWS_API_KEY, NEWSAPI_MAX_PAGES, REQUEST_WORKERS

from collectors.query_builder import build_newsapi_query_targets


def _fetch_query(query, past, today, max_pages=NEWSAPI_MAX_PAGES, state_hint=None, district_hint=None):

    url = "https://newsapi.org/v2/everything"
    articles = []

    for page in range(1, max_pages + 1):

        params = {
            "q": query,
            "from": past.strftime("%Y-%m-%d"),
            "to": today.strftime("%Y-%m-%d"),
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 100,
            "page": page,
            "apiKey": NEWS_API_KEY
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError):
            break

        batch = data.get("articles", [])

        if not batch:
            break

        for article in batch:

            articles.append({
                "title": article.get("title"),
                "content": article.get("content") or article.get("description"),
                "url": article.get("url"),
                "source": "newsapi",
                "published_at": article.get("publishedAt"),
                "state_hint": state_hint,
                "district_hint": district_hint,
            })

        if len(batch) < params["pageSize"]:
            break

    return articles


def fetch_newsapi():

    today = datetime.utcnow()
    past = today - timedelta(days=1)

    articles = []
    query_targets = build_newsapi_query_targets()

    with ThreadPoolExecutor(max_workers=REQUEST_WORKERS) as executor:
        futures = [
            executor.submit(
                _fetch_query,
                target["query"],
                past,
                today,
                NEWSAPI_MAX_PAGES,
                target.get("state_hint"),
                target.get("district_hint"),
            )
            for target in query_targets
        ]

        for future in as_completed(futures):
            try:
                articles.extend(future.result())
            except Exception:
                continue

    return articles