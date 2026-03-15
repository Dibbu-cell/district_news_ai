import json
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd

from collectors.google_news import fetch_google_news
from database.db import engine
from database.schema import ensure_schema
from scheduler.pipeline import (
    _get_existing_urls,
    _prepare_articles,
    _write_articles,
    backfill_missing_locations,
    delete_expired_news,
)


def run_google_backfill():
    ensure_schema(engine)
    delete_expired_news()

    raw_news = fetch_google_news()

    unique_news = []
    seen_keys = set()

    for article in raw_news:
        unique_key = article.get("url") or (article.get("title"), article.get("source"))

        if not unique_key or unique_key in seen_keys:
            continue

        seen_keys.add(unique_key)
        unique_news.append(article)

    existing_urls = _get_existing_urls()
    prepared_df = _prepare_articles(unique_news, existing_urls)

    inserted_count = 0

    if not prepared_df.empty:
        inserted_count = _write_articles(prepared_df)

    backfilled_count = backfill_missing_locations()

    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "collected": len(raw_news),
        "unique": len(unique_news),
        "inserted": inserted_count,
        "backfilled": backfilled_count,
    }

    summary_records = []

    with engine.connect() as conn:
        summary = pd.read_sql(
            """
            SELECT date(published_at) AS day, source, COUNT(*) AS count
            FROM news_articles
            WHERE published_at >= datetime('now', '-5 days')
            GROUP BY day, source
            ORDER BY day DESC, source
            """,
            conn,
        )
        if not summary.empty:
            summary_records = summary.to_dict(orient="records")

    report["last_5_day_source_counts"] = summary_records

    report_path = Path("data") / "reports" / "google_backfill_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    run_google_backfill()
