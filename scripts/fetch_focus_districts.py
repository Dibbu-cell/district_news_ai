import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from collectors.google_news import fetch_google_news_targets
from config.config import FOCUS_BACKFILL_MAX_DISTRICTS, FOCUS_BACKFILL_STATE_BATCH
from database.db import engine
from database.schema import ensure_schema
from scheduler.pipeline import _get_existing_urls, _load_master_districts, _prepare_articles, _select_focus_districts, _write_articles


def run_focus_backfill():
    ensure_schema(engine)

    focus_districts = _select_focus_districts(FOCUS_BACKFILL_MAX_DISTRICTS, FOCUS_BACKFILL_STATE_BATCH)
    raw_news = fetch_google_news_targets(focus_districts)

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

    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "target_districts": len(focus_districts),
        "collected": len(raw_news),
        "unique": len(unique_news),
        "inserted": inserted_count,
    }

    report_path = Path("data") / "reports" / "focus_districts_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    run_focus_backfill()
