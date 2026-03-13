import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from collectors.newsapi import fetch_newsapi
from collectors.gdelt import fetch_gdelt
from collectors.google_news import fetch_google_news
from collectors.local_publishers import fetch_local_publishers
from processing.text_cleaner import clean_text
from processing.ner_location import extract_locations_batch
from processing.geo_resolver import resolve_location_details
from embedding.embedding_model import generate_embeddings

from config.config import COLLECTOR_WORKERS, DB_WRITE_CHUNK_SIZE, PIPELINE_BATCH_SIZE, RETENTION_DAYS
from database.db import engine
from database.schema import ensure_schema
import pandas as pd
from pandas.errors import DatabaseError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


def _get_existing_urls():

    try:
        existing = pd.read_sql("SELECT url FROM news_articles", engine)
    except (SQLAlchemyError, DatabaseError):
        return set()

    return {
        value for value in existing["url"].dropna().tolist()
        if value
    }


def _collect_from_sources():

    fetchers = [fetch_newsapi, fetch_gdelt, fetch_google_news, fetch_local_publishers]
    collected_news = []

    with ThreadPoolExecutor(max_workers=COLLECTOR_WORKERS) as executor:
        future_map = {executor.submit(fetcher): fetcher.__name__ for fetcher in fetchers}

        for future in as_completed(future_map):
            try:
                collected_news.extend(future.result())
            except Exception:
                continue

    return collected_news


def _chunked(items, chunk_size):

    for start in range(0, len(items), chunk_size):
        yield items[start:start + chunk_size]


def _prepare_articles(unique_news, existing_urls):

    candidate_rows = []
    raw_texts = []

    for article in unique_news:

        if article.get("url") in existing_urls:
            continue

        raw_text = ((article.get("title") or "") + " " + (article.get("content") or "")).strip()
        cleaned_text = clean_text(raw_text)

        if not cleaned_text:
            continue

        candidate_rows.append({
            "title": article.get("title"),
            "content": cleaned_text,
            "url": article.get("url"),
            "source": article.get("source"),
            "published_at": article.get("published_at"),
            "state_hint": article.get("state_hint"),
            "district_hint": article.get("district_hint"),
            "raw_text": raw_text,
        })
        raw_texts.append(raw_text)

    if not candidate_rows:
        return pd.DataFrame()

    location_groups = extract_locations_batch(raw_texts, batch_size=PIPELINE_BATCH_SIZE)
    states = []
    districts = []
    state_confidences = []
    district_confidences = []

    for row, raw_text, locations in zip(candidate_rows, raw_texts, location_groups):
        location_details = resolve_location_details(
            locations,
            text=raw_text,
            title=row.get("title") or "",
            state_hint=row.get("state_hint"),
            district_hint=row.get("district_hint"),
        )
        states.append(location_details["state"])
        districts.append(location_details["district"])
        state_confidences.append(location_details["state_confidence"])
        district_confidences.append(location_details["district_confidence"])

    embeddings = []

    for batch in _chunked([row["content"] for row in candidate_rows], PIPELINE_BATCH_SIZE):
        embeddings.extend(generate_embeddings(batch, batch_size=PIPELINE_BATCH_SIZE))

    processed_df = pd.DataFrame(candidate_rows)
    processed_df["state"] = states
    processed_df["district"] = districts
    processed_df["state_confidence"] = state_confidences
    processed_df["district_confidence"] = district_confidences
    processed_df["embedding"] = embeddings

    return processed_df.drop(columns=["raw_text", "state_hint", "district_hint"])


def _write_articles(df):

    if df.empty:
        return 0

    write_df = df.copy()
    write_df["embedding"] = write_df["embedding"].apply(json.dumps)
    write_df["published_at"] = pd.to_datetime(write_df["published_at"], utc=True, errors="coerce")
    write_df["published_at"] = write_df["published_at"].where(write_df["published_at"].notna(), None)
    write_df["ingested_at"] = datetime.now(timezone.utc)

    write_df.to_sql(
        "news_articles",
        engine,
        if_exists="append",
        index=False,
        chunksize=DB_WRITE_CHUNK_SIZE,
        method="multi",
    )

    return len(write_df)


def delete_expired_news(retention_days=RETENTION_DAYS):

    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).strftime("%Y-%m-%d %H:%M:%S")

    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM news_articles WHERE published_at IS NOT NULL AND published_at < :cutoff"),
                {"cutoff": cutoff},
            )
            return result.rowcount or 0
    except SQLAlchemyError:
        return 0


def backfill_missing_locations():

    try:
        pending_df = pd.read_sql(
            "SELECT url, source, title, content, state, district, state_confidence, district_confidence FROM news_articles WHERE state IS NULL OR district IS NULL",
            engine,
        )
    except (SQLAlchemyError, DatabaseError):
        return 0

    if pending_df.empty:
        return 0

    updated_rows = 0

    with engine.begin() as conn:
        for row in pending_df.itertuples(index=False):
            combined_text = f"{row.title or ''} {row.content or ''}"
            location_details = resolve_location_details([], text=combined_text, title=row.title or "")
            state = location_details["state"]
            district = location_details["district"]

            if not state and not district:
                continue

            conn.execute(
                text(
                    """
                    UPDATE news_articles
                    SET state = COALESCE(state, :state),
                        district = COALESCE(district, :district),
                        state_confidence = CASE
                            WHEN state IS NULL AND :state IS NOT NULL THEN :state_confidence
                            ELSE state_confidence
                        END,
                        district_confidence = CASE
                            WHEN district IS NULL AND :district IS NOT NULL THEN :district_confidence
                            ELSE district_confidence
                        END
                    WHERE url = :url AND source = :source
                    """
                ),
                {
                    "state": state,
                    "district": district,
                    "state_confidence": location_details["state_confidence"],
                    "district_confidence": location_details["district_confidence"],
                    "url": row.url,
                    "source": row.source,
                },
            )
            updated_rows += 1

    return updated_rows


def run_pipeline():

    ensure_schema(engine)
    delete_expired_news()

    news = _collect_from_sources()

    unique_news = []
    seen_keys = set()

    for article in news:

        unique_key = article.get("url") or (article.get("title"), article.get("source"))

        if not unique_key or unique_key in seen_keys:
            continue

        seen_keys.add(unique_key)
        unique_news.append(article)

    existing_urls = _get_existing_urls()

    df = _prepare_articles(unique_news, existing_urls)

    if df.empty:
        return {"inserted": 0, "backfilled": 0, "collected": len(news), "unique": len(unique_news)}

    inserted_count = _write_articles(df)

    backfilled_count = backfill_missing_locations()

    return {
        "collected": len(news),
        "unique": len(unique_news),
        "inserted": inserted_count,
        "backfilled": backfilled_count,
    }


if __name__ == "__main__":
    run_pipeline()