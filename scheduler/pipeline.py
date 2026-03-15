import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from collectors.newsapi import fetch_newsapi
from collectors.gdelt import fetch_gdelt
from collectors.google_news import fetch_google_news, fetch_google_news_targets
from collectors.local_publishers import fetch_local_publishers
from processing.text_cleaner import clean_text
from processing.ner_location import extract_locations_batch
from processing.geo_resolver import resolve_location_details
from embedding.embedding_model import generate_embeddings

from config.config import COLLECTOR_WORKERS, DAILY_FOCUS_DISTRICT_BATCH, DAILY_FOCUS_STATE_BATCH, PIPELINE_BATCH_SIZE, RETENTION_DAYS
from database.news_store import (
    append_articles,
    delete_expired_news as delete_expired_news_store,
    ensure_data_store_ready,
    get_assigned_state_district_pairs,
    get_existing_urls,
    get_pending_location_rows,
    update_article_location,
)
import pandas as pd


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

    return append_articles(df)


def delete_expired_news(retention_days=RETENTION_DAYS):

    return delete_expired_news_store(retention_days)


def backfill_missing_locations():

    pending_df = get_pending_location_rows()

    if pending_df.empty:
        return 0

    updated_rows = 0

    for row in pending_df.itertuples(index=False):
        combined_text = f"{row.title or ''} {row.content or ''}"
        location_details = resolve_location_details([], text=combined_text, title=row.title or "")
        state = location_details["state"]
        district = location_details["district"]

        if not state and not district:
            continue

        update_article_location(
            url=row.url,
            source=row.source,
            state=state,
            district=district,
            state_confidence=location_details["state_confidence"],
            district_confidence=location_details["district_confidence"],
        )
        updated_rows += 1

    return updated_rows


def _load_master_districts():

    data_path = Path(__file__).resolve().parent.parent / "data" / "india_districts.csv"
    frame = pd.read_csv(data_path)
    frame.columns = [c.strip().lower() for c in frame.columns]

    if "district" not in frame.columns or "state" not in frame.columns:
        frame = pd.read_csv(data_path, skiprows=1)
        frame.columns = [c.strip().lower() for c in frame.columns]

    frame["district"] = frame["district"].astype(str).str.strip().str.lower()
    frame["state"] = frame["state"].astype(str).str.strip().str.lower()

    return frame[(frame["district"] != "") & (frame["state"] != "")][["state", "district"]].drop_duplicates()


def _select_focus_districts(max_districts, state_batch):

    master = _load_master_districts()
    assigned = get_assigned_state_district_pairs()

    if assigned.empty:
        return []

    coverage = master.merge(assigned, on=["state", "district"], how="left", indicator=True)
    uncovered = coverage[coverage["_merge"] == "left_only"].drop(columns=["_merge"])

    state_master = master.groupby("state")["district"].nunique().reset_index(name="master_districts")
    state_assigned = assigned.groupby("state")["district"].nunique().reset_index(name="assigned_districts")
    state_report = state_master.merge(state_assigned, on="state", how="left").fillna({"assigned_districts": 0})
    state_report["assigned_districts"] = state_report["assigned_districts"].astype(int)
    state_report["coverage_ratio"] = state_report["assigned_districts"] / state_report["master_districts"]
    lowest_states = state_report.sort_values(
        ["coverage_ratio", "master_districts", "state"], ascending=[True, False, True]
    ).head(state_batch)["state"]

    prioritized = uncovered[uncovered["state"].isin(lowest_states)]
    fallback = uncovered[~uncovered["state"].isin(lowest_states)]
    selected = pd.concat([prioritized, fallback], ignore_index=True).head(max_districts)

    return [tuple(row) for row in selected[["state", "district"]].itertuples(index=False, name=None)]


def _collect_focus_districts():

    targets = _select_focus_districts(DAILY_FOCUS_DISTRICT_BATCH, DAILY_FOCUS_STATE_BATCH)

    if not targets:
        return []

    yesterday = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
    today = datetime.utcnow().date().isoformat()

    return fetch_google_news_targets(targets, after_date=yesterday, before_date=today)


def run_pipeline():

    ensure_data_store_ready()
    delete_expired_news()

    news = _collect_from_sources()

    focus_news = _collect_focus_districts()
    news.extend(focus_news)
    focus_fetched = len(focus_news)

    unique_news = []
    seen_keys = set()

    for article in news:

        unique_key = article.get("url") or (article.get("title"), article.get("source"))

        if not unique_key or unique_key in seen_keys:
            continue

        seen_keys.add(unique_key)
        unique_news.append(article)

    existing_urls = get_existing_urls()

    df = _prepare_articles(unique_news, existing_urls)

    if df.empty:
        return {"inserted": 0, "backfilled": 0, "collected": len(news), "unique": len(unique_news)}

    inserted_count = _write_articles(df)

    backfilled_count = backfill_missing_locations()

    return {
        "collected": len(news),
        "focus_districts_fetched": focus_fetched,
        "unique": len(unique_news),
        "inserted": inserted_count,
        "backfilled": backfilled_count,
    }


if __name__ == "__main__":
    run_pipeline()