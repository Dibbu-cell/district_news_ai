from __future__ import annotations

from collections import Counter, defaultdict
from datetime import timedelta

import pandas as pd

from analytics.keyword_packs import ISSUE_KEYWORDS, SENSITIVE_EVENT_KEYWORDS
from analytics.geo_quality import apply_confidence_weighted_fallback
from config.config import ISSUE_BASELINE_RETENTION_DAYS
from database.news_store import load_issue_history, upsert_issue_history


def _normalize_text(value: str | None) -> str:
    return str(value or "").lower()


def _classify_issue(text: str) -> str:
    score_by_issue = {}

    for issue, keywords in ISSUE_KEYWORDS.items():
        score_by_issue[issue] = sum(1 for token in keywords if token in text)

    best_issue = max(score_by_issue, key=score_by_issue.get)

    if score_by_issue[best_issue] == 0:
        return "other"

    return best_issue


def _detect_sensitive_events(text: str) -> list[str]:
    events = []

    for event, keywords in SENSITIVE_EVENT_KEYWORDS.items():
        if any(token in text for token in keywords):
            events.append(event)

    return events


def _safe_parse_published_at(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _normalize_location_value(value: str | None) -> str:
    return str(value or "").strip().lower()


def _build_daily_issue_counts(analysis_df: pd.DataFrame) -> list[dict]:
    dated_df = analysis_df.dropna(subset=["published_at"]).copy()

    if dated_df.empty:
        return []

    dated_df["date"] = dated_df["published_at"].dt.date.astype(str)
    dated_df["state"] = dated_df.get("state", "").fillna("").astype(str).str.strip().str.lower()
    dated_df["district"] = dated_df.get("district", "").fillna("").astype(str).str.strip().str.lower()

    grouped = (
        dated_df[dated_df["issue_category"] != "other"]
        .groupby(["date", "state", "district", "issue_category"])
        .size()
        .reset_index(name="count")
    )

    if grouped.empty:
        return []

    return [
        {
            "date": row["date"],
            "state": row["state"],
            "district": row["district"],
            "issue": row["issue_category"],
            "count": int(row["count"]),
        }
        for _, row in grouped.iterrows()
    ]


def refresh_issue_history_from_articles(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    analysis_df = df.copy()
    analysis_df["title"] = analysis_df["title"].fillna("")
    analysis_df["content"] = analysis_df["content"].fillna("")
    analysis_df["published_at"] = _safe_parse_published_at(analysis_df["published_at"])
    analysis_df["combined_text"] = (analysis_df["title"] + " " + analysis_df["content"]).str.lower().str.strip()
    analysis_df["issue_category"] = analysis_df["combined_text"].apply(_classify_issue)

    latest_counts = _build_daily_issue_counts(analysis_df)

    if not latest_counts:
        return 0

    return upsert_issue_history(latest_counts, retention_days=ISSUE_BASELINE_RETENTION_DAYS)


def _history_rows_for_location(state: str, district: str) -> list[dict]:
    return load_issue_history(
        _normalize_location_value(state),
        _normalize_location_value(district),
        days=max(30, ISSUE_BASELINE_RETENTION_DAYS),
    )


def build_issue_detection_summary(df: pd.DataFrame) -> dict:
    df = apply_confidence_weighted_fallback(df)

    if df.empty:
        return {
            "issues": [],
            "spikes": [],
            "sensitive_events": [],
            "article_count": 0,
        }

    analysis_df = df.copy()
    analysis_df["title"] = analysis_df["title"].fillna("")
    analysis_df["content"] = analysis_df["content"].fillna("")
    analysis_df["published_at"] = _safe_parse_published_at(analysis_df["published_at"])
    analysis_df["combined_text"] = (analysis_df["title"] + " " + analysis_df["content"]).str.lower().str.strip()

    analysis_df["issue_category"] = analysis_df["combined_text"].apply(_classify_issue)
    analysis_df["sensitive_events"] = analysis_df["combined_text"].apply(_detect_sensitive_events)

    reference_time = analysis_df["published_at"].dropna().max()

    if pd.isna(reference_time):
        reference_time = pd.Timestamp.utcnow()

    last_7_cutoff = reference_time - timedelta(days=7)
    last_30_cutoff = reference_time - timedelta(days=30)
    state = _normalize_location_value(analysis_df["state"].dropna().iloc[0] if "state" in analysis_df and not analysis_df["state"].dropna().empty else "")
    district = _normalize_location_value(analysis_df["district"].dropna().iloc[0] if "district" in analysis_df and not analysis_df["district"].dropna().empty else "")
    history_rows = _history_rows_for_location(state, district) if state and district else []
    current_daily_counts = _build_daily_issue_counts(analysis_df)

    merged_counts = {}

    for row in history_rows:
        key = (row["date"], row["issue"])
        merged_counts[key] = max(merged_counts.get(key, 0), int(row.get("count", 0)))

    for row in current_daily_counts:
        key = (row["date"], row["issue"])
        merged_counts[key] = max(merged_counts.get(key, 0), int(row["count"]))

    issue_rows = []
    spike_rows = []

    for issue, group in analysis_df.groupby("issue_category"):
        if issue == "other":
            continue

        last_7_count = 0
        last_30_count = 0

        for (date_value, issue_name), count in merged_counts.items():
            if issue_name != issue:
                continue

            date_ts = pd.Timestamp(date_value, tz="UTC")

            if date_ts >= last_30_cutoff:
                last_30_count += int(count)

            if date_ts >= last_7_cutoff:
                last_7_count += int(count)

        if not merged_counts:
            last_7_count = int((group["published_at"] >= last_7_cutoff).fillna(False).sum())
            last_30_count = int((group["published_at"] >= last_30_cutoff).fillna(False).sum())

        # Average daily mentions in last 30 days.
        last_30_daily_avg = last_30_count / 30 if last_30_count > 0 else 0.0
        spike_ratio = (last_7_count / max(last_30_daily_avg, 0.1)) if last_7_count > 0 else 0.0
        spike_percent = max(0.0, (spike_ratio - 1.0) * 100.0)

        issue_rows.append(
            {
                "issue": issue,
                "last_7_days_count": last_7_count,
                "last_30_days_count": last_30_count,
                "last_30_days_daily_avg": round(last_30_daily_avg, 3),
                "spike_ratio": round(spike_ratio, 3),
                "spike_percent": round(spike_percent, 2),
            }
        )

        if spike_ratio > 2:
            spike_rows.append(
                {
                    "issue": issue,
                    "spike_ratio": round(spike_ratio, 3),
                    "spike_percent": round(spike_percent, 2),
                }
            )

    issue_rows.sort(key=lambda row: row["last_7_days_count"], reverse=True)
    spike_rows.sort(key=lambda row: row["spike_ratio"], reverse=True)

    event_counter = Counter()
    event_issue_map: dict[str, Counter] = defaultdict(Counter)

    for _, row in analysis_df.iterrows():
        issue = row["issue_category"]
        for event in row["sensitive_events"]:
            event_counter[event] += 1
            event_issue_map[event][issue] += 1

    sensitive_events = []

    for event, count in event_counter.most_common():
        dominant_issue = event_issue_map[event].most_common(1)[0][0]
        sensitive_events.append(
            {
                "event": event,
                "count": int(count),
                "dominant_issue": dominant_issue,
            }
        )

    return {
        "issues": issue_rows,
        "spikes": spike_rows,
        "sensitive_events": sensitive_events,
        "article_count": len(analysis_df),
        "baseline_source": "history_file" if history_rows else "live_articles_only",
    }
