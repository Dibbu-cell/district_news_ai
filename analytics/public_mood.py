from __future__ import annotations

import pandas as pd

from analytics.district_insights import score_sentiment
from analytics.geo_quality import apply_confidence_weighted_fallback
from analytics.keyword_packs import PROTEST_KEYWORDS


def _normalize_text(value: str | None) -> str:
    return str(value or "").lower()


def _count_protest_hits(text: str) -> int:
    return sum(1 for token in PROTEST_KEYWORDS if token in text)


def build_public_mood_summary(df: pd.DataFrame) -> dict:
    df = apply_confidence_weighted_fallback(df)

    if df.empty:
        return {
            "article_count": 0,
            "tone_distribution": {"positive": 0, "neutral": 0, "negative": 0},
            "negative_sentiment_score": 0.0,
            "protest_keyword_count": 0,
            "protest_keyword_frequency": 0.0,
            "anger_score": 0.0,
            "public_mood": "neutral",
        }

    analysis_df = df.copy()
    analysis_df["title"] = analysis_df["title"].fillna("")
    analysis_df["content"] = analysis_df["content"].fillna("")
    analysis_df["combined_text"] = (analysis_df["title"] + " " + analysis_df["content"]).apply(_normalize_text)

    analysis_df["sentiment_score"] = analysis_df["combined_text"].apply(score_sentiment)
    analysis_df["negative_component"] = analysis_df["sentiment_score"].apply(lambda value: max(0.0, -float(value)))
    analysis_df["protest_hits"] = analysis_df["combined_text"].apply(_count_protest_hits)

    positive_count = int((analysis_df["sentiment_score"] >= 0.15).sum())
    negative_count = int((analysis_df["sentiment_score"] <= -0.15).sum())
    neutral_count = int(len(analysis_df) - positive_count - negative_count)

    negative_sentiment_score = round(float(analysis_df["negative_component"].mean()), 3)
    protest_keyword_count = int(analysis_df["protest_hits"].sum())
    protest_keyword_frequency = round(min(1.0, protest_keyword_count / max(len(analysis_df), 1)), 3)
    anger_score = round((0.6 * negative_sentiment_score) + (0.4 * protest_keyword_frequency), 3)

    if anger_score >= 0.65:
        public_mood = "angry"
    elif anger_score >= 0.4:
        public_mood = "dissatisfied"
    else:
        public_mood = "stable"

    return {
        "article_count": len(analysis_df),
        "tone_distribution": {
            "positive": positive_count,
            "neutral": neutral_count,
            "negative": negative_count,
        },
        "negative_sentiment_score": negative_sentiment_score,
        "protest_keyword_count": protest_keyword_count,
        "protest_keyword_frequency": protest_keyword_frequency,
        "anger_score": anger_score,
        "public_mood": public_mood,
    }
