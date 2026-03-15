from datetime import timedelta

import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.feature_extraction.text import TfidfVectorizer


NEGATIVE_WORDS = {
    "accident", "anger", "attack", "breakdown", "collapsed", "complaint", "crime", "crisis",
    "damage", "death", "delay", "disease", "disrupted", "flood", "illegal", "injured", "lack",
    "outage", "pollution", "protest", "risk", "scarcity", "shortage", "slow", "stalled", "theft",
    "violence", "worsen", "crash", "choked", "blocked", "unsafe",
}

POSITIVE_WORDS = {
    "approved", "boost", "completed", "improved", "inaugurated", "launched", "repair", "resolved",
    "restored", "resume", "relief", "support", "upgrade", "rehabilitated", "opened",
}

FUTURE_RISK_WORDS = {
    "warning", "alert", "forecast", "expected", "risk", "likely", "may", "could", "concern", "fear",
}


def _normalize_text(text):
    return str(text or "").lower()


def _parse_published_at(series):
    return pd.to_datetime(series, utc=True, errors="coerce")


def score_sentiment(text):
    normalized_text = _normalize_text(text)
    positive_hits = sum(1 for word in POSITIVE_WORDS if word in normalized_text)
    negative_hits = sum(1 for word in NEGATIVE_WORDS if word in normalized_text)
    total_hits = positive_hits + negative_hits

    if total_hits == 0:
        return -0.05

    return round((positive_hits - negative_hits) / total_hits, 3)


def score_future_risk_signal(text):
    normalized_text = _normalize_text(text)
    return sum(1 for word in FUTURE_RISK_WORDS if word in normalized_text)


def _describe_trend(recent_count, previous_count):
    if recent_count > previous_count:
        return "rising"

    if recent_count < previous_count:
        return "cooling"

    return "stable"


def _describe_sentiment(score):
    if score <= -0.4:
        return "strongly negative"

    if score <= -0.15:
        return "negative"

    if score < 0.15:
        return "mixed"

    return "positive"


def _supporting_news(group):
    ordered_group = group.sort_values("published_at", ascending=False)
    news_items = []

    for _, row in ordered_group.head(3).iterrows():
        news_items.append({
            "title": row["title"],
            "url": row["url"],
            "source": row["source"],
            "published_at": None if pd.isna(row["published_at"]) else row["published_at"].isoformat(),
        })

    return news_items


def _build_cluster_label(top_terms):
    if not top_terms:
        return "General Civic Issue"

    return " / ".join(term.title() for term in top_terms[:3])


def _cluster_issue_topics(analysis_df):
    clustered_df = analysis_df.copy()

    if clustered_df.empty:
        clustered_df["problem_key"] = []
        clustered_df["problem_label"] = []
        clustered_df["matched_keywords"] = []
        clustered_df["cluster_strength"] = []
        return clustered_df

    texts = clustered_df["combined_text"].fillna("").tolist()

    if len(texts) < 4:
        clustered_df["problem_key"] = "cluster_0"
        clustered_df["problem_label"] = "General Civic Issue"
        clustered_df["matched_keywords"] = clustered_df["combined_text"].apply(lambda _: [])
        clustered_df["cluster_strength"] = 0.5
        return clustered_df

    try:
        vectorizer = TfidfVectorizer(
            stop_words="english",
            ngram_range=(1, 2),
            min_df=1,
            max_features=5000,
        )
        matrix = vectorizer.fit_transform(texts)
        feature_names = vectorizer.get_feature_names_out()

        cluster_count = max(2, min(len(texts), int(round(len(texts) ** 0.5))))
        model = MiniBatchKMeans(
            n_clusters=cluster_count,
            random_state=42,
            n_init=10,
            batch_size=min(1024, max(256, len(texts))),
        )
        labels = model.fit_predict(matrix)
        strengths = matrix.max(axis=1).toarray().ravel()

        label_to_terms = {}
        for cluster_id in range(cluster_count):
            center = model.cluster_centers_[cluster_id]
            top_indices = center.argsort()[::-1][:6]
            terms = [feature_names[index] for index in top_indices if len(feature_names[index]) >= 4]
            label_to_terms[cluster_id] = list(dict.fromkeys(terms))[:4]

        clustered_df["problem_key"] = [f"cluster_{int(label)}" for label in labels]
        clustered_df["problem_label"] = [
            _build_cluster_label(label_to_terms.get(int(label), []))
            for label in labels
        ]
        clustered_df["matched_keywords"] = [label_to_terms.get(int(label), []) for label in labels]
        clustered_df["cluster_strength"] = [round(float(value), 3) for value in strengths]

        return clustered_df
    except Exception:
        clustered_df["problem_key"] = "cluster_0"
        clustered_df["problem_label"] = "General Civic Issue"
        clustered_df["matched_keywords"] = clustered_df["combined_text"].apply(lambda _: [])
        clustered_df["cluster_strength"] = 0.5
        return clustered_df


def _prepare_analysis_df(df):
    analysis_df = df.copy()
    analysis_df["title"] = analysis_df["title"].fillna("")
    analysis_df["content"] = analysis_df["content"].fillna("")
    analysis_df["published_at"] = _parse_published_at(analysis_df["published_at"])
    analysis_df["combined_text"] = (analysis_df["title"] + " " + analysis_df["content"]).str.strip()
    analysis_df["sentiment_score"] = analysis_df["combined_text"].apply(score_sentiment)
    analysis_df["future_signal_score"] = analysis_df["combined_text"].apply(score_future_risk_signal)

    return _cluster_issue_topics(analysis_df)


def _build_reason(row):
    evidence = ", ".join(row["keywords"][:3]) if row["keywords"] else "recurring civic signals"

    return (
        f"{row['frequency']} articles were grouped into this issue cluster over the last {row['window_days']} days. "
        f"The trend is {row['trend']} with {row['recent_count']} recent articles, sentiment is {row['sentiment_label']}, "
        f"and key recurring terms include {evidence}."
    )


def _build_problem_rows(analysis_df, retention_days):
    reference_time = analysis_df["published_at"].dropna().max()

    if pd.isna(reference_time):
        reference_time = pd.Timestamp.utcnow()

    recent_cutoff = reference_time - timedelta(days=2)
    problem_rows = []

    for problem_key, group in analysis_df.groupby("problem_key"):
        problem_label = group["problem_label"].iloc[0]
        frequency = len(group)
        recent_count = int((group["published_at"] >= recent_cutoff).fillna(False).sum())
        previous_count = max(frequency - recent_count, 0)
        avg_sentiment = round(group["sentiment_score"].mean(), 3)
        sentiment_label = _describe_sentiment(avg_sentiment)
        trend = _describe_trend(recent_count, previous_count)

        keyword_pool = []
        for keywords in group["matched_keywords"]:
            keyword_pool.extend(keywords)

        keywords = list(dict.fromkeys(keyword_pool))[:5]
        avg_cluster_strength = round(float(group["cluster_strength"].mean()), 3)
        future_signal_score = int(group["future_signal_score"].sum())
        impact_score = round((frequency * 2.2) + (max(0, -avg_sentiment) * 4.0) + (recent_count * 1.4) + avg_cluster_strength, 3)
        risk_score = round((max(recent_count - previous_count, 0) * 2.2) + (max(0, -avg_sentiment) * 3.0) + future_signal_score + (avg_cluster_strength * 0.5), 3)

        row = {
            "problem_key": problem_key,
            "problem": problem_label,
            "frequency": frequency,
            "recent_count": recent_count,
            "previous_count": previous_count,
            "average_sentiment": avg_sentiment,
            "sentiment_label": sentiment_label,
            "trend": trend,
            "impact_score": impact_score,
            "risk_score": risk_score,
            "keywords": keywords,
            "window_days": retention_days,
            "supporting_news": _supporting_news(group),
        }
        row["reason"] = _build_reason(row)
        problem_rows.append(row)

    return problem_rows


def build_district_insights(df, state, district, retention_days):
    if df.empty:
        return {
            "state": state,
            "district": district,
            "article_count": 0,
            "top_problems": [],
            "future_risks": [],
        }

    analysis_df = _prepare_analysis_df(df)
    problem_rows = _build_problem_rows(analysis_df, retention_days)
    problem_rows.sort(key=lambda item: item["impact_score"], reverse=True)

    top_problems = [
        {
            "problem": row["problem"],
            "frequency": row["frequency"],
            "trend": row["trend"],
            "sentiment": row["sentiment_label"],
            "impact_score": row["impact_score"],
            "reason": row["reason"],
            "supporting_news": row["supporting_news"],
        }
        for row in problem_rows[:3]
    ]

    future_candidates = sorted(problem_rows, key=lambda item: item["risk_score"], reverse=True)[:3]
    future_risks = []

    for row in future_candidates:
        risk_level = "medium"

        if row["risk_score"] >= 8:
            risk_level = "high"
        elif row["risk_score"] <= 3:
            risk_level = "watch"

        future_risks.append({
            "problem": row["problem"],
            "risk_level": risk_level,
            "risk_score": row["risk_score"],
            "reason": (
                f"{row['problem']} is a future concern because recent mentions are {row['recent_count']}, "
                f"trend is {row['trend']}, and sentiment remains {row['sentiment_label']}."
            ),
            "supporting_news": row["supporting_news"],
        })

    return {
        "state": state,
        "district": district,
        "window_days": retention_days,
        "article_count": len(analysis_df),
        "top_problems": top_problems,
        "future_risks": future_risks,
    }


def build_daily_summary_report(df, retention_days, state_filter=None, limit=25):
    if df.empty:
        return {
            "window_days": retention_days,
            "district_count": 0,
            "district_summaries": [],
        }

    report_df = df.copy()
    report_df["state"] = report_df["state"].fillna("")
    report_df["district"] = report_df["district"].fillna("")
    report_df = report_df[(report_df["state"] != "") & (report_df["district"] != "")]

    if state_filter:
        report_df = report_df[report_df["state"] == state_filter]

    if report_df.empty:
        return {
            "window_days": retention_days,
            "district_count": 0,
            "district_summaries": [],
        }

    district_summaries = []

    for (state, district), group in report_df.groupby(["state", "district"]):
        insights = build_district_insights(group, state, district, retention_days)

        if insights["article_count"] == 0:
            continue

        lead_problem = insights["top_problems"][0] if insights["top_problems"] else None
        lead_future_risk = insights["future_risks"][0] if insights["future_risks"] else None
        district_summaries.append({
            "state": state,
            "district": district,
            "article_count": insights["article_count"],
            "top_problem": None if lead_problem is None else lead_problem["problem"],
            "top_problem_reason": None if lead_problem is None else lead_problem["reason"],
            "future_risk": None if lead_future_risk is None else lead_future_risk["problem"],
            "future_risk_level": None if lead_future_risk is None else lead_future_risk["risk_level"],
            "top_problems": insights["top_problems"],
        })

    district_summaries.sort(key=lambda row: (row["article_count"], row["top_problem"] or ""), reverse=True)

    return {
        "window_days": retention_days,
        "district_count": len(district_summaries),
        "district_summaries": district_summaries[:limit],
    }
