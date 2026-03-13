from datetime import timedelta

import pandas as pd


PROBLEM_TAXONOMY = {
    "water_supply": {
        "label": "Water Supply",
        "keywords": {
            "water": 2.0,
            "drinking water": 3.0,
            "water shortage": 4.0,
            "tankers": 3.0,
            "reservoir": 2.5,
            "pipeline": 2.0,
            "irrigation": 2.0,
            "canal": 2.0,
        },
        "reasons": ["service disruption", "scarcity", "distribution bottleneck"],
    },
    "roads_transport": {
        "label": "Roads And Transport",
        "keywords": {
            "road": 2.0,
            "traffic": 2.0,
            "pothole": 4.0,
            "highway": 2.0,
            "bridge": 2.0,
            "bus": 1.5,
            "rail": 1.5,
            "transport": 2.0,
            "congestion": 3.0,
            "accident": 2.0,
        },
        "reasons": ["mobility disruption", "safety risk", "infrastructure stress"],
    },
    "power_supply": {
        "label": "Power Supply",
        "keywords": {
            "power": 2.0,
            "electricity": 2.0,
            "outage": 4.0,
            "blackout": 4.0,
            "transformer": 3.0,
            "grid": 2.0,
            "load shedding": 3.5,
        },
        "reasons": ["supply outage", "grid instability", "infrastructure fault"],
    },
    "sanitation": {
        "label": "Sanitation And Waste",
        "keywords": {
            "sanitation": 3.0,
            "garbage": 3.0,
            "drainage": 4.0,
            "sewage": 4.0,
            "waste": 2.0,
            "dumping": 2.0,
            "overflow": 2.5,
        },
        "reasons": ["public health risk", "civic neglect", "waste accumulation"],
    },
    "law_order": {
        "label": "Law And Order",
        "keywords": {
            "crime": 3.0,
            "violence": 3.5,
            "murder": 4.0,
            "theft": 3.0,
            "police": 2.0,
            "arrest": 2.0,
            "attack": 3.0,
            "security": 2.0,
        },
        "reasons": ["public safety concern", "criminal activity", "security pressure"],
    },
    "health": {
        "label": "Health",
        "keywords": {
            "hospital": 3.0,
            "health": 2.0,
            "disease": 3.5,
            "medical": 2.0,
            "fever": 2.5,
            "virus": 3.0,
            "doctor": 2.0,
            "ambulance": 3.0,
        },
        "reasons": ["disease spread", "hospital strain", "medical access gap"],
    },
    "agriculture": {
        "label": "Agriculture",
        "keywords": {
            "farmer": 3.0,
            "crop": 3.0,
            "agriculture": 2.5,
            "rainfall": 2.0,
            "drought": 4.0,
            "procurement": 2.0,
            "fertilizer": 2.5,
        },
        "reasons": ["farm income risk", "weather impact", "input shortage"],
    },
    "weather_disaster": {
        "label": "Weather And Disaster",
        "keywords": {
            "flood": 4.0,
            "rain": 2.0,
            "storm": 3.5,
            "cyclone": 4.0,
            "landslide": 4.0,
            "heatwave": 4.0,
            "disaster": 3.0,
        },
        "reasons": ["extreme weather exposure", "disaster response burden", "climate-linked disruption"],
    },
    "governance": {
        "label": "Governance And Civic Administration",
        "keywords": {
            "protest": 3.0,
            "scheme": 2.0,
            "civic": 2.5,
            "municipal": 3.0,
            "administration": 2.5,
            "government": 1.5,
            "complaint": 2.5,
            "corruption": 3.5,
        },
        "reasons": ["administrative failure", "public grievance", "service delivery gap"],
    },
    "employment": {
        "label": "Employment And Economy",
        "keywords": {
            "jobs": 3.0,
            "employment": 3.0,
            "industry": 2.0,
            "factory": 2.0,
            "inflation": 3.0,
            "prices": 2.0,
            "business": 1.5,
            "economy": 2.5,
        },
        "reasons": ["income pressure", "market slowdown", "livelihood instability"],
    },
    "education": {
        "label": "Education",
        "keywords": {
            "school": 3.0,
            "college": 3.0,
            "students": 2.5,
            "teacher": 2.5,
            "exam": 2.0,
            "education": 2.0,
        },
        "reasons": ["learning disruption", "institutional issue", "access gap"],
    },
}

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


def _score_taxonomy(text, title=""):

    normalized_text = _normalize_text(text)
    normalized_title = _normalize_text(title)
    best_key = "governance"
    best_score = 0.0
    best_evidence = []

    for problem_key, config in PROBLEM_TAXONOMY.items():
        score = 0.0
        evidence = []

        for keyword, weight in config["keywords"].items():
            if keyword in normalized_text:
                score += weight
                evidence.append(keyword)

            if normalized_title and keyword in normalized_title:
                score += weight * 1.5
                evidence.append(keyword)

        if score > best_score:
            best_key = problem_key
            best_score = score
            best_evidence = list(dict.fromkeys(evidence))

    return best_key, round(best_score, 3), best_evidence[:5]


def classify_problem(title, content):

    combined_text = f"{title or ''} {content or ''}".strip()
    problem_key, confidence, evidence = _score_taxonomy(combined_text, title)

    return {
        "problem_key": problem_key,
        "problem_label": PROBLEM_TAXONOMY[problem_key]["label"],
        "matched_keywords": evidence,
        "confidence": confidence,
        "reason_hints": PROBLEM_TAXONOMY[problem_key]["reasons"],
    }


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


def _build_reason(row):

    evidence = ", ".join(row["keywords"][:3]) if row["keywords"] else ", ".join(row["reason_hints"][:2])

    return (
        f"{row['frequency']} articles were grouped under this issue in the last {row['window_days']} days. "
        f"Coverage is {row['trend']} with {row['recent_count']} recent articles, sentiment is {row['sentiment_label']}, "
        f"and the strongest signals were {evidence}."
    )


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


def _prepare_analysis_df(df):

    analysis_df = df.copy()
    analysis_df["title"] = analysis_df["title"].fillna("")
    analysis_df["content"] = analysis_df["content"].fillna("")
    analysis_df["published_at"] = _parse_published_at(analysis_df["published_at"])
    analysis_df["combined_text"] = (analysis_df["title"] + " " + analysis_df["content"]).str.strip()
    analysis_df["problem_details"] = analysis_df.apply(
        lambda row: classify_problem(row["title"], row["content"]),
        axis=1,
    )
    analysis_df["problem_key"] = analysis_df["problem_details"].apply(lambda value: value["problem_key"])
    analysis_df["problem_label"] = analysis_df["problem_details"].apply(lambda value: value["problem_label"])
    analysis_df["matched_keywords"] = analysis_df["problem_details"].apply(lambda value: value["matched_keywords"])
    analysis_df["confidence"] = analysis_df["problem_details"].apply(lambda value: value["confidence"])
    analysis_df["reason_hints"] = analysis_df["problem_details"].apply(lambda value: value["reason_hints"])
    analysis_df["sentiment_score"] = analysis_df["combined_text"].apply(score_sentiment)
    analysis_df["future_signal_score"] = analysis_df["combined_text"].apply(score_future_risk_signal)

    return analysis_df


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
        reason_pool = []

        for keywords in group["matched_keywords"]:
            keyword_pool.extend(keywords)

        for reason_hints in group["reason_hints"]:
            reason_pool.extend(reason_hints)

        keywords = list(dict.fromkeys(keyword_pool))[:5]
        reason_hints = list(dict.fromkeys(reason_pool))[:3]
        average_confidence = round(group["confidence"].mean(), 3)
        future_signal_score = int(group["future_signal_score"].sum())
        impact_score = round((frequency * 2.0) + (max(0, -avg_sentiment) * 4.0) + (recent_count * 1.5) + average_confidence, 3)
        risk_score = round((max(recent_count - previous_count, 0) * 2.0) + (max(0, -avg_sentiment) * 3.0) + future_signal_score + (average_confidence * 0.5), 3)

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
            "reason_hints": reason_hints,
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
                f"{row['problem']} is a future concern because the recent article count is {row['recent_count']}, "
                f"the trend is {row['trend']}, and the coverage remains {row['sentiment_label']}."
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