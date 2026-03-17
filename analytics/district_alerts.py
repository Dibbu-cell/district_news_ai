from __future__ import annotations

from analytics.district_insights import build_district_insights
from analytics.issue_detection import build_issue_detection_summary
from analytics.policy_recommendation import build_policy_recommendations
from analytics.protest_risk import predict_protest_risk
from analytics.public_mood import build_public_mood_summary


def build_district_alert(
    *,
    df,
    state: str,
    district: str,
    retention_days: int = 5,
    hospital_density: float = 1.0,
    rainfall: float = 0.0,
) -> dict:
    district_insights = build_district_insights(df, state, district, retention_days)
    issue_summary = build_issue_detection_summary(df)
    mood_summary = build_public_mood_summary(df)

    primary_issue = issue_summary["issues"][0]["issue"] if issue_summary["issues"] else "other"
    max_spike = max((row["spike_ratio"] for row in issue_summary["issues"]), default=0.0)
    issue_repetition_days = min(30, int(sum(1 for row in issue_summary["issues"] if row["last_7_days_count"] > 0) * 3))

    risk_features = {
        "issue_spike_ratio": max_spike,
        "negative_sentiment_score": mood_summary["negative_sentiment_score"],
        "protest_keyword_count": mood_summary["protest_keyword_count"],
        "issue_repetition_days": issue_repetition_days,
        "hospital_density": hospital_density,
        "rainfall": rainfall,
    }
    risk_summary = predict_protest_risk(risk_features)

    sensitive_event_names = [row["event"] for row in issue_summary["sensitive_events"]]
    policy_summary = build_policy_recommendations(
        primary_issue=primary_issue,
        anger_score=mood_summary["anger_score"],
        protest_risk=risk_summary["protest_risk"],
        hospital_density=hospital_density,
        sensitive_events=sensitive_event_names,
    )

    return {
        "state": state,
        "district": district,
        "hidden_issue": primary_issue,
        "top3_problems": district_insights["top_problems"],
        "issue_detection": issue_summary,
        "public_mood": mood_summary,
        "protest_risk": risk_summary,
        "policy_recommendation": policy_summary,
    }
