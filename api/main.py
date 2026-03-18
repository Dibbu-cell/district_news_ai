from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from analytics.district_alerts import build_district_alert
from analytics.district_insights import build_daily_summary_report, build_district_insights
from analytics.geo_quality import apply_confidence_weighted_fallback
from analytics.issue_detection import build_issue_detection_summary
from analytics.policy_recommendation import build_policy_recommendations
from analytics.protest_risk import predict_protest_risk, train_and_save_risk_model
from analytics.public_mood import build_public_mood_summary
from analytics.quality_report import build_daily_quality_report, build_source_mapping_audit_report
from config.config import PIPELINE_RUN_EVERY_MINUTES, PIPELINE_SCHEDULE_HOUR, RETENTION_DAYS
from database.news_store import get_pipeline_status, load_district_articles, load_recent_articles
from processing.geo_resolver import normalize_location_name, state_aliases

app = FastAPI()


@app.get("/")
def get_root():

    return {
        "service": "district-news-api",
        "status": "ok",
    }


@app.get("/health")
def get_health():

    return {
        "status": "ok",
        "service": "district-news-api",
    }


class RiskModelTrainingPayload(BaseModel):
    model_type: str = "logistic_regression"
    training_rows: list[dict]


def _district_variants(district: str):

    normalized = normalize_location_name(district)

    if not normalized:
        return []

    variants = {
        normalized,
        f"{normalized} nagar",
        f"{normalized} dehat",
    }

    if normalized.endswith(" nagar"):
        variants.add(normalized.replace(" nagar", ""))

    if normalized.endswith(" dehat"):
        variants.add(normalized.replace(" dehat", ""))

    return sorted({value for value in variants if value})


def _load_recent_articles(state=None):

    normalized_state = None

    if state:
        normalized_state = normalize_location_name(state)
        normalized_state = state_aliases.get(normalized_state, normalized_state)

    raw_df = load_recent_articles(RETENTION_DAYS, normalized_state)
    return apply_confidence_weighted_fallback(raw_df)

def _load_district_articles(state, district):

    variants = _district_variants(district)
    raw_df = load_district_articles(RETENTION_DAYS, state, district, variants)
    return apply_confidence_weighted_fallback(raw_df)


def _analyze_district(state, district):

    normalized_state = normalize_location_name(state)
    normalized_state = state_aliases.get(normalized_state, normalized_state)
    normalized_district = normalize_location_name(district)

    if not normalized_state or not normalized_district:
        raise HTTPException(status_code=400, detail="State and district are required.")

    try:
        df = _load_district_articles(normalized_state, normalized_district)
    except Exception as exc:
        message = str(exc).lower()

        if "no such table" in message or "does not exist" in message or "neo" in message:
            return {
                "state": normalized_state,
                "district": normalized_district,
                "message": "No data",
                "top_problems": [],
                "future_risks": [],
            }

        raise HTTPException(
            status_code=503,
            detail="Database is unavailable. Check DB_BACKEND and database connection settings."
        ) from exc

    insights = build_district_insights(df, normalized_state, normalized_district, RETENTION_DAYS)

    if insights["article_count"] == 0:
        insights["message"] = "No data"

    return insights


@app.get("/analysis")
def get_district_analysis(state: str, district: str):

    return _analyze_district(state, district)


@app.get("/reports/daily-summary")
def get_daily_summary(state: str | None = None, limit: int = 25):

    try:
        recent_df = _load_recent_articles(state)
    except Exception as exc:
        message = str(exc).lower()

        if "no such table" in message or "does not exist" in message or "neo" in message:
            return {
                "window_days": RETENTION_DAYS,
                "district_count": 0,
                "district_summaries": [],
            }

        raise HTTPException(
            status_code=503,
            detail="Database is unavailable. Check DB_BACKEND and database connection settings."
        ) from exc

    normalized_state = None if state is None else normalize_location_name(state)

    return build_daily_summary_report(recent_df, RETENTION_DAYS, normalized_state, limit=max(1, min(limit, 100)))


@app.get("/reports/daily-quality")
def get_daily_quality_report(state: str | None = None):

    normalized_state = None if state is None else normalize_location_name(state)

    # Quality report needs the raw view to surface low-confidence and unmapped records.
    raw_df = load_recent_articles(RETENTION_DAYS, normalized_state)

    return build_daily_quality_report(raw_df, RETENTION_DAYS)


@app.get("/reports/source-mapping-audit")
def get_source_mapping_audit(state: str | None = None, limit: int = 25):

    normalized_state = None if state is None else normalize_location_name(state)
    raw_df = load_recent_articles(RETENTION_DAYS, normalized_state)

    return build_source_mapping_audit_report(raw_df, RETENTION_DAYS, limit=max(1, min(limit, 100)))


@app.get("/health/pipeline")
def get_pipeline_health():

    cadence = {
        "mode": "interval" if PIPELINE_RUN_EVERY_MINUTES > 0 else "cron",
        "every_minutes": PIPELINE_RUN_EVERY_MINUTES if PIPELINE_RUN_EVERY_MINUTES > 0 else None,
        "cron_hour_utc": None if PIPELINE_RUN_EVERY_MINUTES > 0 else PIPELINE_SCHEDULE_HOUR,
    }

    try:
        status = get_pipeline_status("scheduler")
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Database unavailable while reading pipeline status: {exc}",
        ) from exc

    return {
        "now_utc": datetime.now(timezone.utc).isoformat(),
        "scheduler_cadence": cadence,
        "last_successful_pipeline_run_time": None if status is None else status.get("last_successful_run_at"),
        "last_inserted_article_count": 0 if status is None else int(status.get("last_inserted_article_count") or 0),
        "last_collected_article_count": 0 if status is None else int(status.get("last_collected_count") or 0),
        "status": status,
    }


@app.get("/district/{district}")

def get_problems(district: str, state: str):

    return _analyze_district(state, district)


@app.get("/analysis/issues")
def get_issue_detection(state: str, district: str):

    normalized_state = normalize_location_name(state)
    normalized_state = state_aliases.get(normalized_state, normalized_state)
    normalized_district = normalize_location_name(district)

    if not normalized_state or not normalized_district:
        raise HTTPException(status_code=400, detail="State and district are required.")

    df = _load_district_articles(normalized_state, normalized_district)
    issue_summary = build_issue_detection_summary(df)

    return {
        "state": normalized_state,
        "district": normalized_district,
        **issue_summary,
    }


@app.get("/analysis/public-mood")
def get_public_mood(state: str, district: str):

    normalized_state = normalize_location_name(state)
    normalized_state = state_aliases.get(normalized_state, normalized_state)
    normalized_district = normalize_location_name(district)

    if not normalized_state or not normalized_district:
        raise HTTPException(status_code=400, detail="State and district are required.")

    df = _load_district_articles(normalized_state, normalized_district)
    mood_summary = build_public_mood_summary(df)

    return {
        "state": normalized_state,
        "district": normalized_district,
        **mood_summary,
    }


@app.get("/analysis/protest-risk")
def get_protest_risk(
    state: str,
    district: str,
    issue_spike_ratio: float,
    negative_sentiment_score: float,
    protest_keyword_count: int,
    issue_repetition_days: int = 7,
    hospital_density: float = 1.0,
    rainfall: float = 0.0,
):

    normalized_state = normalize_location_name(state)
    normalized_state = state_aliases.get(normalized_state, normalized_state)
    normalized_district = normalize_location_name(district)

    if not normalized_state or not normalized_district:
        raise HTTPException(status_code=400, detail="State and district are required.")

    risk_summary = predict_protest_risk(
        {
            "issue_spike_ratio": issue_spike_ratio,
            "negative_sentiment_score": negative_sentiment_score,
            "protest_keyword_count": protest_keyword_count,
            "issue_repetition_days": issue_repetition_days,
            "hospital_density": hospital_density,
            "rainfall": rainfall,
        }
    )

    return {
        "state": normalized_state,
        "district": normalized_district,
        **risk_summary,
    }


@app.post("/models/protest-risk/train")
def train_protest_risk_model(payload: RiskModelTrainingPayload):

    try:
        return train_and_save_risk_model(payload.training_rows, model_type=payload.model_type)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/analysis/policy-recommendations")
def get_policy_recommendations(
    issue: str,
    anger_score: float,
    protest_risk: float,
    hospital_density: float = 1.0,
):

    recommendations = build_policy_recommendations(
        primary_issue=issue,
        anger_score=anger_score,
        protest_risk=protest_risk,
        hospital_density=hospital_density,
    )

    return {
        "issue": issue,
        "anger_score": round(anger_score, 3),
        "protest_risk": round(protest_risk, 3),
        **recommendations,
    }


@app.get("/alerts/district")
def get_district_alert(
    state: str,
    district: str,
    hospital_density: float = 1.0,
    rainfall: float = 0.0,
):

    normalized_state = normalize_location_name(state)
    normalized_state = state_aliases.get(normalized_state, normalized_state)
    normalized_district = normalize_location_name(district)

    if not normalized_state or not normalized_district:
        raise HTTPException(status_code=400, detail="State and district are required.")

    df = _load_district_articles(normalized_state, normalized_district)

    return build_district_alert(
        df=df,
        state=normalized_state,
        district=normalized_district,
        retention_days=RETENTION_DAYS,
        hospital_density=hospital_density,
        rainfall=rainfall,
    )


@app.get("/monitoring/live")
def get_live_monitoring(state: str | None = None, limit: int = 25):

    normalized_state = None if state is None else normalize_location_name(state)
    recent_df = _load_recent_articles(normalized_state)

    if recent_df.empty:
        return {
            "window_days": RETENTION_DAYS,
            "district_count": 0,
            "district_alerts": [],
        }

    district_alerts = []

    for (state_name, district_name), group in recent_df.groupby(["state", "district"]):
        if not state_name or not district_name:
            continue

        district_alert = build_district_alert(
            df=group,
            state=str(state_name),
            district=str(district_name),
            retention_days=RETENTION_DAYS,
        )
        district_alerts.append(district_alert)

    district_alerts.sort(
        key=lambda row: (
            row["protest_risk"]["protest_risk"],
            row["public_mood"]["anger_score"],
            row["issue_detection"]["article_count"],
        ),
        reverse=True,
    )

    return {
        "window_days": RETENTION_DAYS,
        "district_count": len(district_alerts),
        "district_alerts": district_alerts[: max(1, min(limit, 100))],
    }