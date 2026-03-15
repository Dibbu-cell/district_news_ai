from fastapi import FastAPI, HTTPException

from analytics.district_insights import build_daily_summary_report, build_district_insights
from config.config import RETENTION_DAYS
from database.news_store import load_district_articles, load_recent_articles
from processing.geo_resolver import normalize_location_name, state_aliases

app = FastAPI()


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

    return load_recent_articles(RETENTION_DAYS, normalized_state)

def _load_district_articles(state, district):

    variants = _district_variants(district)
    return load_district_articles(RETENTION_DAYS, state, district, variants)


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


@app.get("/district/{district}")

def get_problems(district: str, state: str):

    return _analyze_district(state, district)