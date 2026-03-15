from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException
import pandas as pd
from pandas.errors import DatabaseError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from analytics.district_insights import build_daily_summary_report, build_district_insights
from config.config import RETENTION_DAYS
from database.db import engine
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

    cutoff = (datetime.utcnow() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    query = text(
        """
        SELECT title, content, url, source, state, district, published_at
        FROM news_articles
        WHERE (published_at IS NULL OR published_at > :cutoff)
        """
    )

    recent_df = pd.read_sql(query, engine, params={"cutoff": cutoff})

    if state:
        normalized_state = normalize_location_name(state)
        normalized_state = state_aliases.get(normalized_state, normalized_state)
        recent_df = recent_df[recent_df["state"] == normalized_state]

    return recent_df

def _load_district_articles(state, district):

    cutoff = (datetime.utcnow() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

    query = text("""
    SELECT title, content, url, source, state, district, published_at
    FROM news_articles
    WHERE district=:district
    AND lower(trim(state))=:state
    AND (published_at IS NULL OR published_at > :cutoff)
    """)

    direct_df = pd.read_sql(query, engine, params={"district": district, "state": state, "cutoff": cutoff})

    if not direct_df.empty:
        return direct_df

    variants = _district_variants(district)

    if variants:
        conditions = []
        params = {"state": state, "cutoff": cutoff}

        for idx, variant in enumerate(variants):
            param_name = f"district_{idx}"
            params[param_name] = variant
            conditions.append(f"district = :{param_name}")

        variant_query = text(
            f"""
            SELECT title, content, url, source, state, district, published_at
            FROM news_articles
                        WHERE lower(trim(state))=:state
              AND ({' OR '.join(conditions)})
              AND (published_at IS NULL OR published_at > :cutoff)
            """
        )
        variant_df = pd.read_sql(variant_query, engine, params=params)

        if not variant_df.empty:
            return variant_df

    fallback_query = text(
        """
        SELECT title, content, url, source, state, district, published_at
        FROM news_articles
                WHERE (lower(trim(state))=:state OR state IS NULL OR trim(state)='')
          AND (
            lower(title) LIKE :district_like
            OR lower(content) LIKE :district_like
          )
          AND (published_at IS NULL OR published_at > :cutoff)
        """
    )
    fallback_df = pd.read_sql(
        fallback_query,
        engine,
        params={"state": state, "district_like": f"%{district}%", "cutoff": cutoff},
    )

    if fallback_df.empty:
        return fallback_df

    fallback_df = fallback_df.copy()
    fallback_df["district"] = district

    return fallback_df


def _analyze_district(state, district):

    normalized_state = normalize_location_name(state)
    normalized_state = state_aliases.get(normalized_state, normalized_state)
    normalized_district = normalize_location_name(district)

    if not normalized_state or not normalized_district:
        raise HTTPException(status_code=400, detail="State and district are required.")

    try:
        df = _load_district_articles(normalized_state, normalized_district)
    except (SQLAlchemyError, DatabaseError) as exc:
        message = str(exc).lower()

        if "no such table" in message or "does not exist" in message:
            return {
                "state": normalized_state,
                "district": normalized_district,
                "message": "No data",
                "top_problems": [],
                "future_risks": [],
            }

        raise HTTPException(
            status_code=503,
            detail="Database is unavailable. Check DATABASE_URL and network access."
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
    except (SQLAlchemyError, DatabaseError) as exc:
        message = str(exc).lower()

        if "no such table" in message or "does not exist" in message:
            return {
                "window_days": RETENTION_DAYS,
                "district_count": 0,
                "district_summaries": [],
            }

        raise HTTPException(
            status_code=503,
            detail="Database is unavailable. Check DATABASE_URL and network access."
        ) from exc

    normalized_state = None if state is None else normalize_location_name(state)

    return build_daily_summary_report(recent_df, RETENTION_DAYS, normalized_state, limit=max(1, min(limit, 100)))


@app.get("/district/{district}")

def get_problems(district: str, state: str):

    return _analyze_district(state, district)