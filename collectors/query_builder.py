from pathlib import Path

import pandas as pd

from config.config import (
    GDELT_DISTRICT_QUERIES,
    GDELT_STATE_QUERIES,
    GOOGLE_DISTRICT_QUERIES,
    GOOGLE_STATE_QUERIES,
    MAX_DISTRICT_QUERIES,
    MAX_STATE_QUERIES,
    PRIORITY_DISTRICTS,
    PRIORITY_STATES,
)
from processing.geo_resolver import normalize_location_name


BASE_TERMS = [
    "india",
    "district news india",
    "state government india",
    "breaking news india districts",
    "civic issues india",
]


def _load_location_frame():

    data_path = Path(__file__).resolve().parent.parent / "data" / "india_districts.csv"
    frame = pd.read_csv(data_path)
    frame.columns = frame.columns.str.strip().str.lower()

    if "district" not in frame.columns or "state" not in frame.columns:
        frame = pd.read_csv(data_path, skiprows=1)
        frame.columns = frame.columns.str.strip().str.lower()

    frame["district"] = frame["district"].apply(normalize_location_name)
    frame["state"] = frame["state"].apply(normalize_location_name)

    return frame[["district", "state"]].dropna()


LOCATION_FRAME = _load_location_frame()
ALL_STATE_NAMES = sorted(LOCATION_FRAME["state"].dropna().unique().tolist())
ALL_DISTRICT_NAMES = sorted(LOCATION_FRAME["district"].dropna().unique().tolist())


def _prioritize_values(values, priority_values, max_items):

    ordered = []
    seen = set()

    for value in priority_values:
        normalized_value = normalize_location_name(value)

        if normalized_value in values and normalized_value not in seen:
            ordered.append(normalized_value)
            seen.add(normalized_value)

    for value in values:
        if value not in seen:
            ordered.append(value)
            seen.add(value)

    return ordered[:max_items]


STATE_NAMES = _prioritize_values(ALL_STATE_NAMES, PRIORITY_STATES, MAX_STATE_QUERIES)
DISTRICT_NAMES = _prioritize_values(ALL_DISTRICT_NAMES, PRIORITY_DISTRICTS, MAX_DISTRICT_QUERIES)


def build_state_terms(limit=None):

    state_terms = [f"{state} india news" for state in STATE_NAMES]

    if limit is not None:
        return state_terms[:limit]

    return state_terms


def build_district_terms(limit=None):

    district_terms = [f"{district} india district news" for district in DISTRICT_NAMES]

    if limit is not None:
        return district_terms[:limit]

    return district_terms


def build_district_civic_terms(limit=None):
    """Alternative query templates for district-level civic/local news."""

    civic_terms = [f"{district} civic issues news" for district in DISTRICT_NAMES]

    if limit is not None:
        return civic_terms[:limit]

    return civic_terms


def build_district_local_terms(limit=None):
    """State-qualified district queries to reduce geo-ambiguity."""

    state_lookup = dict(zip(LOCATION_FRAME["district"], LOCATION_FRAME["state"]))
    local_terms = [
        f"{district} {state_lookup.get(district, '')} news".strip()
        for district in DISTRICT_NAMES
    ]

    if limit is not None:
        return local_terms[:limit]

    return local_terms


def build_newsapi_terms():

    return BASE_TERMS + build_state_terms(limit=12)


def build_gdelt_terms():

    terms = list(BASE_TERMS)
    terms.extend(build_state_terms(limit=GDELT_STATE_QUERIES))
    terms.extend(build_district_terms(limit=GDELT_DISTRICT_QUERIES))

    return terms


def build_google_news_terms():

    terms = list(BASE_TERMS)
    terms.extend(build_state_terms(limit=GOOGLE_STATE_QUERIES))
    terms.extend(build_district_terms(limit=GOOGLE_DISTRICT_QUERIES))
    terms.extend(build_district_civic_terms(limit=GOOGLE_DISTRICT_QUERIES // 2))
    terms.extend(build_district_local_terms(limit=GOOGLE_DISTRICT_QUERIES // 2))

    return terms