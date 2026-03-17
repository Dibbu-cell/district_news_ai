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

REGIONAL_LANGUAGE_TERMS = [
    "hindi news",
    "marathi news",
    "tamil news",
    "telugu news",
    "bengali news",
    "kannada news",
    "malayalam news",
    "gujarati news",
    "punjabi news",
    "odia news",
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
PAIR_FRAME = LOCATION_FRAME[["district", "state"]].drop_duplicates().reset_index(drop=True)
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


def _prioritize_pairs(pair_frame, max_items):

    rows = []
    seen = set()

    for district in PRIORITY_DISTRICTS:
        normalized_district = normalize_location_name(district)
        district_rows = pair_frame[pair_frame["district"] == normalized_district]

        for row in district_rows.itertuples(index=False):
            key = (row.district, row.state)

            if key not in seen:
                seen.add(key)
                rows.append(key)

    for state in PRIORITY_STATES:
        normalized_state = normalize_location_name(state)
        state_rows = pair_frame[pair_frame["state"] == normalized_state]

        for row in state_rows.itertuples(index=False):
            key = (row.district, row.state)

            if key not in seen:
                seen.add(key)
                rows.append(key)

    for row in pair_frame.itertuples(index=False):
        key = (row.district, row.state)

        if key not in seen:
            seen.add(key)
            rows.append(key)

    return rows[:max_items]


DISTRICT_STATE_TARGETS = _prioritize_pairs(PAIR_FRAME, MAX_DISTRICT_QUERIES)


def build_state_terms(limit=None):

    state_terms = [f"{state} india news" for state in STATE_NAMES]

    if limit is not None:
        return state_terms[:limit]

    return state_terms


def build_district_terms(limit=None):

    district_terms = [f"{district} india district news" for district in DISTRICT_NAMES]
    district_terms.extend([f"{district} {state} district news" for district, state in DISTRICT_STATE_TARGETS])

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

    local_terms = [f"{district} {state} local news" for district, state in DISTRICT_STATE_TARGETS]
    local_terms.extend([f"{district} {state} civic issues" for district, state in DISTRICT_STATE_TARGETS])

    if limit is not None:
        return local_terms[:limit]

    return local_terms


def build_newsapi_terms():

    return BASE_TERMS + build_state_terms(limit=12)


def build_newsapi_query_targets():

    targets = [{"query": term, "state_hint": None, "district_hint": None} for term in BASE_TERMS]

    for state in build_state_terms(limit=12):
        targets.append({"query": state, "state_hint": normalize_location_name(state.replace(" india news", "")), "district_hint": None})

    for district, state in DISTRICT_STATE_TARGETS[: min(80, len(DISTRICT_STATE_TARGETS))]:
        targets.append({
            "query": f"{district} {state} local governance news",
            "state_hint": state,
            "district_hint": district,
        })

    return targets


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
    terms.extend([f"district india {language_term}" for language_term in REGIONAL_LANGUAGE_TERMS])

    return terms


def build_google_news_query_targets():

    targets = [{"query": term, "state_hint": None, "district_hint": None} for term in BASE_TERMS]

    for state in STATE_NAMES[:GOOGLE_STATE_QUERIES]:
        targets.append({"query": f"{state} india news", "state_hint": state, "district_hint": None})

    district_limit = min(len(DISTRICT_STATE_TARGETS), max(GOOGLE_DISTRICT_QUERIES * 2, 120))

    for district, state in DISTRICT_STATE_TARGETS[:district_limit]:
        targets.append({"query": f"{district} {state} district news", "state_hint": state, "district_hint": district})
        targets.append({"query": f"{district} {state} civic issues", "state_hint": state, "district_hint": district})

        for language_term in REGIONAL_LANGUAGE_TERMS[:4]:
            targets.append(
                {
                    "query": f"{district} {state} {language_term}",
                    "state_hint": state,
                    "district_hint": district,
                }
            )

    return targets


def build_gdelt_query_targets():

    targets = [{"query": term, "state_hint": None, "district_hint": None} for term in BASE_TERMS]

    for state in STATE_NAMES[:GDELT_STATE_QUERIES]:
        targets.append({"query": f"{state} india news", "state_hint": state, "district_hint": None})

    for district, state in DISTRICT_STATE_TARGETS[:GDELT_DISTRICT_QUERIES]:
        targets.append({"query": f"{district} {state} district news", "state_hint": state, "district_hint": district})

    return targets