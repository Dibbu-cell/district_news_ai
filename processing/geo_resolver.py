from pathlib import Path
import re

import pandas as pd

from config.config import DISTRICT_CONFIDENCE_THRESHOLD, STATE_CONFIDENCE_THRESHOLD


AMBIGUOUS_DISTRICTS = {
    "north",
    "south",
    "east",
    "west",
    "central",
}

STATE_ALIASES = {
    "odisha": "orissa",
    "orissa": "orissa",
    "uttarakhand": "uttarakhand",
    "uttaranchal": "uttarakhand",
    "andaman and nicobar islands": "andman and nicobar island",
    "andaman nicobar": "andman and nicobar island",
    "jammu and kashmir": "jammu and kashmir",
    "nct of delhi": "delhi",
}

LOCATION_STOPWORDS = {
    "district",
    "city",
    "town",
    "state",
    "rural",
    "urban",
    "metro",
}

LOCATION_CUE_WORDS = {
    "in",
    "at",
    "from",
    "near",
    "across",
    "of",
}


def normalize_location_name(value):

    text = str(value or "").lower()
    text = text.replace("_", " ")
    text = text.replace("/", " ")
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z ]", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def _tokenize_location(text):

    return [token for token in normalize_location_name(text).split() if token and token not in LOCATION_STOPWORDS]


def _load_district_data():
    data_path = Path(__file__).resolve().parent.parent / "data" / "india_districts.csv"
    district_df = pd.read_csv(data_path)
    district_df.columns = district_df.columns.str.strip().str.lower()

    if "district" not in district_df.columns or "state" not in district_df.columns:
        district_df = pd.read_csv(data_path, skiprows=1)
        district_df.columns = district_df.columns.str.strip().str.lower()

    return district_df


district_df = _load_district_data()

district_df["district"] = district_df["district"].apply(normalize_location_name)
district_df["state"] = district_df["state"].apply(normalize_location_name)

district_state_pairs = [
    (row.district, row.state)
    for row in district_df[["district", "state"]].dropna().itertuples(index=False)
    if row.district and row.state
]

district_to_state = {district: state for district, state in district_state_pairs}
districts = sorted(set(district_to_state.keys()), key=len, reverse=True)
states = sorted({STATE_ALIASES.get(state, state) for _, state in district_state_pairs}, key=len, reverse=True)
state_alias_lookup = {alias: canonical for alias, canonical in STATE_ALIASES.items()}


def _build_alias_map(values):

    alias_map = {}

    for value in values:
        tokens = _tokenize_location(value)

        if not tokens:
            continue

        alias_candidates = {
            value,
            " ".join(tokens),
            "-".join(tokens),
        }

        if len(tokens) > 1:
            alias_candidates.add(tokens[-1])

        for alias in alias_candidates:
            alias = normalize_location_name(alias)

            if alias and len(alias) >= 4:
                alias_map.setdefault(alias, value)

    return alias_map


district_aliases = _build_alias_map(districts)
state_values = sorted(set(district_to_state.values()), key=len, reverse=True)
state_aliases = _build_alias_map(state_values)
state_aliases.update(state_alias_lookup)
state_to_districts = {}

for district_name, state_name in district_state_pairs:
    state_to_districts.setdefault(state_name, set()).add(district_name)


def _find_best_match(candidates, values, allow_ambiguous=False):

    for candidate_text in candidates:

        if not candidate_text:
            continue

        wrapped_text = f" {candidate_text} "

        for value in values:

            if not allow_ambiguous and value in AMBIGUOUS_DISTRICTS:
                continue

            if len(value) < 4:
                continue

            if f" {value} " in wrapped_text:
                return value

    return None


def _find_alias_match(candidates, alias_map, allow_ambiguous=False):

    for candidate_text in candidates:

        wrapped_text = f" {candidate_text} "

        for alias, canonical in alias_map.items():

            if not allow_ambiguous and canonical in AMBIGUOUS_DISTRICTS:
                continue

            if len(alias) < 4:
                continue

            if f" {alias} " in wrapped_text:
                return canonical

    return None


def _find_token_match(candidate_text, allowed_districts=None):

    candidate_tokens = set(_tokenize_location(candidate_text))

    if not candidate_tokens:
        return None

    values = allowed_districts if allowed_districts is not None else districts
    best_match = None
    best_score = 0

    for value in values:
        value_tokens = set(_tokenize_location(value))

        if not value_tokens:
            continue

        score = len(candidate_tokens & value_tokens)

        if score >= max(2, len(value_tokens) - 1) and score > best_score:
            best_match = value
            best_score = score

    return best_match


def _score_candidates(candidates, alias_map, canonical_values, allow_ambiguous=False):

    scores = {}

    for raw_candidate in candidates:
        candidate_text = normalize_location_name(raw_candidate)

        if not candidate_text:
            continue

        wrapped_text = f" {candidate_text} "

        for alias, canonical in alias_map.items():

            if not allow_ambiguous and canonical in AMBIGUOUS_DISTRICTS:
                continue

            if len(alias) < 4:
                continue

            if f" {alias} " in wrapped_text:
                scores[canonical] = scores.get(canonical, 0) + max(len(alias.split()), 1) * 3

        for value in canonical_values:

            if not allow_ambiguous and value in AMBIGUOUS_DISTRICTS:
                continue

            if len(value) < 4:
                continue

            if f" {value} " in wrapped_text:
                scores[value] = scores.get(value, 0) + max(len(value.split()), 1) * 2

    return scores


def _extract_location_snippets(text):

    normalized_text = normalize_location_name(text)

    if not normalized_text:
        return []

    snippets = []
    patterns = [
        r"([a-z][a-z ]{2,40}) district",
        r"district of ([a-z][a-z ]{2,40})",
        r"(?:in|at|from|near|across) ([a-z][a-z ]{2,40})",
        r"([a-z][a-z ]{2,40}), ([a-z][a-z ]{2,40})",
    ]

    for pattern in patterns:
        for match in re.findall(pattern, normalized_text):
            if isinstance(match, tuple):
                snippets.extend(part.strip() for part in match if part.strip())
            else:
                snippets.append(match.strip())

    return [snippet for snippet in snippets if snippet and snippet not in LOCATION_CUE_WORDS]


def _choose_best_candidate(score_map, allowed_values=None):

    if not score_map:
        return None

    ranked_items = sorted(score_map.items(), key=lambda item: (item[1], len(item[0])), reverse=True)

    for value, _ in ranked_items:
        if allowed_values is None or value in allowed_values:
            return value

    return None


def _confidence_from_score(score, candidates_count):

    if score <= 0:
        return 0.0

    scale = max(4, min(candidates_count * 2, 12))

    return round(min(score / scale, 1.0), 3)


def resolve_location_details(locs, text="", title="", state_hint=None, district_hint=None):

    district = None
    state = None
    location_candidates = [location for location in locs if location]
    title_candidate = normalize_location_name(title)
    snippet_candidates = _extract_location_snippets(text)
    all_candidates = list(location_candidates)

    if title_candidate:
        all_candidates.append(title_candidate)

    all_candidates.extend(snippet_candidates)

    if state_hint:
        all_candidates.append(state_hint)

    if district_hint:
        all_candidates.append(district_hint)

    state_scores = _score_candidates(all_candidates, state_aliases, states, allow_ambiguous=True)
    district_scores = _score_candidates(all_candidates, district_aliases, districts)

    normalized_state_hint = normalize_location_name(state_hint)
    normalized_district_hint = normalize_location_name(district_hint)

    if normalized_state_hint in state_aliases:
        canonical_state_hint = state_aliases[normalized_state_hint]
        state_scores[canonical_state_hint] = state_scores.get(canonical_state_hint, 0) + 4

    if normalized_district_hint in district_aliases:
        canonical_district_hint = district_aliases[normalized_district_hint]
        district_scores[canonical_district_hint] = district_scores.get(canonical_district_hint, 0) + 5

    state = _choose_best_candidate(state_scores)
    district = _choose_best_candidate(district_scores)

    if district and not state:
        state = district_to_state.get(district)

    if district and state and district_to_state.get(district) != state:
        district = None

    if state and not district:
        allowed_districts = state_to_districts.get(state, set())
        district = _choose_best_candidate(district_scores, allowed_districts)

        if not district and title_candidate:
            district = _find_token_match(title_candidate, allowed_districts)

        if not district:
            for snippet in snippet_candidates:
                district = _find_token_match(snippet, allowed_districts)

                if district:
                    break

    if not district:
        if title_candidate:
            district = _find_token_match(title_candidate)

        if not district:
            for snippet in snippet_candidates:
                district = _find_token_match(snippet)

                if district:
                    break

        if district and not state:
            state = district_to_state.get(district)

    state_confidence = _confidence_from_score(state_scores.get(state, 0), len(all_candidates)) if state else 0.0
    district_confidence = _confidence_from_score(district_scores.get(district, 0), len(all_candidates)) if district else 0.0

    if district and district_confidence < DISTRICT_CONFIDENCE_THRESHOLD:
        district = None
        district_confidence = 0.0

    if state and state_confidence < STATE_CONFIDENCE_THRESHOLD and not district:
        state = None
        state_confidence = 0.0

    if district and not state:
        state = district_to_state.get(district)
        state_confidence = max(state_confidence, 0.8)

    return {
        "state": state,
        "district": district,
        "state_confidence": state_confidence,
        "district_confidence": district_confidence,
    }


def resolve_location(locs, text="", title=""):
    details = resolve_location_details(locs, text=text, title=title)
    return details["state"], details["district"]