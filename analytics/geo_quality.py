from __future__ import annotations

import pandas as pd

from config.config import DISTRICT_CONFIDENCE_THRESHOLD, STATE_CONFIDENCE_THRESHOLD


def _safe_confidence(series: pd.Series, default: float) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.fillna(default).clip(lower=0.0, upper=1.0)


def apply_confidence_weighted_fallback(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if "district_confidence" not in df.columns:
        df = df.copy()
        df["district_confidence"] = 0.5

    if "state_confidence" not in df.columns:
        df = df.copy()
        df["state_confidence"] = 0.5

    quality_df = df.copy()
    quality_df["district_confidence"] = _safe_confidence(quality_df["district_confidence"], 0.0)
    quality_df["state_confidence"] = _safe_confidence(quality_df["state_confidence"], 0.0)

    # Confidence-weighted fallback:
    # - high confidence records get full weight
    # - medium confidence records get partial weight
    # - very low confidence records are removed from district analytics
    quality_df["geo_weight"] = (
        (quality_df["district_confidence"] * 0.7) + (quality_df["state_confidence"] * 0.3)
    ).clip(lower=0.0, upper=1.0)

    high_conf_mask = quality_df["district_confidence"] >= DISTRICT_CONFIDENCE_THRESHOLD
    medium_conf_mask = (
        (quality_df["district_confidence"] >= max(0.25, DISTRICT_CONFIDENCE_THRESHOLD - 0.2))
        | (quality_df["state_confidence"] >= STATE_CONFIDENCE_THRESHOLD)
    )

    quality_df.loc[high_conf_mask, "geo_weight"] = quality_df.loc[high_conf_mask, "geo_weight"].clip(lower=0.85)
    quality_df.loc[~high_conf_mask & medium_conf_mask, "geo_weight"] = quality_df.loc[
        ~high_conf_mask & medium_conf_mask, "geo_weight"
    ].clip(lower=0.45, upper=0.8)
    quality_df.loc[~medium_conf_mask, "geo_weight"] = quality_df.loc[~medium_conf_mask, "geo_weight"].clip(upper=0.25)

    return quality_df[quality_df["geo_weight"] >= 0.35].copy()
