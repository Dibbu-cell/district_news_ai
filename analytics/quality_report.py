from __future__ import annotations

import pandas as pd

from config.config import DISTRICT_CONFIDENCE_THRESHOLD


def _safe_confidence(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default).clip(lower=0.0, upper=1.0)


def build_daily_quality_report(df: pd.DataFrame, window_days: int) -> dict:
    if df.empty:
        return {
            "window_days": window_days,
            "total_collected": 0,
            "district_mapped_count": 0,
            "district_mapped_percent": 0.0,
            "unmapped_count": 0,
            "unmapped_percent": 0.0,
            "top_ambiguous_districts": [],
        }

    report_df = df.copy()
    report_df["state"] = report_df["state"].fillna("").astype(str).str.strip().str.lower()
    report_df["district"] = report_df["district"].fillna("").astype(str).str.strip().str.lower()

    if "district_confidence" not in report_df.columns:
        report_df["district_confidence"] = 0.0

    if "state_confidence" not in report_df.columns:
        report_df["state_confidence"] = 0.0

    report_df["district_confidence"] = _safe_confidence(report_df["district_confidence"])
    report_df["state_confidence"] = _safe_confidence(report_df["state_confidence"])

    total_collected = int(len(report_df))
    mapped_mask = (
        (report_df["district"] != "")
        & (report_df["state"] != "")
        & (report_df["district_confidence"] >= DISTRICT_CONFIDENCE_THRESHOLD)
    )
    district_mapped_count = int(mapped_mask.sum())

    unmapped_mask = (report_df["district"] == "") | (report_df["state"] == "")
    unmapped_count = int(unmapped_mask.sum())

    ambiguous_mask = (~unmapped_mask) & (report_df["district_confidence"] < DISTRICT_CONFIDENCE_THRESHOLD)
    ambiguous_df = report_df[ambiguous_mask]

    ambiguous_rows = []

    if not ambiguous_df.empty:
        grouped = (
            ambiguous_df.groupby(["state", "district"]) 
            .agg(
                articles=("district", "count"),
                avg_district_confidence=("district_confidence", "mean"),
                avg_state_confidence=("state_confidence", "mean"),
            )
            .reset_index()
            .sort_values(["articles", "avg_district_confidence"], ascending=[False, True])
            .head(10)
        )

        for _, row in grouped.iterrows():
            ambiguous_rows.append(
                {
                    "state": row["state"],
                    "district": row["district"],
                    "articles": int(row["articles"]),
                    "avg_district_confidence": round(float(row["avg_district_confidence"]), 3),
                    "avg_state_confidence": round(float(row["avg_state_confidence"]), 3),
                }
            )

    return {
        "window_days": window_days,
        "total_collected": total_collected,
        "district_mapped_count": district_mapped_count,
        "district_mapped_percent": round((district_mapped_count / max(total_collected, 1)) * 100, 2),
        "unmapped_count": unmapped_count,
        "unmapped_percent": round((unmapped_count / max(total_collected, 1)) * 100, 2),
        "top_ambiguous_districts": ambiguous_rows,
    }


def build_source_mapping_audit_report(df: pd.DataFrame, window_days: int, limit: int = 25) -> dict:
    if df.empty:
        return {
            "window_days": window_days,
            "source_count": 0,
            "sources": [],
        }

    report_df = df.copy()
    report_df["source"] = report_df["source"].fillna("unknown").astype(str).str.strip().str.lower()
    report_df["state"] = report_df["state"].fillna("").astype(str).str.strip().str.lower()
    report_df["district"] = report_df["district"].fillna("").astype(str).str.strip().str.lower()

    if "district_confidence" not in report_df.columns:
        report_df["district_confidence"] = 0.0

    if "state_confidence" not in report_df.columns:
        report_df["state_confidence"] = 0.0

    report_df["district_confidence"] = _safe_confidence(report_df["district_confidence"])
    report_df["state_confidence"] = _safe_confidence(report_df["state_confidence"])

    mapped_mask = (
        (report_df["district"] != "")
        & (report_df["state"] != "")
        & (report_df["district_confidence"] >= DISTRICT_CONFIDENCE_THRESHOLD)
    )
    unmapped_mask = (report_df["district"] == "") | (report_df["state"] == "")
    low_conf_mask = (~unmapped_mask) & (report_df["district_confidence"] < DISTRICT_CONFIDENCE_THRESHOLD)

    rows = []

    for source_name, group in report_df.groupby("source"):
        source_total = len(group)
        source_mapped = int(mapped_mask.loc[group.index].sum())
        source_unmapped = int(unmapped_mask.loc[group.index].sum())
        source_low_conf = int(low_conf_mask.loc[group.index].sum())

        rows.append(
            {
                "source": source_name,
                "articles": int(source_total),
                "mapped_percent": round((source_mapped / max(source_total, 1)) * 100, 2),
                "unmapped_percent": round((source_unmapped / max(source_total, 1)) * 100, 2),
                "low_confidence_percent": round((source_low_conf / max(source_total, 1)) * 100, 2),
                "avg_district_confidence": round(float(group["district_confidence"].mean()), 3),
                "avg_state_confidence": round(float(group["state_confidence"].mean()), 3),
            }
        )

    rows.sort(
        key=lambda row: (row["low_confidence_percent"], row["unmapped_percent"], row["articles"]),
        reverse=True,
    )

    return {
        "window_days": window_days,
        "source_count": len(rows),
        "sources": rows[: max(1, min(limit, 100))],
    }
