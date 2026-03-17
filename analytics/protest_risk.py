from __future__ import annotations

from pathlib import Path

import pandas as pd
from joblib import dump, load
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


MODEL_PATH = Path(__file__).resolve().parent.parent / "data" / "models" / "risk_model.pkl"

FEATURE_COLUMNS = [
    "issue_spike_ratio",
    "negative_sentiment_score",
    "protest_keyword_count",
    "issue_repetition_days",
    "hospital_density",
    "rainfall",
]


def _coerce_feature_payload(feature_payload: dict | None) -> dict:
    payload = feature_payload or {}

    return {
        "issue_spike_ratio": float(payload.get("issue_spike_ratio", 0.0)),
        "negative_sentiment_score": float(payload.get("negative_sentiment_score", 0.0)),
        "protest_keyword_count": float(payload.get("protest_keyword_count", 0.0)),
        "issue_repetition_days": float(payload.get("issue_repetition_days", 0.0)),
        "hospital_density": float(payload.get("hospital_density", 0.0)),
        "rainfall": float(payload.get("rainfall", 0.0)),
    }


def _heuristic_risk_score(features: dict) -> float:
    spike_component = min(features["issue_spike_ratio"] / 4.0, 1.0)
    sentiment_component = min(max(features["negative_sentiment_score"], 0.0), 1.0)
    protest_component = min(features["protest_keyword_count"] / 15.0, 1.0)
    repetition_component = min(features["issue_repetition_days"] / 21.0, 1.0)
    infra_component = max(0.0, min((1.5 - features["hospital_density"]) / 1.5, 1.0))
    rainfall_component = min(features["rainfall"] / 250.0, 1.0)

    score = (
        0.28 * spike_component
        + 0.24 * sentiment_component
        + 0.16 * protest_component
        + 0.14 * repetition_component
        + 0.12 * infra_component
        + 0.06 * rainfall_component
    )

    return round(float(max(0.0, min(score, 1.0))), 3)


def predict_protest_risk(feature_payload: dict, model_path: Path | None = None) -> dict:
    model_file = model_path or MODEL_PATH
    features = _coerce_feature_payload(feature_payload)
    feature_df = pd.DataFrame([features], columns=FEATURE_COLUMNS)

    if model_file.exists():
        model = load(model_file)

        if hasattr(model, "predict_proba"):
            score = float(model.predict_proba(feature_df)[0][1])
        else:
            score = float(model.predict(feature_df)[0])

        risk_score = round(max(0.0, min(score, 1.0)), 3)
        source = "ml_model"
    else:
        risk_score = _heuristic_risk_score(features)
        source = "heuristic_fallback"

    if risk_score >= 0.7:
        risk_level = "high"
    elif risk_score >= 0.45:
        risk_level = "medium"
    else:
        risk_level = "low"

    return {
        "protest_risk": risk_score,
        "risk_level": risk_level,
        "model_source": source,
        "features": features,
        "model_path": str(model_file),
    }


def train_and_save_risk_model(training_rows: list[dict], model_type: str = "logistic_regression") -> dict:
    if not training_rows:
        raise ValueError("training_rows cannot be empty")

    train_df = pd.DataFrame(training_rows)
    missing_columns = [column for column in FEATURE_COLUMNS + ["label"] if column not in train_df.columns]

    if missing_columns:
        raise ValueError(f"Missing training columns: {', '.join(missing_columns)}")

    x = train_df[FEATURE_COLUMNS].astype(float)
    y = train_df["label"].astype(int)

    if model_type == "random_forest":
        model = RandomForestClassifier(n_estimators=150, random_state=42, class_weight="balanced")
    else:
        model = Pipeline(
            steps=[
                ("scaler", StandardScaler()),
                ("classifier", LogisticRegression(max_iter=800, class_weight="balanced")),
            ]
        )

    model.fit(x, y)

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    dump(model, MODEL_PATH)

    return {
        "message": "Risk model trained and saved",
        "model_type": model_type,
        "rows_used": len(train_df),
        "model_path": str(MODEL_PATH),
    }
