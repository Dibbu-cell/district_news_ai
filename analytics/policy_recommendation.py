from __future__ import annotations


ISSUE_POLICY_MAP = {
    "water": [
        "Deploy emergency water tankers",
        "Start water quality testing",
        "Install water filtration units",
    ],
    "health": [
        "Deploy mobile medical units",
        "Increase critical-care and specialist beds",
        "Run district-level screening camps",
    ],
    "road_safety": [
        "Repair road black spots and potholes",
        "Install speed cameras at high-risk junctions",
        "Improve street lighting and signage",
    ],
    "crime": [
        "Increase law-enforcement patrolling in hotspots",
        "Launch rapid grievance and witness support cells",
        "Deploy conflict-prevention teams during sensitive periods",
    ],
    "infrastructure": [
        "Prioritize emergency infrastructure maintenance budget",
        "Conduct safety audits for public assets",
        "Create district-level execution war room",
    ],
}


def build_policy_recommendations(
    *,
    primary_issue: str,
    anger_score: float,
    protest_risk: float,
    hospital_density: float | None = None,
    sensitive_events: list[str] | None = None,
) -> dict:
    issue_key = (primary_issue or "other").strip().lower()
    sensitive_events = sensitive_events or []

    recommendations = []
    recommendations.extend(ISSUE_POLICY_MAP.get(issue_key, ["Start district field assessment for root-cause validation"]))

    if issue_key == "health" and hospital_density is not None and hospital_density < 0.8:
        recommendations.extend(
            [
                "Deploy mobile dialysis and nephrology outreach units",
                "Increase dialysis and kidney-care capacity",
            ]
        )

    if issue_key == "water" and anger_score > 0.6:
        recommendations.extend(
            [
                "Publish daily water safety bulletin",
                "Open temporary clean-water distribution points",
            ]
        )

    if issue_key == "crime" and any(event in {"violent_crime", "mob_violence", "police_clash"} for event in sensitive_events):
        recommendations.extend(
            [
                "Activate district peace committee and rapid response protocol",
                "Deploy evidence-led preventive policing in high-tension zones",
            ]
        )

    if protest_risk >= 0.7:
        recommendations.extend(
            [
                "Convene district emergency coordination meeting within 24 hours",
                "Issue public communication with timeline-bound action commitments",
            ]
        )

    unique_recommendations = []

    for item in recommendations:
        if item not in unique_recommendations:
            unique_recommendations.append(item)

    return {
        "primary_issue": issue_key,
        "recommended_actions": unique_recommendations[:8],
        "action_priority": "urgent" if protest_risk >= 0.7 or anger_score >= 0.65 else "standard",
    }
