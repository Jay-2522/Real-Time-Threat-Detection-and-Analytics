def explain_alert(rule_name: str, event_type: str, severity: str) -> tuple[str, str]:
    if rule_name == "large_network_transfer":
        return (
            "This single network event transferred at least eight million bytes.",
            "Validate the destination and expected transfer size before escalating.",
        )
    if rule_name == "high_anomaly":
        return (
            f"The ML detector classified this {event_type} signal as {severity} based on abnormal feature patterns.",
            "Review the related entity timeline and compare recent behavior against normal operating baselines.",
        )
    if rule_name == "model_positive":
        return (
            "The model flagged this window as slightly outside normal behavior, below the medium alert threshold.",
            "Treat as context only; escalate if other alerts exist for the same entity.",
        )
    return (
        "Correlated telemetry and risk signals indicate activity outside the expected baseline.",
        "Review the incident timeline, validate business context, and escalate if the activity is unauthorized.",
    )

