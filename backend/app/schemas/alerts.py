from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

AlertStatus = Literal["open", "acknowledged", "investigating", "resolved"]


class AlertResponse(BaseModel):
    id: int
    alert_id: str
    timestamp: datetime
    severity: str
    title: str
    description: str
    source_event_id: str | None = None
    entity_id: str | None = None
    event_type: str | None = None
    source_topic: str
    anomaly_score: float | None = None
    ml_risk_score: float | None = None
    correlation_id: str
    incident_id: str | None = None
    explanation: str
    recommended_action: str
    status: str
    tags: list[str]
    raw_payload: dict[str, Any]
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class AlertSummary(AlertResponse):
    """List rows leave out the stored source payload; it is large and only the detail views need it."""

    raw_payload: dict[str, Any] = Field(default_factory=dict, exclude=True)


class IncidentResponse(BaseModel):
    id: int
    incident_id: str
    created_at: datetime
    updated_at: datetime
    severity: str
    title: str
    description: str
    related_alerts: list[str]
    entity_ids: list[str]
    status: str
    event_count: int
    escalation_level: int
    resolution_notes: str | None = None
    raw_payload: dict[str, Any]

    model_config = ConfigDict(from_attributes=True)


class IncidentSummary(IncidentResponse):
    raw_payload: dict[str, Any] = Field(default_factory=dict, exclude=True)


class StatusUpdateRequest(BaseModel):
    status: AlertStatus
    resolution_notes: str | None = None


class AlertStatsResponse(BaseModel):
    total_alerts: int
    open_alerts: int
    critical_alerts: int
    high_alerts: int
    total_incidents: int
    open_incidents: int
    severity_counts: dict[str, int]


class AlertsLiveMessage(BaseModel):
    timestamp: datetime
    recent_alerts: list[dict[str, Any]]
    critical_alerts: list[dict[str, Any]]
    recent_incidents: list[dict[str, Any]]
    severity_counters: dict[str, int]
    stats: AlertStatsResponse

