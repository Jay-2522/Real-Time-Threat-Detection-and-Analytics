from datetime import datetime
from typing import Any

from pydantic import BaseModel


class DashboardMetric(BaseModel):
    label: str
    value: int | float | str
    change: float | None = None


class DashboardOverviewResponse(BaseModel):
    total_events: int  # raw telemetry events
    total_records: int  # telemetry plus the analytics, risk and feature records derived from it
    total_predictions: int
    anomaly_count: int
    high_risk_count: int
    average_risk_score: float
    system_status: str
    generated_at: datetime


class SystemComponentHealth(BaseModel):
    name: str
    status: str
    detail: str


class SystemHealthResponse(BaseModel):
    status: str
    components: list[SystemComponentHealth]
    generated_at: datetime


class ChartPoint(BaseModel):
    label: str
    value: float
    secondary_value: float | None = None


class EntityRiskPoint(BaseModel):
    entity_id: str
    risk_score: float
    event_count: int
    severity: str | None = None


class DashboardLiveMessage(BaseModel):
    timestamp: datetime
    system_status: str
    total_events: int
    total_predictions: int
    anomaly_count: int
    high_risk_count: int
    avg_risk_score: float
    overview: DashboardOverviewResponse
    latest_predictions: list[dict[str, Any]]
    recent_predictions: list[dict[str, Any]]
    recent_events: list[dict[str, Any]]
    high_risk_events: list[dict[str, Any]]
    event_counters: dict[str, int]
