import asyncio
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session

from app.db.session import SessionLocal, get_db
from app.core.metrics import ACTIVE_WEBSOCKET_CONNECTIONS, DASHBOARD_WEBSOCKET_MESSAGES_TOTAL
from app.schemas.dashboard import ChartPoint, DashboardLiveMessage, DashboardOverviewResponse, EntityRiskPoint, SystemHealthResponse
from app.services.analytics_query_service import AnalyticsQueryService

router = APIRouter()
service = AnalyticsQueryService()
logger = logging.getLogger(__name__)


@router.get("/dashboard/overview", response_model=DashboardOverviewResponse)
def dashboard_overview(db: Annotated[Session, Depends(get_db)]) -> DashboardOverviewResponse:
    return service.dashboard_overview(db)


@router.get("/dashboard/system-health", response_model=SystemHealthResponse)
def dashboard_system_health(db: Annotated[Session, Depends(get_db)]) -> SystemHealthResponse:
    return service.system_health(db)


@router.get("/dashboard/risk-trends", response_model=list[ChartPoint])
def dashboard_risk_trends(db: Annotated[Session, Depends(get_db)]) -> list[ChartPoint]:
    return service.risk_trends(db)


@router.get("/dashboard/event-volume", response_model=list[ChartPoint])
def dashboard_event_volume(db: Annotated[Session, Depends(get_db)]) -> list[ChartPoint]:
    return service.event_volume(db)


@router.get("/dashboard/severity-distribution", response_model=list[ChartPoint])
def dashboard_severity_distribution(db: Annotated[Session, Depends(get_db)]) -> list[ChartPoint]:
    return service.severity_distribution(db)


@router.get("/dashboard/event-type-distribution", response_model=list[ChartPoint])
def dashboard_event_type_distribution(db: Annotated[Session, Depends(get_db)]) -> list[ChartPoint]:
    return service.event_type_distribution(db)


@router.get("/dashboard/top-entities", response_model=list[EntityRiskPoint])
def dashboard_top_entities(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=10, ge=1, le=25)) -> list[EntityRiskPoint]:
    return service.top_entities(db, limit)


@router.websocket("/ws/dashboard")
async def dashboard_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    ACTIVE_WEBSOCKET_CONNECTIONS.labels("dashboard").inc()
    logger.info(
        "dashboard_websocket_connected client=%s origin=%s",
        websocket.client.host if websocket.client else "unknown",
        websocket.headers.get("origin", "unknown"),
    )
    try:
        while True:
            message = _build_live_dashboard_message()
            await websocket.send_json(message.model_dump(mode="json"))
            DASHBOARD_WEBSOCKET_MESSAGES_TOTAL.inc()
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        logger.info("dashboard_websocket_disconnected reason=client_disconnect")
        return
    except Exception:
        logger.exception("dashboard_websocket_disconnected reason=unexpected_error")
        raise
    finally:
        ACTIVE_WEBSOCKET_CONNECTIONS.labels("dashboard").dec()


def _build_live_dashboard_message() -> DashboardLiveMessage:
    with SessionLocal() as db:
        overview = service.dashboard_overview(db)
        latest_predictions = [_live_record(item) for item in service.recent_predictions(db, 10)]
        recent_events = [_live_record(item) for item in service.recent_events(db, 10)]
        high_risk_events = [_live_record(item) for item in service.high_risks(db, 10)]
        event_counters = {point.label: int(point.value) for point in service.event_type_distribution(db)}
        return DashboardLiveMessage(
            timestamp=overview.generated_at,
            system_status=overview.system_status,
            total_events=overview.total_events,
            total_predictions=overview.total_predictions,
            anomaly_count=overview.anomaly_count,
            high_risk_count=overview.high_risk_count,
            avg_risk_score=overview.average_risk_score,
            overview=overview,
            latest_predictions=latest_predictions,
            recent_predictions=latest_predictions,
            recent_events=recent_events,
            high_risk_events=high_risk_events,
            event_counters=event_counters,
        )


def _live_record(item) -> dict:
    payload = dict(item.raw_payload or {})
    payload["source_topic"] = item.source_topic
    return payload
