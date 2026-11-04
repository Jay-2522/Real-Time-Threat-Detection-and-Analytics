import asyncio
import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session

from app.db.session import SessionLocal, get_db
from app.auth.service import audit, authorize_write
from app.core.metrics import ACTIVE_WEBSOCKET_CONNECTIONS, ALERT_WEBSOCKET_MESSAGES_TOTAL
from app.schemas.alerts import AlertResponse, AlertStatsResponse, AlertSummary, IncidentResponse, IncidentSummary, StatusUpdateRequest
from app.services.alert_service import AlertQueryService

router = APIRouter()
service = AlertQueryService()
logger = logging.getLogger(__name__)


@router.get("/alerts/recent", response_model=list[AlertSummary])
def recent_alerts(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0)) -> list:
    return service.recent_alerts(db, limit, offset)


@router.get("/alerts/high", response_model=list[AlertSummary])
def high_alerts(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0)) -> list:
    return service.high_alerts(db, limit, offset)


@router.get("/alerts/critical", response_model=list[AlertSummary])
def critical_alerts(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0)) -> list:
    return service.critical_alerts(db, limit, offset)


@router.get("/incidents/recent", response_model=list[IncidentSummary])
def recent_incidents(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0)) -> list:
    return service.recent_incidents(db, limit, offset)


@router.get("/incidents/open", response_model=list[IncidentSummary])
def open_incidents(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0)) -> list:
    return service.open_incidents(db, limit, offset)


@router.get("/incidents/{incident_id}", response_model=IncidentResponse)
def incident(incident_id: str, db: Annotated[Session, Depends(get_db)]):
    result = service.incident(db, incident_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return result


@router.get("/alerts/stats", response_model=AlertStatsResponse)
def alert_stats(db: Annotated[Session, Depends(get_db)]) -> AlertStatsResponse:
    return service.stats(db)


@router.get("/alerts/search", response_model=list[dict[str, Any]])
def search_alerts(q: str = Query(min_length=1), limit: int = Query(default=25, ge=1, le=100)) -> list[dict[str, Any]]:
    return service.search(q, limit)


@router.patch("/alerts/{alert_id}/status", response_model=AlertResponse)
def update_alert_status(alert_id: str, request: StatusUpdateRequest, http_request: Request, db: Annotated[Session, Depends(get_db)]):
    authorize_write(db, http_request.state.principal, "analyst")
    result = service.update_alert_status(db, alert_id, request.status)
    if result is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    audit(db, "alert_status_changed", alert_id, actor=http_request.state.principal["id"])
    db.commit()
    return result


@router.patch("/incidents/{incident_id}/status", response_model=IncidentResponse)
def update_incident_status(incident_id: str, request: StatusUpdateRequest, http_request: Request, db: Annotated[Session, Depends(get_db)]):
    authorize_write(db, http_request.state.principal, "analyst")
    result = service.update_incident_status(db, incident_id, request.status, request.resolution_notes)
    if result is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    audit(db, "incident_status_changed", incident_id, actor=http_request.state.principal["id"])
    db.commit()
    return result


@router.websocket("/ws/alerts")
async def alerts_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    ACTIVE_WEBSOCKET_CONNECTIONS.labels("alerts").inc()
    logger.info("alerts_websocket_connected client=%s", websocket.client.host if websocket.client else "unknown")
    try:
        while True:
            with SessionLocal() as db:
                await websocket.send_json(service.live_message(db))
                ALERT_WEBSOCKET_MESSAGES_TOTAL.inc()
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        logger.info("alerts_websocket_disconnected reason=client_disconnect")
    finally:
        ACTIVE_WEBSOCKET_CONNECTIONS.labels("alerts").dec()
