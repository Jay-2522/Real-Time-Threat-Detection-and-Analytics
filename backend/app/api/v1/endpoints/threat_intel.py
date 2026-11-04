import asyncio
import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session

from app.db.session import SessionLocal, get_db
from app.services.threat_intel_service import ThreatIntelQueryService

router = APIRouter()
service = ThreatIntelQueryService()
logger = logging.getLogger(__name__)


@router.get("/threat-intel/overview")
def overview(db: Annotated[Session, Depends(get_db)]) -> dict[str, Any]:
    return service.overview(db)


@router.get("/threat-intel/iocs")
def iocs(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=100, ge=1, le=500), ioc_type: str | None = None) -> list:
    return [service._row(row) for row in service.iocs(db, limit, ioc_type)]


@router.get("/threat-intel/entities")
def entities(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=100, ge=1, le=500)) -> list:
    return [service._row(row) for row in service.entities(db, limit)]


@router.get("/threat-intel/mitre")
def mitre(db: Annotated[Session, Depends(get_db)]) -> list[dict[str, Any]]:
    return service.mitre_summary(db)


@router.get("/threat-intel/attack-timeline")
def attack_timeline(db: Annotated[Session, Depends(get_db)], entity_id: str | None = None, limit: int = Query(default=20, ge=1, le=100)) -> list:
    return [service._row(row) for row in service.attack_timeline(db, entity_id, limit)]


@router.get("/threat-intel/risk-heatmap")
def risk_heatmap(db: Annotated[Session, Depends(get_db)]) -> list[dict[str, Any]]:
    return service.risk_heatmap(db)


@router.get("/threat-intel/threat-graph")
def threat_graph(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=80, ge=1, le=200)) -> dict[str, Any]:
    return service.threat_graph(db, limit)


@router.get("/threat-intel/search")
def threat_search(q: str = Query(min_length=1), limit: int = Query(default=50, ge=1, le=200)) -> list[dict[str, Any]]:
    return service.search(q, limit)


@router.get("/detections/rules")
def detection_rules(db: Annotated[Session, Depends(get_db)]) -> list[dict[str, Any]]:
    return service.detection_rules(db)


@router.get("/detections/loaded-rules")
def loaded_detection_rules(db: Annotated[Session, Depends(get_db)]) -> list[dict[str, Any]]:
    return service.loaded_detection_rules(db)


@router.get("/detections/rules/{rule_id}")
def detection_rule(rule_id: str, db: Annotated[Session, Depends(get_db)]) -> dict[str, Any]:
    result = service.detection_rule(db, rule_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Detection rule not found")
    return result


@router.get("/detections/rule-stats")
def rule_stats(db: Annotated[Session, Depends(get_db)]) -> dict[str, Any]:
    return service.rule_stats(db)


@router.get("/entities/high-risk")
def high_risk_entities(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=50, ge=1, le=200)) -> list:
    return [service._row(row) for row in service.high_risk_entities(db, limit)]


@router.get("/entities/search")
def search_entities(db: Annotated[Session, Depends(get_db)], q: str = Query(min_length=1), limit: int = Query(default=50, ge=1, le=200)) -> list:
    return [service._row(row) for row in service.search_entities(db, q, limit)]


@router.get("/entities/{entity_id}")
def entity(entity_id: str, db: Annotated[Session, Depends(get_db)]) -> dict[str, Any]:
    result = service.entity(db, entity_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return service._row(result)


@router.get("/mitre/tactics")
def mitre_tactics() -> list[str]:
    return service.mitre_tactics()


@router.get("/mitre/techniques")
def mitre_techniques() -> list[dict[str, Any]]:
    return service.mitre_techniques()


@router.websocket("/ws/threat-intel")
async def threat_intel_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    logger.info("threat_intel_websocket_connected client=%s", websocket.client.host if websocket.client else "unknown")
    try:
        while True:
            with SessionLocal() as db:
                await websocket.send_json(service.live_message(db))
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        logger.info("threat_intel_websocket_disconnected")
