from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.analytics import AnalyticsSummaryResponse, PredictionResponse, RecordResponse
from app.services.analytics_query_service import AnalyticsQueryService

router = APIRouter()
service = AnalyticsQueryService()


@router.get("/analytics/summary", response_model=AnalyticsSummaryResponse)
def analytics_summary(db: Annotated[Session, Depends(get_db)]) -> AnalyticsSummaryResponse:
    return AnalyticsSummaryResponse(**service.summary(db))


@router.get("/events/recent", response_model=list[RecordResponse])
def recent_events(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=50, ge=1, le=500), offset: int = Query(default=0, ge=0)) -> list:
    return service.recent_events(db, limit, offset)


@router.get("/predictions/recent", response_model=list[PredictionResponse])
def recent_predictions(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=50, ge=1, le=500), offset: int = Query(default=0, ge=0)) -> list:
    return service.recent_predictions(db, limit, offset)


@router.get("/predictions/{entity_id}", response_model=PredictionResponse)
def latest_prediction(entity_id: str, db: Annotated[Session, Depends(get_db)]):
    prediction = service.latest_prediction(db, entity_id)
    if prediction is None:
        raise HTTPException(status_code=404, detail="Prediction not found for entity")
    return prediction


@router.get("/risks/high", response_model=list[PredictionResponse])
def high_risks(db: Annotated[Session, Depends(get_db)], limit: int = Query(default=50, ge=1, le=500), offset: int = Query(default=0, ge=0)) -> list:
    return service.high_risks(db, limit, offset)


@router.get("/search/events", response_model=list[dict[str, Any]])
def search_events(q: str = Query(min_length=1), limit: int = Query(default=25, ge=1, le=100)) -> list[dict[str, Any]]:
    return service.search_events(q, limit)

