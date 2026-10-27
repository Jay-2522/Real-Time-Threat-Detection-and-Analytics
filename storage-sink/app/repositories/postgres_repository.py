from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from collections import defaultdict

from app.db.models import AnalyticsMetric, AnomalyPrediction, DeadLetterEvent, FeatureSnapshot, RiskMetric, TelemetryEvent
from app.schemas.records import StorageRecord


class PostgresStorageRepository:
    def persist(self, session: Session, record: StorageRecord) -> None:
        model = self._model_for_record(record)
        values = self._values_for_record(record)
        statement = insert(model).values(**values)

        if model is TelemetryEvent and record.event_id:
            statement = statement.on_conflict_do_update(
                constraint="uq_telemetry_events_source_event",
                set_={"raw_payload": record.raw_payload, "risk_score": record.risk_score, "severity": record.severity},
            )
        elif model is AnomalyPrediction and record.prediction_id:
            statement = statement.on_conflict_do_update(
                constraint="uq_anomaly_predictions_prediction_id",
                set_={
                    "raw_payload": record.raw_payload,
                    "ml_risk_score": record.ml_risk_score,
                    "severity": record.severity,
                    "explanation": record.explanation,
                },
            )
        else:
            statement = statement.on_conflict_do_nothing()

        session.execute(statement)

    def persist_many(self, session: Session, records: list[StorageRecord]) -> None:
        grouped = defaultdict(list)
        for record in records:
            grouped[self._model_for_record(record)].append(self._values_for_record(record))
        for model, values in grouped.items():
            statement = insert(model).values(values)
            if model is TelemetryEvent:
                statement = statement.on_conflict_do_update(
                    constraint="uq_telemetry_events_source_event",
                    set_={
                        "raw_payload": statement.excluded.raw_payload,
                        "risk_score": statement.excluded.risk_score,
                        "severity": statement.excluded.severity,
                    },
                )
            elif model is AnomalyPrediction:
                statement = statement.on_conflict_do_update(
                    constraint="uq_anomaly_predictions_prediction_id",
                    set_={
                        "raw_payload": statement.excluded.raw_payload,
                        "ml_risk_score": statement.excluded.ml_risk_score,
                        "severity": statement.excluded.severity,
                        "explanation": statement.excluded.explanation,
                    },
                )
            else:
                statement = statement.on_conflict_do_nothing()
            session.execute(statement)

    @staticmethod
    def _model_for_record(record: StorageRecord):
        if record.record_kind == "prediction":
            return AnomalyPrediction
        if record.record_kind == "deadletter":
            return DeadLetterEvent
        if record.record_kind == "risk":
            return RiskMetric
        if record.record_kind == "feature":
            return FeatureSnapshot
        if record.record_kind == "analytics":
            return AnalyticsMetric
        return TelemetryEvent

    @staticmethod
    def _values_for_record(record: StorageRecord) -> dict:
        common = {
            "event_type": record.event_type,
            "entity_id": record.entity_id,
            "source_topic": record.source_topic,
            "timestamp": record.timestamp_or_created,
            "severity": record.severity,
            "raw_payload": record.raw_payload,
        }
        if record.record_kind == "prediction":
            return {**common, "prediction_id": record.prediction_id, "ml_risk_score": record.ml_risk_score, "explanation": record.explanation}
        if record.record_kind == "deadletter":
            return {**common, "event_id": record.event_id, "reason": record.reason}
        return {**common, "event_id": record.event_id, "risk_score": record.risk_score}

