from sqlalchemy import desc, func, select, update
from sqlalchemy.orm import Session

from app.db.models import Alert, Incident


class AlertRepository:
    def recent_alerts(self, db: Session, limit: int = 50, offset: int = 0) -> list[Alert]:
        return list(db.scalars(select(Alert).order_by(desc(Alert.created_at), desc(Alert.id)).limit(limit).offset(offset)))

    def alerts_by_severity(self, db: Session, severities: list[str], limit: int = 50, offset: int = 0) -> list[Alert]:
        return list(db.scalars(select(Alert).where(Alert.severity.in_(severities)).order_by(desc(Alert.timestamp), desc(Alert.id)).limit(limit).offset(offset)))

    def recent_incidents(self, db: Session, limit: int = 50, offset: int = 0) -> list[Incident]:
        return list(db.scalars(select(Incident).order_by(desc(Incident.updated_at), desc(Incident.id)).limit(limit).offset(offset)))

    def open_incidents(self, db: Session, limit: int = 50, offset: int = 0) -> list[Incident]:
        return list(db.scalars(select(Incident).where(Incident.status != "resolved").order_by(desc(Incident.updated_at), desc(Incident.id)).limit(limit).offset(offset)))

    def incident(self, db: Session, incident_id: str) -> Incident | None:
        return db.scalars(select(Incident).where(Incident.incident_id == incident_id).limit(1)).first()

    def stats(self, db: Session) -> dict:
        severity_rows = db.execute(select(Alert.severity, func.count()).group_by(Alert.severity)).all()
        severity_counts = {row[0]: int(row[1]) for row in severity_rows}
        return {
            "total_alerts": db.scalar(select(func.count()).select_from(Alert)) or 0,
            "open_alerts": db.scalar(select(func.count()).select_from(Alert).where(Alert.status == "open")) or 0,
            "critical_alerts": severity_counts.get("critical", 0),
            "high_alerts": severity_counts.get("high", 0),
            "total_incidents": db.scalar(select(func.count()).select_from(Incident)) or 0,
            "open_incidents": db.scalar(select(func.count()).select_from(Incident).where(Incident.status != "resolved")) or 0,
            "severity_counts": severity_counts,
        }

    def update_alert_status(self, db: Session, alert_id: str, status: str) -> Alert | None:
        db.execute(update(Alert).where(Alert.alert_id == alert_id).values(status=status))
        db.flush()
        return db.scalars(select(Alert).where(Alert.alert_id == alert_id)).first()

    def update_incident_status(self, db: Session, incident_id: str, status: str, notes: str | None) -> Incident | None:
        values = {"status": status}
        if notes is not None:
            values["resolution_notes"] = notes
        db.execute(update(Incident).where(Incident.incident_id == incident_id).values(**values))
        db.flush()
        return self.incident(db, incident_id)

