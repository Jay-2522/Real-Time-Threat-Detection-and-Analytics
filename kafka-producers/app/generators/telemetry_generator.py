from datetime import UTC, datetime

from pipeline_runtime.traffic_model import TrafficModel

from app.schemas.events import AnomalyEvent, ApiEvent, LoginEvent, NetworkEvent, TelemetryEvent

_EVENT_CLASSES = {"login": LoginEvent, "api": ApiEvent, "network": NetworkEvent, "anomaly": AnomalyEvent}


class SyntheticTelemetryGenerator:
    """Publishes the shared traffic model: stable users and hosts plus labelled attack scenarios.

    `anomaly_rate` is the long-run fraction of events that belong to an attack scenario.
    The same model trains the ML service, so live traffic matches the training distribution.
    """

    def __init__(self, anomaly_rate: float, seed: int | None = None) -> None:
        self._model = TrafficModel(anomaly_rate=anomaly_rate, seed=seed)

    def next_event(self) -> TelemetryEvent:
        fields = self._model.next_event(datetime.now(UTC)).fields
        return _EVENT_CLASSES[fields["event_type"]](**fields)
