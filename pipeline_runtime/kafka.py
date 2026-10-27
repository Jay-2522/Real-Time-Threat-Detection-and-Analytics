"""One synchronous in-flight record; fail closed and replay after a bounded restart delay."""
import logging
import time
from prometheus_client import Counter, Gauge

logger = logging.getLogger(__name__)
FAILURES = Counter("pipeline_processing_failures_total", "Unacknowledged processing/commit failures", ["service"])
COMMITS = Counter("pipeline_commits_total", "Confirmed input offset commits", ["service"])
REBALANCES = Counter("pipeline_rebalances_total", "Assignment/revocation/loss callbacks", ["service", "event"])

LAST_SUCCESS = Gauge("pipeline_last_commit_timestamp_seconds", "Last successful acknowledgement", ["service"])
LAG = Gauge("pipeline_consumer_lag", "Observed high watermark minus committed next offset", ["service", "topic", "partition"])

CONSUMER_OPTIONS = {"enable.auto.commit": False, "enable.auto.offset.store": False,
                    "auto.offset.reset": "earliest", "max.poll.interval.ms": 300000}


class DeliveryLoop:
    def __init__(self, consumer, service):
        self.consumer = consumer
        self.service = service
        self.epoch = 0
        self.last_lag_check = 0.0

    def _rebalance(self, event):
        def callback(consumer, partitions):
            self.epoch += 1
            REBALANCES.labels(self.service, event).inc()
            logger.info("consumer_%s service=%s partitions=%s epoch=%s", event, self.service, partitions, self.epoch)
            # No offsets are stored implicitly and no callback commits in-flight work.
        return callback

    def subscribe(self, topics):
        self.consumer.subscribe(list(topics), on_assign=self._rebalance("assigned"),
                                on_revoke=self._rebalance("revoked"), on_lost=self._rebalance("lost"))

    def process(self, message, handler):
        epoch = self.epoch
        try:
            handler(message)
            if epoch != self.epoch:
                raise RuntimeError("Partition ownership changed before acknowledgement")
            results = self.consumer.commit(message=message, asynchronous=False)
            if results and any(p.error for p in results):
                raise RuntimeError(f"Offset commit failed: {results}")
            COMMITS.labels(self.service).inc()
            LAST_SUCCESS.labels(self.service).set(time.time())
            # Monitoring failure must not turn a successfully committed record into an error.
            if time.monotonic() - self.last_lag_check >= 10:
                try:
                    from confluent_kafka import TopicPartition
                    _, high = self.consumer.get_watermark_offsets(
                        TopicPartition(message.topic(), message.partition()), timeout=1, cached=False)
                    LAG.labels(self.service, message.topic(), str(message.partition())).set(
                        max(0, high - message.offset() - 1))
                    self.last_lag_check = time.monotonic()
                except Exception:
                    logger.warning("consumer_lag_observation_unavailable service=%s", self.service)
        except Exception:
            FAILURES.labels(self.service).inc()
            logger.exception("input_unacknowledged service=%s topic=%s partition=%s offset=%s",
                             self.service, message.topic(), message.partition(), message.offset())
            raise

    def process_batch(self, messages, handler):
        if not messages:
            return
        epoch = self.epoch
        try:
            handler(messages)
            if epoch != self.epoch:
                raise RuntimeError("Partition ownership changed before batch acknowledgement")
            from confluent_kafka import TopicPartition
            latest = {}
            for message in messages:
                coordinate = (message.topic(), message.partition())
                latest[coordinate] = max(message.offset(), latest.get(coordinate, -1))
            offsets = [
                TopicPartition(topic, partition, offset + 1)
                for (topic, partition), offset in latest.items()
            ]
            results = self.consumer.commit(offsets=offsets, asynchronous=False)
            if results and any(partition.error for partition in results):
                raise RuntimeError(f"Batch offset commit failed: {results}")
            COMMITS.labels(self.service).inc(len(messages))
            LAST_SUCCESS.labels(self.service).set(time.time())
            if time.monotonic() - self.last_lag_check >= 10:
                try:
                    for position in offsets:
                        _, high = self.consumer.get_watermark_offsets(
                            TopicPartition(position.topic, position.partition), timeout=1, cached=False)
                        LAG.labels(self.service, position.topic, str(position.partition)).set(
                            max(0, high - position.offset))
                    self.last_lag_check = time.monotonic()
                except Exception:
                    logger.warning("consumer_lag_observation_unavailable service=%s", self.service)
        except Exception:
            FAILURES.labels(self.service).inc()
            first, last = messages[0], messages[-1]
            logger.exception(
                "input_batch_unacknowledged service=%s count=%s first=%s:%s:%s last=%s:%s:%s",
                self.service, len(messages), first.topic(), first.partition(), first.offset(),
                last.topic(), last.partition(), last.offset())
            raise

    def run(self, topics, handler, running):
        try:
            self.subscribe(topics)
            while running():
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(str(message.error()))
                self.process(message, handler)
        except Exception:
            # Stop this process; never poll/commit a later record over a failed one.
            # Compose restarts it with unchanged group offsets. Local callers rerun it.
            time.sleep(5)
            raise
        finally:
            self.consumer.close()  # auto commit is disabled, including on close.

    def run_batch(self, topics, handler, running, *, batch_size=200, batch_wait_seconds=0.05):
        try:
            self.subscribe(topics)
            while running():
                first = self.consumer.poll(1.0)
                if first is None:
                    continue
                messages = [first]
                if batch_size > 1:
                    messages.extend(self.consumer.consume(
                        num_messages=batch_size - 1, timeout=batch_wait_seconds))
                for message in messages:
                    if message.error():
                        raise RuntimeError(str(message.error()))
                self.process_batch(messages, handler)
        except Exception:
            time.sleep(5)
            raise
        finally:
            self.consumer.close()


def publish_confirmed(producer, *, topic, key, value, timeout=15):
    outcomes = []
    producer.produce(topic=topic, key=key, value=value, callback=lambda error, message: outcomes.append(error))
    remaining = producer.flush(timeout)
    if remaining or len(outcomes) != 1 or outcomes[0] is not None:
        raise RuntimeError(f"Kafka delivery unconfirmed topic={topic} remaining={remaining} errors={outcomes}")
