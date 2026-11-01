from types import SimpleNamespace
import pytest
from sqlalchemy.exc import IntegrityError, OperationalError
from pipeline_runtime.kafka import DeliveryLoop, publish_confirmed, CONSUMER_OPTIONS
from pipeline_runtime.identity import logical_key


class Message:
    def topic(self): return 'test'
    def partition(self): return 0
    def offset(self): return 17


class Client:
    def __init__(self): self.calls=[]
    def commit(self, **kwargs): self.calls.append('commit'); return []


class BatchClient:
    def __init__(self): self.commits=[]
    def commit(self, **kwargs): self.commits.append(kwargs); return []


def test_success_commits_after_processing():
    c=Client(); loop=DeliveryLoop(c,'test')
    loop.process(Message(), lambda msg:c.calls.append('processed'))
    assert c.calls==['processed','commit']
    assert CONSUMER_OPTIONS['enable.auto.commit'] is False
    assert CONSUMER_OPTIONS['enable.auto.offset.store'] is False


@pytest.mark.parametrize('failure',['processing','commit','revoke','lost'])
def test_failure_never_acknowledges_inflight_record(failure):
    c=Client(); loop=DeliveryLoop(c,'test')
    def process(msg):
        if failure=='processing': raise RuntimeError('database failed')
        if failure in ('revoke','lost'): loop._rebalance(failure)(c,[])
    if failure=='commit':
        def commit(**kwargs): raise RuntimeError('coordinator unavailable')
        c.commit=commit
    with pytest.raises(RuntimeError): loop.process(Message(),process)
    assert not c.calls


def test_failure_mid_batch_never_commits_offsets():
    client = BatchClient()
    loop = DeliveryLoop(client, 'test-batch')
    messages = [Message(), Message()]

    with pytest.raises(RuntimeError, match='database failed'):
        loop.process_batch(messages, lambda _messages: (_ for _ in ()).throw(RuntimeError('database failed')))

    assert client.commits == []


@pytest.mark.parametrize('result',['error','timeout','missing'])
def test_producer_must_confirm_delivery(result):
    class Producer:
        def produce(self, **kwargs): self.callback=kwargs['callback']
        def flush(self, timeout):
            if result=='error': self.callback(RuntimeError('broker unavailable'),None)
            return 1 if result=='timeout' else 0
    with pytest.raises(RuntimeError): publish_confirmed(Producer(),topic='test',key='k',value=b'{}')


def test_window_identity_preserves_intentional_overlap():
    p={'entity_id':'192.0.2.1','entity_type':'source_ip','feature_contract_version':'2.0',
       'window_start':'2026-01-01T00:00:00Z','window_end':'2026-01-01T00:01:00Z','source_event_ids':['one']}
    same={**p,'window_start':'2026-01-01T00:00:00+00:00','calculated_at':'later'}
    other={**p,'window_start':'2026-01-01T00:00:30Z','window_end':'2026-01-01T00:01:30Z'}
    assert logical_key('features',p)==logical_key('features',same)
    assert logical_key('features',p)!=logical_key('features',other)


def _database_down():
    return OperationalError("INSERT INTO telemetry_events", {}, Exception("connection refused"))


def test_transient_database_outage_is_retried_in_place_and_committed_once():
    c = Client(); sleeps = []; attempts = []
    loop = DeliveryLoop(c, 'test', sleep=sleeps.append)

    def handler(_message):
        attempts.append(1)
        if len(attempts) <= 2:
            raise _database_down()
        c.calls.append('processed')

    loop.process(Message(), handler)
    assert len(attempts) == 3 and sleeps == [1.0, 2.0]
    assert c.calls == ['processed', 'commit']  # one acknowledgement, only after the successful attempt


def test_transient_outage_in_a_batch_is_retried_and_offsets_commit_after_success():
    client = BatchClient(); attempts = []
    loop = DeliveryLoop(client, 'test-batch', sleep=lambda _seconds: None)

    def handler(_messages):
        attempts.append(1)
        if len(attempts) == 1:
            raise _database_down()

    loop.process_batch([Message(), Message()], handler)
    assert len(attempts) == 2 and len(client.commits) == 1


@pytest.mark.parametrize('error', [
    RuntimeError('logic error'), ValueError('bad payload'),
    IntegrityError('INSERT', {}, Exception('duplicate key')),  # a data error, not a lost connection
])
def test_only_transient_failures_are_retried_everything_else_fails_closed(error):
    c = Client(); attempts = []
    loop = DeliveryLoop(c, 'test', sleep=lambda _seconds: pytest.fail('must not wait'))

    def handler(_message):
        attempts.append(1)
        raise error

    with pytest.raises(type(error)): loop.process(Message(), handler)
    assert len(attempts) == 1 and not c.calls


def test_outage_longer_than_the_retry_window_fails_closed(monkeypatch):
    from pipeline_runtime import kafka
    clock = [0.0]
    monkeypatch.setattr(kafka.time, 'monotonic', lambda: clock[0])
    c = Client()
    loop = DeliveryLoop(c, 'test', sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    with pytest.raises(OperationalError):
        loop.process(Message(), lambda _message: (_ for _ in ()).throw(_database_down()))
    assert clock[0] <= kafka.TRANSIENT_RETRY_SECONDS  # never waits past max.poll.interval.ms
    assert not c.calls


def test_shutdown_request_stops_retrying_immediately():
    c = Client()
    loop = DeliveryLoop(c, 'test', sleep=lambda _seconds: pytest.fail('must not wait'))
    with pytest.raises(OperationalError):
        loop.process(Message(), lambda _message: (_ for _ in ()).throw(_database_down()), running=lambda: False)
    assert not c.calls
