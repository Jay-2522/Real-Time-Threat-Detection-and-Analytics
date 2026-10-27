from types import SimpleNamespace
import pytest
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
