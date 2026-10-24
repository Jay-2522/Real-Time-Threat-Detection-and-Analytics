import json
import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit, parse_qs


def hardened():
    return os.environ.get('ENVIRONMENT') == 'production'


@lru_cache
def configuration():
    path = os.environ.get('SERVICE_SECURITY_FILE')
    if not path:
        if hardened(): raise RuntimeError('SERVICE_SECURITY_FILE is required in production')
        return {}
    try:
        result = json.loads(Path(path).read_text())
        if not isinstance(result, dict): raise ValueError()
        return result
    except Exception:
        raise RuntimeError('Service security file is missing or invalid') from None


def section(name):
    result = configuration().get(name)
    if result is None and hardened(): raise RuntimeError(f'Missing required security section: {name}')
    return result


def database_url(fallback):
    value = section('database_url')
    if value is None: return fallback
    parsed = urlsplit(value)
    query = parse_qs(parsed.query)
    if (parsed.scheme != 'postgresql+psycopg' or not parsed.password or
        query.get('sslmode') != ['verify-full'] or not query.get('sslrootcert')):
        raise RuntimeError('Database requires credentials and verified TLS')
    return value


def elasticsearch_options(host=None):
    if hardened() and host is not None and urlsplit(host).scheme != 'https':
        raise RuntimeError('Elasticsearch requires HTTPS')
    value = section('elasticsearch')
    if value is None: return {}
    if not value.get('username') or not value.get('password') or not value.get('ca_certs'):
        raise RuntimeError('Elasticsearch requires credentials and a trusted CA')
    return {'basic_auth': (value['username'], value['password']), 'ca_certs': value['ca_certs'], 'verify_certs': True}


def kafka_options():
    value = section('kafka')
    if value is None: return {}
    if (value.get('security.protocol') != 'SSL' or not all(value.get(k) for k in
        ('ssl.ca.location','ssl.certificate.location','ssl.key.location')) or
        value.get('ssl.endpoint.identification.algorithm', 'https') != 'https'):
        raise RuntimeError('Kafka requires mutually authenticated, hostname-verified TLS')
    return {**value, 'enable.ssl.certificate.verification':True, 'ssl.endpoint.identification.algorithm':'https'}


def kafka_java_options():
    value = section('kafka_java')
    if value is None: return {'kafka.security.protocol':'PLAINTEXT'}
    if (value.get('kafka.security.protocol') != 'SSL' or
        value.get('kafka.ssl.endpoint.identification.algorithm') != 'https' or
        not value.get('kafka.ssl.truststore.location') or not value.get('kafka.ssl.keystore.location')):
        raise RuntimeError('Spark Kafka connections require mutually authenticated verified TLS')
    return value
