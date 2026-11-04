"""A search that cannot be answered must not look like a search with no matches."""
import secrets

import pytest
from elastic_transport import ApiResponseMeta, ConnectionError as TransportConnectionError, ConnectionTimeout
from elastic_transport import HttpHeaders, NodeConfig
from elasticsearch import ApiError, BadRequestError, NotFoundError
from fastapi.testclient import TestClient

from test_api_contracts import load_backend_app

ENDPOINTS = ["/api/v1/search/events", "/api/v1/alerts/search", "/api/v1/threat-intel/search"]
HEADERS = {"Origin": "http://localhost:5173", "X-Requested-With": "SOC"}


def _meta(status):
    return ApiResponseMeta(status=status, http_version="1.1", headers=HttpHeaders(), duration=0.0,
                           node=NodeConfig("http", "localhost", 9200))


class FakeElasticsearch:
    outcome = None
    requests = []

    def __init__(self, *args, **kwargs):
        pass

    def search(self, **request):
        FakeElasticsearch.requests.append(request)
        if isinstance(FakeElasticsearch.outcome, Exception):
            raise FakeElasticsearch.outcome
        return {"hits": {"hits": [{"_source": hit} for hit in FakeElasticsearch.outcome]}}


@pytest.fixture
def client(tmp_path, monkeypatch):
    app = load_backend_app(tmp_path)
    from app.auth.service import create_user
    from app.db.session import SessionLocal
    from app.services import search_client

    monkeypatch.setattr(search_client, "Elasticsearch", FakeElasticsearch)
    FakeElasticsearch.requests = []
    password = secrets.token_urlsafe(24)
    with SessionLocal() as db, db.begin():
        create_user(db, "search-admin", password, "administrator", initial=True)
    with TestClient(app, base_url="http://localhost") as session:
        assert session.post("/api/v1/auth/login", json={"username": "search-admin", "password": password},
                            headers=HEADERS).status_code == 200
        yield session


@pytest.mark.parametrize("endpoint", ENDPOINTS)
@pytest.mark.parametrize("failure", [
    TransportConnectionError("connection refused"), ConnectionTimeout("timed out"), ApiError("overloaded", _meta(503), {}),
    ApiError("too many requests", _meta(429), {}),
])
def test_an_unreachable_or_failing_search_service_is_reported_not_shown_as_no_matches(client, endpoint, failure):
    FakeElasticsearch.outcome = failure
    response = client.get(endpoint, params={"q": "203.0.113.42"})
    assert response.status_code == 503
    assert response.json() == {"detail": "Search is temporarily unavailable"}
    assert response.headers["Retry-After"] == "30"


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_a_query_that_cannot_be_parsed_is_a_client_error(client, endpoint):
    FakeElasticsearch.outcome = BadRequestError("parse_exception", _meta(400), {})
    response = client.get(endpoint, params={"q": "(unbalanced"})
    assert response.status_code == 400
    assert response.json() == {"detail": "The search query could not be parsed"}


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_nothing_indexed_yet_is_an_empty_result_and_matches_are_returned(client, endpoint):
    FakeElasticsearch.outcome = NotFoundError("index_not_found_exception", _meta(404), {})
    assert client.get(endpoint, params={"q": "anything"}).json() == []

    FakeElasticsearch.outcome = [{"entity_id": "203.0.113.42"}]
    response = client.get(endpoint, params={"q": "203.0.113.42"})
    assert response.status_code == 200 and response.json() == [{"entity_id": "203.0.113.42"}]
    assert FakeElasticsearch.requests[-1]["query"] == {"query_string": {"query": "203.0.113.42", "default_field": "*"}}
