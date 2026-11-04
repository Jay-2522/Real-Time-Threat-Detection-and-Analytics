"""Run user searches against Elasticsearch and say clearly when they cannot be answered.

A search that fails must not look like a search with no matches: the caller gets an unavailable or invalid-query
error, and only a genuinely empty result is an empty list.
"""
from elasticsearch import ApiError, BadRequestError, Elasticsearch, NotFoundError, TransportError

from infrastructure_runtime.security import elasticsearch_options
from app.core.settings import settings


class SearchUnavailable(Exception):
    """Elasticsearch is unreachable, timed out, or is rejecting requests."""


class InvalidSearchQuery(ValueError):
    """The query string cannot be parsed, so no search was run."""


def run_search(indices: str, query: str, limit: int, *, newest_first: bool = False) -> list[dict]:
    client = Elasticsearch(settings.elasticsearch_host, request_timeout=2, max_retries=0,
                           **elasticsearch_options(settings.elasticsearch_host))
    request = {"index": indices, "size": limit, "query": {"query_string": {"query": query, "default_field": "*"}}}
    if newest_first:
        request["sort"] = [{"timestamp": {"order": "desc", "unmapped_type": "date"}}]
    try:
        response = client.search(**request)
    except BadRequestError as exc:
        raise InvalidSearchQuery("The search query could not be parsed") from exc
    except NotFoundError:
        return []  # none of the indices exists yet: nothing has been indexed, so there is nothing to find
    except (TransportError, ApiError) as exc:  # unreachable, timed out, or an overloaded/failing cluster
        raise SearchUnavailable("Search is temporarily unavailable") from exc
    return [hit["_source"] for hit in response["hits"]["hits"]]
