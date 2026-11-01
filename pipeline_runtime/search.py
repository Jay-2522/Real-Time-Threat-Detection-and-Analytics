"""Create search indices with their intended mappings, whenever the search service becomes reachable.

An index that is created implicitly by the first document gets a dynamic mapping (text instead of keyword, string
instead of ip), which later breaks exact filters, sorting and aggregations. Services therefore ensure their indices
before every write until it succeeds once, instead of only at startup, when the search service may be down.
"""


def ensure_indices(client, mappings):
    """Create each missing index with its mapping. Safe to repeat, and safe when another process creates it first."""
    for name, mapping in mappings.items():
        if client.indices.exists(index=name):
            continue
        try:
            client.indices.create(index=name, mappings=mapping)
        except Exception as exc:
            if "resource_already_exists_exception" not in str(exc):
                raise
