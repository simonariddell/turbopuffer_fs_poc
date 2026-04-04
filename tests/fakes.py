from __future__ import annotations

from collections import deque


class FakeRow:
    def __init__(self, **values):
        self._values = dict(values)

    def model_dump(self):
        return dict(self._values)


class FakeQueryResponse:
    def __init__(self, rows=None, *, billing=None, performance=None, aggregations=None, aggregation_groups=None):
        self.rows = list(rows or [])
        self.billing = billing or {"units": 1}
        self.performance = performance or {"latency_ms": 1}
        self.aggregations = aggregations
        self.aggregation_groups = aggregation_groups


class FakeWriteResponse:
    def __init__(self, **values):
        self._values = {
            "status": "OK",
            "message": "ok",
            "rows_affected": values.get("rows_affected", 0),
            **values,
        }

    def model_dump(self):
        return dict(self._values)


class FakeNamespaceList:
    def __init__(self, namespaces):
        self._namespaces = list(namespaces)
        self.next_cursor = None

    def __iter__(self):
        return iter(self._namespaces)


class FakeNamespace:
    def __init__(self, name: str, *, query_responses=None, write_responses=None):
        self.name = name
        self.query_calls = []
        self.write_calls = []
        self.query_responses = deque(query_responses or [])
        self.write_responses = deque(write_responses or [])

    def query(self, **payload):
        self.query_calls.append(dict(payload))
        if self.query_responses:
            response = self.query_responses.popleft()
            return response(payload) if callable(response) else response
        return FakeQueryResponse(rows=[])

    def write(self, **payload):
        self.write_calls.append(dict(payload))
        if self.write_responses:
            response = self.write_responses.popleft()
            return response(payload) if callable(response) else response
        deletes = payload.get("deletes", [])
        upserts = payload.get("upsert_rows", [])
        return FakeWriteResponse(
            rows_affected=len(deletes) + len(upserts),
            rows_deleted=len(deletes) or None,
            rows_upserted=len(upserts) or None,
            deleted_ids=list(deletes) or None,
            upserted_ids=[row["id"] for row in upserts] or None,
        )


class FakeClient:
    def __init__(
        self,
        *,
        namespaces=None,
        namespace=None,
        namespace_ids=None,
        namespace_lists=None,
        query_pages=None,
    ):
        self._namespaces = dict(namespaces or {})
        if namespace is not None:
            self._namespaces[namespace.name] = namespace
        if query_pages is not None:
            for name, response_map in query_pages.items():
                query_responses = []
                for step_name in response_map:
                    for rows in response_map[step_name]:
                        query_responses.append(FakeQueryResponse(rows=rows))
                self._namespaces[name] = FakeNamespace(name, query_responses=query_responses)
        self.namespace_calls = []
        self.namespace_list_calls = []
        if namespace_lists is not None:
            self.namespace_lists = deque(namespace_lists)
        elif namespace_ids is not None:
            self.namespace_lists = deque([FakeNamespaceList([{"id": value} for value in namespace_ids])])
        else:
            self.namespace_lists = deque()

    def namespace(self, name: str):
        self.namespace_calls.append(name)
        if name not in self._namespaces:
            self._namespaces[name] = FakeNamespace(name)
        return self._namespaces[name]

    def namespaces(self, **payload):
        self.namespace_list_calls.append(dict(payload))
        if self.namespace_lists:
            response = self.namespace_lists.popleft()
            return response(payload) if callable(response) else response
        return FakeNamespaceList([])
