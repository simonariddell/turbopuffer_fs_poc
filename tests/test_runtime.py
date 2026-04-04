from __future__ import annotations

from types import SimpleNamespace

from turbopuffer_fs.runtime import execute_plan, paginate_ordered_query, to_plain

from .fakes import FakeClient, FakeNamespace, FakeNamespaceList, FakeQueryResponse, FakeWriteResponse


class ModelLike:
    def __init__(self, payload):
        self.payload = payload

    def model_dump(self):
        return self.payload


def test_to_plain_handles_model_dump():
    assert to_plain(ModelLike({"a": [1, 2]})) == {"a": [1, 2]}


def test_paginate_ordered_query_uses_after_filter():
    namespace = FakeNamespace(
        "docs__fs",
        query_responses=[
            FakeQueryResponse(rows=[{"path": "/a"}, {"path": "/b"}]),
            FakeQueryResponse(rows=[{"path": "/c"}]),
        ],
    )
    result = paginate_ordered_query(
        namespace,
        {
            "name": "matches",
            "payload": {"filters": ("kind", "Eq", "file"), "rank_by": ("path", "asc")},
            "page_size": 2,
            "paginate": True,
        },
    )
    assert [row["path"] for row in result["rows"]] == ["/a", "/b", "/c"]
    assert namespace.query_calls[1]["filters"] == (
        "And",
        (("kind", "Eq", "file"), ("path", "Gt", "/b")),
    )


def test_execute_plan_normalizes_query_write_and_namespaces():
    client = FakeClient(
        namespaces={
            "docs__fs": FakeNamespace(
                "docs__fs",
                query_responses=[FakeQueryResponse(rows=[{"path": "/a", "kind": "file"}])],
                write_responses=[FakeWriteResponse(status="OK", rows_affected=1, message="ok")],
            )
        },
        namespace_lists=[FakeNamespaceList([SimpleNamespace(id="docs__fs"), {"id": "logs"}])],
    )
    plan = {
        "namespace": "docs__fs",
        "steps": [
            {
                "kind": "query",
                "name": "target",
                "payload": {"filters": ("path", "Eq", "/a"), "limit": 1},
            },
            {
                "kind": "write",
                "name": "write",
                "payload": {"deletes": ["id-1"]},
            },
            {
                "kind": "namespaces",
                "name": "ns",
                "payload": {},
            },
        ],
    }
    executed = execute_plan(client, plan)
    assert executed["results"]["target"]["rows"][0]["path"] == "/a"
    assert executed["results"]["write"]["status"] == "OK"
    assert executed["results"]["ns"]["namespaces"] == [{"id": "docs__fs"}, {"id": "logs"}]
