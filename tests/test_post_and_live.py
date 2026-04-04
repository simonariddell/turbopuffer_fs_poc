from __future__ import annotations

import pytest

from turbopuffer_fs.live import list_mounts, mount_namespace, stat
from turbopuffer_fs.post import (
    content_bytes,
    content_text,
    finalize_cat,
    finalize_find,
    finalize_grep,
    finalize_ls,
    finalize_read_bytes,
    finalize_read_text,
    finalize_rm,
    finalize_stat,
)
from turbopuffer_fs.schema import bytes_row, directory_row, text_row
from tests.fakes import FakeClient, FakeNamespace


def test_finalize_stat_returns_metadata() -> None:
    row = text_row("/notes/a.txt", "hello")
    value = finalize_stat({}, {"target": {"rows": [row]}})
    assert value == {
        "id": row["id"],
        "path": "/notes/a.txt",
        "parent": "/notes",
        "basename": "a.txt",
        "kind": "file",
        "ext": ".txt",
        "mime": "text/plain",
        "size_bytes": 5,
        "is_text": 1,
        "sha256": row["sha256"],
        "source_size_bytes": 5,
    }


def test_finalize_ls_requires_directory() -> None:
    row = text_row("/notes/a.txt", "hello")
    with pytest.raises(NotADirectoryError):
        finalize_ls({"path": "/notes/a.txt"}, {"target": {"rows": [row]}, "children": {"rows": []}})


def test_finalize_find_filters_file_root_to_self() -> None:
    row = text_row("/notes/a.txt", "hello")
    other = text_row("/notes/b.txt", "world")
    matches = finalize_find({"root": "/notes/a.txt"}, {"target": {"rows": [row]}, "matches": {"rows": [row, other]}})
    assert [item["path"] for item in matches] == ["/notes/a.txt"]


def test_finalize_cat_and_read_text_reject_binary() -> None:
    row = bytes_row("/photos/a.jpg", b"\x00\x01")
    with pytest.raises(ValueError):
        finalize_cat({"path": "/photos/a.jpg"}, {"target": {"rows": [row]}})
    with pytest.raises(ValueError):
        finalize_read_text({"path": "/photos/a.jpg"}, {"target": {"rows": [row]}})


def test_finalize_read_bytes_returns_bytes_for_text_and_binary() -> None:
    text_value = finalize_read_bytes({"path": "/notes/a.txt"}, {"target": {"rows": [text_row("/notes/a.txt", "hello")]}})
    binary_value = finalize_read_bytes({"path": "/photos/a.jpg"}, {"target": {"rows": [bytes_row("/photos/a.jpg", b"\x00\x01")]}})
    assert text_value == b"hello"
    assert binary_value == b"\x00\x01"


def test_finalize_grep_applies_exact_local_match() -> None:
    rows = [
        {"path": "/notes/a.txt", "text": "oauth token\nother\nOAuth done"},
        {"path": "/notes/b.txt", "text": "different"},
    ]
    matches = finalize_grep(
        {"root": "/notes", "pattern": "oauth", "ignore_case": True},
        {"target": {"rows": [directory_row("/notes")]}, "candidates": {"rows": rows}},
    )
    assert matches == [
        {"path": "/notes/a.txt", "line_number": 1, "line": "oauth token"},
        {"path": "/notes/a.txt", "line_number": 3, "line": "OAuth done"},
    ]


def test_finalize_rm_returns_noop_for_missing_target() -> None:
    value = finalize_rm({"path": "/notes/missing", "recursive": False}, {"target": {"rows": []}})
    assert value["deleted"] is False
    assert value["ids"] == []


def test_content_helpers() -> None:
    assert content_text(text_row("/notes/a.txt", "hello")) == "hello"
    assert content_bytes(text_row("/notes/a.txt", "hello")) == b"hello"
    assert content_bytes(bytes_row("/photos/a.jpg", b"\x00\x01")) == b"\x00\x01"
    with pytest.raises(IsADirectoryError):
        content_text(directory_row("/notes"))


def test_mount_namespace_and_list_mounts() -> None:
    client = FakeClient(namespace_ids=["documents__fs", "logs__fs", "misc"])
    assert mount_namespace("documents") == "documents__fs"
    assert list_mounts(client) == ["documents", "logs"]


def test_live_stat_delegates_through_runtime() -> None:
    row = text_row("/notes/a.txt", "hello")
    client = FakeClient(
        namespaces={"documents__fs": FakeNamespace("documents__fs", query_responses=[{"rows": [row]}])}
    )
    value = stat(client, "documents", "/notes/a.txt")
    assert value["id"] == row["id"]
    assert value["path"] == "/notes/a.txt"
