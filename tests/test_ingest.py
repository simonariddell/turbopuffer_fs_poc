from __future__ import annotations

from pathlib import Path

from turbopuffer_fs.ingest import batched, ingest_directory, mounted_path, scan_directory, write_rows
from turbopuffer_fs.live import mount_namespace
from turbopuffer_fs.paths import path_id
from tests.fakes import FakeClient


def test_mounted_path_under_root(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    child = root / "a.txt"
    child.write_text("hello\n", encoding="utf-8")

    assert mounted_path(root, root) == "/"
    assert mounted_path(root, child) == "/a.txt"
    assert mounted_path(root, child, mount_root="/archive") == "/archive/a.txt"


def test_scan_directory_builds_rows(tmp_path: Path):
    root = tmp_path / "docs"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (root / "notes.txt").write_text("alpha\nbeta\n", encoding="utf-8")
    (nested / "data.bin").write_bytes(b"\x00\x01\x02")

    rows = scan_directory(root, mount_root="/archive")
    rows_by_path = {row["path"]: row for row in rows}

    assert rows_by_path["/archive"]["kind"] == "dir"
    assert rows_by_path["/archive/nested"]["kind"] == "dir"
    assert rows_by_path["/archive/notes.txt"]["is_text"] == 1
    assert rows_by_path["/archive/nested/data.bin"]["is_text"] == 0


def test_batched_splits_rows():
    rows = [{"path": f"/{index}"} for index in range(5)]
    assert batched(rows, 2) == [
        [{"path": "/0"}, {"path": "/1"}],
        [{"path": "/2"}, {"path": "/3"}],
        [{"path": "/4"}],
    ]


def test_write_rows_writes_batches():
    client = FakeClient()
    rows = [{"id": path_id(f"/{index}"), "path": f"/{index}"} for index in range(3)]

    responses = write_rows(client, "documents__fs", rows, batch_size=2)

    assert len(responses) == 2
    namespace_handle = client.namespace("documents__fs")
    assert len(namespace_handle.write_calls) == 2
    assert namespace_handle.write_calls[0]["upsert_rows"][0]["path"] == "/0"


def test_ingest_directory_returns_summary(tmp_path: Path):
    root = tmp_path / "docs"
    root.mkdir()
    (root / "hello.txt").write_text("hello\n", encoding="utf-8")

    client = FakeClient()
    summary = ingest_directory(client, "documents", root)

    assert summary["mount"] == "documents"
    assert summary["namespace"] == mount_namespace("documents")
    assert summary["row_count"] == 2
    assert len(summary["writes"]) == 1
