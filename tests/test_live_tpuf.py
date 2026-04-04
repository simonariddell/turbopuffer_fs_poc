from __future__ import annotations

import os
import uuid

import pytest

from turbopuffer_fs import (
    find,
    grep,
    list_mounts,
    ls,
    make_client,
    mount_namespace,
    put_bytes,
    put_text,
    read_bytes,
    read_text,
    rm,
    stat,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("TURBOPUFFER_FS_LIVE") != "1"
    or not os.environ.get("TURBOPUFFER_API_KEY")
    or not os.environ.get("TURBOPUFFER_REGION"),
    reason="live turbopuffer tests require TURBOPUFFER_FS_LIVE=1 plus turbopuffer credentials",
)


def test_live_round_trip() -> None:
    client = make_client()
    mount = f"livefs{uuid.uuid4().hex[:10]}"
    namespace = mount_namespace(mount)
    text_path = "/notes/hello.txt"
    binary_path = "/bin/data.bin"

    put_text(client, mount, text_path, "hello\noauth token\n")
    put_bytes(client, mount, binary_path, b"\x00\x01\x02")

    mounts = list_mounts(client)
    assert mount in mounts

    text_stat = stat(client, mount, text_path)
    assert text_stat is not None
    assert text_stat["path"] == text_path

    children = ls(client, mount, "/")
    assert {row["path"] for row in children} >= {"/notes", "/bin"}

    matches = find(client, mount, "/notes", glob="*.txt")
    assert [row["path"] for row in matches] == ["/notes/hello.txt"]

    assert read_text(client, mount, text_path) == "hello\noauth token\n"
    assert read_bytes(client, mount, binary_path) == b"\x00\x01\x02"

    grep_matches = grep(client, mount, "/", "oauth", ignore_case=True)
    assert grep_matches == [{"path": text_path, "line_number": 2, "line": "oauth token"}]

    rm(client, mount, "/", recursive=True)
