from __future__ import annotations

import pytest

from turbopuffer_fs.dogfood import (
    apply_model_operation,
    expected_grep_matches,
    model_find,
    model_ls,
    new_model_state,
)


def test_model_mkdir_and_ls() -> None:
    state = new_model_state()
    apply_model_operation(state, {"op": "mkdir", "path": "/notes"})
    apply_model_operation(state, {"op": "mkdir", "path": "/notes/archive"})

    children = model_ls(state, "/notes")
    assert [row["path"] for row in children] == ["/notes/archive"]


def test_model_put_and_find() -> None:
    state = new_model_state()
    apply_model_operation(state, {"op": "put_text", "path": "/notes/todo.txt", "text": "hello\nworld\n"})
    apply_model_operation(state, {"op": "put_bytes", "path": "/bin/data.bin", "data": b"\x00\x01"})

    matches = model_find(state, "/")
    assert [row["path"] for row in matches] == ["/", "/bin", "/bin/data.bin", "/notes", "/notes/todo.txt"]


def test_model_grep_matches_literal_lines() -> None:
    state = new_model_state()
    apply_model_operation(state, {"op": "put_text", "path": "/notes/a.txt", "text": "oauth token\nother\nOAuth done\n"})

    matches = expected_grep_matches(state, root="/", pattern="oauth", ignore_case=True)
    assert matches == [
        {"path": "/notes/a.txt", "line_number": 1, "line": "oauth token"},
        {"path": "/notes/a.txt", "line_number": 3, "line": "OAuth done"},
    ]


def test_model_rm_recursive() -> None:
    state = new_model_state()
    apply_model_operation(state, {"op": "put_text", "path": "/notes/a.txt", "text": "hello\n"})
    apply_model_operation(state, {"op": "put_text", "path": "/notes/b.txt", "text": "world\n"})

    apply_model_operation(state, {"op": "rm", "path": "/notes", "recursive": True})
    assert [row["path"] for row in model_find(state, "/")] == ["/"]


def test_model_rejects_non_recursive_non_empty_dir_delete() -> None:
    state = new_model_state()
    apply_model_operation(state, {"op": "put_text", "path": "/notes/a.txt", "text": "hello\n"})

    with pytest.raises(OSError):
        apply_model_operation(state, {"op": "rm", "path": "/notes", "recursive": False})
