from __future__ import annotations

from posixpath import join as posix_join

from hypothesis import given
from hypothesis import strategies as st
import pytest

from turbopuffer_fs.paths import (
    ancestor_paths,
    basename,
    extension,
    join_glob,
    join_path,
    normalize_glob_path,
    normalize_path,
    path_id,
    parent_path,
    scoped_glob_filter,
    subtree_filter,
)


def test_normalize_root_and_repeated_slashes() -> None:
    assert normalize_path("/") == "/"
    assert normalize_path("//a///b/") == "/a/b"


@pytest.mark.parametrize("value", [None, 3.14, 1, object()])
def test_normalize_rejects_non_string_paths(value: object) -> None:
    with pytest.raises(TypeError):
        normalize_path(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", ["", "a/b", "./a", "/a/./b", "/a/../b", "/a\x00b"])
def test_normalize_rejects_unsafe_paths(value: str) -> None:
    with pytest.raises((ValueError, TypeError)):
        normalize_path(value)


def test_normalize_glob_path_preserves_glob_tokens() -> None:
    assert normalize_glob_path("/a/**/b/*.txt") == "/a/**/b/*.txt"


def test_basename_extension_and_ancestors() -> None:
    assert basename("/a/b/c.txt") == "c.txt"
    assert extension("/a/b/c.txt") == ".txt"
    assert ancestor_paths("/a/b/c.txt", include_self=False) == ["/", "/a", "/a/b"]
    assert ancestor_paths("/a/b/c.txt", include_self=True) == ["/", "/a", "/a/b", "/a/b/c.txt"]


def test_join_helpers() -> None:
    assert join_path("/a", "b/c.txt") == "/a/b/c.txt"
    assert join_glob("/a", "**/*.txt") == "/a/**/*.txt"
    assert join_path("/a/b", "") == "/a/b"
    assert join_path("/a/b", "/c/d") == "/c/d"


def test_parent_path_edge_cases() -> None:
    assert parent_path("/") is None
    assert parent_path("/a") == "/"
    assert parent_path("/a/b") == "/a"


def test_subtree_filter_and_scoped_glob_filter() -> None:
    assert subtree_filter("/") is None
    assert subtree_filter("/a") == ("Or", (("path", "Eq", "/a"), ("path", "Glob", "/a/**")))
    assert scoped_glob_filter("/a", "*.txt") == ("basename", "Glob", "*.txt")
    assert scoped_glob_filter("/a", "docs/*.txt", ignore_case=True) == ("path", "IGlob", "/a/docs/*.txt")


def test_path_id_is_stable() -> None:
    assert path_id("/a/b") == path_id("/a//b/")


path_segments = st.lists(
    st.text(
        alphabet=st.characters(
            blacklist_characters="/\x00",
            blacklist_categories=("Cc", "Cs"),
        ),
        min_size=1,
        max_size=12,
    ).filter(lambda value: value not in {".", ".."} and not any(token in value for token in "*?[]")),
    min_size=1,
    max_size=6,
)


@given(path_segments)
def test_normalize_path_is_idempotent(segments: list[str]) -> None:
    raw = "///" + "///".join(segments) + "///"
    normalized = normalize_path(raw)
    assert normalize_path(normalized) == normalized


@given(path_segments)
def test_join_path_matches_posix_for_relative_segments(segments: list[str]) -> None:
    root = "/base"
    tail = "/".join(segments)
    expected = normalize_path(posix_join(root, tail))
    assert join_path(root, tail) == expected


@given(path_segments)
def test_parent_path_round_trips_with_basename(segments: list[str]) -> None:
    path = normalize_path("/" + "/".join(segments))
    if path == "/":
        assert parent_path(path) is None
        return
    parent = parent_path(path)
    assert parent is not None
    assert join_path(parent, basename(path)) == path
