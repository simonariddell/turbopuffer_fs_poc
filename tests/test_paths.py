from __future__ import annotations

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
    scoped_glob_filter,
    subtree_filter,
)


def test_normalize_root_and_repeated_slashes() -> None:
    assert normalize_path("/") == "/"
    assert normalize_path("//a///b/") == "/a/b"


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


def test_subtree_filter_and_scoped_glob_filter() -> None:
    assert subtree_filter("/") is None
    assert subtree_filter("/a") == ("Or", (("path", "Eq", "/a"), ("path", "Glob", "/a/**")))
    assert scoped_glob_filter("/a", "*.txt") == ("basename", "Glob", "*.txt")
    assert scoped_glob_filter("/a", "docs/*.txt", ignore_case=True) == ("path", "IGlob", "/a/docs/*.txt")


def test_path_id_is_stable() -> None:
    assert path_id("/a/b") == path_id("/a//b/")
