"""Pure path helpers for normalized absolute filesystem paths."""

from __future__ import annotations

import hashlib
from glob import escape as glob_escape
from pathlib import PurePosixPath
from posixpath import join as posix_join
from posixpath import normpath
from typing import Iterable


PATH_ID_PREFIX = "path:"


def _require_text(value: object, *, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if value == "":
        raise ValueError(f"{label} must not be empty")
    if "\x00" in value:
        raise ValueError(f"{label} must not contain NUL bytes")
    return value


def _normalize_candidate(raw: str, *, label: str, allow_glob: bool) -> str:
    if not raw.startswith("/"):
        raise ValueError(f"{label} must be absolute: {raw!r}")
    if raw == "/":
        return "/"

    raw_segments = [segment for segment in raw.split("/") if segment]
    if any(segment in {".", ".."} for segment in raw_segments):
        raise ValueError(f"{label} must not contain '.' or '..' segments: {raw!r}")
    normalized = normpath("/" + raw.lstrip("/"))
    normalized_segments = [segment for segment in normalized.split("/") if segment]
    if any(segment == ".." for segment in normalized_segments):
        raise ValueError(f"{label} must stay within root: {raw!r}")
    if allow_glob:
        return "/" + "/".join(raw_segments)
    return normalized


def normalize_path(path: str) -> str:
    """Normalize an absolute POSIX-like path.

    Rules:
    - path must be a non-empty string
    - path must be absolute
    - repeated slashes are collapsed
    - trailing slashes are removed except for root
    - ``.`` and ``..`` path segments are rejected
    - NUL bytes are rejected
    """

    return _normalize_candidate(_require_text(path, label="path"), label="path", allow_glob=False)


def normalize_glob_path(pattern: str) -> str:
    return _normalize_candidate(_require_text(pattern, label="glob pattern"), label="glob pattern", allow_glob=True)


def join_path(root: str, tail: str) -> str:
    root_value = normalize_path(root)
    tail_value = _require_text(tail, label="tail") if tail != "" else ""
    if tail_value == "":
        return root_value
    if tail_value.startswith("/"):
        return normalize_path(tail_value)
    return normalize_path(posix_join(root_value, tail_value))


def join_glob(root: str, pattern: str) -> str:
    root_value = normalize_path(root)
    pattern_value = _require_text(pattern, label="glob pattern")
    if pattern_value.startswith("/"):
        return normalize_glob_path(pattern_value)
    return normalize_glob_path(posix_join(root_value, pattern_value))


def parent_path(path: str) -> str | None:
    value = normalize_path(path)
    if value == "/":
        return None
    parent = str(PurePosixPath(value).parent)
    return "/" if parent == "." else parent


def basename(path: str) -> str:
    value = normalize_path(path)
    return "/" if value == "/" else PurePosixPath(value).name


def extension(path: str) -> str:
    name = basename(path)
    if name in {"/", "", ".", ".."}:
        return ""
    return PurePosixPath(name).suffix


def ancestor_paths(path: str, *, include_self: bool = False) -> list[str]:
    value = normalize_path(path)
    if value == "/":
        return ["/"] if include_self else []

    path_obj = PurePosixPath(value)
    ancestors = [str(parent) for parent in reversed(path_obj.parents)]
    rows = ["/", *[parent for parent in ancestors if parent != "/"]]
    return rows + ([value] if include_self else [])


def path_id(path: str) -> str:
    value = normalize_path(path)
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return digest


def and_filter(*parts: tuple | None) -> tuple | None:
    kept = tuple(part for part in parts if part is not None)
    if not kept:
        return None
    if len(kept) == 1:
        return kept[0]
    return ("And", kept)


def or_filter(*parts: tuple | None) -> tuple | None:
    kept = tuple(part for part in parts if part is not None)
    if not kept:
        return None
    if len(kept) == 1:
        return kept[0]
    return ("Or", kept)


def paths_filter(paths: Iterable[str]) -> tuple | None:
    values = tuple(dict.fromkeys(normalize_path(path) for path in paths))
    return or_filter(*tuple(("path", "Eq", value) for value in values))


def direct_children_filter(path: str) -> tuple[str, str, str]:
    return ("parent", "Eq", normalize_path(path))


def subtree_filter(root: str) -> tuple | None:
    value = normalize_path(root)
    if value == "/":
        return None
    return or_filter(
        ("path", "Eq", value),
        ("path", "Glob", f"{value.rstrip('/')}/**"),
    )


def scoped_glob_filter(root: str, pattern: str | None, *, ignore_case: bool = False) -> tuple | None:
    if not pattern:
        return None
    operator = "IGlob" if ignore_case else "Glob"
    if "/" in pattern:
        return ("path", operator, join_glob(root, pattern))
    return ("basename", operator, pattern)


def text_substring_filter(pattern: str, *, ignore_case: bool = False) -> tuple | None:
    if pattern == "":
        return None
    operator = "IGlob" if ignore_case else "Glob"
    return ("text", operator, f"*{glob_escape(pattern)}*")


def with_after_filter(filters: tuple | None, field: str, last_value: str | None) -> tuple | None:
    if last_value is None:
        return filters
    return and_filter(filters, (field, "Gt", last_value))
