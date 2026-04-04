"""Pure path helpers for normalized absolute filesystem paths."""

from __future__ import annotations

import hashlib
from glob import escape as glob_escape
from typing import Iterable


PATH_ID_PREFIX = "path:"


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

    if not isinstance(path, str):
        raise TypeError("path must be a string")
    if path == "":
        raise ValueError("path must not be empty")
    if "\x00" in path:
        raise ValueError("path must not contain NUL bytes")
    if not path.startswith("/"):
        raise ValueError(f"path must be absolute: {path!r}")
    if path == "/":
        return "/"

    segments = [segment for segment in path.split("/") if segment]
    if any(segment in {".", ".."} for segment in segments):
        raise ValueError(f"path must not contain '.' or '..' segments: {path!r}")
    return "/" + "/".join(segments)


def normalize_glob_path(pattern: str) -> str:
    if not isinstance(pattern, str):
        raise TypeError("glob pattern must be a string")
    if pattern == "":
        raise ValueError("glob pattern must not be empty")
    if "\x00" in pattern:
        raise ValueError("glob pattern must not contain NUL bytes")
    if not pattern.startswith("/"):
        raise ValueError(f"glob pattern must be absolute: {pattern!r}")
    if pattern == "/":
        return "/"

    segments = [segment for segment in pattern.split("/") if segment]
    if any(segment in {".", ".."} for segment in segments):
        raise ValueError(f"glob pattern must not contain '.' or '..' segments: {pattern!r}")
    return "/" + "/".join(segments)


def join_path(root: str, tail: str) -> str:
    root_value = normalize_path(root)
    if not isinstance(tail, str):
        raise TypeError("tail must be a string")
    if tail == "":
        return root_value
    if tail.startswith("/"):
        return normalize_path(tail)
    joined = f"{root_value.rstrip('/')}/{tail}"
    return normalize_path(joined)


def join_glob(root: str, pattern: str) -> str:
    root_value = normalize_path(root)
    if pattern.startswith("/"):
        return normalize_glob_path(pattern)
    joined = f"{root_value.rstrip('/')}/{pattern}"
    return normalize_glob_path(joined)


def parent_path(path: str) -> str | None:
    value = normalize_path(path)
    if value == "/":
        return None
    parts = value.rstrip("/").split("/")
    return "/" if len(parts) == 2 else "/".join(parts[:-1])


def basename(path: str) -> str:
    value = normalize_path(path)
    return "/" if value == "/" else value.rsplit("/", 1)[-1]


def extension(path: str) -> str:
    name = basename(path)
    if name in {"/", "", ".", ".."}:
        return ""
    if "." not in name.lstrip("."):
        return ""
    return "." + name.rsplit(".", 1)[-1]


def ancestor_paths(path: str, *, include_self: bool = False) -> list[str]:
    value = normalize_path(path)
    if value == "/":
        return ["/"] if include_self else []

    pieces = value.strip("/").split("/")
    limit = len(pieces) if include_self else len(pieces) - 1
    if limit < 1:
        return ["/"]

    rows = ["/"]
    for index in range(1, limit + 1):
        rows.append("/" + "/".join(pieces[:index]))
    return rows


def path_id(path: str) -> str:
    value = normalize_path(path)
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"{PATH_ID_PREFIX}{digest}"


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
