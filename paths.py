"""Small path helpers."""

from __future__ import annotations

import hashlib
from functools import partial
from pathlib import Path, PurePosixPath
from posixpath import join as posix_join
from posixpath import normpath


def normalize(path: str | PurePosixPath) -> str:
    raw = str(path or "/")
    rooted = raw if raw.startswith("/") else f"/{raw}"
    normalized = normpath(rooted)
    return normalized if normalized.startswith("/") else f"/{normalized}"


def parent(path: str | PurePosixPath) -> str | None:
    value = normalize(path)
    return None if value == "/" else normalize(str(PurePosixPath(value).parent))


def basename(path: str | PurePosixPath) -> str:
    value = normalize(path)
    return "/" if value == "/" else PurePosixPath(value).name


def extension(path: str | PurePosixPath) -> str:
    name = basename(path)
    return "" if name in {"/", ""} else Path(name).suffix.lstrip(".")


def path_id(path: str | PurePosixPath) -> str:
    value = normalize(path)
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()
    return f"p:{digest}"


def subtree_filter(root: str) -> tuple | None:
    value = normalize(root)
    if value == "/":
        return None
    return (
        "Or",
        (
            ("path", "Eq", value),
            ("path", "Glob", f"{value.rstrip('/')}/**"),
        ),
    )


def scoped_glob_filter(root: str, pattern: str | None, *, ignore_case: bool = False) -> tuple | None:
    if not pattern:
        return None
    op = "IGlob" if ignore_case else "Glob"
    if "/" in pattern:
        scope = normalize(root)
        absolute = pattern if pattern.startswith("/") else posix_join(scope.rstrip("/") or "/", pattern)
        return ("path", op, absolute)
    return ("basename", op, pattern)


def and_filter(*parts: tuple | None) -> tuple | None:
    kept = tuple(part for part in parts if part is not None)
    if not kept:
        return None
    if len(kept) == 1:
        return kept[0]
    return ("And", kept)


normalize_all = partial(map, normalize)
