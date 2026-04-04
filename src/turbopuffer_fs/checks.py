"""Pure precondition checks for mutation plans."""

from __future__ import annotations

import errno


def rows_for(results: dict[str, dict[str, object]], name: str) -> list[dict[str, object]]:
    return list(results.get(name, {}).get("rows", []))


def first_row(results: dict[str, dict[str, object]], name: str) -> dict[str, object] | None:
    rows = rows_for(results, name)
    return rows[0] if rows else None


def _rows_by_path(rows: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    return {str(row["path"]): row for row in rows}


def check_mkdir_preconditions(context: dict[str, object], results: dict[str, dict[str, object]]) -> None:
    path = str(context["path"])
    existing = _rows_by_path(rows_for(results, "existing"))
    parent_paths = list(context["parent_paths"])
    for ancestor in parent_paths:
        row = existing.get(ancestor)
        if row is not None and row.get("kind") != "dir":
            raise NotADirectoryError(ancestor)
    target = existing.get(path)
    if target is not None and target.get("kind") != "dir":
        raise FileExistsError(path)


def check_put_preconditions(context: dict[str, object], results: dict[str, dict[str, object]]) -> None:
    path = str(context["path"])
    existing = _rows_by_path(rows_for(results, "existing"))
    parent_paths = list(context["parent_paths"])
    for ancestor in parent_paths:
        row = existing.get(ancestor)
        if row is not None and row.get("kind") != "dir":
            raise NotADirectoryError(ancestor)
    target = existing.get(path)
    if target is not None and target.get("kind") == "dir":
        raise IsADirectoryError(path)


def check_rm_preconditions(context: dict[str, object], results: dict[str, dict[str, object]]) -> None:
    path = str(context["path"])
    recursive = bool(context.get("recursive", False))
    target = first_row(results, "target")
    if target is None:
        return
    if recursive:
        return
    if target.get("kind") == "dir" and rows_for(results, "child_probe"):
        raise OSError(errno.ENOTEMPTY, "Directory not empty", path)


CHECKS = {
    "mkdir_preconditions": check_mkdir_preconditions,
    "put_preconditions": check_put_preconditions,
    "rm_preconditions": check_rm_preconditions,
}


def run_check(name: str, context: dict[str, object], results: dict[str, dict[str, object]]) -> None:
    CHECKS[name](context, results)
