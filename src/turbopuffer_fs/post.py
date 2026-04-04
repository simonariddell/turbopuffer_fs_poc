"""Pure post-processing for filesystem-shaped turbopuffer results."""

from __future__ import annotations

from base64 import b64decode
from itertools import chain

from .schema import content_row, metadata_row


def _rows(results: dict[str, dict[str, object]], name: str) -> list[dict[str, object]]:
    return list(results.get(name, {}).get("rows", []))


def _row(results: dict[str, dict[str, object]], name: str) -> dict[str, object] | None:
    rows = _rows(results, name)
    return rows[0] if rows else None


def _require_target(results: dict[str, dict[str, object]], *, path: str) -> dict[str, object]:
    row = _row(results, "target")
    if row is None:
        raise FileNotFoundError(path)
    return row


def _require_directory(row: dict[str, object], *, path: str) -> dict[str, object]:
    if row.get("kind") != "dir":
        raise NotADirectoryError(path)
    return row


def _require_file(row: dict[str, object], *, path: str) -> dict[str, object]:
    if row.get("kind") == "dir":
        raise IsADirectoryError(path)
    return row


def _require_text(row: dict[str, object], *, path: str) -> str:
    _require_file(row, path=path)
    if int(row.get("is_text", 0)) != 1:
        raise ValueError(f"path is a binary file: {path}")
    return str(row.get("text", ""))


def content_text(row: dict[str, object] | None) -> str | None:
    if row is None:
        return None
    if row.get("kind") == "dir":
        raise IsADirectoryError(str(row.get("path", "")))
    if int(row.get("is_text", 0)) != 1:
        raise ValueError(f"path is a binary file: {row.get('path')}")
    return str(row.get("text", ""))


def content_bytes(row: dict[str, object] | None) -> bytes | None:
    if row is None:
        return None
    if row.get("kind") == "dir":
        raise IsADirectoryError(str(row.get("path", "")))
    if int(row.get("is_text", 0)) == 1:
        return str(row.get("text", "")).encode("utf-8")
    blob = row.get("blob_b64")
    if blob in {None, ""}:
        return b""
    return b64decode(str(blob))


def _grep_matches(row: dict[str, object], pattern: str, *, ignore_case: bool) -> list[dict[str, object]]:
    text = str(row.get("text", ""))
    lines = text.splitlines()
    needle = pattern.casefold() if ignore_case else pattern
    matches: list[dict[str, object]] = []
    for index, line in enumerate(lines, start=1):
        haystack = line.casefold() if ignore_case else line
        if needle in haystack:
            matches.append({"path": row["path"], "line_number": index, "line": line})
    return matches


def finalize_stat(context: dict[str, object], results: dict[str, dict[str, object]]) -> dict[str, object] | None:
    row = _row(results, "target")
    return None if row is None else metadata_row(row)


def finalize_ls(context: dict[str, object], results: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    path = str(context["path"])
    target = _require_directory(_require_target(results, path=path), path=path)
    del target
    return [metadata_row(row) for row in _rows(results, "children")]


def finalize_find(context: dict[str, object], results: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    root = str(context["root"])
    target = _require_target(results, path=root)
    matches = _rows(results, "matches")
    if target.get("kind") == "file":
        return [metadata_row(row) for row in matches if row.get("path") == root]
    return [metadata_row(row) for row in matches]


def finalize_cat(context: dict[str, object], results: dict[str, dict[str, object]]) -> str:
    path = str(context["path"])
    return _require_text(_require_target(results, path=path), path=path)


def finalize_read_text(context: dict[str, object], results: dict[str, dict[str, object]]) -> str:
    path = str(context["path"])
    return _require_text(_require_target(results, path=path), path=path)


def finalize_read_bytes(context: dict[str, object], results: dict[str, dict[str, object]]) -> bytes:
    path = str(context["path"])
    row = _require_file(_require_target(results, path=path), path=path)
    return content_bytes(row) or b""


def finalize_head(context: dict[str, object], results: dict[str, dict[str, object]]) -> list[str]:
    text = finalize_read_text(context, results)
    return text.splitlines()[: int(context["n"])]


def finalize_tail(context: dict[str, object], results: dict[str, dict[str, object]]) -> list[str]:
    text = finalize_read_text(context, results)
    count = int(context["n"])
    return text.splitlines()[-count:] if count else []


def finalize_grep(context: dict[str, object], results: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    root = str(context["root"])
    _require_target(results, path=root)
    pattern = str(context["pattern"])
    ignore_case = bool(context.get("ignore_case", False))
    return list(chain.from_iterable(_grep_matches(row, pattern, ignore_case=ignore_case) for row in _rows(results, "candidates")))


def finalize_write_summary(context: dict[str, object], results: dict[str, dict[str, object]]) -> dict[str, object]:
    write = dict(results["write"])
    write.pop("name", None)
    return write


def finalize_write_target_meta(context: dict[str, object], results: dict[str, dict[str, object]]) -> dict[str, object]:
    write = finalize_write_summary(context, results)
    row = dict(context["target_row"])
    return {
        "path": context["path"],
        "row": content_row(row),
        "write": write,
    }


def finalize_rm(context: dict[str, object], results: dict[str, dict[str, object]]) -> dict[str, object]:
    path = str(context["path"])
    target = _row(results, "target")
    if target is None:
        return {
            "path": path,
            "recursive": bool(context["recursive"]),
            "deleted": False,
            "ids": [],
        }
    write = dict(results["write"])
    deleted_ids = list(write.get("deleted_ids", []))
    if not deleted_ids and target.get("id") is not None and not bool(context["recursive"]):
        deleted_ids = [target["id"]]
    return {
        "path": path,
        "recursive": bool(context["recursive"]),
        "deleted": True,
        "ids": deleted_ids,
        "write": {key: value for key, value in write.items() if key != "name"},
    }


def finalize_mounts(context: dict[str, object], results: dict[str, dict[str, object]]) -> list[str]:
    suffix = str(context["suffix"])
    names = [row["id"] for row in results["namespaces"]["namespaces"]]
    mounts = [name[: -len(suffix)] for name in names if name.endswith(suffix)]
    return sorted(mounts)


FINALIZERS = {
    "stat": finalize_stat,
    "ls": finalize_ls,
    "find": finalize_find,
    "cat": finalize_cat,
    "read_text": finalize_read_text,
    "read_bytes": finalize_read_bytes,
    "head": finalize_head,
    "tail": finalize_tail,
    "grep": finalize_grep,
    "write_summary": finalize_write_summary,
    "write_target_meta": finalize_write_target_meta,
    "rm": finalize_rm,
    "mounts": finalize_mounts,
}
