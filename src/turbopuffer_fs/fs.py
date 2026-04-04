"""Pure filesystem intent -> explicit turbopuffer plan dictionaries."""

from __future__ import annotations

from .paths import (
    and_filter,
    ancestor_paths,
    direct_children_filter,
    normalize_path,
    path_id,
    paths_filter,
    scoped_glob_filter,
    subtree_filter,
    text_substring_filter,
)
from .schema import CONTENT_FIELDS, META_FIELDS, bytes_row, directory_row, parent_directory_rows, target_directory_rows, text_row, upsert_rows_payload


DEFAULT_PAGE_SIZE = 256


def _limit_value(limit: int | None) -> int | None:
    if limit is None:
        return None
    value = int(limit)
    if value < 1:
        raise ValueError("limit must be a positive integer")
    return value


def _line_count(n: int) -> int:
    value = int(n)
    if value < 0:
        raise ValueError("n must be non-negative")
    return value


def _query_step(
    name: str,
    payload: dict[str, object],
    *,
    paginate: bool = False,
    limit: int | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    order_field: str = "path",
) -> dict[str, object]:
    step = {
        "kind": "query",
        "name": name,
        "payload": dict(payload),
    }
    if paginate:
        step["paginate"] = True
        step["limit"] = _limit_value(limit)
        step["page_size"] = min(page_size, _limit_value(limit) or page_size)
        step["order_field"] = order_field
    return step


def _write_step(name: str, payload: dict[str, object]) -> dict[str, object]:
    return {
        "kind": "write",
        "name": name,
        "payload": dict(payload),
    }


def _assert_step(name: str, check: str) -> dict[str, object]:
    return {
        "kind": "assert",
        "name": name,
        "check": check,
    }


def _plan(namespace: str, steps: list[dict[str, object]], finalize: str, **context: object) -> dict[str, object]:
    return {
        "namespace": namespace,
        "steps": steps,
        "finalize": finalize,
        "context": context,
    }


def _lookup_payload(path: str, fields: list[str]) -> dict[str, object]:
    return {
        "filters": ("path", "Eq", normalize_path(path)),
        "rank_by": ("path", "asc"),
        "limit": 1,
        "include_attributes": fields,
    }


def _ordered_payload(filters: tuple | None, fields: list[str]) -> dict[str, object]:
    return {
        "filters": filters,
        "rank_by": ("path", "asc"),
        "include_attributes": fields,
    }


def stat_plan(namespace: str, path: str) -> dict[str, object]:
    value = normalize_path(path)
    return _plan(
        namespace,
        [_query_step("target", _lookup_payload(value, META_FIELDS))],
        "stat",
        path=value,
    )


def ls_plan(namespace: str, path: str = "/", limit: int | None = None) -> dict[str, object]:
    value = normalize_path(path)
    return _plan(
        namespace,
        [
            _query_step("target", _lookup_payload(value, META_FIELDS)),
            _query_step(
                "children",
                _ordered_payload(direct_children_filter(value), META_FIELDS),
                paginate=True,
                limit=limit,
            ),
        ],
        "ls",
        path=value,
        limit=_limit_value(limit),
    )


def find_plan(
    namespace: str,
    root: str = "/",
    *,
    glob: str | None = None,
    kind: str | None = None,
    ignore_case: bool = False,
    limit: int | None = None,
) -> dict[str, object]:
    value = normalize_path(root)
    filters = and_filter(
        subtree_filter(value),
        None if kind is None else ("kind", "Eq", kind),
        scoped_glob_filter(value, glob, ignore_case=ignore_case),
    )
    return _plan(
        namespace,
        [
            _query_step("target", _lookup_payload(value, META_FIELDS)),
            _query_step(
                "matches",
                _ordered_payload(filters, META_FIELDS),
                paginate=True,
                limit=limit,
            ),
        ],
        "find",
        root=value,
        glob=glob,
        kind=kind,
        ignore_case=ignore_case,
        limit=_limit_value(limit),
    )


def cat_plan(namespace: str, path: str) -> dict[str, object]:
    value = normalize_path(path)
    return _plan(
        namespace,
        [_query_step("target", _lookup_payload(value, CONTENT_FIELDS))],
        "cat",
        path=value,
    )


def read_text_plan(namespace: str, path: str) -> dict[str, object]:
    value = normalize_path(path)
    return _plan(
        namespace,
        [_query_step("target", _lookup_payload(value, CONTENT_FIELDS))],
        "read_text",
        path=value,
    )


def read_bytes_plan(namespace: str, path: str) -> dict[str, object]:
    value = normalize_path(path)
    return _plan(
        namespace,
        [_query_step("target", _lookup_payload(value, CONTENT_FIELDS))],
        "read_bytes",
        path=value,
    )


def head_plan(namespace: str, path: str, n: int = 10) -> dict[str, object]:
    value = normalize_path(path)
    count = _line_count(n)
    return _plan(
        namespace,
        [_query_step("target", _lookup_payload(value, CONTENT_FIELDS))],
        "head",
        path=value,
        n=count,
    )


def tail_plan(namespace: str, path: str, n: int = 10) -> dict[str, object]:
    value = normalize_path(path)
    count = _line_count(n)
    return _plan(
        namespace,
        [_query_step("target", _lookup_payload(value, CONTENT_FIELDS))],
        "tail",
        path=value,
        n=count,
    )


def grep_plan(
    namespace: str,
    root: str,
    pattern: str,
    *,
    ignore_case: bool = False,
    glob: str | None = None,
    limit: int | None = None,
) -> dict[str, object]:
    if pattern == "":
        raise ValueError("pattern must not be empty")
    value = normalize_path(root)
    filters = and_filter(
        ("kind", "Eq", "file"),
        ("is_text", "Eq", 1),
        subtree_filter(value),
        scoped_glob_filter(value, glob, ignore_case=ignore_case),
        text_substring_filter(pattern, ignore_case=ignore_case),
    )
    return _plan(
        namespace,
        [
            _query_step("target", _lookup_payload(value, META_FIELDS)),
            _query_step(
                "candidates",
                _ordered_payload(filters, ["path", "text"]),
                paginate=True,
                limit=limit,
            ),
        ],
        "grep",
        root=value,
        pattern=pattern,
        ignore_case=ignore_case,
        glob=glob,
        limit=_limit_value(limit),
    )


def mkdir_plan(namespace: str, path: str) -> dict[str, object]:
    value = normalize_path(path)
    directory_paths = ancestor_paths(value, include_self=True)
    return _plan(
        namespace,
        [
            _query_step("existing", _ordered_payload(paths_filter(directory_paths), META_FIELDS) | {"limit": len(directory_paths)}),
            _assert_step("validate", "mkdir_preconditions"),
            _write_step("write", upsert_rows_payload(target_directory_rows(value))),
        ],
        "write_target_meta",
        path=value,
        parent_paths=ancestor_paths(value, include_self=False),
        target_row=directory_row(value),
    )


def put_text_plan(namespace: str, path: str, text: str, mime: str | None = None) -> dict[str, object]:
    value = normalize_path(path)
    if value == "/":
        raise IsADirectoryError(value)
    check_paths = ancestor_paths(value, include_self=False) + [value]
    target = text_row(value, text, mime=mime)
    return _plan(
        namespace,
        [
            _query_step("existing", _ordered_payload(paths_filter(check_paths), META_FIELDS) | {"limit": len(check_paths)}),
            _assert_step("validate", "put_preconditions"),
            _write_step("write", upsert_rows_payload([*parent_directory_rows(value), target])),
        ],
        "write_target_meta",
        path=value,
        parent_paths=ancestor_paths(value, include_self=False),
        target_row=target,
    )


def put_bytes_plan(namespace: str, path: str, data: bytes, mime: str | None = None) -> dict[str, object]:
    value = normalize_path(path)
    if value == "/":
        raise IsADirectoryError(value)
    check_paths = ancestor_paths(value, include_self=False) + [value]
    target = bytes_row(value, data, mime=mime)
    return _plan(
        namespace,
        [
            _query_step("existing", _ordered_payload(paths_filter(check_paths), META_FIELDS) | {"limit": len(check_paths)}),
            _assert_step("validate", "put_preconditions"),
            _write_step("write", upsert_rows_payload([*parent_directory_rows(value), target])),
        ],
        "write_target_meta",
        path=value,
        parent_paths=ancestor_paths(value, include_self=False),
        target_row=target,
    )


def rm_plan(namespace: str, path: str, recursive: bool = False) -> dict[str, object]:
    value = normalize_path(path)
    if value == "/":
        raise ValueError("rm('/') is not supported")
    steps = [
        _query_step("target", _lookup_payload(value, META_FIELDS)),
    ]
    if recursive:
        steps.append(
            _query_step(
                "delete_targets",
                _ordered_payload(subtree_filter(value), ["id", "path", "kind"]),
                paginate=True,
            )
        )
    else:
        steps.append(
            _query_step(
                "child_probe",
                {
                    "filters": direct_children_filter(value),
                    "rank_by": ("path", "asc"),
                    "limit": 1,
                    "include_attributes": ["path"],
                },
            )
        )
    steps.extend(
        [
            _assert_step("validate", "rm_preconditions"),
            _write_step(
                "write",
                {
                    "delete_rows_from": "delete_targets",
                    "delete_batch_size": DEFAULT_PAGE_SIZE,
                    "return_affected_ids": True,
                }
                if recursive
                else {
                    "delete_rows_from": "target",
                    "delete_batch_size": 1,
                    "return_affected_ids": True,
                },
            ),
        ]
    )
    return _plan(
        namespace,
        steps,
        "rm",
        path=value,
        recursive=bool(recursive),
    )


def upsert_rows_plan(namespace: str, rows: list[dict[str, object]]) -> dict[str, object]:
    return _plan(
        namespace,
        [_write_step("write", upsert_rows_payload(rows))],
        "write_summary",
        rows=list(rows),
    )
