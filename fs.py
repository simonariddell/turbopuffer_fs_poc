"""Pure filesystem intent -> turbopuffer query plans."""

from __future__ import annotations

from functools import partial
from glob import escape as glob_escape

from .paths import and_filter, normalize, scoped_glob_filter, subtree_filter

FILE_FIELDS = ["path", "parent", "basename", "kind", "ext", "mime", "size_bytes", "is_text", "text", "blob_b64"]
META_FIELDS = ["path", "parent", "basename", "kind", "ext", "mime", "size_bytes", "is_text"]


step = lambda name, payload: {"name": name, "payload": payload}
plan = lambda namespace, queries, finalize, **context: {
    "namespace": namespace,
    "queries": list(queries),
    "finalize": finalize,
    "context": context,
}


lookup_payload = lambda path, attrs: {
    "filters": ("path", "Eq", normalize(path)),
    "rank_by": ("path", "asc"),
    "limit": {"total": 1},
    "include_attributes": attrs,
}


ordered_lookup = lambda filters, attrs, limit: {
    "filters": filters,
    "rank_by": ("path", "asc"),
    "limit": {"total": limit},
    "include_attributes": attrs,
}


text_glob_filter = lambda pattern, ignore_case: None if pattern == "" else (
    "text",
    "IGlob" if ignore_case else "Glob",
    f"*{glob_escape(pattern)}*",
)


def stat_plan(namespace: str, path: str) -> dict[str, object]:
    value = normalize(path)
    return plan(namespace, [step("stat", lookup_payload(value, META_FIELDS))], "stat", path=value)


def ls_plan(namespace: str, path: str = "/", *, limit: int = 1000) -> dict[str, object]:
    value = normalize(path)
    payload = ordered_lookup(("parent", "Eq", value), META_FIELDS, limit)
    return plan(namespace, [step("ls", payload)], "rows", path=value)


def find_plan(
    namespace: str,
    root: str = "/",
    *,
    glob: str | None = None,
    kind: str | None = None,
    ignore_case: bool = False,
    limit: int = 1000,
) -> dict[str, object]:
    value = normalize(root)
    filters = and_filter(
        subtree_filter(value),
        None if kind is None else ("kind", "Eq", kind),
        scoped_glob_filter(value, glob, ignore_case=ignore_case),
    )
    payload = ordered_lookup(filters, META_FIELDS, limit)
    return plan(namespace, [step("find", payload)], "rows", root=value, glob=glob, kind=kind)


def cat_plan(namespace: str, path: str) -> dict[str, object]:
    value = normalize(path)
    return plan(namespace, [step("cat", lookup_payload(value, FILE_FIELDS))], "row", path=value)


def head_plan(namespace: str, path: str, *, n: int = 10) -> dict[str, object]:
    value = normalize(path)
    return plan(namespace, [step("head", lookup_payload(value, FILE_FIELDS))], "head", path=value, n=n)


def tail_plan(namespace: str, path: str, *, n: int = 10) -> dict[str, object]:
    value = normalize(path)
    return plan(namespace, [step("tail", lookup_payload(value, FILE_FIELDS))], "tail", path=value, n=n)


def grep_plan(
    namespace: str,
    root: str,
    pattern: str,
    *,
    ignore_case: bool = False,
    glob: str | None = None,
    limit: int = 1000,
) -> dict[str, object]:
    value = normalize(root)
    filters = and_filter(
        ("kind", "Eq", "file"),
        ("is_text", "Eq", 1),
        subtree_filter(value),
        scoped_glob_filter(value, glob, ignore_case=ignore_case),
        text_glob_filter(pattern, ignore_case),
    )
    payload = ordered_lookup(filters, ["path", "text"], limit)
    return plan(
        namespace,
        [step("grep", payload)],
        "grep",
        root=value,
        pattern=pattern,
        ignore_case=ignore_case,
        glob=glob,
    )
