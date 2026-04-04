"""Pure post-processing over normalized turbopuffer rows."""

from __future__ import annotations

from base64 import b64decode
from functools import partial
from itertools import chain


first = lambda rows: rows[0] if rows else None
rows_of = lambda query_results, index=0: query_results[index]["rows"]
row_of = lambda query_results, index=0: first(rows_of(query_results, index))
lines_of = lambda row: [] if not row or row.get("text") is None else row.get("text", "").splitlines()
casefold = lambda text, enabled: text.casefold() if enabled else text


def content_bytes(row: dict[str, object] | None) -> bytes | None:
    if row is None:
        return None
    if row.get("is_text"):
        return str(row.get("text", "")).encode("utf-8")
    blob = row.get("blob_b64")
    return None if blob in {None, ""} else b64decode(str(blob))


def content_text(row: dict[str, object] | None) -> str | None:
    return None if row is None else row.get("text")


def finalize_row(context: dict[str, object], query_results: list[dict[str, object]]) -> dict[str, object] | None:
    return row_of(query_results)


def finalize_rows(context: dict[str, object], query_results: list[dict[str, object]]) -> list[dict[str, object]]:
    return rows_of(query_results)


def finalize_head(context: dict[str, object], query_results: list[dict[str, object]]) -> list[str]:
    return lines_of(row_of(query_results))[: int(context.get("n", 10))]


def finalize_tail(context: dict[str, object], query_results: list[dict[str, object]]) -> list[str]:
    count = int(context.get("n", 10))
    return lines_of(row_of(query_results))[-count:]


def grep_matches(row: dict[str, object], pattern: str, *, ignore_case: bool) -> list[dict[str, object]]:
    needle = casefold(pattern, ignore_case)
    matcher = partial(casefold, enabled=ignore_case)
    return [
        {"path": row["path"], "line_number": index, "line": line}
        for index, line in enumerate(lines_of(row), start=1)
        if needle in matcher(line)
    ]


def finalize_grep(context: dict[str, object], query_results: list[dict[str, object]]) -> list[dict[str, object]]:
    pattern = str(context.get("pattern", ""))
    ignore_case = bool(context.get("ignore_case", False))
    rows = rows_of(query_results)
    return list(chain.from_iterable(map(lambda row: grep_matches(row, pattern, ignore_case=ignore_case), rows)))


FINALIZERS = {
    "row": finalize_row,
    "rows": finalize_rows,
    "stat": finalize_row,
    "head": finalize_head,
    "tail": finalize_tail,
    "grep": finalize_grep,
}
