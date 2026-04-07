"""
tpfs.py — A turbopuffer-backed filesystem.

Single-file proof-of-concept demonstrating how to build a complete,
durable filesystem abstraction over turbopuffer. Every file and directory
is a document in a turbopuffer namespace. Session state (cwd) is persisted
as a document. An agent with zero local disk can boot, work, die, and
reboot on another machine — recovering all state from turbopuffer alone.

Data model:
  - Mount "demo" → namespace "demo__fs"
  - Document ID  = SHA-256(normalized_absolute_path)
  - Directories  = explicit documents with kind="dir"
  - Text files   = documents with kind="file", text in `text` field
  - Binary files  = documents with kind="file", bytes in `blob_b64` field

Usage:
  pip install turbopuffer click
  export TURBOPUFFER_API_KEY=...
  python tpfs.py init
  python tpfs.py put /project/hello.py --text 'print("hello")'
  python tpfs.py cat /project/hello.py
  python tpfs.py grep "hello" /
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import posixpath
import re
import sys
from datetime import datetime, timezone
from typing import Any

import click
import turbopuffer as tpuf

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Section 1: Constants & Schema                                             ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

MOUNT_SUFFIX = "__fs"

TEXT_EXTENSIONS: frozenset[str] = frozenset({
    ".c", ".cfg", ".conf", ".cpp", ".css", ".csv", ".dockerfile",
    ".env", ".gitignore", ".go", ".h", ".hpp", ".html", ".ini",
    ".java", ".js", ".json", ".jsonl", ".jsx", ".kt", ".lua",
    ".makefile", ".md", ".nix", ".py", ".r", ".rb", ".rs", ".rst",
    ".scala", ".sh", ".sql", ".svg", ".tf", ".toml", ".ts", ".tsx",
    ".txt", ".tsv", ".xml", ".yaml", ".yml", ".zig",
})

MIME_TABLE: dict[str, str] = {
    ".txt":  "text/plain",
    ".md":   "text/markdown",
    ".csv":  "text/csv",
    ".tsv":  "text/tab-separated-values",
    ".json": "application/json",
    ".jsonl": "application/x-ndjson",
    ".py":   "text/x-python",
    ".js":   "text/javascript",
    ".ts":   "text/typescript",
    ".tsx":  "text/typescript",
    ".jsx":  "text/javascript",
    ".html": "text/html",
    ".css":  "text/css",
    ".xml":  "application/xml",
    ".yaml": "application/yaml",
    ".yml":  "application/yaml",
    ".toml": "application/toml",
    ".sql":  "application/sql",
    ".svg":  "image/svg+xml",
    ".sh":   "application/x-sh",
    ".rs":   "text/x-rust",
    ".go":   "text/x-go",
    ".java": "text/x-java",
    ".c":    "text/x-c",
    ".cpp":  "text/x-c++",
    ".h":    "text/x-c",
    ".hpp":  "text/x-c++",
    ".rb":   "text/x-ruby",
    ".lua":  "text/x-lua",
    ".r":    "text/x-r",
}

FS_SCHEMA: dict[str, Any] = {
    "path":    {"type": "string", "filterable": True},
    "parent":  "string",
    "basename": {"type": "string", "filterable": True},
    "kind":    "string",
    "ext":     "string",
    "mime":    "string",
    "size_bytes": "uint",
    "is_text": "uint",
    "text": {
        "type": "string",
        "filterable": True,
        "full_text_search": {
            "tokenizer": "word_v3",
            "remove_stopwords": False,
            "stemming": False,
        },
    },
    "blob_b64": {"type": "string", "filterable": False},
    "sha256":  "string",
}

META_FIELDS: list[str] = [
    "id", "path", "parent", "basename", "kind", "ext",
    "mime", "size_bytes", "is_text", "sha256",
]

CONTENT_FIELDS: list[str] = META_FIELDS + ["text", "blob_b64"]


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Section 2: Path Utilities  (pure functions)                               ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def normalize_path(path: str) -> str:
    """Normalize an absolute POSIX path.

    - Requires leading '/'
    - Collapses repeated slashes
    - Rejects '.' and '..' segments (use resolve_path for user input)
    - Strips trailing slash (except for root)
    """
    if not isinstance(path, str) or len(path) == 0:
        raise ValueError("path must be a non-empty string")
    if "\x00" in path:
        raise ValueError("path must not contain NUL bytes")
    if not path.startswith("/"):
        raise ValueError(f"path must be absolute (start with '/'): {path!r}")
    if path == "/":
        return "/"
    segments = [s for s in path.split("/") if s]
    for seg in segments:
        if seg in (".", ".."):
            raise ValueError(f"path must not contain '.' or '..' segments: {path!r}")
    normalized = "/" + "/".join(segments)
    return normalized


def parent_path(path: str) -> str | None:
    """Parent directory of a normalized path.  Returns None for '/'."""
    norm = normalize_path(path)
    if norm == "/":
        return None
    return posixpath.dirname(norm)


def path_basename(path: str) -> str:
    """Last segment of a normalized path.  Returns '/' for root."""
    norm = normalize_path(path)
    if norm == "/":
        return "/"
    return posixpath.basename(norm)


def extension(path: str) -> str:
    """File extension including the dot.  Empty string for no extension."""
    name = path_basename(path)
    if name in ("/", "", ".", ".."):
        return ""
    return posixpath.splitext(name)[1]


def path_id(path: str) -> str:
    """SHA-256 hex digest of the normalized path — used as document ID."""
    norm = normalize_path(path)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def ancestor_paths(path: str, include_self: bool = False) -> list[str]:
    """All ancestor paths from '/' down toward *path*.

    >>> ancestor_paths('/a/b/c')
    ['/', '/a', '/a/b']
    >>> ancestor_paths('/a/b/c', include_self=True)
    ['/', '/a', '/a/b', '/a/b/c']
    """
    norm = normalize_path(path)
    if norm == "/":
        return ["/"] if include_self else []
    parts = norm.lstrip("/").split("/")
    limit = len(parts) if include_self else len(parts) - 1
    if limit < 1:
        return ["/"]
    result = ["/"]
    for i in range(1, limit + 1):
        result.append("/" + "/".join(parts[:i]))
    return result


def resolve_path(user_path: str | None, cwd: str) -> str:
    """Resolve a user-supplied path against a working directory.

    Handles None / '' / '.' / '..' / relative / absolute inputs.
    Resolution is purely lexical — no host-filesystem consultation.
    """
    if user_path is None or user_path == "" or user_path in (".", "./"):
        return normalize_path(cwd)
    raw = str(user_path)
    if raw.startswith("/"):
        return normalize_path(raw)
    segments = raw.split("/")
    current = normalize_path(cwd)
    for seg in segments:
        if seg == "" or seg == ".":
            continue
        if seg == "..":
            current = parent_path(current) or "/"
            continue
        if current == "/":
            current = f"/{seg}"
        else:
            current = f"{current}/{seg}"
    return normalize_path(current)


# ── Filter builders ──────────────────────────────────────────────────────────

def and_filter(*parts: Any) -> Any:
    """Combine filters with And.  Skips None.  Returns None if all None."""
    kept = [p for p in parts if p is not None]
    if len(kept) == 0:
        return None
    if len(kept) == 1:
        return kept[0]
    return ["And", kept]


def or_filter(*parts: Any) -> Any:
    """Combine filters with Or.  Skips None.  Returns None if all None."""
    kept = [p for p in parts if p is not None]
    if len(kept) == 0:
        return None
    if len(kept) == 1:
        return kept[0]
    return ["Or", kept]


def subtree_filter(path: str) -> Any:
    """Filter for *path* itself plus all descendants.

    Returns None for root (i.e. match everything).
    """
    norm = normalize_path(path)
    if norm == "/":
        return None
    prefix = norm.rstrip("/")
    return or_filter(
        ["path", "Eq", norm],
        ["path", "Glob", f"{prefix}/**"],
    )


def children_filter(path: str) -> list:
    """Filter for direct children of *path*."""
    return ["parent", "Eq", normalize_path(path)]


def paths_filter(paths: list[str]) -> Any:
    """Filter matching any of the given paths."""
    unique = list(dict.fromkeys(normalize_path(p) for p in paths))
    return or_filter(*(["path", "Eq", p] for p in unique))


def glob_filter(
    root: str,
    pattern: str,
    ignore_case: bool = False,
) -> Any | None:
    """Build a turbopuffer Glob/IGlob filter for a glob pattern.

    If *pattern* contains '/' it is resolved against *root* and matched
    against the full ``path`` attribute.  Otherwise it is matched against
    ``basename`` only (so ``*.py`` matches files anywhere in the subtree).
    """
    if not pattern:
        return None
    op = "IGlob" if ignore_case else "Glob"
    if "/" in pattern:
        # Full-path glob
        norm_root = normalize_path(root)
        if pattern.startswith("/"):
            full = normalize_path(pattern)
        else:
            full = normalize_path(posixpath.join(norm_root, pattern))
        return ["path", op, full]
    return ["basename", op, pattern]


def text_substring_filter(
    pattern: str,
    ignore_case: bool = False,
) -> Any | None:
    """Filter for documents whose ``text`` contains *pattern* as a substring.

    Uses a Glob wrapper: ``*<escaped_pattern>*``.  The remote filter may
    over-approximate in edge cases; callers should do exact local matching.
    """
    if not pattern:
        return None
    op = "IGlob" if ignore_case else "Glob"
    # Escape glob meta-characters inside the pattern
    escaped = pattern.replace("\\", "\\\\").replace("*", "\\*").replace("?", "\\?")
    return ["text", op, f"*{escaped}*"]


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Section 3: Row Builders  (pure functions)                                 ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def sha256_hex(data: bytes) -> str:
    """SHA-256 hex digest of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def infer_mime(path: str, fallback: str = "text/plain") -> str:
    """Infer MIME type from file extension."""
    ext = extension(path).lower()
    return MIME_TABLE.get(ext, fallback)


def _base_row(path: str, kind: str, mime: str, size_bytes: int,
              is_text: int, digest: str | None = None) -> dict[str, Any]:
    """Construct the fields common to every filesystem document."""
    norm = normalize_path(path)
    row: dict[str, Any] = {
        "id":         path_id(norm),
        "path":       norm,
        "basename":   path_basename(norm),
        "kind":       kind,
        "ext":        "" if kind == "dir" else extension(norm),
        "mime":       mime,
        "size_bytes": size_bytes,
        "is_text":    is_text,
    }
    par = parent_path(norm)
    if par is not None:
        row["parent"] = par
    if digest is not None:
        row["sha256"] = digest
    return row


def directory_row(path: str) -> dict[str, Any]:
    """Build a directory document."""
    return _base_row(path, kind="dir", mime="inode/directory",
                     size_bytes=0, is_text=0)


def text_row(
    path: str,
    text: str,
    mime: str | None = None,
) -> dict[str, Any]:
    """Build a text-file document."""
    data = text.encode("utf-8")
    row = _base_row(
        path,
        kind="file",
        mime=mime or infer_mime(path, "text/plain"),
        size_bytes=len(data),
        is_text=1,
        digest=sha256_hex(data),
    )
    row["text"] = text
    return row


def bytes_row(
    path: str,
    data: bytes,
    mime: str | None = None,
) -> dict[str, Any]:
    """Build a binary-file document."""
    row = _base_row(
        path,
        kind="file",
        mime=mime or infer_mime(path, "application/octet-stream"),
        size_bytes=len(data),
        is_text=0,
        digest=sha256_hex(data),
    )
    row["blob_b64"] = base64.b64encode(data).decode("ascii")
    return row


def parent_directory_rows(path: str) -> list[dict[str, Any]]:
    """Build directory rows for every ancestor of *path* (excluding self)."""
    return [directory_row(p) for p in ancestor_paths(path, include_self=False)]


def metadata_row(row: dict[str, Any]) -> dict[str, Any]:
    """Strip content fields from a row, keeping only metadata."""
    return {k: v for k, v in row.items() if k in META_FIELDS}


def now_iso() -> str:
    """Current UTC timestamp in ISO-8601."""
    return datetime.now(timezone.utc).isoformat()


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Section 4: TpFS — the filesystem class                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class TpFS:
    """A turbopuffer-backed filesystem.

    Holds a turbopuffer client and mount configuration.  Every method
    makes direct API calls — no intermediate plan/execute/finalize
    pipeline.
    """

    def __init__(
        self,
        api_key: str,
        region: str = "aws-us-west-2",
        mount: str = "demo",
    ) -> None:
        self.client = tpuf.Turbopuffer(api_key=api_key, region=region)
        self.mount = mount
        self.namespace = f"{mount}{MOUNT_SUFFIX}"
        self._ns = self.client.namespace(self.namespace)

    # ── internal query / write helpers ───────────────────────────────────────

    def _rows_from_response(self, response: Any) -> list[dict[str, Any]]:
        """Extract a list of plain dicts from a turbopuffer query response.

        The Python SDK returns a pydantic ``NamespaceQueryResponse`` with
        a ``.rows`` attribute containing row dicts.  Each row dict has
        ``id``, ``vector``, and attribute fields.  We normalize them into
        plain dicts with the vector field stripped.
        """
        if response is None:
            return []
        # SDK response: pydantic model with .rows list of dicts
        if hasattr(response, "rows"):
            raw_rows = response.rows
            if raw_rows is None:
                return []
            result: list[dict[str, Any]] = []
            for row in raw_rows:
                if isinstance(row, dict):
                    d = {k: v for k, v in row.items() if k != "vector"}
                    # Rename 'dist' to '$dist' if present (BM25 scores)
                    if "dist" in d:
                        d["$dist"] = d.pop("dist")
                    result.append(d)
                elif hasattr(row, "model_dump"):
                    dumped = row.model_dump()
                    d = {k: v for k, v in dumped.items()
                         if k != "vector" and v is not None}
                    result.append(d)
                else:
                    result.append(dict(row))
            return result
        # Fallback: iterable of items
        rows: list[dict[str, Any]] = []
        for item in response:
            if isinstance(item, dict):
                rows.append(item)
            else:
                rows.append(dict(item))
        return rows

    def _query(
        self,
        filters: Any,
        fields: list[str],
        *,
        rank_by: Any = ("path", "asc"),
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a single query.  Returns empty list on 404."""
        params: dict[str, Any] = {
            "rank_by": rank_by,
            "include_attributes": fields,
        }
        if filters is not None:
            params["filters"] = filters
        if limit is not None:
            params["top_k"] = limit
        try:
            resp = self._ns.query(**params)
        except tpuf.NotFoundError:
            return []
        except Exception as exc:
            if _is_not_found(exc):
                return []
            raise
        return self._rows_from_response(resp)

    def _query_one(
        self,
        path: str,
        fields: list[str],
    ) -> dict[str, Any] | None:
        """Look up a single document by exact path."""
        norm = normalize_path(path)
        rows = self._query(
            ["path", "Eq", norm],
            fields,
            limit=1,
        )
        return rows[0] if rows else None

    def _paginated_query(
        self,
        filters: Any,
        fields: list[str],
        *,
        limit: int | None = None,
        page_size: int = 256,
    ) -> list[dict[str, Any]]:
        """Ordered paginated query using ``path > last_path`` cursor.

        Keeps fetching pages until exhausted or *limit* is reached.
        """
        all_rows: list[dict[str, Any]] = []
        last_path: str | None = None
        remaining = limit

        while True:
            effective_limit = page_size if remaining is None else min(page_size, remaining)
            page_filter = filters
            if last_path is not None:
                page_filter = and_filter(filters, ["path", "Gt", last_path])
            rows = self._query(
                page_filter,
                fields,
                rank_by=("path", "asc"),
                limit=effective_limit,
            )
            if not rows:
                break
            all_rows.extend(rows)
            if remaining is not None:
                remaining -= len(rows)
                if remaining <= 0:
                    break
            if len(rows) < effective_limit:
                break
            last_path = str(rows[-1].get("path", ""))

        return all_rows

    def _upsert(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        """Upsert document rows with the filesystem schema."""
        resp = self._ns.write(upsert_rows=rows, schema=FS_SCHEMA)
        if resp is None:
            return {"status": "ok", "rows": len(rows)}
        if hasattr(resp, "model_dump"):
            return resp.model_dump()
        if isinstance(resp, dict):
            return resp
        return {"status": "ok", "rows": len(rows)}

    def _delete_ids(self, ids: list[str], batch_size: int = 256) -> None:
        """Delete documents by ID in batches."""
        for i in range(0, len(ids), batch_size):
            batch = ids[i : i + batch_size]
            self._ns.write(deletes=batch)

    # ── read operations ──────────────────────────────────────────────────────

    def stat(self, path: str) -> dict[str, Any] | None:
        """Return metadata for *path*, or ``None`` if it doesn't exist."""
        row = self._query_one(path, META_FIELDS)
        return metadata_row(row) if row else None

    def exists(self, path: str) -> bool:
        """Check whether *path* exists."""
        return self.stat(path) is not None

    def ls(self, path: str = "/", *, limit: int | None = None) -> list[dict[str, Any]]:
        """List direct children of a directory.

        Raises FileNotFoundError if *path* doesn't exist.
        Raises NotADirectoryError if *path* is a file.
        """
        norm = normalize_path(path)
        target = self._query_one(norm, META_FIELDS)
        if target is None:
            raise FileNotFoundError(norm)
        if target.get("kind") != "dir":
            raise NotADirectoryError(norm)
        rows = self._paginated_query(
            children_filter(norm), META_FIELDS, limit=limit,
        )
        return [metadata_row(r) for r in rows]

    def find(
        self,
        root: str = "/",
        *,
        glob: str | None = None,
        kind: str | None = None,
        ignore_case: bool = False,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Recursive find under *root*.

        Optional filters:
          - *glob*: shell-glob pattern (basename or full path)
          - *kind*: ``"file"`` or ``"dir"``
          - *ignore_case*: case-insensitive glob matching
        """
        norm = normalize_path(root)
        target = self._query_one(norm, META_FIELDS)
        if target is None:
            raise FileNotFoundError(norm)
        filters = and_filter(
            subtree_filter(norm),
            ["kind", "Eq", kind] if kind else None,
            glob_filter(norm, glob, ignore_case) if glob else None,
        )
        rows = self._paginated_query(filters, META_FIELDS, limit=limit)
        # If root is a file, constrain to that file
        if target.get("kind") == "file":
            rows = [r for r in rows if r.get("path") == norm]
        return [metadata_row(r) for r in rows]

    def cat(self, path: str) -> str:
        """Read the full text content of a text file.

        Raises FileNotFoundError, IsADirectoryError, or ValueError (binary).
        """
        norm = normalize_path(path)
        row = self._query_one(norm, CONTENT_FIELDS)
        if row is None:
            raise FileNotFoundError(norm)
        if row.get("kind") == "dir":
            raise IsADirectoryError(norm)
        if not row.get("is_text"):
            raise ValueError(f"binary file: {norm}")
        return str(row.get("text", ""))

    def head(self, path: str, n: int = 10) -> list[str]:
        """First *n* lines of a text file."""
        text = self.cat(path)
        return text.split("\n")[:n]

    def tail(self, path: str, n: int = 10) -> list[str]:
        """Last *n* lines of a text file."""
        text = self.cat(path)
        lines = text.split("\n")
        return lines[-n:] if n > 0 else []

    def read_bytes(self, path: str) -> bytes:
        """Read file content as raw bytes (works for both text and binary)."""
        norm = normalize_path(path)
        row = self._query_one(norm, CONTENT_FIELDS)
        if row is None:
            raise FileNotFoundError(norm)
        if row.get("kind") == "dir":
            raise IsADirectoryError(norm)
        if row.get("is_text"):
            return str(row.get("text", "")).encode("utf-8")
        blob = row.get("blob_b64", "")
        return base64.b64decode(blob) if blob else b""

    # ── grep engine ──────────────────────────────────────────────────────────

    def grep(
        self,
        root: str,
        pattern: str,
        *,
        mode: str = "literal",
        ignore_case: bool = False,
        glob: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Search text files under *root*.

        Modes:
          ``literal`` — Remote text-substring filter narrows candidates,
                        then exact line-by-line matching locally.
          ``regex``   — Remote fetches all text files in scope, then regex
                        line matching locally.
          ``bm25``    — Remote BM25 ranked retrieval.  Returns scored hits
                        with snippet extraction.
        """
        if not pattern:
            raise ValueError("pattern must not be empty")
        if mode == "literal":
            return self._grep_literal(root, pattern, ignore_case, glob, limit)
        if mode == "regex":
            return self._grep_regex(root, pattern, ignore_case, glob, limit)
        if mode == "bm25":
            return self._grep_bm25(root, pattern, ignore_case, glob, limit)
        raise ValueError(f"unknown grep mode: {mode!r}")

    def _grep_literal(
        self,
        root: str,
        pattern: str,
        ignore_case: bool,
        glob_pat: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Two-phase literal grep.

        Phase 1 (remote): Query turbopuffer with a text-substring Glob
        filter to narrow the candidate set.  Also filters on kind=file,
        is_text=1, subtree, and optional glob.

        Phase 2 (local): For each candidate document, split text into
        lines and do exact substring matching.  The remote filter may
        over-approximate (Glob escaping edge cases) but local matching
        is always exact.
        """
        norm_root = normalize_path(root)
        # Validate root exists
        self._require_exists(norm_root)

        filters = and_filter(
            ["kind", "Eq", "file"],
            ["is_text", "Eq", 1],
            subtree_filter(norm_root),
            glob_filter(norm_root, glob_pat, ignore_case) if glob_pat else None,
            text_substring_filter(pattern, ignore_case),
        )
        candidates = self._paginated_query(
            filters, ["path", "text"], limit=limit,
        )

        needle = pattern.lower() if ignore_case else pattern
        results: list[dict[str, Any]] = []
        for row in candidates:
            text = str(row.get("text", ""))
            for line_num, line in enumerate(text.split("\n"), start=1):
                haystack = line.lower() if ignore_case else line
                if needle in haystack:
                    results.append({
                        "kind": "line_match",
                        "path": str(row.get("path", "")),
                        "line_number": line_num,
                        "line": line,
                    })
                    if len(results) >= limit:
                        return results
        return results

    def _grep_regex(
        self,
        root: str,
        pattern: str,
        ignore_case: bool,
        glob_pat: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Regex grep.

        Remote: fetch all text files in the subtree (with optional glob).
        No remote text filter — regex can't be approximated by Glob.
        Local: compile the regex, test each line.
        """
        norm_root = normalize_path(root)
        self._require_exists(norm_root)

        filters = and_filter(
            ["kind", "Eq", "file"],
            ["is_text", "Eq", 1],
            subtree_filter(norm_root),
            glob_filter(norm_root, glob_pat, ignore_case) if glob_pat else None,
        )
        candidates = self._paginated_query(
            filters, ["path", "text"], limit=limit,
        )

        flags = re.IGNORECASE if ignore_case else 0
        compiled = re.compile(pattern, flags)

        results: list[dict[str, Any]] = []
        for row in candidates:
            text = str(row.get("text", ""))
            for line_num, line in enumerate(text.split("\n"), start=1):
                if compiled.search(line):
                    results.append({
                        "kind": "line_match",
                        "path": str(row.get("path", "")),
                        "line_number": line_num,
                        "line": line,
                    })
                    if len(results) >= limit:
                        return results
        return results

    def _grep_bm25(
        self,
        root: str,
        pattern: str,
        ignore_case: bool,
        glob_pat: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """BM25 ranked full-text search.

        Uses turbopuffer's BM25 ranking on the ``text`` field.  Returns
        scored hits with snippet extraction (the most relevant line).
        """
        norm_root = normalize_path(root)
        self._require_exists(norm_root)

        scope = and_filter(
            ["kind", "Eq", "file"],
            ["is_text", "Eq", 1],
            subtree_filter(norm_root),
            glob_filter(norm_root, glob_pat, ignore_case) if glob_pat else None,
        )

        params: dict[str, Any] = {
            "rank_by": ("text", "BM25", pattern),
            "top_k": limit,
            "include_attributes": ["path", "text"],
        }
        if scope is not None:
            params["filters"] = scope

        try:
            resp = self._ns.query(**params)
        except tpuf.NotFoundError:
            return []
        except Exception as exc:
            if _is_not_found(exc):
                return []
            raise

        results: list[dict[str, Any]] = []
        for row in self._rows_from_response(resp):
            text = str(row.get("text", ""))
            snippet = _extract_snippet(text, pattern, ignore_case)
            results.append({
                "kind": "search_hit",
                "mode": "bm25",
                "path": str(row.get("path", "")),
                "score": float(row.get("$dist", 0.0)),
                "snippet": snippet,
            })
        return results

    def _require_exists(self, path: str) -> dict[str, Any]:
        """Assert that *path* exists; raise FileNotFoundError otherwise."""
        row = self._query_one(path, META_FIELDS)
        if row is None:
            raise FileNotFoundError(path)
        return row

    # ── write operations ─────────────────────────────────────────────────────

    def mkdir(self, path: str) -> dict[str, Any]:
        """Create a directory (and all missing parents).

        Validates that no ancestor is a file and that the target is not
        an existing file.
        """
        norm = normalize_path(path)
        all_paths = ancestor_paths(norm, include_self=True)

        # Check existing docs for conflicts
        existing = self._query(
            paths_filter(all_paths), META_FIELDS, limit=len(all_paths),
        )
        existing_map = {str(r["path"]): r for r in existing}

        for anc in ancestor_paths(norm, include_self=False):
            if anc in existing_map and existing_map[anc].get("kind") != "dir":
                raise NotADirectoryError(anc)
        if norm in existing_map and existing_map[norm].get("kind") != "dir":
            raise FileExistsError(norm)

        rows = [directory_row(p) for p in all_paths]
        self._upsert(rows)
        return {"path": norm, "created": True}

    def put_text(
        self,
        path: str,
        text: str,
        mime: str | None = None,
    ) -> dict[str, Any]:
        """Write (or overwrite) a text file.  Creates parent directories.

        Validates that the target is not a directory and no ancestor is
        a file.
        """
        norm = normalize_path(path)
        if norm == "/":
            raise ValueError("cannot write to root directory")

        check_paths = ancestor_paths(norm, include_self=False) + [norm]
        existing = self._query(
            paths_filter(check_paths), META_FIELDS, limit=len(check_paths),
        )
        existing_map = {str(r["path"]): r for r in existing}

        for anc in ancestor_paths(norm, include_self=False):
            if anc in existing_map and existing_map[anc].get("kind") != "dir":
                raise NotADirectoryError(anc)
        if norm in existing_map and existing_map[norm].get("kind") == "dir":
            raise IsADirectoryError(norm)

        rows = parent_directory_rows(norm) + [text_row(norm, text, mime)]
        self._upsert(rows)
        return metadata_row(text_row(norm, text, mime))

    def put_bytes(
        self,
        path: str,
        data: bytes,
        mime: str | None = None,
    ) -> dict[str, Any]:
        """Write (or overwrite) a binary file.  Creates parent directories."""
        norm = normalize_path(path)
        if norm == "/":
            raise ValueError("cannot write to root directory")

        check_paths = ancestor_paths(norm, include_self=False) + [norm]
        existing = self._query(
            paths_filter(check_paths), META_FIELDS, limit=len(check_paths),
        )
        existing_map = {str(r["path"]): r for r in existing}

        for anc in ancestor_paths(norm, include_self=False):
            if anc in existing_map and existing_map[anc].get("kind") != "dir":
                raise NotADirectoryError(anc)
        if norm in existing_map and existing_map[norm].get("kind") == "dir":
            raise IsADirectoryError(norm)

        rows = parent_directory_rows(norm) + [bytes_row(norm, data, mime)]
        self._upsert(rows)
        return metadata_row(bytes_row(norm, data, mime))

    def rm(self, path: str, recursive: bool = False) -> dict[str, Any]:
        """Remove a file or directory.

        - Non-recursive delete of a non-empty directory raises OSError.
        - Recursive delete removes the entire subtree.
        - ``rm('/')`` is forbidden.
        """
        norm = normalize_path(path)
        if norm == "/":
            raise ValueError("cannot remove root")

        target = self._query_one(norm, META_FIELDS)
        if target is None:
            return {"path": norm, "deleted": False, "count": 0}

        if target.get("kind") == "dir" and not recursive:
            children = self._query(children_filter(norm), ["id"], limit=1)
            if children:
                raise OSError(f"directory not empty: {norm}")

        if recursive:
            all_rows = self._paginated_query(
                or_filter(["path", "Eq", norm], subtree_filter(norm)),
                ["id", "path"],
            )
            ids = [str(r["id"]) for r in all_rows if r.get("id")]
        else:
            ids = [str(target["id"])]

        if ids:
            self._delete_ids(ids)
        return {"path": norm, "deleted": True, "count": len(ids)}

    def cp(self, src: str, dst: str) -> dict[str, Any]:
        """Copy a file to a new path.

        If *dst* is an existing directory, copies into it with the
        source basename.
        """
        src_n = normalize_path(src)
        dst_n = normalize_path(dst)
        src_row = self._query_one(src_n, CONTENT_FIELDS)
        if src_row is None:
            raise FileNotFoundError(src_n)
        if src_row.get("kind") == "dir":
            raise IsADirectoryError(f"cp does not support directories: {src_n}")

        # If dst is an existing directory, copy into it
        dst_stat = self.stat(dst_n)
        if dst_stat and dst_stat.get("kind") == "dir":
            dst_n = normalize_path(f"{dst_n.rstrip('/')}/{path_basename(src_n)}")

        if src_row.get("is_text"):
            return self.put_text(dst_n, str(src_row.get("text", "")),
                                 str(src_row.get("mime")) if src_row.get("mime") else None)
        data = base64.b64decode(src_row.get("blob_b64", ""))
        return self.put_bytes(dst_n, data,
                              str(src_row.get("mime")) if src_row.get("mime") else None)

    def mv(self, src: str, dst: str) -> dict[str, Any]:
        """Move a file (copy + delete source)."""
        result = self.cp(src, dst)
        self.rm(src)
        return result

    def touch(self, path: str) -> dict[str, Any]:
        """Create an empty file if it doesn't exist.  No-op if it exists."""
        norm = normalize_path(path)
        if self.exists(norm):
            return {"path": norm, "touched": True, "created": False}
        self.put_text(norm, "")
        return {"path": norm, "touched": True, "created": True}

    def append(self, path: str, text: str) -> dict[str, Any]:
        """Append *text* to a file.  Creates the file if it doesn't exist.

        Implemented as a durable read-modify-write.
        """
        try:
            existing = self.cat(path)
        except FileNotFoundError:
            existing = ""
        return self.put_text(path, existing + text)

    # ── edit operations ──────────────────────────────────────────────────────

    def replace_text(
        self,
        path: str,
        search: str,
        replace: str,
        *,
        ignore_case: bool = False,
    ) -> dict[str, Any]:
        """Deterministic search-and-replace in a text file.

        Fails with ValueError on zero matches.
        Returns before/after sha256 digests.
        """
        norm = normalize_path(path)
        text = self.cat(norm)
        flags = re.IGNORECASE if ignore_case else 0
        compiled = re.compile(re.escape(search), flags)
        matches = len(compiled.findall(text))
        if matches == 0:
            raise ValueError(f"no matches for {search!r} in {norm}")
        new_text = compiled.sub(replace, text)
        if new_text != text:
            self.put_text(norm, new_text)
        return {
            "path": norm,
            "matches": matches,
            "changed": text != new_text,
            "before_sha256": sha256_hex(text.encode("utf-8")),
            "after_sha256": sha256_hex(new_text.encode("utf-8")),
        }

    # ── session operations ───────────────────────────────────────────────────

    def load_session(self) -> dict[str, Any]:
        """Load durable session state from ``/state/session.json``.

        Returns a default session if the document doesn't exist yet.
        """
        try:
            text = self.cat("/state/session.json")
            return json.loads(text)
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            return {"cwd": "/", "mount": self.mount, "updated_at": now_iso()}

    def save_session(self, session: dict[str, Any]) -> dict[str, Any]:
        """Persist session state durably to ``/state/session.json``."""
        session["updated_at"] = now_iso()
        session["mount"] = self.mount
        session.setdefault("path", "/state/session.json")
        self.put_text(
            "/state/session.json",
            json.dumps(session, indent=2),
            mime="application/json",
        )
        return session

    def pwd(self) -> str:
        """Return the durable current working directory."""
        return str(self.load_session().get("cwd", "/"))

    def cd(self, path: str) -> str:
        """Change the durable working directory.

        Validates that the target exists and is a directory before
        persisting.  A failing ``cd`` never persists an invalid cwd.
        """
        session = self.load_session()
        resolved = resolve_path(path, session.get("cwd", "/"))
        target = self.stat(resolved)
        if target is None:
            raise FileNotFoundError(resolved)
        if target.get("kind") != "dir":
            raise NotADirectoryError(resolved)
        session["cwd"] = resolved
        self.save_session(session)
        return resolved

    # ── hydration & sync ─────────────────────────────────────────────────────

    def hydrate(
        self,
        local_root: str,
        *,
        root: str = "/",
    ) -> dict[str, Any]:
        """Hydrate: pull the workspace from turbopuffer to a local directory.

        Creates a local mirror of the durable filesystem.  Returns a manifest
        that records the snapshot state for later sync.
        """
        import pathlib

        norm_root = normalize_path(root)
        local = pathlib.Path(local_root).resolve()
        local.mkdir(parents=True, exist_ok=True)

        entries = self.find(norm_root)
        manifest_entries: dict[str, dict[str, Any]] = {}

        for entry in entries:
            entry_path = str(entry["path"])
            # Compute local path
            if norm_root == "/":
                rel = entry_path.lstrip("/")
            else:
                rel = entry_path[len(norm_root):].lstrip("/")
            local_path = local / rel if rel else local

            if entry.get("kind") == "dir":
                local_path.mkdir(parents=True, exist_ok=True)
            else:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                if entry.get("is_text"):
                    text = self.cat(entry_path)
                    local_path.write_text(text, encoding="utf-8")
                else:
                    data = self.read_bytes(entry_path)
                    local_path.write_bytes(data)

            manifest_entries[entry_path] = {
                "path": entry_path,
                "kind": entry.get("kind", "file"),
                "sha256": entry.get("sha256"),
                "mime": entry.get("mime"),
                "size_bytes": entry.get("size_bytes", 0),
                "is_text": entry.get("is_text", 0),
            }

        session = self.load_session()
        manifest = {
            "mount": self.mount,
            "hydrated_at": now_iso(),
            "root": norm_root,
            "cwd": session.get("cwd", "/"),
            "workspace_metadata_path": "/state/workspace.json",
            "local_root": str(local),
            "entries": manifest_entries,
            "snapshot": manifest_entries,
        }
        return manifest

    def sync(
        self,
        local_root: str,
        manifest: dict[str, Any],
    ) -> dict[str, Any]:
        """Sync: push local changes back to turbopuffer.

        Compares the local directory against the hydration snapshot to detect
        created, modified, and deleted files.  Detects remote conflicts
        (files changed in turbopuffer since hydration).
        """
        import pathlib

        local = pathlib.Path(local_root).resolve()
        sync_root = str(manifest.get("root", "/"))
        snapshot = manifest.get("snapshot", manifest.get("entries", {}))

        # Scan local directory
        local_entries: dict[str, dict[str, Any]] = {}
        for local_path in sorted(local.rglob("*")):
            rel = str(local_path.relative_to(local))
            if sync_root == "/":
                mount_path = normalize_path(f"/{rel}")
            else:
                mount_path = normalize_path(f"{sync_root}/{rel}")
            if local_path.is_dir():
                local_entries[mount_path] = {
                    "path": mount_path, "kind": "dir",
                    "size_bytes": 0, "is_text": 0,
                }
            else:
                data = local_path.read_bytes()
                digest = sha256_hex(data)
                is_text = _is_probably_text(mount_path, data)
                local_entries[mount_path] = {
                    "path": mount_path, "kind": "file",
                    "sha256": digest,
                    "size_bytes": len(data),
                    "is_text": 1 if is_text else 0,
                }
        # Also add the root dir itself
        if sync_root == "/":
            local_entries["/"] = {"path": "/", "kind": "dir", "size_bytes": 0, "is_text": 0}

        # Fetch current remote state
        current_rows = self.find(sync_root)
        current_map = {str(r["path"]): r for r in current_rows}

        created: list[str] = []
        modified: list[str] = []
        deleted: list[str] = []
        unchanged: list[str] = []
        conflicts: list[dict[str, str]] = []

        all_paths = sorted(set(list(local_entries.keys()) + list(snapshot.keys())))
        for path in all_paths:
            if path == "/":
                continue
            snap = snapshot.get(path)
            curr = current_map.get(path)
            loc = local_entries.get(path)

            # Did local change vs snapshot?
            local_changed = _entry_changed(snap, loc)
            if not local_changed:
                unchanged.append(path)
                continue

            # Did remote change vs snapshot?
            remote_changed = _entry_changed(snap, curr)
            if remote_changed:
                conflicts.append({"path": path, "reason": "remote_changed_since_hydration"})
                continue

            if loc is None:
                # Deleted locally
                self.rm(path, recursive=(curr or {}).get("kind") == "dir")
                deleted.append(path)
                continue

            local_file = local / path.lstrip("/") if sync_root == "/" else local / path[len(sync_root):].lstrip("/")

            if loc["kind"] == "dir":
                if not curr:
                    self.mkdir(path)
                    created.append(path)
                else:
                    unchanged.append(path)
                continue

            data = local_file.read_bytes()
            if loc.get("is_text"):
                self.put_text(path, data.decode("utf-8"),
                              mime=(curr or {}).get("mime") or loc.get("mime"))
            else:
                self.put_bytes(path, data,
                               mime=(curr or {}).get("mime") or loc.get("mime"))

            if curr:
                modified.append(path)
            else:
                created.append(path)

        return {
            "mount": self.mount,
            "root": sync_root,
            "created": created,
            "modified": modified,
            "deleted": deleted,
            "unchanged": unchanged,
            "conflicts": conflicts,
        }

    # ── ingest ───────────────────────────────────────────────────────────────

    def ingest(
        self,
        local_root: str,
        *,
        mount_root: str = "/",
        batch_size: int = 256,
    ) -> dict[str, Any]:
        """Ingest: upload an entire local directory to turbopuffer.

        Scans the local directory recursively and upserts all files and
        directories as documents.
        """
        import pathlib

        local = pathlib.Path(local_root).resolve()
        if not local.is_dir():
            raise NotADirectoryError(str(local))

        norm_mount_root = normalize_path(mount_root)
        rows: list[dict[str, Any]] = []

        for local_path in sorted(local.rglob("*")):
            rel = str(local_path.relative_to(local))
            if norm_mount_root == "/":
                mount_path = normalize_path(f"/{rel}")
            else:
                mount_path = normalize_path(f"{norm_mount_root}/{rel}")

            if local_path.is_dir():
                rows.append(directory_row(mount_path))
            else:
                data = local_path.read_bytes()
                if _is_probably_text(mount_path, data):
                    rows.append(text_row(mount_path, data.decode("utf-8")))
                else:
                    rows.append(bytes_row(mount_path, data))

        # Also add mount_root itself and ancestors
        for ancestor in ancestor_paths(norm_mount_root, include_self=True):
            rows.insert(0, directory_row(ancestor))

        # Upsert in batches
        total_written = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            self._upsert(batch)
            total_written += len(batch)

        return {
            "mount": self.mount,
            "namespace": self.namespace,
            "local_root": str(local),
            "mount_root": norm_mount_root,
            "files": total_written,
        }

    # ── command logging ──────────────────────────────────────────────────────

    def log_command(
        self,
        command: str,
        *,
        cwd_before: str | None = None,
        cwd_after: str | None = None,
        exit_code: int = 0,
        stdout_preview: str = "",
        stderr_preview: str = "",
    ) -> dict[str, Any]:
        """Append a command log entry to ``/logs/run.jsonl``.

        Each entry is a JSON line with timestamp, command, cwd, and
        exit information.  The log is append-only and durable.
        """
        cwd = cwd_before or self.pwd()
        entry = {
            "timestamp": now_iso(),
            "command": command,
            "cwd_before": cwd,
            "cwd_after": cwd_after or cwd,
            "exit_code": exit_code,
            "stdout_preview": stdout_preview[:2000],
            "stderr_preview": stderr_preview[:2000],
        }
        line = json.dumps(entry) + "\n"
        self.append("/logs/run.jsonl", line)
        return entry

    def read_log(self) -> list[dict[str, Any]]:
        """Read the command log from ``/logs/run.jsonl``."""
        text = self.cat("/logs/run.jsonl")
        entries: list[dict[str, Any]] = []
        for line in text.strip().split("\n"):
            if line.strip():
                entries.append(json.loads(line))
        return entries

    # ── workspace operations ─────────────────────────────────────────────────

    def init_workspace(self) -> dict[str, Any]:
        """Initialize a standard workspace layout.

        Creates:
          /state  /logs  /output  /scratch  /project  /input

        Writes initial session state (cwd=/project) and workspace
        metadata documents.
        """
        dirs = ["/state", "/logs", "/output", "/scratch", "/project", "/input"]
        for d in dirs:
            self.mkdir(d)

        ts = now_iso()
        session: dict[str, Any] = {
            "cwd": "/project",
            "mount": self.mount,
            "updated_at": ts,
            "path": "/state/session.json",
        }
        self.save_session(session)

        metadata: dict[str, Any] = {
            "path": "/state/workspace.json",
            "mount": self.mount,
            "workspace_kind": "interactive",
            "created_at": ts,
            "updated_at": ts,
            "status": "active",
            "session_state": "/state/session.json",
            "entrypoint": "/TASK.md",
            "bundle_manifest": "/bundle.json",
            "logs_dir": "/logs",
            "output_dir": "/output",
            "scratch_dir": "/scratch",
            "project_dir": "/project",
            "input_dir": "/input",
        }
        self.put_text(
            "/state/workspace.json",
            json.dumps(metadata, indent=2),
            mime="application/json",
        )

        return {
            "mount": self.mount,
            "namespace": self.namespace,
            "dirs_created": dirs,
            "session_cwd": "/project",
        }

    # ── mount operations ─────────────────────────────────────────────────────

    def list_mounts(self) -> list[str]:
        """List all filesystem mounts (namespaces ending in ``__fs``)."""
        mounts: list[str] = []
        for ns in self.client.namespaces():
            ns_id = ns.id if hasattr(ns, "id") else str(ns)
            if ns_id.endswith(MOUNT_SUFFIX):
                mounts.append(ns_id[: -len(MOUNT_SUFFIX)])
        return sorted(mounts)

    def delete_mount(self) -> dict[str, Any]:
        """Delete the entire mount namespace and all its data."""
        self._ns.delete_all()
        return {"mount": self.mount, "namespace": self.namespace, "deleted": True}

    # ── tree rendering ───────────────────────────────────────────────────────

    def tree(self, path: str = "/", max_depth: int = 4) -> str:
        """Render a pretty directory tree with box-drawing characters.

        Uses ``find()`` to enumerate all entries, then builds and formats
        a nested tree structure.
        """
        norm = normalize_path(path)
        entries = self.find(norm)

        # Build a set of directory paths for rendering
        dir_paths: set[str] = set()
        for entry in entries:
            if entry.get("kind") == "dir":
                dir_paths.add(str(entry["path"]))

        # Build nested dict structure
        root_label = norm
        tree_dict: dict[str, Any] = {}
        for entry in entries:
            entry_path = str(entry["path"])
            if entry_path == norm:
                continue
            # Get relative path
            if norm == "/":
                rel = entry_path.lstrip("/")
            else:
                if not entry_path.startswith(norm + "/"):
                    continue
                rel = entry_path[len(norm) + 1:]
            parts = rel.split("/")
            # Respect max_depth
            if len(parts) > max_depth:
                continue
            node = tree_dict
            for part in parts:
                node = node.setdefault(part, {})

        # Render tree
        lines = [root_label]
        _render_tree(tree_dict, lines, "", dir_paths, norm)
        return "\n".join(lines)

    # ── word count ───────────────────────────────────────────────────────────

    def wc(self, path: str) -> dict[str, Any]:
        """Count lines, words, characters, and bytes in a text file."""
        norm = normalize_path(path)
        text = self.cat(norm)
        data = text.encode("utf-8")
        line_list = text.split("\n")
        word_list = text.split()
        return {
            "path": norm,
            "lines": len(line_list),
            "words": len(word_list),
            "chars": len(text),
            "bytes": len(data),
        }


# ── tree-rendering helper ───────────────────────────────────────────────────

def _render_tree(
    node: dict[str, Any],
    lines: list[str],
    prefix: str,
    dir_paths: set[str],
    current_path: str,
) -> None:
    """Recursively render a tree dict into lines with box-drawing chars."""
    keys = sorted(node.keys())
    for i, key in enumerate(keys):
        is_last = i == len(keys) - 1
        connector = "└── " if is_last else "├── "
        child = node[key]
        # Build the full path for this node
        if current_path == "/":
            child_path = f"/{key}"
        else:
            child_path = f"{current_path}/{key}"
        is_dir = child_path in dir_paths or bool(child)
        suffix = "/" if is_dir else ""
        lines.append(f"{prefix}{connector}{key}{suffix}")
        if child:
            ext_prefix = f"{prefix}{'    ' if is_last else '│   '}"
            _render_tree(child, lines, ext_prefix, dir_paths, child_path)


# ── error helpers ────────────────────────────────────────────────────────────

def _is_not_found(exc: Exception) -> bool:
    """Check if an exception represents a 404 / not-found error."""
    if hasattr(exc, "status") and getattr(exc, "status", None) == 404:
        return True
    if hasattr(exc, "status_code") and getattr(exc, "status_code", None) == 404:
        return True
    name = type(exc).__name__
    return "NotFound" in name


def _extract_snippet(
    text: str,
    query: str,
    ignore_case: bool,
    max_len: int = 240,
) -> str:
    """Extract the most relevant line from *text* for a search query.

    Used by BM25 grep to provide context snippets.
    """
    lines = text.split("\n")
    needle = query.lower() if ignore_case else query
    for line in lines:
        haystack = line.lower() if ignore_case else line
        if needle in haystack:
            return line[:max_len]
    # Fall back to first non-empty line
    for line in lines:
        stripped = line.strip()
        if stripped:
            return stripped[:max_len]
    return text[:max_len]


def _is_probably_text(path: str, data: bytes) -> bool:
    """Heuristic: is this file text?  Checks for NUL bytes and known extensions."""
    if b"\x00" in data:
        return False
    ext = extension(path).lower()
    if ext in TEXT_EXTENSIONS:
        return True
    # Try UTF-8 decode
    try:
        data.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False


def _entry_changed(
    a: dict[str, Any] | None,
    b: dict[str, Any] | None,
) -> bool:
    """Check if two snapshot entries differ."""
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    return (
        a.get("kind") != b.get("kind")
        or a.get("sha256") != b.get("sha256")
        or a.get("size_bytes") != b.get("size_bytes")
        or a.get("is_text") != b.get("is_text")
    )


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Section 5: Output Formatting  (pure functions)                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def _json_out(data: Any) -> None:
    """Print structured JSON to stdout."""
    click.echo(json.dumps(data, indent=2, default=str))


def _format_size(size: int) -> str:
    """Human-readable file size."""
    if size < 1024:
        return f"{size}B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f}K"
    return f"{size / (1024 * 1024):.1f}M"


def _format_ls_row(row: dict[str, Any]) -> str:
    """Format a single ls entry as a human-readable line."""
    kind = row.get("kind", "?")
    name = str(row.get("basename", row.get("path", "?")))
    size = int(row.get("size_bytes", 0))
    mime = str(row.get("mime", ""))
    if kind == "dir":
        return click.style(f"  {name}/", fg="blue", bold=True)
    size_str = _format_size(size)
    return f"  {name}  {click.style(size_str, dim=True)}  {click.style(mime, dim=True)}"


def _format_stat(row: dict[str, Any]) -> str:
    """Format stat output for human display."""
    lines = []
    for key in ("path", "kind", "mime", "size_bytes", "is_text", "sha256", "ext"):
        if key in row:
            val = row[key]
            label = click.style(f"{key}:", bold=True)
            lines.append(f"  {label} {val}")
    return "\n".join(lines)


def _format_grep_result(result: dict[str, Any]) -> str:
    """Format a single grep match for human display."""
    if result.get("kind") == "search_hit":
        path = click.style(str(result["path"]), fg="magenta")
        score = click.style(f'{result.get("score", 0):.4f}', fg="yellow")
        snippet = str(result.get("snippet", ""))
        return f"{path}  score={score}\n    {snippet}"
    path = click.style(str(result["path"]), fg="magenta")
    line_num = click.style(str(result.get("line_number", "?")), fg="green")
    line = str(result.get("line", ""))
    return f"{path}:{line_num}: {line}"


def _format_find_row(row: dict[str, Any]) -> str:
    """Format a find result for human display."""
    kind = row.get("kind", "?")
    path = str(row.get("path", "?"))
    if kind == "dir":
        return click.style(path + "/", fg="blue")
    return path


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Section 6: CLI                                                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class TpFSState:
    """Click context object carrying a TpFS instance and output mode."""
    def __init__(self, fs: TpFS, use_json: bool) -> None:
        self.fs = fs
        self.use_json = use_json


pass_state = click.make_pass_decorator(TpFSState)


@click.group()
@click.option("--api-key", envvar="TURBOPUFFER_API_KEY", default=None,
              help="Turbopuffer API key (or set TURBOPUFFER_API_KEY).")
@click.option("--region", envvar="TURBOPUFFER_REGION", default="aws-us-west-2",
              help="Turbopuffer region.")
@click.option("--mount", "-m", default="demo",
              help="Mount name (default: demo).")
@click.option("--json", "use_json", is_flag=True, default=False,
              help="Emit JSON output.")
@click.pass_context
def cli(ctx: click.Context, api_key: str | None, region: str,
        mount: str, use_json: bool) -> None:
    """tpfs — a turbopuffer-backed filesystem.

    Every file and directory lives as a document in turbopuffer.
    Agents can boot, work, die, and reboot — recovering all state
    from turbopuffer alone.
    """
    if not api_key:
        click.echo("Error: --api-key or TURBOPUFFER_API_KEY is required.", err=True)
        ctx.exit(1)
        return
    fs = TpFS(api_key=api_key, region=region, mount=mount)
    ctx.obj = TpFSState(fs, use_json)


# ── init ─────────────────────────────────────────────────────────────────────

@cli.command()
@pass_state
def init(state: TpFSState) -> None:
    """Initialize a workspace with standard directory layout."""
    result = state.fs.init_workspace()
    if state.use_json:
        _json_out(result)
    else:
        click.echo(click.style("✓ Workspace initialized", fg="green", bold=True))
        click.echo(f"  mount:     {result['mount']}")
        click.echo(f"  namespace: {result['namespace']}")
        click.echo(f"  created:   {' '.join(result['dirs_created'])}")
        click.echo(f"  cwd:       {result['session_cwd']}")


# ── mounts ───────────────────────────────────────────────────────────────────

@cli.command()
@pass_state
def mounts(state: TpFSState) -> None:
    """List all filesystem mounts."""
    result = state.fs.list_mounts()
    if state.use_json:
        _json_out(result)
    else:
        if not result:
            click.echo("(no mounts)")
        for m in result:
            click.echo(f"  {m}")


# ── pwd ──────────────────────────────────────────────────────────────────────

@cli.command()
@pass_state
def pwd(state: TpFSState) -> None:
    """Print the durable working directory."""
    cwd = state.fs.pwd()
    if state.use_json:
        _json_out({"cwd": cwd, "mount": state.fs.mount})
    else:
        click.echo(cwd)


# ── cd ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@pass_state
def cd(state: TpFSState, path: str) -> None:
    """Change the durable working directory."""
    cwd = state.fs.cd(path)
    if state.use_json:
        _json_out({"cwd": cwd, "mount": state.fs.mount})
    else:
        click.echo(cwd)


# ── ls ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path", default=".")
@click.option("--limit", type=int, default=None, help="Max entries.")
@pass_state
def ls(state: TpFSState, path: str, limit: int | None) -> None:
    """List directory contents."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    entries = state.fs.ls(resolved, limit=limit)
    if state.use_json:
        _json_out(entries)
    else:
        if not entries:
            click.echo("(empty)")
        for entry in entries:
            click.echo(_format_ls_row(entry))


# ── stat ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@pass_state
def stat(state: TpFSState, path: str) -> None:
    """Show metadata for a file or directory."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    result = state.fs.stat(resolved)
    if result is None:
        if state.use_json:
            _json_out(None)
        else:
            click.echo(click.style(f"not found: {path}", fg="red"), err=True)
        raise SystemExit(1)
    if state.use_json:
        _json_out(result)
    else:
        click.echo(_format_stat(result))


# ── cat ──────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@pass_state
def cat(state: TpFSState, path: str) -> None:
    """Print the contents of a text file."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    text = state.fs.cat(resolved)
    if state.use_json:
        _json_out({"path": resolve_path(path, state.fs.pwd()), "text": text})
    else:
        click.echo(text, nl=False)
        if text and not text.endswith("\n"):
            click.echo()


# ── head ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@click.option("-n", "--lines", "n", type=int, default=10, help="Number of lines.")
@pass_state
def head(state: TpFSState, path: str, n: int) -> None:
    """Print the first N lines of a text file."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    result = state.fs.head(resolved, n)
    if state.use_json:
        _json_out(result)
    else:
        for line in result:
            click.echo(line)


# ── tail ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@click.option("-n", "--lines", "n", type=int, default=10, help="Number of lines.")
@pass_state
def tail(state: TpFSState, path: str, n: int) -> None:
    """Print the last N lines of a text file."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    result = state.fs.tail(resolved, n)
    if state.use_json:
        _json_out(result)
    else:
        for line in result:
            click.echo(line)


# ── tree ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path", default=".")
@click.option("--depth", type=int, default=4, help="Max depth.")
@pass_state
def tree(state: TpFSState, path: str, depth: int) -> None:
    """Show a directory tree."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    result = state.fs.tree(resolved, max_depth=depth)
    if state.use_json:
        _json_out({"tree": result})
    else:
        click.echo(result)


# ── find ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("root", default=".")
@click.option("--glob", "glob_pattern", default=None, help="Glob filter.")
@click.option("--kind", default=None, type=click.Choice(["file", "dir"]),
              help="Filter by kind.")
@click.option("--ignore-case", is_flag=True, help="Case-insensitive glob.")
@click.option("--limit", type=int, default=None, help="Max results.")
@pass_state
def find(state: TpFSState, root: str, glob_pattern: str | None,
         kind: str | None, ignore_case: bool, limit: int | None) -> None:
    """Recursively find files and directories."""
    cwd = state.fs.pwd()
    resolved = resolve_path(root, cwd)
    results = state.fs.find(
        resolved, glob=glob_pattern, kind=kind,
        ignore_case=ignore_case, limit=limit,
    )
    if state.use_json:
        _json_out(results)
    else:
        for entry in results:
            click.echo(_format_find_row(entry))


# ── grep ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("pattern")
@click.argument("root", default="/")
@click.option("--mode", default="literal",
              type=click.Choice(["literal", "regex", "bm25"]),
              help="Search mode.")
@click.option("--ignore-case", "-i", is_flag=True, help="Case-insensitive.")
@click.option("--glob", "glob_pattern", default=None, help="Glob filter.")
@click.option("--limit", type=int, default=100, help="Max results.")
@pass_state
def grep(state: TpFSState, pattern: str, root: str, mode: str,
         ignore_case: bool, glob_pattern: str | None, limit: int) -> None:
    """Search text files.

    Modes: literal (exact substring), regex, bm25 (ranked full-text).
    """
    cwd = state.fs.pwd()
    resolved = resolve_path(root, cwd)
    results = state.fs.grep(
        resolved, pattern, mode=mode, ignore_case=ignore_case,
        glob=glob_pattern, limit=limit,
    )
    if state.use_json:
        _json_out(results)
    else:
        if not results:
            click.echo("(no matches)")
        for r in results:
            click.echo(_format_grep_result(r))


# ── mkdir ────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@pass_state
def mkdir(state: TpFSState, path: str) -> None:
    """Create a directory (and parents)."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    result = state.fs.mkdir(resolved)
    if state.use_json:
        _json_out(result)
    else:
        click.echo(click.style(f"✓ {result['path']}", fg="green"))


# ── put ──────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@click.option("--text", "text_content", default=None, help="Text content.")
@click.option("--stdin", "use_stdin", is_flag=True, help="Read from stdin.")
@click.option("--file", "from_file", default=None, help="Read from local file.",
              type=click.Path(exists=True))
@click.option("--mime", default=None, help="MIME type override.")
@pass_state
def put(state: TpFSState, path: str, text_content: str | None,
        use_stdin: bool, from_file: str | None, mime: str | None) -> None:
    """Write a text file."""
    if text_content is not None:
        content = text_content
    elif use_stdin:
        content = sys.stdin.read()
    elif from_file is not None:
        with open(from_file, "r") as f:
            content = f.read()
    else:
        raise click.UsageError("provide --text, --stdin, or --file")
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    result = state.fs.put_text(resolved, content, mime=mime)
    if state.use_json:
        _json_out(result)
    else:
        click.echo(click.style(f"✓ {result['path']}  ({_format_size(result.get('size_bytes', 0))})", fg="green"))


# ── rm ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@click.option("-r", "--recursive", is_flag=True, help="Recursive delete.")
@pass_state
def rm(state: TpFSState, path: str, recursive: bool) -> None:
    """Remove a file or directory."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    result = state.fs.rm(resolved, recursive=recursive)
    if state.use_json:
        _json_out(result)
    else:
        if result.get("deleted"):
            click.echo(click.style(
                f"✓ removed {result['path']} ({result.get('count', 1)} docs)",
                fg="green"))
        else:
            click.echo(f"  not found: {result['path']}")


# ── cp ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("src")
@click.argument("dst")
@pass_state
def cp(state: TpFSState, src: str, dst: str) -> None:
    """Copy a file."""
    cwd = state.fs.pwd()
    src_r = resolve_path(src, cwd)
    dst_r = resolve_path(dst, cwd)
    result = state.fs.cp(src_r, dst_r)
    if state.use_json:
        _json_out(result)
    else:
        click.echo(click.style(f"✓ copied → {result['path']}", fg="green"))


# ── mv ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("src")
@click.argument("dst")
@pass_state
def mv(state: TpFSState, src: str, dst: str) -> None:
    """Move a file."""
    cwd = state.fs.pwd()
    src_r = resolve_path(src, cwd)
    dst_r = resolve_path(dst, cwd)
    result = state.fs.mv(src_r, dst_r)
    if state.use_json:
        _json_out(result)
    else:
        click.echo(click.style(f"✓ moved → {result['path']}", fg="green"))


# ── touch ────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@pass_state
def touch(state: TpFSState, path: str) -> None:
    """Create an empty file (no-op if exists)."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    result = state.fs.touch(resolved)
    if state.use_json:
        _json_out(result)
    else:
        status = "exists" if not result.get("created") else "created"
        click.echo(click.style(f"✓ {result['path']} ({status})", fg="green"))


# ── wc ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@pass_state
def wc(state: TpFSState, path: str) -> None:
    """Count lines, words, characters, and bytes."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    result = state.fs.wc(resolved)
    if state.use_json:
        _json_out(result)
    else:
        click.echo(
            f"  {result['lines']} lines  {result['words']} words  "
            f"{result['chars']} chars  {result['bytes']} bytes  {result['path']}"
        )


# ── replace ──────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("path")
@click.option("--search", required=True, help="Text to search for.")
@click.option("--replace", "replace_with", required=True, help="Replacement text.")
@click.option("--ignore-case", is_flag=True, help="Case-insensitive search.")
@pass_state
def replace(state: TpFSState, path: str, search: str, replace_with: str,
            ignore_case: bool) -> None:
    """Search and replace text in a file."""
    cwd = state.fs.pwd()
    resolved = resolve_path(path, cwd)
    result = state.fs.replace_text(
        resolved, search, replace_with, ignore_case=ignore_case,
    )
    if state.use_json:
        _json_out(result)
    else:
        if result["changed"]:
            click.echo(click.style(
                f"✓ {result['path']}: {result['matches']} match(es) replaced",
                fg="green"))
        else:
            click.echo(f"  {result['path']}: {result['matches']} match(es), no change")


# ── hydrate ──────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("local_root")
@click.option("--root", default="/", help="tpfs root to hydrate from.")
@click.option("--manifest-out", default=None, help="Write manifest JSON to file.")
@pass_state
def hydrate(state: TpFSState, local_root: str, root: str, manifest_out: str | None) -> None:
    """Hydrate: pull workspace from turbopuffer to a local directory."""
    result = state.fs.hydrate(local_root, root=root)
    if manifest_out:
        import pathlib
        pathlib.Path(manifest_out).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(manifest_out).write_text(json.dumps(result, indent=2))
        result["manifest_file"] = manifest_out
    if state.use_json:
        _json_out(result)
    else:
        n_files = sum(1 for e in result["entries"].values() if e["kind"] == "file")
        n_dirs = sum(1 for e in result["entries"].values() if e["kind"] == "dir")
        click.echo(click.style("✓ Hydrated", fg="green", bold=True))
        click.echo(f"  local:  {local_root}")
        click.echo(f"  root:   {root}")
        click.echo(f"  files:  {n_files}")
        click.echo(f"  dirs:   {n_dirs}")
        if manifest_out:
            click.echo(f"  manifest: {manifest_out}")


# ── sync ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("local_root")
@click.option("--manifest", "manifest_file", required=True,
              help="Path to hydration manifest JSON.", type=click.Path(exists=True))
@pass_state
def sync(state: TpFSState, local_root: str, manifest_file: str) -> None:
    """Sync: push local changes back to turbopuffer."""
    with open(manifest_file, "r") as f:
        manifest = json.loads(f.read())
    result = state.fs.sync(local_root, manifest)
    if state.use_json:
        _json_out(result)
    else:
        click.echo(click.style("✓ Synced", fg="green", bold=True))
        click.echo(f"  created:   {len(result['created'])}")
        click.echo(f"  modified:  {len(result['modified'])}")
        click.echo(f"  deleted:   {len(result['deleted'])}")
        click.echo(f"  unchanged: {len(result['unchanged'])}")
        if result["conflicts"]:
            click.echo(click.style(f"  conflicts: {len(result['conflicts'])}", fg="red"))
            for c in result["conflicts"]:
                click.echo(f"    {c['path']}: {c['reason']}")


# ── ingest ───────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("local_root", type=click.Path(exists=True))
@click.option("--mount-root", default="/", help="Mount root path (default: /).")
@click.option("--batch-size", type=int, default=256, help="Batch size for upserts.")
@pass_state
def ingest(state: TpFSState, local_root: str, mount_root: str, batch_size: int) -> None:
    """Ingest: upload a local directory to turbopuffer."""
    result = state.fs.ingest(local_root, mount_root=mount_root, batch_size=batch_size)
    if state.use_json:
        _json_out(result)
    else:
        click.echo(click.style("✓ Ingested", fg="green", bold=True))
        click.echo(f"  from:   {result['local_root']}")
        click.echo(f"  to:     {result['mount_root']}")
        click.echo(f"  docs:   {result['files']}")


# ── log ──────────────────────────────────────────────────────────────────────

@cli.command()
@pass_state
def log(state: TpFSState) -> None:
    """Show the durable command log."""
    entries = state.fs.read_log()
    if state.use_json:
        _json_out(entries)
    else:
        if not entries:
            click.echo("(no log entries)")
        for e in entries:
            ts = e.get("timestamp", "?")[:19]
            cmd = e.get("command", "?")
            code = e.get("exit_code", "?")
            click.echo(f"  [{ts}] {cmd}  (exit {code})")


# ── delete-mount ─────────────────────────────────────────────────────────────

@cli.command("delete-mount")
@pass_state
def delete_mount(state: TpFSState) -> None:
    """Delete the entire mount and all its data."""
    result = state.fs.delete_mount()
    if state.use_json:
        _json_out(result)
    else:
        click.echo(click.style(
            f"✓ Deleted mount {result['mount']} ({result['namespace']})",
            fg="red", bold=True))


# ── entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli()
