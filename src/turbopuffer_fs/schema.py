"""Schema and row builders for the turbopuffer filesystem model."""

from __future__ import annotations

import base64
import hashlib
import mimetypes
from typing import Iterable

from .paths import ancestor_paths, basename, extension, normalize_path, parent_path, path_id


TEXT_EXTENSIONS = {
    ".c",
    ".cfg",
    ".conf",
    ".cpp",
    ".css",
    ".csv",
    ".html",
    ".ini",
    ".java",
    ".js",
    ".json",
    ".jsx",
    ".md",
    ".py",
    ".r",
    ".rb",
    ".rs",
    ".rst",
    ".sh",
    ".sql",
    ".svg",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".tsv",
    ".xml",
    ".yaml",
    ".yml",
}

META_FIELDS = [
    "id",
    "path",
    "parent",
    "basename",
    "kind",
    "ext",
    "mime",
    "size_bytes",
    "is_text",
    "sha256",
    "source_mtime_ns",
    "source_size_bytes",
]

CONTENT_FIELDS = META_FIELDS + ["text", "blob_b64"]


def fs_schema() -> dict[str, object]:
    return {
        "path": {"type": "string", "regex": True, "filterable": True},
        "parent": "string",
        "basename": {"type": "string", "regex": True, "filterable": True},
        "kind": "string",
        "ext": "string",
        "mime": "string",
        "size_bytes": "uint",
        "is_text": "uint",
        "text": {"type": "string", "regex": True, "filterable": True},
        "blob_b64": {"type": "string", "filterable": False},
        "sha256": "string",
        "source_mtime_ns": "uint",
        "source_size_bytes": "uint",
    }


def metadata_row(row: dict[str, object]) -> dict[str, object]:
    return {field: row[field] for field in META_FIELDS if field in row}


def content_row(row: dict[str, object]) -> dict[str, object]:
    return {field: row[field] for field in CONTENT_FIELDS if field in row}


def content_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _base_row(path: str, *, kind: str, mime: str, size_bytes: int, is_text: int, sha256: str | None = None) -> dict[str, object]:
    value = normalize_path(path)
    row: dict[str, object] = {
        "id": path_id(value),
        "path": value,
        "basename": basename(value),
        "kind": kind,
        "ext": "" if kind == "dir" else extension(value),
        "mime": mime,
        "size_bytes": size_bytes,
        "is_text": is_text,
    }
    parent = parent_path(value)
    if parent is not None:
        row["parent"] = parent
    if sha256 is not None:
        row["sha256"] = sha256
    return row


def _with_source_metadata(row: dict[str, object], *, source_mtime_ns: int | None = None, source_size_bytes: int | None = None) -> dict[str, object]:
    if source_mtime_ns is not None:
        row["source_mtime_ns"] = int(source_mtime_ns)
    if source_size_bytes is not None:
        row["source_size_bytes"] = int(source_size_bytes)
    return row


def directory_row(path: str, *, source_mtime_ns: int | None = None) -> dict[str, object]:
    row = _base_row(
        path,
        kind="dir",
        mime="inode/directory",
        size_bytes=0,
        is_text=0,
    )
    return _with_source_metadata(row, source_mtime_ns=source_mtime_ns, source_size_bytes=0)


def infer_mime(path: str, *, fallback: str) -> str:
    guessed, _ = mimetypes.guess_type(path)
    return guessed or fallback


def text_row(
    path: str,
    text: str,
    *,
    mime: str | None = None,
    source_mtime_ns: int | None = None,
    source_size_bytes: int | None = None,
) -> dict[str, object]:
    data = text.encode("utf-8")
    row = _base_row(
        path,
        kind="file",
        mime=mime or infer_mime(path, fallback="text/plain"),
        size_bytes=len(data),
        is_text=1,
        sha256=content_sha256(data),
    )
    row["text"] = text
    return _with_source_metadata(
        row,
        source_mtime_ns=source_mtime_ns,
        source_size_bytes=len(data) if source_size_bytes is None else source_size_bytes,
    )


def bytes_row(
    path: str,
    data: bytes,
    *,
    mime: str | None = None,
    source_mtime_ns: int | None = None,
    source_size_bytes: int | None = None,
) -> dict[str, object]:
    row = _base_row(
        path,
        kind="file",
        mime=mime or infer_mime(path, fallback="application/octet-stream"),
        size_bytes=len(data),
        is_text=0,
        sha256=content_sha256(data),
    )
    row["blob_b64"] = base64.b64encode(data).decode("ascii")
    return _with_source_metadata(
        row,
        source_mtime_ns=source_mtime_ns,
        source_size_bytes=len(data) if source_size_bytes is None else source_size_bytes,
    )


def parent_directory_rows(path: str) -> list[dict[str, object]]:
    return [directory_row(value) for value in ancestor_paths(path, include_self=False)]


def target_directory_rows(path: str) -> list[dict[str, object]]:
    return [directory_row(value) for value in ancestor_paths(path, include_self=True)]


def is_probably_text(path: str, data: bytes) -> bool:
    suffix = extension(path).lower()
    if b"\x00" in data:
        return False
    if suffix in TEXT_EXTENSIONS:
        return True
    mime = infer_mime(path, fallback="application/octet-stream")
    if mime.startswith("text/"):
        return True
    if mime in {
        "application/json",
        "application/sql",
        "application/xml",
        "application/x-sh",
        "image/svg+xml",
    }:
        return True
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def row_from_bytes(
    path: str,
    data: bytes,
    *,
    mime: str | None = None,
    source_mtime_ns: int | None = None,
    source_size_bytes: int | None = None,
) -> dict[str, object]:
    if is_probably_text(path, data):
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            return bytes_row(
                path,
                data,
                mime=mime,
                source_mtime_ns=source_mtime_ns,
                source_size_bytes=source_size_bytes,
            )
        return text_row(
            path,
            text,
            mime=mime,
            source_mtime_ns=source_mtime_ns,
            source_size_bytes=source_size_bytes,
        )
    return bytes_row(
        path,
        data,
        mime=mime,
        source_mtime_ns=source_mtime_ns,
        source_size_bytes=source_size_bytes,
    )


def upsert_rows_payload(rows: Iterable[dict[str, object]]) -> dict[str, object]:
    return {
        "upsert_rows": list(rows),
        "schema": fs_schema(),
    }
