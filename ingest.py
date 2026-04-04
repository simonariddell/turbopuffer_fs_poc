"""Local directory -> single-namespace turbopuffer rows."""

from __future__ import annotations

import base64
import mimetypes
from functools import partial
from itertools import chain
from pathlib import Path

from .paths import basename, extension, normalize, parent, path_id
from .schema import fs_schema


text_from_bytes = lambda data: data.decode("utf-8")
looks_text = lambda path, data: (mimetypes.guess_type(str(path))[0] or "").startswith("text/") or Path(path).suffix.lower() in {
    ".txt",
    ".md",
    ".py",
    ".json",
    ".yaml",
    ".yml",
    ".csv",
    ".tsv",
    ".html",
    ".css",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".xml",
    ".toml",
    ".ini",
    ".sh",
    ".sql",
}


def file_row(local_path: Path, mount_path: str) -> dict[str, object]:
    data = local_path.read_bytes()
    mime = mimetypes.guess_type(str(local_path))[0] or "application/octet-stream"
    try:
        text = text_from_bytes(data) if looks_text(local_path, data) else None
    except UnicodeDecodeError:
        text = None
    return {
        "id": path_id(mount_path),
        "path": mount_path,
        "parent": parent(mount_path),
        "basename": basename(mount_path),
        "kind": "file",
        "ext": extension(mount_path),
        "mime": mime,
        "size_bytes": len(data),
        "is_text": 1 if text is not None else 0,
        "text": text,
        "blob_b64": None if text is not None else base64.b64encode(data).decode("ascii"),
    }


def dir_row(mount_path: str) -> dict[str, object]:
    return {
        "id": path_id(mount_path),
        "path": mount_path,
        "parent": parent(mount_path),
        "basename": basename(mount_path),
        "kind": "dir",
        "ext": "",
        "mime": "inode/directory",
        "size_bytes": 0,
        "is_text": 0,
        "text": None,
        "blob_b64": None,
    }


def mounted_path(local_root: Path, local_path: Path, mount_root: str = "/") -> str:
    rel = local_path.relative_to(local_root).as_posix()
    base = normalize(mount_root)
    return base if rel == "." else normalize(f"{base.rstrip('/')}/{rel}")


def scan_directory(local_root: str | Path, *, mount_root: str = "/") -> list[dict[str, object]]:
    root = Path(local_root).expanduser().resolve()
    paths = [root, *sorted(root.rglob("*"))]
    rows = []
    for local_path in paths:
        mount_path = mounted_path(root, local_path, mount_root)
        rows.append(dir_row(mount_path) if local_path.is_dir() else file_row(local_path, mount_path))
    return rows


def batched(rows: list[dict[str, object]], size: int):
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def write_rows(client, namespace: str, rows: list[dict[str, object]], *, batch_size: int = 256, schema: dict[str, object] | None = None):
    handle = client.namespace(namespace)
    selected_schema = fs_schema() if schema is None else schema
    return [handle.write(upsert_rows=batch, schema=selected_schema) for batch in batched(rows, batch_size)]


def ingest_directory(client, mount: str, local_root: str | Path, *, mount_root: str = "/", batch_size: int = 256):
    from .live import mount_namespace

    rows = scan_directory(local_root, mount_root=mount_root)
    writes = write_rows(client, mount_namespace(mount), rows, batch_size=batch_size)
    return {"rows": rows, "writes": writes}
