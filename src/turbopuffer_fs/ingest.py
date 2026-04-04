"""Directory ingest helpers."""

from __future__ import annotations

from pathlib import Path

from .live import mount_namespace
from .schema import directory_row, row_from_bytes, upsert_rows_payload


def mounted_path(local_root: Path, local_path: Path, mount_root: str = "/") -> str:
    from .paths import join_path, normalize_path

    root = normalize_path(mount_root)
    relative = local_path.relative_to(local_root).as_posix()
    return root if relative == "." else join_path(root, relative)


def scan_directory(local_root: str | Path, *, mount_root: str = "/") -> list[dict[str, object]]:
    root = Path(local_root).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(str(root))
    if not root.is_dir():
        raise NotADirectoryError(str(root))

    rows: list[dict[str, object]] = []
    for local_path in [root, *sorted(root.rglob("*"))]:
        mount_path = mounted_path(root, local_path, mount_root=mount_root)
        stat = local_path.stat()
        if local_path.is_dir():
            rows.append(directory_row(mount_path, source_mtime_ns=stat.st_mtime_ns))
            continue
        rows.append(
            row_from_bytes(
                mount_path,
                local_path.read_bytes(),
                source_mtime_ns=stat.st_mtime_ns,
                source_size_bytes=stat.st_size,
            )
        )
    return rows


def batched(rows: list[dict[str, object]], batch_size: int) -> list[list[dict[str, object]]]:
    size = int(batch_size)
    if size < 1:
        raise ValueError("batch_size must be a positive integer")
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def write_rows(client, namespace: str, rows: list[dict[str, object]], *, batch_size: int = 256) -> list[dict[str, object]]:
    handle = client.namespace(namespace)
    responses = []
    for batch in batched(rows, batch_size):
        responses.append(handle.write(**upsert_rows_payload(batch)))
    return responses


def ingest_directory(client, mount: str, local_root: str | Path, *, mount_root: str = "/", batch_size: int = 256) -> dict[str, object]:
    rows = scan_directory(local_root, mount_root=mount_root)
    writes = write_rows(client, mount_namespace(mount), rows, batch_size=batch_size)
    return {
        "mount": mount,
        "namespace": mount_namespace(mount),
        "row_count": len(rows),
        "rows": rows,
        "writes": writes,
    }
