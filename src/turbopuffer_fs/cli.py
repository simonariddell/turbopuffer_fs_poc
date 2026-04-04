"""Tiny JSON-first CLI for turbopuffer_fs."""

from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path

from . import ingest_directory
from .live import (
    cat,
    find,
    grep,
    list_mounts,
    ls,
    make_client,
    mkdir,
    put_bytes,
    put_text,
    read_bytes,
    read_text,
    rm,
    stat,
)


def _json_dump(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True)


def _print_json(value: object) -> None:
    print(_json_dump(value))


def _load_text_input(args: argparse.Namespace) -> str:
    if getattr(args, "text", None) is not None:
        return str(args.text)
    if getattr(args, "file", None) is not None:
        return Path(args.file).read_text(encoding="utf-8")
    return sys.stdin.read()


def _load_bytes_input(args: argparse.Namespace) -> bytes:
    if getattr(args, "file", None) is not None:
        return Path(args.file).read_bytes()
    return sys.stdin.buffer.read()


def _client_from_args(args: argparse.Namespace):
    return make_client(
        api_key=getattr(args, "api_key", None),
        region=getattr(args, "region", None),
        base_url=getattr(args, "base_url", None),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tpfs", description="Filesystem-shaped turbopuffer CLI.")
    parser.add_argument("--api-key", dest="api_key")
    parser.add_argument("--region")
    parser.add_argument("--base-url", dest="base_url")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("mounts")

    stat_parser = subparsers.add_parser("stat")
    stat_parser.add_argument("mount")
    stat_parser.add_argument("path")

    ls_parser = subparsers.add_parser("ls")
    ls_parser.add_argument("mount")
    ls_parser.add_argument("path", nargs="?", default="/")
    ls_parser.add_argument("--limit", type=int)

    find_parser = subparsers.add_parser("find")
    find_parser.add_argument("mount")
    find_parser.add_argument("root", nargs="?", default="/")
    find_parser.add_argument("--glob")
    find_parser.add_argument("--kind", choices=("file", "dir"))
    find_parser.add_argument("--ignore-case", action="store_true")
    find_parser.add_argument("--limit", type=int)

    cat_parser = subparsers.add_parser("cat")
    cat_parser.add_argument("mount")
    cat_parser.add_argument("path")

    read_text_parser = subparsers.add_parser("read-text")
    read_text_parser.add_argument("mount")
    read_text_parser.add_argument("path")

    read_bytes_parser = subparsers.add_parser("read-bytes")
    read_bytes_parser.add_argument("mount")
    read_bytes_parser.add_argument("path")
    read_bytes_parser.add_argument("--out")

    grep_parser = subparsers.add_parser("grep")
    grep_parser.add_argument("mount")
    grep_parser.add_argument("root")
    grep_parser.add_argument("pattern")
    grep_parser.add_argument("--glob")
    grep_parser.add_argument("--ignore-case", action="store_true")
    grep_parser.add_argument("--limit", type=int)

    mkdir_parser = subparsers.add_parser("mkdir")
    mkdir_parser.add_argument("mount")
    mkdir_parser.add_argument("path")

    put_text_parser = subparsers.add_parser("put-text")
    put_text_parser.add_argument("mount")
    put_text_parser.add_argument("path")
    text_source = put_text_parser.add_mutually_exclusive_group(required=True)
    text_source.add_argument("--text")
    text_source.add_argument("--file")
    text_source.add_argument("--stdin", action="store_true")
    put_text_parser.add_argument("--mime")

    put_bytes_parser = subparsers.add_parser("put-bytes")
    put_bytes_parser.add_argument("mount")
    put_bytes_parser.add_argument("path")
    byte_source = put_bytes_parser.add_mutually_exclusive_group(required=True)
    byte_source.add_argument("--file")
    byte_source.add_argument("--stdin", action="store_true")
    put_bytes_parser.add_argument("--mime")

    rm_parser = subparsers.add_parser("rm")
    rm_parser.add_argument("mount")
    rm_parser.add_argument("path")
    rm_parser.add_argument("--recursive", action="store_true")

    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument("mount")
    ingest_parser.add_argument("local_root")
    ingest_parser.add_argument("--mount-root", default="/")
    ingest_parser.add_argument("--batch-size", type=int, default=256)

    return parser


def run_cli(args: argparse.Namespace):
    client = _client_from_args(args)
    command = args.command

    if command == "mounts":
        return list_mounts(client)
    if command == "stat":
        return stat(client, args.mount, args.path)
    if command == "ls":
        return ls(client, args.mount, args.path, limit=args.limit)
    if command == "find":
        return find(
            client,
            args.mount,
            args.root,
            glob=args.glob,
            kind=args.kind,
            ignore_case=args.ignore_case,
            limit=args.limit,
        )
    if command == "cat":
        return cat(client, args.mount, args.path)
    if command == "read-text":
        return read_text(client, args.mount, args.path)
    if command == "read-bytes":
        data = read_bytes(client, args.mount, args.path)
        if args.out:
            Path(args.out).write_bytes(data)
            return {"path": args.path, "out": args.out, "bytes_written": len(data), "size_bytes": len(data)}
        return {"path": args.path, "size_bytes": len(data), "blob_b64": base64.b64encode(data).decode("ascii")}
    if command == "grep":
        return grep(
            client,
            args.mount,
            args.root,
            args.pattern,
            glob=args.glob,
            ignore_case=args.ignore_case,
            limit=args.limit,
        )
    if command == "mkdir":
        return mkdir(client, args.mount, args.path)
    if command == "put-text":
        return put_text(client, args.mount, args.path, _load_text_input(args), mime=args.mime)
    if command == "put-bytes":
        return put_bytes(client, args.mount, args.path, _load_bytes_input(args), mime=args.mime)
    if command == "rm":
        return rm(client, args.mount, args.path, recursive=args.recursive)
    if command == "ingest":
        return ingest_directory(
            client,
            args.mount,
            args.local_root,
            mount_root=args.mount_root,
            batch_size=args.batch_size,
        )
    raise ValueError(f"unsupported command: {command!r}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:  # pragma: no cover - argparse exit path
        return int(exc.code)
    try:
        result = run_cli(args)
    except Exception as exc:  # pragma: no cover - exercised via CLI behavior
        print(_json_dump({"error": type(exc).__name__, "message": str(exc)}), file=sys.stderr)
        return 1
    _print_json(result)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
