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
from .dogfood import (
    bundle_config,
    bundle_entrypoint,
    bundle_task_prompt,
    list_allowed_outputs,
    load_bundle_spec,
    run_dogfood,
    seed_bundle,
    validate_bundle_outputs,
)
from .workspace import (
    load_session_state,
    resolve_cli_path,
    resolve_workspace_config,
    save_session_state,
    workspace_init,
)

read_session_state = load_session_state
write_session_state = save_session_state


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


def workspace_show(client, mount: str, *, workspace_config: dict[str, str]):
    return {"workspace": workspace_config, "session": load_session_state(client, mount, workspace_config=workspace_config)}


def workspace_pwd(client, mount: str, *, workspace_config: dict[str, str]):
    state = load_session_state(client, mount, workspace_config=workspace_config)
    return {"cwd": state["cwd"], "mount": mount}


def workspace_cd(client, mount: str, path: str, *, workspace_config: dict[str, str]):
    target = resolve_cli_path(path, cwd=load_session_state(client, mount, workspace_config=workspace_config)["cwd"])
    updated = save_session_state(client, mount, {"cwd": target, "mount": mount}, workspace_config=workspace_config)
    return {"cwd": updated["cwd"], "mount": mount}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tpfs", description="Filesystem-shaped turbopuffer CLI.")
    parser.add_argument("--api-key", dest="api_key")
    parser.add_argument("--region")
    parser.add_argument("--base-url", dest="base_url")
    parser.add_argument("--workspace-config")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("mounts")

    workspace_show_parser = subparsers.add_parser("workspace-show")
    workspace_show_parser.add_argument("mount", nargs="?", default="documents")
    workspace_show_parser.add_argument("--bundle-root")

    workspace_init_parser = subparsers.add_parser("workspace-init")
    workspace_init_parser.add_argument("mount", nargs="?", default="documents")
    workspace_init_parser.add_argument("--bundle-root")

    pwd_parser = subparsers.add_parser("pwd")
    pwd_parser.add_argument("mount", nargs="?", default="documents")
    pwd_parser.add_argument("--bundle-root")

    cd_parser = subparsers.add_parser("cd")
    cd_parser.add_argument("mount", nargs="?", default="documents")
    cd_parser.add_argument("path")
    cd_parser.add_argument("--bundle-root")

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

    bundle_parser = subparsers.add_parser("bundle-show")
    bundle_parser.add_argument("local_root")

    bundle_seed_parser = subparsers.add_parser("bundle-seed")
    bundle_seed_parser.add_argument("mount")
    bundle_seed_parser.add_argument("local_root")
    bundle_seed_parser.add_argument("--mount-root", default="/")

    bundle_validate_parser = subparsers.add_parser("bundle-validate")
    bundle_validate_parser.add_argument("mount")
    bundle_validate_parser.add_argument("local_root")

    prompt_parser = subparsers.add_parser("bundle-prompt")
    prompt_parser.add_argument("local_root")

    dogfood_parser = subparsers.add_parser("dogfood")
    dogfood_parser.add_argument("--mount-prefix", default="dogfood")
    dogfood_parser.add_argument("--seed", type=int, default=1)
    dogfood_parser.add_argument("--steps", type=int, default=50)
    dogfood_parser.add_argument("--check-every", type=int, default=5)
    dogfood_parser.add_argument("--keep-on-fail", action="store_true")
    dogfood_parser.add_argument("--keep-always", action="store_true")
    dogfood_parser.add_argument("--no-cleanup", dest="cleanup", action="store_false")
    dogfood_parser.set_defaults(cleanup=True)
    dogfood_parser.add_argument("--artifact-dir")

    return parser


def run_cli(args: argparse.Namespace):
    client = _client_from_args(args)
    command = args.command
    workspace_config = resolve_workspace_config(
        config_path=getattr(args, "workspace_config", None),
        bundle_root=getattr(args, "bundle_root", None),
    )

    if command == "mounts":
        return list_mounts(client)
    if command == "workspace-show":
        return workspace_show(client, args.mount, workspace_config=workspace_config)
    if command == "workspace-init":
        return workspace_init(client, args.mount, workspace_config=workspace_config)
    if command == "pwd":
        return {"cwd": workspace_pwd(client, args.mount, workspace_config=workspace_config), "mount": args.mount}
    if command == "cd":
        updated = workspace_cd(client, args.mount, args.path, workspace_config=workspace_config)
        return {"cwd": updated["cwd"], "mount": args.mount}
    if command == "stat":
        return stat(client, args.mount, resolve_cli_path(args.path, cwd=workspace_pwd(client, args.mount, workspace_config=workspace_config)))
    if command == "ls":
        cwd = workspace_pwd(client, args.mount, workspace_config=workspace_config)
        return ls(client, args.mount, resolve_cli_path(args.path, cwd=cwd), limit=args.limit)
    if command == "find":
        cwd = workspace_pwd(client, args.mount, workspace_config=workspace_config)
        return find(
            client,
            args.mount,
            resolve_cli_path(args.root, cwd=cwd),
            glob=args.glob,
            kind=args.kind,
            ignore_case=args.ignore_case,
            limit=args.limit,
        )
    if command == "cat":
        cwd = workspace_pwd(client, args.mount, workspace_config=workspace_config)
        return cat(client, args.mount, resolve_cli_path(args.path, cwd=cwd))
    if command == "read-text":
        cwd = workspace_pwd(client, args.mount, workspace_config=workspace_config)
        return read_text(client, args.mount, resolve_cli_path(args.path, cwd=cwd))
    if command == "read-bytes":
        cwd = workspace_pwd(client, args.mount, workspace_config=workspace_config)
        data = read_bytes(client, args.mount, resolve_cli_path(args.path, cwd=cwd))
        if args.out:
            Path(args.out).write_bytes(data)
            return {"path": resolve_cli_path(args.path, cwd=cwd), "out": args.out, "bytes_written": len(data), "size_bytes": len(data)}
        return {"path": resolve_cli_path(args.path, cwd=cwd), "size_bytes": len(data), "blob_b64": base64.b64encode(data).decode("ascii")}
    if command == "grep":
        cwd = workspace_pwd(client, args.mount, workspace_config=workspace_config)
        return grep(
            client,
            args.mount,
            resolve_cli_path(args.root, cwd=cwd),
            args.pattern,
            glob=args.glob,
            ignore_case=args.ignore_case,
            limit=args.limit,
        )
    if command == "mkdir":
        cwd = workspace_pwd(client, args.mount, workspace_config=workspace_config)
        return mkdir(client, args.mount, resolve_cli_path(args.path, cwd=cwd))
    if command == "put-text":
        cwd = workspace_pwd(client, args.mount, workspace_config=workspace_config)
        return put_text(client, args.mount, resolve_cli_path(args.path, cwd=cwd), _load_text_input(args), mime=args.mime)
    if command == "put-bytes":
        cwd = workspace_pwd(client, args.mount, workspace_config=workspace_config)
        return put_bytes(client, args.mount, resolve_cli_path(args.path, cwd=cwd), _load_bytes_input(args), mime=args.mime)
    if command == "rm":
        cwd = workspace_pwd(client, args.mount, workspace_config=workspace_config)
        return rm(client, args.mount, resolve_cli_path(args.path, cwd=cwd), recursive=args.recursive)
    if command == "ingest":
        return ingest_directory(
            client,
            args.mount,
            args.local_root,
            mount_root=args.mount_root,
            batch_size=args.batch_size,
        )
    if command == "bundle-show":
        spec = load_bundle_spec(args.local_root)
        return {
            "spec": spec,
            "entrypoint": bundle_entrypoint(args.local_root),
            "allowed_outputs": list_allowed_outputs(args.local_root),
            "workspace": bundle_config(args.local_root),
        }
    if command == "bundle-seed":
        return seed_bundle(client, args.mount, args.local_root, mount_root=args.mount_root)
    if command == "bundle-validate":
        missing = validate_bundle_outputs(client, args.mount, args.local_root)
        return {"missing": missing, "ok": not missing}
    if command == "bundle-prompt":
        return {"prompt": bundle_task_prompt(args.local_root)}
    if command == "dogfood":
        return run_dogfood(
            api_key=args.api_key,
            region=args.region,
            base_url=args.base_url,
            mount_prefix=args.mount_prefix,
            seed=args.seed,
            steps=args.steps,
            check_every=args.check_every,
            keep_on_fail=args.keep_on_fail,
            keep_always=args.keep_always,
            cleanup=args.cleanup,
            artifact_dir=args.artifact_dir,
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
