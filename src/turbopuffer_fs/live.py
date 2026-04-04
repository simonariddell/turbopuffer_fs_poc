"""Thin live wrappers over plans, runtime, and finalizers."""

from __future__ import annotations

from .fs import (
    cat_plan,
    find_plan,
    grep_plan,
    head_plan,
    ls_plan,
    mkdir_plan,
    put_bytes_plan,
    put_text_plan,
    read_bytes_plan,
    read_text_plan,
    rm_plan,
    stat_plan,
    tail_plan,
)
from .runtime import run


MOUNT_SUFFIX = "__fs"


def mount_namespace(mount: str) -> str:
    if not isinstance(mount, str):
        raise TypeError("mount must be a string")
    if mount == "":
        raise ValueError("mount must not be empty")
    if "/" in mount:
        raise ValueError(f"mount must not contain '/': {mount!r}")
    return f"{mount}{MOUNT_SUFFIX}"


def make_client(*, api_key: str | None = None, region: str | None = None, base_url: str | None = None, **kwargs):
    import turbopuffer

    values = dict(kwargs)
    if api_key is not None:
        values["api_key"] = api_key
    if region is not None:
        values["region"] = region
    if base_url is not None:
        values["base_url"] = base_url
    return turbopuffer.Turbopuffer(**values)


def _mounts_plan(*, suffix: str = MOUNT_SUFFIX) -> dict[str, object]:
    return {
        "namespace": "",
        "steps": [{"kind": "namespaces", "name": "namespaces", "payload": {}}],
        "finalize": "mounts",
        "context": {"suffix": suffix},
    }


def list_mounts(client, *, suffix: str = MOUNT_SUFFIX) -> list[str]:
    return run(client, _mounts_plan(suffix=suffix))


def stat(client, mount: str, path: str):
    return run(client, stat_plan(mount_namespace(mount), path))


def ls(client, mount: str, path: str = "/", *, limit: int | None = None):
    return run(client, ls_plan(mount_namespace(mount), path, limit=limit))


def find(
    client,
    mount: str,
    root: str = "/",
    *,
    glob: str | None = None,
    kind: str | None = None,
    ignore_case: bool = False,
    limit: int | None = None,
):
    return run(
        client,
        find_plan(
            mount_namespace(mount),
            root,
            glob=glob,
            kind=kind,
            ignore_case=ignore_case,
            limit=limit,
        ),
    )


def cat(client, mount: str, path: str) -> str:
    return run(client, cat_plan(mount_namespace(mount), path))


def head(client, mount: str, path: str, n: int = 10) -> list[str]:
    return run(client, head_plan(mount_namespace(mount), path, n=n))


def tail(client, mount: str, path: str, n: int = 10) -> list[str]:
    return run(client, tail_plan(mount_namespace(mount), path, n=n))


def grep(
    client,
    mount: str,
    root: str,
    pattern: str,
    *,
    ignore_case: bool = False,
    glob: str | None = None,
    limit: int | None = None,
):
    return run(
        client,
        grep_plan(
            mount_namespace(mount),
            root,
            pattern,
            ignore_case=ignore_case,
            glob=glob,
            limit=limit,
        ),
    )


def read_text(client, mount: str, path: str) -> str:
    return run(client, read_text_plan(mount_namespace(mount), path))


def read_bytes(client, mount: str, path: str) -> bytes:
    return run(client, read_bytes_plan(mount_namespace(mount), path))


def mkdir(client, mount: str, path: str):
    return run(client, mkdir_plan(mount_namespace(mount), path))


def put_text(client, mount: str, path: str, text: str, *, mime: str | None = None):
    return run(client, put_text_plan(mount_namespace(mount), path, text, mime=mime))


def put_bytes(client, mount: str, path: str, data: bytes, *, mime: str | None = None):
    return run(client, put_bytes_plan(mount_namespace(mount), path, data, mime=mime))


def rm(client, mount: str, path: str, *, recursive: bool = False):
    return run(client, rm_plan(mount_namespace(mount), path, recursive=recursive))
