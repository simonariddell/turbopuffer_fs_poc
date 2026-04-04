"""Tiny live adapter: env, client, mount naming."""

from __future__ import annotations

import os
from functools import partial


mount_namespace = lambda mount: f"{mount}__fs"


def load_env(env: dict[str, str] | None = None) -> dict[str, str | None]:
    source = os.environ if env is None else env
    return {
        "api_key": source.get("TURBOPUFFER_API_KEY"),
        "region": source.get("TURBOPUFFER_REGION"),
        "base_url": source.get("TURBOPUFFER_BASE_URL"),
    }


def make_client(env: dict[str, str] | None = None, **overrides):
    import turbopuffer

    values = {k: v for k, v in load_env(env).items() if v}
    values.update({k: v for k, v in overrides.items() if v is not None})
    return turbopuffer.Turbopuffer(**values)


def namespace_handle(client, mount: str):
    return client.namespace(mount_namespace(mount))


def list_mounts(client, *, suffix: str = "__fs") -> list[str]:
    namespace_ids = map(lambda ns: getattr(ns, "id", None) or ns["id"], client.namespaces())
    return sorted(name[: -len(suffix)] for name in namespace_ids if name.endswith(suffix))
