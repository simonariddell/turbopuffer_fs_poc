"""Workspace conventions and durable session state for stateless agents."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from posixpath import join as posix_join

from .live import mount_namespace, put_text, read_text, stat
from .paths import normalize_path, parent_path


DEFAULT_WORKSPACE_CONFIG = {
    "entrypoint": "/TASK.md",
    "bundle_manifest": "/bundle.json",
    "session_state": "/state/session.json",
    "logs_dir": "/logs",
    "output_dir": "/output",
    "scratch_dir": "/scratch",
    "project_dir": "/project",
    "input_dir": "/input",
}


def default_workspace_config() -> dict[str, str]:
    return dict(DEFAULT_WORKSPACE_CONFIG)


def merge_workspace_config(*configs: dict[str, object] | None) -> dict[str, str]:
    merged: dict[str, str] = default_workspace_config()
    for config in configs:
        if not config:
            continue
        for key, value in config.items():
            if value is None:
                continue
            merged[str(key)] = str(value)
    return validate_workspace_config(merged)


def validate_workspace_config(config: dict[str, object]) -> dict[str, str]:
    validated: dict[str, str] = {}
    for key, value in config.items():
        validated[str(key)] = normalize_path(str(value))
    return validated


def load_workspace_config_file(path: str | Path | None) -> dict[str, str] | None:
    if path is None:
        return None
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(str(file_path))
    return validate_workspace_config(json.loads(file_path.read_text(encoding="utf-8")))


def resolve_workspace_config(
    config_path: str | Path | None = None,
    bundle_root: str | Path | None = None,
    *,
    deployment_config: dict[str, object] | None = None,
    bundle_spec: dict[str, object] | None = None,
    bundle_workspace: dict[str, object] | None = None,
    overrides: dict[str, object] | None = None,
) -> dict[str, str]:
    deployment_values = load_workspace_config_file(config_path)
    workspace_from_bundle = bundle_workspace
    bundle_values = bundle_spec
    if bundle_root is not None and bundle_values is None:
        bundle_path = Path(bundle_root) / "bundle.json"
        bundle_values = json.loads(bundle_path.read_text(encoding="utf-8"))
    if workspace_from_bundle is None and bundle_spec is not None:
        workspace_from_bundle = bundle_spec.get("workspace")
    if workspace_from_bundle is None and bundle_values is not None:
        workspace_from_bundle = bundle_values.get("workspace")
    return merge_workspace_config(default_workspace_config(), deployment_values, deployment_config, workspace_from_bundle, overrides)


def session_state_doc(
    *,
    mount: str,
    cwd: str,
    config: dict[str, str] | None = None,
    bundle_id: str | None = None,
) -> dict[str, object]:
    workspace = default_workspace_config() if config is None else dict(config)
    doc: dict[str, object] = {
        "cwd": normalize_path(cwd),
        "mount": mount,
        "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    if bundle_id is not None:
        doc["bundle_id"] = bundle_id
    doc["path"] = workspace["session_state"]
    return doc


def load_session_state(client, mount: str, *, workspace_config: dict[str, str]) -> dict[str, object]:
    path = workspace_config["session_state"]
    value = stat(client, mount, path)
    if value is None:
        default_cwd = workspace_config.get("project_dir", "/")
        return session_state_doc(mount=mount, cwd=default_cwd, config=workspace_config)
    namespace = mount_namespace(mount)
    query = client.namespace(namespace).query(
        filters=("path", "Eq", path),
        rank_by=("path", "asc"),
        limit=1,
        include_attributes=True,
    )
    rows = getattr(query, "rows", None) or query.get("rows", [])
    row = rows[0]
    text = row.get("text") if isinstance(row, dict) else getattr(row, "text", None)
    payload = json.loads(str(text))
    payload["path"] = path
    return payload


def save_session_state(client, mount: str, state: dict[str, object], *, workspace_config: dict[str, str]) -> dict[str, object]:
    payload = {
        "cwd": normalize_path(str(state["cwd"])),
        "mount": str(state.get("mount", mount)),
        "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    if "bundle_id" in state:
        payload["bundle_id"] = state["bundle_id"]
    path = workspace_config["session_state"]
    put_text(client, mount, path, json.dumps(payload, indent=2, sort_keys=True), mime="application/json")
    saved = dict(payload)
    saved["path"] = path
    return saved


def resolve_user_path(user_path: str | None, *, cwd: str) -> str:
    if user_path in {None, ""}:
        return normalize_path(cwd)
    value = str(user_path)
    if value.startswith("/"):
        return normalize_path(value)
    if value in {".", "./"}:
        return normalize_path(cwd)
    segments = [segment for segment in value.split("/") if segment not in {"", "."}]
    current = normalize_path(cwd)
    for segment in segments:
        if segment == "..":
            current = parent_path(current) or "/"
            continue
        current = normalize_path(posix_join(current, segment))
    return current


def workspace_init(
    client,
    mount: str,
    *,
    workspace_config: dict[str, str],
    bundle_id: str | None = None,
    cwd: str | None = None,
) -> dict[str, object]:
    from .live import mkdir

    created = []
    for key in ["logs_dir", "output_dir", "scratch_dir", "project_dir", "input_dir"]:
        path = workspace_config[key]
        mkdir(client, mount, path)
        created.append(path)

    session_parent = parent_path(workspace_config["session_state"])
    if session_parent is not None:
        mkdir(client, mount, session_parent)

    default_cwd = workspace_config.get("project_dir", "/") if cwd is None else normalize_path(cwd)
    state = session_state_doc(mount=mount, cwd=default_cwd, config=workspace_config, bundle_id=bundle_id)
    save_session_state(client, mount, state, workspace_config=workspace_config)
    return {
        "mount": mount,
        "namespace": mount_namespace(mount),
        "created": created,
        "session_state": workspace_config["session_state"],
        "cwd": default_cwd,
        "session": state,
    }


def initialize_workspace(
    client,
    mount: str,
    *,
    workspace_config: dict[str, str],
    bundle_id: str | None = None,
    cwd: str | None = None,
) -> dict[str, object]:
    return workspace_init(client, mount, workspace_config=workspace_config, bundle_id=bundle_id, cwd=cwd)


def read_session_state(client, mount: str, *, workspace_config: dict[str, str]) -> dict[str, object]:
    return load_session_state(client, mount, workspace_config=workspace_config)


def write_session_state(
    client,
    mount: str,
    state: dict[str, object] | None = None,
    *,
    cwd: str | None = None,
    bundle_id: str | None = None,
    workspace_config: dict[str, str],
) -> dict[str, object]:
    payload = dict(state or {})
    if cwd is not None:
        payload["cwd"] = cwd
    if bundle_id is not None:
        payload["bundle_id"] = bundle_id
    if "cwd" not in payload:
        raise ValueError("session state requires cwd")
    return save_session_state(client, mount, payload, workspace_config=workspace_config)


def resolve_cli_path(
    client_or_path,
    mount: str | None = None,
    user_path: str | None = None,
    *,
    workspace_config: dict[str, str] | None = None,
    cwd: str | None = None,
) -> str:
    if mount is None and user_path is None:
        raise TypeError("resolve_cli_path requires either explicit path+cowd or client+mount+path")
    if user_path is None:
        user_path = str(client_or_path)
        if cwd is None:
            raise TypeError("cwd is required when client is not provided")
        return resolve_user_path(user_path, cwd=cwd)
    if workspace_config is None:
        raise TypeError("workspace_config is required when resolving against session state")
    state = read_session_state(client_or_path, str(mount), workspace_config=workspace_config)
    return resolve_user_path(user_path, cwd=str(state["cwd"]))


initialize_workspace = workspace_init
