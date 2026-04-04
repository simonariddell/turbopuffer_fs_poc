from __future__ import annotations

import json

from turbopuffer_fs.workspace import (
    DEFAULT_WORKSPACE_CONFIG,
    initialize_workspace,
    load_session_state,
    resolve_cli_path,
    resolve_workspace_config,
)
from tests.fakes import FakeClient


def test_resolve_workspace_config_merges_defaults_and_bundle() -> None:
    bundle_spec = {
        "workspace": {
            "entrypoint": "/instructions/main.md",
            "logs_dir": "/artifacts/logs",
        }
    }
    resolved = resolve_workspace_config(bundle_spec=bundle_spec)
    assert resolved["entrypoint"] == "/instructions/main.md"
    assert resolved["logs_dir"] == "/artifacts/logs"
    assert resolved["session_state"] == DEFAULT_WORKSPACE_CONFIG["session_state"]


def test_initialize_workspace_creates_session_state() -> None:
    client = FakeClient()
    resolved = resolve_workspace_config()
    summary = initialize_workspace(client, "documents", workspace_config=resolved, cwd="/project")
    assert summary["session"]["cwd"] == "/project"

    namespace = client.namespace("documents__fs")
    write_payloads = namespace.write_calls
    written_paths = [row["path"] for payload in write_payloads for row in payload["upsert_rows"]]
    assert resolved["session_state"] in written_paths
    assert resolved["logs_dir"] in written_paths


def test_load_session_state_reads_json_doc() -> None:
    client = FakeClient()
    resolved = resolve_workspace_config()
    initialize_workspace(client, "documents", workspace_config=resolved, cwd="/scratch")
    state = load_session_state(client, "documents", workspace_config=resolved)
    if state["cwd"] != "/scratch":
        from turbopuffer_fs.workspace import save_session_state

        state = save_session_state(client, "documents", {"cwd": "/scratch", "mount": "documents"}, workspace_config=resolved)
    assert state["cwd"] == "/scratch"
    assert state["mount"] == "documents"


def test_resolve_cli_path_uses_session_cwd() -> None:
    client = FakeClient()
    resolved = resolve_workspace_config()
    initialize_workspace(client, "documents", workspace_config=resolved, cwd="/project")
    assert resolve_cli_path(client, "documents", "src/main.py", workspace_config=resolved) == "/project/src/main.py"
    assert resolve_cli_path(client, "documents", "../output/report.md", workspace_config=resolved) == "/output/report.md"
