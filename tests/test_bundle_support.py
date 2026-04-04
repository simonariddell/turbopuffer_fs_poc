from __future__ import annotations

from pathlib import Path

from turbopuffer_fs.dogfood import (
    bundle_entrypoint,
    bundle_task_prompt,
    list_allowed_outputs,
    load_bundle_spec,
)


def bundle_path(name: str) -> Path:
    return Path("/workspace/examples/task-bundles") / name


def test_load_bundle_spec() -> None:
    spec = load_bundle_spec(bundle_path("csv-cleaning-v1"))
    assert spec["id"] == "csv-cleaning-v1"
    assert "/output/sales.cleaned.csv" in spec["allowed_outputs"]


def test_bundle_helpers() -> None:
    local_root = bundle_path("code-maintenance-v1")
    assert bundle_entrypoint(local_root) == "/TASK.md"
    outputs = list_allowed_outputs(local_root)
    assert "/logs/run.jsonl" in outputs
    prompt = bundle_task_prompt(local_root)
    assert "Allowed outputs:" in prompt
    assert "code-maintenance-v1" in prompt
