"""Seeded dogfood harness for live turbopuffer filesystem testing."""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import random
import tempfile
import uuid
from collections import Counter
from pathlib import Path
from typing import Callable

from . import (
    find,
    grep,
    ingest_directory,
    ls,
    make_client,
    mkdir,
    mount_namespace,
    put_bytes,
    put_text,
    read_bytes,
    read_text,
    rm,
    stat,
)
from .paths import basename, normalize_path, parent_path
from .workspace import initialize_workspace, resolve_workspace_config


DogfoodOp = dict[str, object]
ModelState = dict[str, dict[str, object]]
BundleSpec = dict[str, object]


TEXT_PAYLOADS = [
    "hello world\n",
    "oauth token exchange\ncallback success\n",
    "alpha\nbeta\ngamma\n",
    "notes\nline two\nline three\n",
]

BINARY_PAYLOADS = [
    b"\x00\x01\x02",
    b"\x10\x20\x30\x40",
    bytes(range(16)),
]


def load_bundle_spec(local_root: str | Path) -> BundleSpec:
    root = Path(local_root)
    spec_path = root / "bundle.json"
    return json.loads(spec_path.read_text(encoding="utf-8"))


def _bundle_task_text(local_root: str | Path) -> str:
    root = Path(local_root)
    return (root / "TASK.md").read_text(encoding="utf-8")


def bundle_config(local_root: str | Path) -> dict[str, str]:
    spec = load_bundle_spec(local_root)
    return resolve_workspace_config(bundle_spec=spec)


def _default_run_log_path(local_root: str | Path) -> str:
    spec = load_bundle_spec(local_root)
    for path in spec.get("allowed_outputs", []):
        if str(path).endswith("run.jsonl"):
            return str(path)
    return "/logs/run.jsonl"


def _default_summary_path(local_root: str | Path) -> str:
    spec = load_bundle_spec(local_root)
    for path in spec.get("allowed_outputs", []):
        if str(path).endswith("summary.md"):
            return str(path)
    return "/logs/summary.md"


def list_allowed_outputs(local_root: str | Path) -> list[str]:
    spec = load_bundle_spec(local_root)
    return [normalize_path(str(path)) for path in spec.get("allowed_outputs", [])]


def bundle_entrypoint(local_root: str | Path) -> str:
    spec = load_bundle_spec(local_root)
    return normalize_path(str(spec.get("entrypoint", "/TASK.md")))


def bundle_task_prompt(local_root: str | Path) -> str:
    spec = load_bundle_spec(local_root)
    allowed_outputs = "\n".join(f"- {path}" for path in list_allowed_outputs(local_root))
    workspace = resolve_workspace_config(bundle_spec=spec)
    return (
        "You are working inside a filesystem-shaped workspace backed by turbopuffer.\n\n"
        "Rules:\n"
        "- Use the filesystem interface for all persistent reads and writes.\n"
        "- Read /bundle.json and /TASK.md first.\n"
        f"- Log every meaningful action to {workspace['logs_dir']}/run.jsonl.\n"
        f"- Write a final summary to {workspace['logs_dir']}/summary.md.\n"
        "- Only write outputs to allowed output locations.\n\n"
        f"Bundle ID: {spec.get('id', 'unknown')}\n"
        f"Entrypoint: {bundle_entrypoint(local_root)}\n"
        f"Session state file: {workspace['session_state']}\n"
        "Allowed outputs:\n"
        f"{allowed_outputs}\n\n"
        "Task:\n"
        f"{_bundle_task_text(local_root)}"
    )


def validate_bundle_outputs(client, mount: str, local_root: str | Path) -> list[str]:
    missing = []
    for path in list_allowed_outputs(local_root):
        if stat(client, mount, path) is None:
            missing.append(path)
    return missing


def seed_bundle(client, mount: str, local_root: str | Path, *, mount_root: str = "/") -> dict[str, object]:
    summary = ingest_directory(client, mount, local_root, mount_root=mount_root)
    spec = load_bundle_spec(local_root)
    workspace = resolve_workspace_config(bundle_spec=spec)
    initialize_workspace(client, mount, workspace_config=workspace, cwd=workspace["project_dir"])
    return {
        **summary,
        "workspace": workspace,
    }


def new_model_state() -> ModelState:
    return {"/": {"kind": "dir"}}


def _children_of(model: ModelState, path: str) -> list[str]:
    value = normalize_path(path)
    return sorted(
        candidate
        for candidate in model
        if candidate != value and parent_path(candidate) == value
    )


def _descendants_of(model: ModelState, path: str) -> list[str]:
    value = normalize_path(path)
    return sorted(
        candidate
        for candidate in model
        if candidate == value or candidate.startswith(f"{value.rstrip('/')}/")
    )


def _ensure_parent_dirs(model: ModelState, path: str) -> None:
    current = parent_path(path)
    stack: list[str] = []
    while current is not None and current not in model:
        stack.append(current)
        current = parent_path(current)
    for directory in reversed(stack):
        model[directory] = {"kind": "dir"}


def apply_model_operation(model: ModelState, operation: DogfoodOp) -> None:
    op = str(operation["op"])
    path = normalize_path(str(operation["path"]))
    if op == "mkdir":
        _ensure_parent_dirs(model, path)
        model[path] = {"kind": "dir"}
        return
    if op == "put_text":
        _ensure_parent_dirs(model, path)
        text = str(operation["text"])
        model[path] = {"kind": "file", "is_text": 1, "text": text, "bytes": text.encode("utf-8")}
        return
    if op == "put_bytes":
        _ensure_parent_dirs(model, path)
        data = bytes(operation["data"])
        model[path] = {"kind": "file", "is_text": 0, "bytes": data}
        return
    if op == "rm":
        recursive = bool(operation.get("recursive", False))
        if path not in model:
            return
        if model[path]["kind"] == "dir":
            children = _children_of(model, path)
            if children and not recursive:
                raise OSError(f"directory not empty: {path}")
            for candidate in sorted(_descendants_of(model, path), reverse=True):
                if candidate != "/":
                    model.pop(candidate, None)
            return
        model.pop(path, None)
        return
    raise ValueError(f"unsupported model operation: {op!r}")


def model_stat(model: ModelState, path: str) -> dict[str, object] | None:
    value = normalize_path(path)
    if value not in model:
        return None
    row = dict(model[value])
    row["path"] = value
    return row


def model_ls(model: ModelState, path: str) -> list[dict[str, object]]:
    value = normalize_path(path)
    if value not in model:
        raise FileNotFoundError(value)
    if model[value]["kind"] != "dir":
        raise NotADirectoryError(value)
    return [{"path": child, "kind": model[child]["kind"]} for child in _children_of(model, value)]


def model_find(model: ModelState, root: str) -> list[dict[str, object]]:
    value = normalize_path(root)
    if value not in model:
        raise FileNotFoundError(value)
    if model[value]["kind"] == "file":
        return [{"path": value, "kind": "file"}]
    return [{"path": child, "kind": model[child]["kind"]} for child in _descendants_of(model, value)]


def _model_read_text(model: ModelState, path: str) -> str:
    value = normalize_path(path)
    if value not in model:
        raise FileNotFoundError(value)
    row = model[value]
    if row["kind"] == "dir":
        raise IsADirectoryError(value)
    if row["is_text"] != 1:
        raise ValueError(f"binary file: {value}")
    return str(row["text"])


def _model_read_bytes(model: ModelState, path: str) -> bytes:
    value = normalize_path(path)
    if value not in model:
        raise FileNotFoundError(value)
    row = model[value]
    if row["kind"] == "dir":
        raise IsADirectoryError(value)
    return bytes(row["bytes"])


def expected_grep_matches(
    model: ModelState,
    *,
    root: str,
    pattern: str,
    ignore_case: bool = False,
    glob: str | None = None,
) -> list[dict[str, object]]:
    rows = []
    needle = pattern.casefold() if ignore_case else pattern
    for row in model_find(model, root):
        path = row["path"]
        state = model[path]
        if state["kind"] != "file" or state.get("is_text") != 1:
            continue
        if glob and not fnmatch.fnmatch(basename(path), glob):
            continue
        for index, line in enumerate(str(state["text"]).splitlines(), start=1):
            haystack = line.casefold() if ignore_case else line
            if needle in haystack:
                rows.append({"path": path, "line_number": index, "line": line})
    return rows


def _existing_dirs(model: ModelState) -> list[str]:
    return sorted(path for path, row in model.items() if row["kind"] == "dir")


def _existing_files(model: ModelState) -> list[str]:
    return sorted(path for path, row in model.items() if row["kind"] == "file")


def _random_name(rng: random.Random, *, suffix: str = "") -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    stem = "".join(rng.choice(alphabet) for _ in range(rng.randint(4, 8)))
    return f"{stem}{suffix}"


def _new_path_under(model: ModelState, rng: random.Random, *, suffix: str = "") -> str:
    parent = rng.choice(_existing_dirs(model))
    return normalize_path(f"{parent.rstrip('/')}/{_random_name(rng, suffix=suffix)}")


def _choose_operation(model: ModelState, rng: random.Random) -> str:
    options: list[tuple[str, int]] = [
        ("mkdir", 10),
        ("put_text", 18),
        ("put_bytes", 12),
        ("stat", 8),
        ("ls", 8),
        ("find", 8),
        ("read_text", 8),
        ("read_bytes", 6),
        ("grep", 8),
        ("rm", 8),
        ("ingest", 2),
    ]
    names = [name for name, _ in options]
    weights = [weight for _, weight in options]
    return rng.choices(names, weights=weights, k=1)[0]


def _make_temp_tree(rng: random.Random) -> Path:
    tempdir = Path(tempfile.mkdtemp(prefix="tpfs-dogfood-"))
    (tempdir / "notes.txt").write_text(rng.choice(TEXT_PAYLOADS), encoding="utf-8")
    nested = tempdir / "nested"
    nested.mkdir()
    (nested / "data.bin").write_bytes(rng.choice(BINARY_PAYLOADS))
    return tempdir


def _verify_sampled_state(client, mount: str, model: ModelState, rng: random.Random) -> None:
    sample_paths = ["/"]
    candidates = sorted(model.keys())
    if candidates:
        sample_paths.extend(rng.sample(candidates, k=min(5, len(candidates))))
    for path in dict.fromkeys(sample_paths):
        expected = model_stat(model, path)
        actual = stat(client, mount, path)
        if expected is None:
            assert actual is None, f"expected missing path {path}, got {actual!r}"
            continue
        assert actual is not None, f"expected path {path} to exist"
        assert actual["path"] == path
        assert actual["kind"] == expected["kind"]

    root_listing = ls(client, mount, "/")
    assert {row["path"] for row in root_listing} == {row["path"] for row in model_ls(model, "/")}

    real_rows = find(client, mount, "/")
    assert [row["path"] for row in real_rows] == [row["path"] for row in model_find(model, "/")]


def _op_mkdir(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    path = _new_path_under(model, rng)
    apply_model_operation(model, {"op": "mkdir", "path": path})
    result = mkdir(client, mount, path)
    return {"op": "mkdir", "path": path, "result": result}


def _op_put_text(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    path = _new_path_under(model, rng, suffix=".txt")
    text = rng.choice(TEXT_PAYLOADS)
    apply_model_operation(model, {"op": "put_text", "path": path, "text": text})
    result = put_text(client, mount, path, text)
    return {"op": "put_text", "path": path, "text": text, "result": result}


def _op_put_bytes(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    path = _new_path_under(model, rng, suffix=".bin")
    data = rng.choice(BINARY_PAYLOADS)
    apply_model_operation(model, {"op": "put_bytes", "path": path, "data": data})
    result = put_bytes(client, mount, path, data)
    return {"op": "put_bytes", "path": path, "bytes_len": len(data), "result": result}


def _op_stat(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    path = rng.choice(sorted(model))
    expected = model_stat(model, path)
    actual = stat(client, mount, path)
    assert (actual is None) == (expected is None)
    return {"op": "stat", "path": path, "result": actual}


def _op_ls(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    path = rng.choice(_existing_dirs(model))
    expected = model_ls(model, path)
    actual = ls(client, mount, path)
    assert [row["path"] for row in actual] == [row["path"] for row in expected]
    return {"op": "ls", "path": path, "count": len(actual)}


def _op_find(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    path = rng.choice(sorted(model))
    expected = model_find(model, path)
    actual = find(client, mount, path)
    assert [row["path"] for row in actual] == [row["path"] for row in expected]
    return {"op": "find", "path": path, "count": len(actual)}


def _op_read_text(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    candidates = [path for path in _existing_files(model) if model[path]["is_text"] == 1]
    if not candidates:
        return _op_put_text(client, mount, model, rng)
    path = rng.choice(candidates)
    expected = _model_read_text(model, path)
    actual = read_text(client, mount, path)
    assert actual == expected
    return {"op": "read_text", "path": path}


def _op_read_bytes(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    candidates = _existing_files(model)
    if not candidates:
        return _op_put_bytes(client, mount, model, rng)
    path = rng.choice(candidates)
    expected = _model_read_bytes(model, path)
    actual = read_bytes(client, mount, path)
    assert actual == expected
    return {"op": "read_bytes", "path": path, "bytes_len": len(actual)}


def _op_grep(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    candidates = [path for path in _existing_files(model) if model[path]["is_text"] == 1]
    if not candidates:
        return _op_put_text(client, mount, model, rng)
    root = rng.choice(_existing_dirs(model))
    pattern = "oauth"
    expected = expected_grep_matches(model, root=root, pattern=pattern, ignore_case=True)
    actual = grep(client, mount, root, pattern, ignore_case=True)
    assert actual == expected
    return {"op": "grep", "root": root, "count": len(actual)}


def _op_rm(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    candidates = [path for path in sorted(model) if path != "/"]
    if not candidates:
        return _op_put_text(client, mount, model, rng)
    path = rng.choice(candidates)
    recursive = model[path]["kind"] == "dir"
    apply_model_operation(model, {"op": "rm", "path": path, "recursive": recursive})
    result = rm(client, mount, path, recursive=recursive)
    return {"op": "rm", "path": path, "recursive": recursive, "result": result}


def _op_ingest(client, mount: str, model: ModelState, rng: random.Random) -> DogfoodOp:
    tempdir = _make_temp_tree(rng)
    mount_root = _new_path_under(model, rng)
    apply_model_operation(model, {"op": "mkdir", "path": mount_root})
    apply_model_operation(
        model,
        {"op": "put_text", "path": f"{mount_root}/notes.txt", "text": (tempdir / "notes.txt").read_text(encoding="utf-8")},
    )
    apply_model_operation(model, {"op": "mkdir", "path": f"{mount_root}/nested"})
    apply_model_operation(
        model,
        {"op": "put_bytes", "path": f"{mount_root}/nested/data.bin", "data": (tempdir / "nested/data.bin").read_bytes()},
    )
    result = ingest_directory(client, mount, tempdir, mount_root=mount_root)
    return {"op": "ingest", "mount_root": mount_root, "row_count": result["row_count"]}


OPERATIONS: dict[str, Callable] = {
    "mkdir": _op_mkdir,
    "put_text": _op_put_text,
    "put_bytes": _op_put_bytes,
    "stat": _op_stat,
    "ls": _op_ls,
    "find": _op_find,
    "read_text": _op_read_text,
    "read_bytes": _op_read_bytes,
    "grep": _op_grep,
    "rm": _op_rm,
    "ingest": _op_ingest,
}


def run_dogfood(
    *,
    api_key: str | None = None,
    region: str | None = None,
    base_url: str | None = None,
    mount_prefix: str = "dogfood",
    seed: int = 1,
    steps: int = 50,
    check_every: int = 5,
    keep_on_fail: bool = False,
    keep_always: bool = False,
    cleanup: bool = True,
    artifact_dir: str | None = None,
) -> dict[str, object]:
    if steps < 1:
        raise ValueError("steps must be positive")
    if check_every < 1:
        raise ValueError("check_every must be positive")

    client = make_client(api_key=api_key, region=region, base_url=base_url)
    rng = random.Random(seed)
    mount = f"{mount_prefix}{seed:x}{uuid.uuid4().hex[:6]}"
    namespace = mount_namespace(mount)
    model = new_model_state()
    log: list[DogfoodOp] = []
    counts: Counter[str] = Counter()
    artifact_path = None if artifact_dir is None else Path(artifact_dir) / f"dogfood-{mount}.json"
    failed = False
    checks_run = 0

    try:
        for index in range(steps):
            op_name = _choose_operation(model, rng)
            op = OPERATIONS[op_name]
            entry = {"index": index, "seed": seed, "mount": mount, "op_name": op_name}
            try:
                entry["details"] = op(client, mount, model, rng)
                counts[op_name] += 1
                if (index + 1) % check_every == 0:
                    _verify_sampled_state(client, mount, model, rng)
                    checks_run += 1
            except Exception as exc:
                failed = True
                entry["error"] = {"type": type(exc).__name__, "message": str(exc)}
                log.append(entry)
                if artifact_path is not None:
                    artifact_path.write_text(
                        json.dumps(
                            {
                                "seed": seed,
                                "mount": mount,
                                "namespace": namespace,
                                "steps": steps,
                                "failed_at": index,
                                "log": log,
                                "state_paths": sorted(model),
                            },
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
                raise
            log.append(entry)
        _verify_sampled_state(client, mount, model, rng)
        checks_run += 1
        return {
            "seed": seed,
            "mount": mount,
            "namespace": namespace,
            "steps": steps,
            "steps_completed": steps,
            "checks_run": checks_run,
            "counts": dict(counts),
            "log": log,
            "log_path": None if artifact_path is None else str(artifact_path),
        }
    finally:
        if not cleanup or keep_always or (failed and keep_on_fail):
            return
        namespace_handle = client.namespace(namespace)
        if hasattr(namespace_handle, "delete_all"):
            try:
                namespace_handle.delete_all()
            except Exception:
                pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tpfs-dogfood", description="Run seeded live turbopuffer_fs dogfood scenarios.")
    parser.add_argument("--api-key", default=os.environ.get("TURBOPUFFER_API_KEY"))
    parser.add_argument("--region", default=os.environ.get("TURBOPUFFER_REGION"))
    parser.add_argument("--base-url", default=os.environ.get("TURBOPUFFER_BASE_URL"))
    parser.add_argument("--mount-prefix", default="dogfood")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--steps", type=int, default=50)
    parser.add_argument("--check-every", type=int, default=5)
    parser.add_argument("--keep-on-fail", action="store_true")
    parser.add_argument("--keep-always", action="store_true")
    parser.add_argument("--no-cleanup", dest="cleanup", action="store_false")
    parser.set_defaults(cleanup=True)
    parser.add_argument("--artifact-dir", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    summary = run_dogfood(
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
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
