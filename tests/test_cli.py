from __future__ import annotations

import json
from pathlib import Path

import pytest

from turbopuffer_fs import cli


def run_cli(args: list[str], monkeypatch: pytest.MonkeyPatch, calls: list[tuple[str, tuple, dict]], *, stdin: str = ""):
    monkeypatch.setattr(cli.sys, "stdin", type("FakeStdin", (), {"read": lambda self: stdin})())

    def recorder(name: str):
        def _call(*call_args, **call_kwargs):
            calls.append((name, call_args, call_kwargs))
            if name == "read_bytes":
                return b"\x00\x01"
            return {"ok": True, "name": name}

        return _call

    monkeypatch.setattr(cli, "list_mounts", recorder("list_mounts"))
    monkeypatch.setattr(cli, "stat", recorder("stat"))
    monkeypatch.setattr(cli, "ls", recorder("ls"))
    monkeypatch.setattr(cli, "find", recorder("find"))
    monkeypatch.setattr(cli, "cat", recorder("cat"))
    monkeypatch.setattr(cli, "read_text", recorder("read_text"))
    monkeypatch.setattr(cli, "read_bytes", recorder("read_bytes"))
    monkeypatch.setattr(cli, "grep", recorder("grep"))
    monkeypatch.setattr(cli, "mkdir", recorder("mkdir"))
    monkeypatch.setattr(cli, "put_text", recorder("put_text"))
    monkeypatch.setattr(cli, "put_bytes", recorder("put_bytes"))
    monkeypatch.setattr(cli, "rm", recorder("rm"))
    monkeypatch.setattr(cli, "ingest_directory", recorder("ingest_directory"))
    monkeypatch.setattr(cli, "load_bundle_spec", lambda local_root: {"id": "csv-cleaning-v1", "allowed_outputs": ["/logs/run.jsonl"]})
    monkeypatch.setattr(cli, "bundle_entrypoint", lambda local_root: "/TASK.md")
    monkeypatch.setattr(cli, "list_allowed_outputs", lambda local_root: ["/logs/run.jsonl"])
    monkeypatch.setattr(cli, "bundle_task_prompt", lambda local_root: "Allowed outputs:\n- /logs/run.jsonl")
    monkeypatch.setattr(cli, "seed_bundle", recorder("seed_bundle"))
    monkeypatch.setattr(cli, "validate_bundle_outputs", lambda client, mount, local_root: [])
    monkeypatch.setattr(cli, "run_dogfood", lambda **kwargs: {"steps_completed": kwargs["steps"], "checks_run": 1})
    monkeypatch.setattr(cli, "resolve_workspace_config", lambda **kwargs: {
        "entrypoint": "/TASK.md",
        "bundle_manifest": "/bundle.json",
        "session_state": "/state/session.json",
        "logs_dir": "/logs",
        "output_dir": "/output",
        "scratch_dir": "/scratch",
        "project_dir": "/project",
        "input_dir": "/input",
    })
    monkeypatch.setattr(cli, "workspace_show", lambda client, mount, *, workspace_config: {"workspace": workspace_config, "session": {"cwd": "/project", "mount": mount}})
    monkeypatch.setattr(cli, "workspace_pwd", lambda client, mount, *, workspace_config: "/project")
    monkeypatch.setattr(cli, "workspace_cd", lambda client, mount, path, *, workspace_config: {"cwd": "/output", "mount": mount})
    monkeypatch.setattr(cli, "write_session_state", lambda client, mount, state, *, workspace_config: state)
    monkeypatch.setattr(cli, "workspace_init", lambda client, mount, *, workspace_config, bundle_id=None, cwd=None: {
        "workspace": workspace_config,
        "session": {"cwd": cwd or workspace_config["project_dir"], "mount": mount},
    })
    monkeypatch.setattr(cli, "resolve_cli_path", lambda path, *, cwd: path if str(path).startswith("/") else f"{cwd.rstrip('/')}/{path}")
    monkeypatch.setattr(cli, "make_client", lambda **kwargs: {"client": kwargs})
    return cli.main(args)


def test_cli_ls_outputs_json(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, tuple, dict]] = []
    exit_code = run_cli(["ls", "documents", "/notes"], monkeypatch, calls)
    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["name"] == "ls"
    assert calls[0][0] == "ls"
    assert calls[0][1][1] == "documents"
    assert calls[0][1][2] == "/notes"


def test_cli_put_text_supports_stdin(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, tuple, dict]] = []
    exit_code = run_cli(["put-text", "documents", "/notes/a.txt", "--stdin"], monkeypatch, calls, stdin="hello\n")
    assert exit_code == 0
    json.loads(capsys.readouterr().out)
    assert calls[0][0] == "put_text"
    assert calls[0][1][3] == "hello\n"


def test_cli_put_bytes_supports_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    source = tmp_path / "data.bin"
    source.write_bytes(b"\x01\x02")
    calls: list[tuple[str, tuple, dict]] = []
    exit_code = run_cli(["put-bytes", "documents", "/bin/data.bin", "--file", str(source)], monkeypatch, calls)
    assert exit_code == 0
    json.loads(capsys.readouterr().out)
    assert calls[0][0] == "put_bytes"
    assert calls[0][1][3] == b"\x01\x02"


def test_cli_read_bytes_can_write_output_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    target = tmp_path / "out.bin"
    calls: list[tuple[str, tuple, dict]] = []
    exit_code = run_cli(["read-bytes", "documents", "/bin/data.bin", "--out", str(target)], monkeypatch, calls)
    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["bytes_written"] == 2
    assert target.read_bytes() == b"\x00\x01"


def test_cli_ingest_calls_wrapper(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[tuple[str, tuple, dict]] = []
    exit_code = run_cli(["ingest", "documents", ".", "--mount-root", "/archive", "--batch-size", "5"], monkeypatch, calls)
    assert exit_code == 0
    json.loads(capsys.readouterr().out)
    assert calls[0][0] == "ingest_directory"
    assert calls[0][2]["mount_root"] == "/archive"
    assert calls[0][2]["batch_size"] == 5


def test_cli_bundle_show(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[tuple[str, tuple, dict]] = []
    exit_code = run_cli(["bundle-show", "/workspace/examples/task-bundles/csv-cleaning-v1"], monkeypatch, calls)
    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["spec"]["id"] == "csv-cleaning-v1"
    assert output["entrypoint"] == "/TASK.md"


def test_cli_bundle_prompt(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[tuple[str, tuple, dict]] = []
    exit_code = run_cli(["bundle-prompt", "/workspace/examples/task-bundles/csv-cleaning-v1"], monkeypatch, calls)
    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert "Allowed outputs:" in output["prompt"]


def test_cli_workspace_show(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[tuple[str, tuple, dict]] = []
    exit_code = run_cli(["workspace-show", "documents"], monkeypatch, calls)
    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["workspace"]["session_state"] == "/state/session.json"


def test_cli_pwd_and_cd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[tuple[str, tuple, dict]] = []
    assert run_cli(["pwd", "documents"], monkeypatch, calls) == 0
    pwd_output = json.loads(capsys.readouterr().out)
    assert pwd_output["cwd"] == "/project"

    calls = []
    assert run_cli(["cd", "documents", "/output"], monkeypatch, calls) == 0
    cd_output = json.loads(capsys.readouterr().out)
    assert cd_output["cwd"] == "/output"


def test_cli_requires_text_source(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, tuple, dict]] = []
    exit_code = run_cli(["put-text", "documents", "/notes/a.txt"], monkeypatch, calls)
    assert exit_code == 2
