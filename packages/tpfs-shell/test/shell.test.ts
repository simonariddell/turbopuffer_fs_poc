import { describe, expect, it, vi } from "vitest";

import { createBootContext } from "../src/boot.js";
import { runShellCommand } from "../src/shell.js";

vi.mock("@workspace/turbopuffer-fs", async () => {
  const actual = await vi.importActual<object>("@workspace/turbopuffer-fs");
  return {
    ...actual,
    makeClient: vi.fn(() => ({ client: true })),
    resolveWorkspaceConfig: vi.fn(() => ({
      entrypoint: "/TASK.md",
      bundle_manifest: "/bundle.json",
      session_state: "/state/session.json",
      logs_dir: "/logs",
      output_dir: "/output",
      scratch_dir: "/scratch",
      project_dir: "/project",
      input_dir: "/input",
    })),
    loadSessionState: vi.fn(async () => ({ cwd: "/project", mount: "documents" })),
    workspaceInit: vi.fn(async () => ({
      session: { cwd: "/project", mount: "documents", path: "/state/session.json" },
    })),
    saveSessionState: vi.fn(async (_client, mount, state) => ({
      cwd: state.cwd,
      mount,
      path: "/state/session.json",
    })),
    appendCommandLog: undefined,
    listMounts: vi.fn(async () => ["documents"]),
    stat: vi.fn(async (client, mount, path) => ({ path, kind: "file", is_text: 1, text: "hello\n" })),
    ls: vi.fn(async () => [{ path: "/project/readme.txt", kind: "file" }]),
    readText: vi.fn(async () => "hello\n"),
    readBytes: vi.fn(async () => Uint8Array.from([1, 2, 3])),
    putText: vi.fn(async () => ({ ok: true })),
    putBytes: vi.fn(async () => ({ ok: true })),
    mkdir: vi.fn(async () => ({ ok: true })),
    rm: vi.fn(async () => ({ ok: true })),
    find: vi.fn(async () => [{ path: "/project/readme.txt", kind: "file" }]),
    grep: vi.fn(async () => [{ path: "/project/readme.txt", line_number: 1, line: "hello" }]),
    mountNamespace: vi.fn((mount: string) => `${mount}__fs`),
  };
});

describe("tpfs shell", () => {
  it("runs pwd and cat commands with durable session state", async () => {
    const context = await createBootContext({ mount: "documents" });
    const pwdResult = await runShellCommand(context, "pwd");
    expect(pwdResult.stdout.trim()).toBe("/project");

    const catResult = await runShellCommand(context, "cat readme.txt");
    expect(catResult.stdout).toContain("hello");
  });
});
