import { describe, expect, it, vi } from "vitest";

import * as tpfs from "@workspace/turbopuffer-fs";
import * as boot from "../src/boot.js";
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
    loadSessionState: vi.fn(async () => ({ cwd: "/project", mount: "documents", bundle_id: "bundle-1" })),
    workspaceInit: vi.fn(async () => ({
      session: { cwd: "/project", mount: "documents", bundle_id: "bundle-1", path: "/state/session.json" },
    })),
    saveSessionState: vi.fn(async (_client, mount, state) => ({
      cwd: state.cwd,
      mount,
      bundle_id: state.bundle_id,
      path: "/state/session.json",
    })),
    appendCommandLog: undefined,
    listMounts: vi.fn(async () => ["documents"]),
    stat: vi.fn(async (_client, _mount, path) =>
      String(path).endsWith("/notes")
        ? { path, kind: "dir", is_text: 0 }
        : { path, kind: "file", is_text: 1, text: "hello\n" }),
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
    const context = await boot.createBootContext({ mount: "documents" });
    const pwdResult = await runShellCommand(context, "pwd");
    expect(pwdResult.stdout.trim()).toBe("/project");

    const catResult = await runShellCommand(context, "cat readme.txt");
    expect(catResult.stdout).toContain("hello");
  });

  it("persists cwd changes without dropping bundle metadata", async () => {
    const persistSession = vi.fn(async () => undefined);
    const context = {
      ...(await boot.createBootContext({ mount: "documents", bundleSpec: { id: "bundle-1" } })),
      persistSession,
      session: { cwd: "/project", mount: "documents", bundle_id: "bundle-1" },
    };

    await runShellCommand(context, "cd notes");

    expect(persistSession).toHaveBeenCalledWith("/project/notes");
  });

  it("passes bundle id into workspace initialization when bootstrapping a new mount", async () => {
    vi.mocked(tpfs.stat).mockResolvedValueOnce(null);

    await boot.createBootContext({ mount: "documents", bundleSpec: { id: "bundle-1" } });

    expect(tpfs.workspaceInit).toHaveBeenCalledWith(
      expect.anything(),
      "documents",
      expect.objectContaining({
        bundleId: "bundle-1",
      }),
    );
  });
});
