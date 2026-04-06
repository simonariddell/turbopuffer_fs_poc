import { mkdtemp, mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { beforeEach, describe, expect, it, vi } from "vitest";

import * as bundles from "../src/bundles.js";
import * as dogfood from "../src/dogfood.js";
import * as hydration from "../src/hydration.js";
import * as live from "../src/live.js";
import * as workspace from "../src/workspace.js";
import { runCli, type CliIO } from "../src/cli.js";

function createIo(stdinText = "", stdinBytes = new Uint8Array()): {
  io: CliIO;
  stdout: string[];
  stderr: string[];
} {
  const stdout: string[] = [];
  const stderr: string[] = [];
  return {
    io: {
      stdout: (text) => stdout.push(text),
      stderr: (text) => stderr.push(text),
      stdinText: async () => stdinText,
      stdinBytes: async () => stdinBytes,
    },
    stdout,
    stderr,
  };
}

describe("cli", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("runs ls and prints json", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue({
      entrypoint: "/TASK.md",
      bundle_manifest: "/bundle.json",
      session_state: "/state/session.json",
      logs_dir: "/logs",
      output_dir: "/output",
      scratch_dir: "/scratch",
      project_dir: "/project",
      input_dir: "/input",
    });
    vi.spyOn(workspace, "loadSessionState").mockResolvedValue({
      cwd: "/project",
      mount: "documents",
      updated_at: "2026-04-05T00:00:00.000Z",
      path: "/state/session.json",
    });
    vi.spyOn(live, "ls").mockResolvedValue([{ path: "/project/file.txt", kind: "file" }] as never);

    const { io, stdout, stderr } = createIo();
    const code = await runCli(["ls", "documents", "."], io);

    expect(code).toBe(0);
    expect(stderr).toEqual([]);
    expect(JSON.parse(stdout.join(""))).toEqual([{ path: "/project/file.txt", kind: "file" }]);
  });

  it("supports bundle-show and bundle-prompt", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(bundles, "loadBundleSpec").mockResolvedValue({
      id: "csv-cleaning-v1",
      entrypoint: "/TASK.md",
      allowed_outputs: ["/logs/run.jsonl"],
      workspace: { project_dir: "/workspace" },
    });
    vi.spyOn(bundles, "bundleTaskPrompt").mockResolvedValue("hello prompt");

    const shown = createIo();
    expect(await runCli(["bundle-show", "/tmp/bundle"], shown.io)).toBe(0);
    expect(JSON.parse(shown.stdout.join("")).spec.id).toBe("csv-cleaning-v1");

    const prompted = createIo();
    expect(await runCli(["bundle-prompt", "/tmp/bundle"], prompted.io)).toBe(0);
    expect(JSON.parse(prompted.stdout.join("")).prompt).toBe("hello prompt");
  });

  it("supports pwd and cd through durable session helpers", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "loadSessionState").mockResolvedValue({
      cwd: "/project",
      mount: "documents",
      updated_at: "2026-04-05T00:00:00.000Z",
      path: "/state/session.json",
    });
    const workspaceCdSpy = vi.spyOn(workspace, "workspaceCd").mockResolvedValue({
      cwd: "/output",
      mount: "documents",
    });

    const pwdIo = createIo();
    expect(await runCli(["pwd", "documents"], pwdIo.io)).toBe(0);
    expect(JSON.parse(pwdIo.stdout.join(""))).toEqual({ cwd: "/project", mount: "documents" });

    const cdIo = createIo();
    expect(await runCli(["cd", "documents", "../output"], cdIo.io)).toBe(0);
    expect(JSON.parse(cdIo.stdout.join(""))).toEqual({ cwd: "/output", mount: "documents" });
    expect(workspaceCdSpy).toHaveBeenCalledWith(
      expect.anything(),
      "documents",
      "../output",
      { workspaceConfig: workspace.defaultWorkspaceConfig() },
    );
  });

  it("supports workspace show and existence commands", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "workspaceShow").mockResolvedValue({
      exists: true,
      workspace: workspace.defaultWorkspaceConfig(),
      metadata: {
        path: "/state/workspace.json",
        mount: "documents",
        workspace_kind: "task",
        created_at: "2026-04-05T00:00:00.000Z",
        updated_at: "2026-04-05T00:00:00.000Z",
        status: "active",
        session_state: "/state/session.json",
        entrypoint: "/TASK.md",
        bundle_manifest: "/bundle.json",
        logs_dir: "/logs",
        output_dir: "/output",
        scratch_dir: "/scratch",
        project_dir: "/project",
        input_dir: "/input",
      },
      session: {
        cwd: "/project",
        mount: "documents",
        updated_at: "2026-04-05T00:00:00.000Z",
        path: "/state/session.json",
      },
    } as never);
    vi.spyOn(workspace, "workspaceExists").mockResolvedValue(true);

    const showIo = createIo();
    expect(await runCli(["workspace-show", "documents"], showIo.io)).toBe(0);
    expect(JSON.parse(showIo.stdout.join(""))).toMatchObject({
      exists: true,
      metadata: {
        workspace_kind: "task",
      },
    });

    const existsIo = createIo();
    expect(await runCli(["workspace-exists", "documents"], existsIo.io)).toBe(0);
    expect(JSON.parse(existsIo.stdout.join(""))).toEqual({ mount: "documents", exists: true });
  });

  it("supports workspace delete and archive commands", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({
      namespace: vi.fn(() => ({ deleteAll: vi.fn(async () => undefined) })),
    } as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "archiveWorkspace").mockResolvedValue({
      mount: "documents",
      namespace: "documents__fs",
      archived: true,
      metadata: {
        status: "archived",
      },
    } as never);

    const deleteIo = createIo();
    expect(await runCli(["workspace-delete", "documents"], deleteIo.io)).toBe(0);
    expect(JSON.parse(deleteIo.stdout.join(""))).toEqual({
      mount: "documents",
      namespace: "documents__fs",
      deleted: true,
    });

    const archiveIo = createIo();
    expect(await runCli(["workspace-archive", "documents"], archiveIo.io)).toBe(0);
    expect(JSON.parse(archiveIo.stdout.join(""))).toMatchObject({
      archived: true,
      metadata: {
        status: "archived",
      },
    });
  });

  it("supports put-text from stdin and put-bytes from stdin", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "loadSessionState").mockResolvedValue({
      cwd: "/project",
      mount: "documents",
      updated_at: "2026-04-05T00:00:00.000Z",
      path: "/state/session.json",
    });
    const putTextSpy = vi.spyOn(live, "putText").mockResolvedValue({ ok: true } as never);
    const putBytesSpy = vi.spyOn(live, "putBytes").mockResolvedValue({ ok: true } as never);

    const textIo = createIo("hello\n");
    expect(await runCli(["put-text", "documents", "notes.txt", "--stdin"], textIo.io)).toBe(0);
    expect(putTextSpy).toHaveBeenCalledWith(expect.anything(), "documents", "/project/notes.txt", "hello\n", { mime: undefined });

    const bytesIo = createIo("", Uint8Array.from([1, 2]));
    expect(await runCli(["put-bytes", "documents", "blob.bin", "--stdin"], bytesIo.io)).toBe(0);
    expect(putBytesSpy).toHaveBeenCalledWith(expect.anything(), "documents", "/project/blob.bin", Uint8Array.from([1, 2]), { mime: undefined });
  });

  it("runs dogfood command", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(dogfood, "runDogfood").mockResolvedValue({ stepsCompleted: 3, checksRun: 1 });

    const { io, stdout } = createIo();
    expect(await runCli(["dogfood", "--steps", "3"], io)).toBe(0);
    expect(JSON.parse(stdout.join(""))).toEqual({ stepsCompleted: 3, checksRun: 1 });
  });

  it("writes bytes metadata to an output file", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "loadSessionState").mockResolvedValue({
      cwd: "/project",
      mount: "documents",
      updated_at: "2026-04-05T00:00:00.000Z",
      path: "/state/session.json",
    });
    vi.spyOn(live, "readBytes").mockResolvedValue(Uint8Array.from([1, 2, 3]) as never);

    const { io, stdout } = createIo();
    const code = await runCli(["read-bytes", "documents", "data.bin"], io);

    expect(code).toBe(0);
    expect(JSON.parse(stdout.join(""))).toEqual({
      path: "/project/data.bin",
      sizeBytes: 3,
      blobB64: "AQID",
    });
  });

  it("writes read-bytes output to a file when requested", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "loadSessionState").mockResolvedValue({
      cwd: "/project",
      mount: "documents",
      updated_at: "2026-04-05T00:00:00.000Z",
      path: "/state/session.json",
    });
    vi.spyOn(live, "readBytes").mockResolvedValue(Uint8Array.from([1, 2, 3, 4]) as never);

    const tempRoot = await mkdtemp(join(tmpdir(), "tpfs-cli-"));
    const outPath = join(tempRoot, "bytes.bin");
    const { io, stdout } = createIo();

    expect(await runCli(["read-bytes", "documents", "blob.bin", "--out", outPath], io)).toBe(0);
    expect(new Uint8Array(await readFile(outPath))).toEqual(Uint8Array.from([1, 2, 3, 4]));
    expect(JSON.parse(stdout.join(""))).toEqual({
      path: "/project/blob.bin",
      out: outPath,
      bytesWritten: 4,
    });
  });

  it("supports put-text from a file", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "loadSessionState").mockResolvedValue({
      cwd: "/project",
      mount: "documents",
      updated_at: "2026-04-05T00:00:00.000Z",
      path: "/state/session.json",
    });
    const putTextSpy = vi.spyOn(live, "putText").mockResolvedValue({ ok: true } as never);

    const tempRoot = await mkdtemp(join(tmpdir(), "tpfs-cli-"));
    const sourcePath = join(tempRoot, "input.txt");
    await writeFile(sourcePath, "from file\n", "utf8");
    const { io } = createIo();

    expect(await runCli(["put-text", "documents", "notes.txt", "--file", sourcePath], io)).toBe(0);
    expect(putTextSpy).toHaveBeenCalledWith(
      expect.anything(),
      "documents",
      "/project/notes.txt",
      "from file\n",
      { mime: undefined },
    );
  });

  it("supports regex and bm25 grep modes", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "loadSessionState").mockResolvedValue({
      cwd: "/project",
      mount: "documents",
      updated_at: "2026-04-05T00:00:00.000Z",
      path: "/state/session.json",
    });
    const grepSpy = vi.spyOn(live, "grep").mockResolvedValue([
      { kind: "search_hit", mode: "bm25", path: "/project/readme.md", score: 12.3, snippet: "oauth token" },
    ] as never);

    const regexIo = createIo();
    expect(
      await runCli(["grep", "documents", ".", "oauth.*token", "--mode", "regex", "--ignore-case"], regexIo.io),
    ).toBe(0);
    expect(grepSpy).toHaveBeenLastCalledWith(
      expect.anything(),
      "documents",
      "/project",
      "oauth.*token",
      expect.objectContaining({ mode: "regex", ignoreCase: true }),
    );

    const bm25Io = createIo();
    expect(
      await runCli(["grep", "documents", ".", "oauth token", "--mode", "bm25", "--last-as-prefix"], bm25Io.io),
    ).toBe(0);
    expect(grepSpy).toHaveBeenLastCalledWith(
      expect.anything(),
      "documents",
      "/project",
      "oauth token",
      expect.objectContaining({ mode: "bm25", lastAsPrefix: true }),
    );
    expect(JSON.parse(bm25Io.stdout.join(""))[0]).toMatchObject({
      kind: "search_hit",
      mode: "bm25",
      path: "/project/readme.md",
    });
  });

  it("supports search as an alias for grep", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "loadSessionState").mockResolvedValue({
      cwd: "/project",
      mount: "documents",
      updated_at: "2026-04-05T00:00:00.000Z",
      path: "/state/session.json",
    });
    const searchSpy = vi.spyOn(live, "search").mockResolvedValue([
      { kind: "search_hit", mode: "bm25", path: "/project/readme.md", score: 9.9, snippet: "oauth token" },
    ] as never);

    const { io } = createIo();
    expect(
      await runCli(["search", "documents", ".", "oauth token", "--mode", "bm25"], io),
    ).toBe(0);
    expect(searchSpy).toHaveBeenCalledWith(
      expect.anything(),
      "documents",
      "/project",
      "oauth token",
      expect.objectContaining({ mode: "bm25" }),
    );
  });

  it("supports replace-text with structured success output", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "loadSessionState").mockResolvedValue({
      cwd: "/project",
      mount: "documents",
      updated_at: "2026-04-05T00:00:00.000Z",
      path: "/state/session.json",
      bundle_id: "bundle-1",
    });
    const replaceSpy = vi.spyOn(live, "replaceTextInFile").mockResolvedValue({
      path: "/project/notes.txt",
      matches: 1,
      changed: true,
      before_text: "hello world",
      after_text: "hello tpfs",
      before_sha256: "before",
      after_sha256: "after",
      mime: "text/plain",
    } as never);

    const { io, stdout, stderr } = createIo();
    const code = await runCli([
      "replace-text",
      "documents",
      "notes.txt",
      "--search",
      "world",
      "--replace",
      "tpfs",
    ], io);

    expect(code).toBe(0);
    expect(stderr).toEqual([]);
    expect(replaceSpy).toHaveBeenCalledWith(
      expect.anything(),
      "documents",
      "/project/notes.txt",
      expect.objectContaining({
        search: "world",
        replace: "tpfs",
        requireUnique: true,
      }),
    );
    expect(JSON.parse(stdout.join(""))).toMatchObject({
      path: "/project/notes.txt",
      matches: 1,
      changed: true,
      before_sha256: "before",
      after_sha256: "after",
    });
  });

  it("supports hydrate and sync commands with manifest files", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    const hydrateSpy = vi.spyOn(hydration, "hydrateWorkspace").mockResolvedValue({
      mount: "documents",
      hydrated_at: "2026-04-05T00:00:00.000Z",
      root: "/",
      cwd: "/project",
      workspace_metadata_path: "/state/workspace.json",
      entries: {
        "/project/notes.txt": {
          path: "/project/notes.txt",
          kind: "file",
          sha256: "abc",
          mime: "text/plain",
          size_bytes: 11,
          is_text: 1,
        },
      },
      snapshot: {
        "/project/notes.txt": {
          path: "/project/notes.txt",
          kind: "file",
          sha256: "abc",
          mime: "text/plain",
          size_bytes: 11,
          is_text: 1,
        },
      },
    } as never);
    const syncSpy = vi.spyOn(hydration, "syncWorkspace").mockResolvedValue({
      mount: "documents",
      root: "/",
      created: ["/project/new.txt"],
      modified: ["/project/notes.txt"],
      deleted: [],
      unchanged: ["/project/unchanged.txt"],
      conflicts: [],
    } as never);

    const tempRoot = await mkdtemp(join(tmpdir(), "tpfs-cli-hydration-"));
    const localRoot = join(tempRoot, "sandbox");
    const manifestPath = join(tempRoot, "manifest.json");
    await mkdir(localRoot, { recursive: true });

    const hydrateIo = createIo();
    expect(
      await runCli(["hydrate", "documents", localRoot, "--manifest-out", manifestPath], hydrateIo.io),
    ).toBe(0);
    expect(hydrateSpy).toHaveBeenCalledWith(
      expect.anything(),
      "documents",
      localRoot,
      expect.objectContaining({ workspaceConfig: workspace.defaultWorkspaceConfig() }),
    );
    const savedManifest = JSON.parse(await readFile(manifestPath, "utf8"));
    expect(savedManifest).toMatchObject({
      mount: "documents",
      cwd: "/project",
      workspace_metadata_path: "/state/workspace.json",
    });

    const syncIo = createIo();
    expect(
      await runCli(["sync", "documents", localRoot, "--manifest-file", manifestPath], syncIo.io),
    ).toBe(0);
    expect(syncSpy).toHaveBeenCalledWith(
      expect.anything(),
      "documents",
      localRoot,
      expect.objectContaining({
        mount: "documents",
        snapshot: expect.any(Object),
      }),
      { workspaceConfig: workspace.defaultWorkspaceConfig() },
    );
    expect(JSON.parse(syncIo.stdout.join(""))).toEqual({
      mount: "documents",
      root: "/",
      created: ["/project/new.txt"],
      modified: ["/project/notes.txt"],
      deleted: [],
      unchanged: ["/project/unchanged.txt"],
      conflicts: [],
    });
  });

  it("emits structured parse and domain errors", async () => {
    vi.spyOn(live, "makeClient").mockReturnValue({} as never);
    vi.spyOn(workspace, "resolveWorkspaceConfig").mockReturnValue(workspace.defaultWorkspaceConfig());
    vi.spyOn(workspace, "loadSessionState").mockResolvedValue({
      cwd: "/project",
      mount: "documents",
      updated_at: "2026-04-05T00:00:00.000Z",
      path: "/state/session.json",
    });
    vi.spyOn(live, "replaceTextInFile").mockRejectedValue(
      new Error("ReplaceTextNoMatchError:/project/notes.txt"),
    );

    const parseIo = createIo();
    expect(await runCli([], parseIo.io)).toBe(2);
    expect(JSON.parse(parseIo.stderr.join(""))).toEqual({
      error: {
        error: "Error",
        code: "ParseError",
        message: "missing command",
      },
    });

    const domainIo = createIo();
    expect(
      await runCli([
        "replace-text",
        "documents",
        "notes.txt",
        "--search",
        "missing",
        "--replace",
        "tpfs",
      ], domainIo.io),
    ).toBe(1);
    expect(JSON.parse(domainIo.stderr.join(""))).toEqual({
      error: {
        error: "ReplaceTextNoMatchError",
        code: "ReplaceTextNoMatchError",
        message: "ReplaceTextNoMatchError:/project/notes.txt",
        details: {
          path: "/project/notes.txt",
        },
      },
    });
  });
});
