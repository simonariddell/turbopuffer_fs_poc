import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { beforeEach, describe, expect, it, vi } from "vitest";

import * as bundles from "../src/bundles.js";
import * as dogfood from "../src/dogfood.js";
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
    vi.spyOn(workspace, "saveSessionState").mockResolvedValue({
      cwd: "/output",
      mount: "documents",
      updated_at: "2026-04-05T00:00:01.000Z",
      path: "/state/session.json",
    });

    const pwdIo = createIo();
    expect(await runCli(["pwd", "documents"], pwdIo.io)).toBe(0);
    expect(JSON.parse(pwdIo.stdout.join(""))).toEqual({ cwd: "/project", mount: "documents" });

    const cdIo = createIo();
    expect(await runCli(["cd", "documents", "../output"], cdIo.io)).toBe(0);
    expect(JSON.parse(cdIo.stdout.join(""))).toEqual({ cwd: "/output", mount: "documents" });
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
});
