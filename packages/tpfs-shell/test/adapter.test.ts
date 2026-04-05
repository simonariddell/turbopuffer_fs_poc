import { beforeEach, describe, expect, it, vi } from "vitest";

import { TpufFsAdapter } from "../src/adapter.js";

const fsApi = vi.hoisted(() => ({
  stat: vi.fn(),
  ls: vi.fn(),
  readText: vi.fn(),
  readBytes: vi.fn(),
  putText: vi.fn(),
  putBytes: vi.fn(),
  mkdir: vi.fn(),
  rm: vi.fn(),
  find: vi.fn(),
  resolveUserPath: vi.fn(),
}));

vi.mock("@workspace/turbopuffer-fs", () => fsApi);

describe("TpufFsAdapter", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    fsApi.resolveUserPath.mockImplementation((value: string, options: { cwd: string }) =>
      value.startsWith("/") ? value : `${options.cwd.replace(/\/$/, "")}/${value}`,
    );
  });

  it("reads text files", async () => {
    fsApi.readText.mockResolvedValue("hello");
    const adapter = new TpufFsAdapter({
      client: {} as never,
      mount: "documents",
      cwdProvider: async () => "/project",
    });
    await expect(adapter.readFile("notes.txt")).resolves.toBe("hello");
    expect(fsApi.readText).toHaveBeenCalledWith(expect.anything(), "documents", "/project/notes.txt");
  });

  it("writes bytes files", async () => {
    const adapter = new TpufFsAdapter({
      client: {} as never,
      mount: "documents",
      cwdProvider: async () => "/project",
    });
    await adapter.writeFile("data.bin", new Uint8Array([1, 2, 3]));
    expect(fsApi.putBytes).toHaveBeenCalledWith(expect.anything(), "documents", "/project/data.bin", new Uint8Array([1, 2, 3]));
  });

  it("lists directories with type info", async () => {
    fsApi.ls.mockResolvedValue([
      { path: "/project/a.txt", kind: "file" },
      { path: "/project/subdir", kind: "dir" },
    ]);
    const adapter = new TpufFsAdapter({
      client: {} as never,
      mount: "documents",
      cwdProvider: async () => "/project",
    });
    await expect(adapter.readdir("/project")).resolves.toEqual(["a.txt", "subdir"]);
    await expect(adapter.readdirWithFileTypes("/project")).resolves.toEqual([
      { name: "a.txt", isFile: true, isDirectory: false, isSymbolicLink: false },
      { name: "subdir", isFile: false, isDirectory: true, isSymbolicLink: false },
    ]);
  });

  it("appends text to existing text files", async () => {
    fsApi.stat.mockResolvedValue({ kind: "file", is_text: 1 });
    fsApi.readBytes.mockResolvedValue(new Uint8Array(Buffer.from("hello ")));
    const adapter = new TpufFsAdapter({
      client: {} as never,
      mount: "documents",
      cwdProvider: async () => "/project",
    });

    await adapter.appendFile("notes.txt", "world");

    expect(fsApi.putText).toHaveBeenCalledWith(
      expect.anything(),
      "documents",
      "/project/notes.txt",
      "hello world",
    );
  });

  it("reports unsupported filesystem features explicitly", async () => {
    const adapter = new TpufFsAdapter({
      client: {} as never,
      mount: "documents",
      cwdProvider: async () => "/project",
    });

    await expect(adapter.cp("a", "b")).rejects.toThrow("ENOTSUP");
    await expect(adapter.symlink("a", "b")).rejects.toThrow("ENOTSUP");
  });

  it("returns empty path inventory conservatively", () => {
    const adapter = new TpufFsAdapter({
      client: {} as never,
      mount: "documents",
      cwdProvider: async () => "/project",
    });

    expect(adapter.getAllPaths()).toEqual([]);
  });
});
