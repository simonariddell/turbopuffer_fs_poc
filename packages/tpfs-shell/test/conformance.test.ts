import { beforeEach, describe, expect, it, vi } from "vitest";

import { TpfsSpecError } from "../src/errors.js";
import { TpufFsAdapter } from "../src/adapter.js";

const fsApi = vi.hoisted(() => ({
  ancestorPaths: vi.fn((value: string, includeSelf?: boolean | { includeSelf?: boolean }) => {
    const include =
      typeof includeSelf === "boolean" ? includeSelf : Boolean(includeSelf?.includeSelf);
    const parts = value.split("/").filter(Boolean);
    const limit = include ? parts.length : Math.max(parts.length - 1, 0);
    const paths = ["/"];
    for (let index = 1; index <= limit; index += 1) {
      paths.push(`/${parts.slice(0, index).join("/")}`);
    }
    return value === "/" ? ["/"] : paths;
  }),
  basename: vi.fn((value: string) => value.split("/").filter(Boolean).at(-1) ?? "/"),
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

function createAdapter(initialPaths: string[] = ["/project"]): TpufFsAdapter {
  return new TpufFsAdapter({
    client: {} as never,
    mount: "documents",
    cwdProvider: async () => "/project",
    initialPaths,
  });
}

describe("tpfs conformance matrix", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    fsApi.resolveUserPath.mockImplementation((value: string, options: { cwd: string }) =>
      value.startsWith("/") ? value : `${options.cwd.replace(/\/$/, "")}/${value}`,
    );
  });

  it("keeps root in inventory across mutations", async () => {
    fsApi.stat.mockResolvedValue(null);
    const adapter = createAdapter();

    await adapter.writeFile("alpha.txt", "hello\n");
    await adapter.rm("alpha.txt");

    expect(adapter.getAllPaths()).toEqual(["/", "/project"]);
  });

  it("surfaces a table-driven unsupported-operation contract", async () => {
    const adapter = createAdapter();
    const unsupportedCases = [
      {
        name: "chmod",
        run: () => adapter.chmod("alpha.txt", 0o644),
        code: "TPFS_UNSUPPORTED_BY_DESIGN",
      },
      {
        name: "symlink",
        run: () => adapter.symlink("target", "link"),
        code: "TPFS_UNSUPPORTED_BY_DESIGN",
      },
      {
        name: "link",
        run: () => adapter.link("existing", "new"),
        code: "TPFS_UNSUPPORTED_BY_DESIGN",
      },
      {
        name: "readlink",
        run: () => adapter.readlink("link"),
        code: "TPFS_UNSUPPORTED_BY_DESIGN",
      },
      {
        name: "utimes",
        run: () => adapter.utimes("alpha.txt", new Date(), new Date()),
        code: "TPFS_UNSUPPORTED_BY_DESIGN",
      },
    ] as const;

    for (const unsupportedCase of unsupportedCases) {
      await expect(unsupportedCase.run()).rejects.toBeInstanceOf(TpfsSpecError);
      await expect(unsupportedCase.run()).rejects.toThrow(unsupportedCase.code);
      await expect(unsupportedCase.run()).rejects.toThrow("SPEC.tpfs.md");
    }
  });

  it("requires recursive copy for directory sources", async () => {
    fsApi.stat.mockImplementation(async (_client: unknown, _mount: string, path: string) =>
      path === "/project/docs" ? { kind: "dir" } : null,
    );
    const adapter = createAdapter();

    await expect(adapter.cp("docs", "docs-copy")).rejects.toBeInstanceOf(TpfsSpecError);
    await expect(adapter.cp("docs", "docs-copy")).rejects.toThrow("TPFS_INVALID_OPERATION");
    await expect(adapter.cp("docs", "docs-copy")).rejects.toThrow("recursive:true");
  });

  it("rejects moving a directory into its own descendant", async () => {
    fsApi.stat.mockImplementation(async (_client: unknown, _mount: string, path: string) =>
      path === "/project/docs" ? { kind: "dir" } : null,
    );
    const adapter = createAdapter(["/project/docs"]);

    await expect(adapter.mv("docs", "docs/archive")).rejects.toBeInstanceOf(TpfsSpecError);
    await expect(adapter.mv("docs", "docs/archive")).rejects.toThrow("TPFS_INVALID_OPERATION");
    await expect(adapter.mv("docs", "docs/archive")).rejects.toThrow("own descendant");
  });
});
