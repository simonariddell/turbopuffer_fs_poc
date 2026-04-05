import { Buffer } from "node:buffer";

import { describe, expect, it } from "vitest";

import { contentBytes, contentText, FINALIZERS } from "../src/finalize.js";
import { listMounts, mountNamespace, stat as liveStat } from "../src/live.js";
import { bytesRow, directoryRow, textRow } from "../src/schema.js";
import { FakeClient, FakeNamespace, FakeNamespaceList, FakeQueryResponse } from "./fakes.js";

describe("finalizers", () => {
  it("returns metadata for stat", () => {
    const row = textRow("/notes/a.txt", "hello");
    const value = FINALIZERS.stat({}, { target: { name: "target", rows: [row] } });
    expect(value).toEqual({
      id: row.id,
      path: "/notes/a.txt",
      parent: "/notes",
      basename: "a.txt",
      kind: "file",
      ext: ".txt",
      mime: "text/plain",
      size_bytes: 5,
      is_text: 1,
      sha256: row.sha256,
      source_size_bytes: 5,
    });
  });

  it("requires directories for ls", () => {
    const row = textRow("/notes/a.txt", "hello");
    expect(() =>
      FINALIZERS.ls(
        { path: "/notes/a.txt" },
        { target: { name: "target", rows: [row] }, children: { name: "children", rows: [] } },
      ),
    ).toThrowError("NotADirectoryError:/notes/a.txt");
  });

  it("filters file-root find results to self", () => {
    const row = textRow("/notes/a.txt", "hello");
    const other = textRow("/notes/b.txt", "world");
    const matches = FINALIZERS.find(
      { root: "/notes/a.txt" },
      {
        target: { name: "target", rows: [row] },
        matches: { name: "matches", rows: [row, other] },
      },
    ) as Array<{ path: string }>;
    expect(matches.map((item) => item.path)).toEqual(["/notes/a.txt"]);
  });

  it("rejects binary text reads and returns bytes for text/binary", () => {
    const binary = bytesRow("/photos/a.jpg", Uint8Array.from([0, 1]));
    expect(() =>
      FINALIZERS.cat({ path: "/photos/a.jpg" }, { target: { name: "target", rows: [binary] } }),
    ).toThrowError("ValueError:path is a binary file: /photos/a.jpg");
    expect(() =>
      FINALIZERS.read_text({ path: "/photos/a.jpg" }, { target: { name: "target", rows: [binary] } }),
    ).toThrowError("ValueError:path is a binary file: /photos/a.jpg");

    expect(
      FINALIZERS.read_bytes(
        { path: "/notes/a.txt" },
        { target: { name: "target", rows: [textRow("/notes/a.txt", "hello")] } },
      ),
    ).toEqual(Buffer.from("hello"));
    expect(
      FINALIZERS.read_bytes(
        { path: "/photos/a.jpg" },
        { target: { name: "target", rows: [binary] } },
      ),
    ).toEqual(Buffer.from([0, 1]));
  });

  it("matches grep rows locally and reports rm no-op", () => {
    const matches = FINALIZERS.grep(
      { root: "/notes", pattern: "oauth", ignoreCase: true },
      {
        target: { name: "target", rows: [directoryRow("/notes")] },
        candidates: {
          name: "candidates",
          rows: [
            { path: "/notes/a.txt", text: "oauth token\nother\nOAuth done" },
            { path: "/notes/b.txt", text: "different" },
          ],
        },
      },
    );
    expect(matches).toEqual([
      { path: "/notes/a.txt", line_number: 1, line: "oauth token" },
      { path: "/notes/a.txt", line_number: 3, line: "OAuth done" },
    ]);

    expect(FINALIZERS.rm({ path: "/notes/missing", recursive: false }, { target: { name: "target", rows: [] } })).toEqual({
      path: "/notes/missing",
      recursive: false,
      deleted: false,
      ids: [],
    });
  });

  it("exposes content helpers", () => {
    expect(contentText(textRow("/notes/a.txt", "hello"))).toBe("hello");
    expect(contentBytes(textRow("/notes/a.txt", "hello"))).toEqual(Buffer.from("hello"));
    expect(contentBytes(bytesRow("/photos/a.jpg", Uint8Array.from([0, 1])))).toEqual(Buffer.from([0, 1]));
    expect(() => contentText(directoryRow("/notes"))).toThrowError("IsADirectoryError:/notes");
  });
});

describe("live wrappers", () => {
  it("maps namespaces back to mounts", async () => {
    const client = new FakeClient({
      namespaceLists: [new FakeNamespaceList([{ id: "documents__fs" }, { id: "logs__fs" }, { id: "misc" }])],
    });
    expect(mountNamespace("documents")).toBe("documents__fs");
    await expect(listMounts(client as never)).resolves.toEqual(["documents", "logs"]);
  });

  it("runs stat through the plan runtime", async () => {
    const row = textRow("/notes/a.txt", "hello");
    const client = new FakeClient({
      namespaces: {
        documents__fs: new FakeNamespace("documents__fs", {
          queryResponses: [new FakeQueryResponse({ rows: [row] })],
        }),
      },
    });

    await expect(liveStat(client as never, "documents", "/notes/a.txt")).resolves.toMatchObject({
      id: row.id,
      path: "/notes/a.txt",
    });
  });
});
