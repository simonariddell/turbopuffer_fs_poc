import { describe, expect, it } from "vitest";

import { replaceTextInFile } from "../src/edit.js";
import { resolveWorkspaceConfig, workspaceInit } from "../src/workspace.js";
import { FakeClient } from "./fakes.js";

describe("edit helper", () => {
  it("replaces a unique literal match", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();
    await workspaceInit(client as never, "documents", { workspaceConfig });
    const namespace = client.namespace("documents__fs");
    await namespace.write({
      upsert_rows: [
        {
          id: "notes-id",
          path: "/project/notes.txt",
          parent: "/project",
          basename: "notes.txt",
          kind: "file",
          ext: ".txt",
          mime: "text/plain",
          size_bytes: 11,
          is_text: 1,
          text: "hello world",
          sha256: "before",
        },
      ],
      schema: {},
    });

    const result = await replaceTextInFile(client as never, "documents", "/project/notes.txt", {
      search: "world",
      replace: "tpfs",
    });

    expect(result.path).toBe("/project/notes.txt");
    expect(result.matches).toBe(1);
    expect(result.changed).toBe(true);
    expect(result.before_text).toBe("hello world");
    expect(result.after_text).toBe("hello tpfs");
    expect(result.mime).toBe("text/plain");
    expect(result.after_sha256).not.toBe(result.before_sha256);
  });

  it("fails when no match is found", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();
    await workspaceInit(client as never, "documents", { workspaceConfig });
    const namespace = client.namespace("documents__fs");
    await namespace.write({
      upsert_rows: [
        {
          id: "notes-id",
          path: "/project/notes.txt",
          parent: "/project",
          basename: "notes.txt",
          kind: "file",
          ext: ".txt",
          mime: "text/plain",
          size_bytes: 11,
          is_text: 1,
          text: "hello world",
          sha256: "before",
        },
      ],
      schema: {},
    });

    await expect(
      replaceTextInFile(client as never, "documents", "/project/notes.txt", {
        search: "missing",
        replace: "tpfs",
      }),
    ).rejects.toThrow("ReplaceTextNoMatchError:/project/notes.txt");
  });

  it("fails when unique replacement sees multiple matches", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();
    await workspaceInit(client as never, "documents", { workspaceConfig });
    const namespace = client.namespace("documents__fs");
    await namespace.write({
      upsert_rows: [
        {
          id: "notes-id",
          path: "/project/notes.txt",
          parent: "/project",
          basename: "notes.txt",
          kind: "file",
          ext: ".txt",
          mime: "text/plain",
          size_bytes: 11,
          is_text: 1,
          text: "hello hello",
          sha256: "before",
        },
      ],
      schema: {},
    });

    await expect(
      replaceTextInFile(client as never, "documents", "/project/notes.txt", {
        search: "hello",
        replace: "tpfs",
      }),
    ).rejects.toThrow("ReplaceTextMatchCountError:/project/notes.txt");
  });

  it("fails for binary targets", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();
    await workspaceInit(client as never, "documents", { workspaceConfig });
    const namespace = client.namespace("documents__fs");
    await namespace.write({
      upsert_rows: [
        {
          id: "bin-id",
          path: "/project/blob.bin",
          parent: "/project",
          basename: "blob.bin",
          kind: "file",
          ext: ".bin",
          mime: "application/octet-stream",
          size_bytes: 3,
          is_text: 0,
          blob_b64: "AQID",
          sha256: "before",
        },
      ],
      schema: {},
    });

    await expect(
      replaceTextInFile(client as never, "documents", "/project/blob.bin", {
        search: "A",
        replace: "B",
      }),
    ).rejects.toThrow("ValueError:path is a binary file: /project/blob.bin");
  });
});
