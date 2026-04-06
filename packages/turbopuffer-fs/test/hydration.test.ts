import { mkdtemp, mkdir, readFile, rm as rmFs, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { afterEach, describe, expect, it } from "vitest";

import { hydrateWorkspace, syncWorkspace } from "../src/hydration.js";
import { resolveWorkspaceConfig, workspaceInit } from "../src/workspace.js";
import { FakeClient } from "./fakes.js";

describe("hydration", () => {
  const tempRoots: string[] = [];

  afterEach(async () => {
    for (const root of tempRoots.splice(0)) {
      await rmFs(root, { recursive: true, force: true });
    }
  });

  async function makeTempRoot(): Promise<string> {
    const root = await mkdtemp(join(tmpdir(), "tpfs-hydration-"));
    tempRoots.push(root);
    return root;
  }

  it("hydrates a workspace to a local tree", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();
    await workspaceInit(client as never, "documents", {
      workspaceConfig,
      cwd: "/project",
    });
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

    const localRoot = await makeTempRoot();
    const manifest = await hydrateWorkspace(client as never, "documents", localRoot, {
      workspaceConfig,
    });

    expect(manifest.cwd).toBe("/project");
    const notesEntry = manifest.entries["/project/notes.txt"];
    expect(notesEntry).toMatchObject({
      kind: "file",
      mime: "text/plain",
    });
    expect(await readFile(join(localRoot, "project", "notes.txt"), "utf8")).toBe("hello world");
  });

  it("syncs creates, updates, and deletions back to tpfs", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();
    await workspaceInit(client as never, "documents", {
      workspaceConfig,
      cwd: "/project",
    });
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
          size_bytes: 5,
          is_text: 1,
          text: "hello",
          sha256: "before",
        },
        {
          id: "old-id",
          path: "/project/old.txt",
          parent: "/project",
          basename: "old.txt",
          kind: "file",
          ext: ".txt",
          mime: "text/plain",
          size_bytes: 3,
          is_text: 1,
          text: "old",
          sha256: "oldsha",
        },
      ],
      schema: {},
    });

    const localRoot = await makeTempRoot();
    const manifest = await hydrateWorkspace(client as never, "documents", localRoot, {
      workspaceConfig,
    });

    await writeFile(join(localRoot, "project", "notes.txt"), "hello tpfs", "utf8");
    await writeFile(join(localRoot, "project", "new.txt"), "brand new", "utf8");
    await rmFs(join(localRoot, "project", "old.txt"));

    const result = await syncWorkspace(client as never, "documents", localRoot, manifest, {
      workspaceConfig,
    });

    expect(result.modified).toContain("/project/notes.txt");
    expect(result.created).toContain("/project/new.txt");
    expect(result.deleted).toContain("/project/old.txt");

    const rows = namespace.snapshotRows();
    expect(rows.find((row) => row.path === "/project/notes.txt")?.text).toBe("hello tpfs");
    expect(rows.find((row) => row.path === "/project/new.txt")?.text).toBe("brand new");
    expect(rows.find((row) => row.path === "/project/old.txt")).toBeUndefined();
  });

  it("detects conflicts on touched paths", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();
    await workspaceInit(client as never, "documents", {
      workspaceConfig,
      cwd: "/project",
    });
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
          size_bytes: 5,
          is_text: 1,
          text: "hello",
          sha256: "before",
        },
      ],
      schema: {},
    });

    const localRoot = await makeTempRoot();
    const manifest = await hydrateWorkspace(client as never, "documents", localRoot, {
      workspaceConfig,
    });

    await writeFile(join(localRoot, "project", "notes.txt"), "local change", "utf8");
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
          size_bytes: 13,
          is_text: 1,
          text: "remote change",
          sha256: "remote",
        },
      ],
      schema: {},
    });

    const result = await syncWorkspace(client as never, "documents", localRoot, manifest);
    expect(result.conflicts).toContainEqual({
      path: "/project/notes.txt",
      reason: "remote_changed_since_hydration",
    });
  });

  it("round-trips binary files", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();
    await workspaceInit(client as never, "documents", {
      workspaceConfig,
      cwd: "/project",
    });
    const namespace = client.namespace("documents__fs");
    await namespace.write({
      upsert_rows: [
        {
          id: "blob-id",
          path: "/project/blob.bin",
          parent: "/project",
          basename: "blob.bin",
          kind: "file",
          ext: ".bin",
          mime: "application/octet-stream",
          size_bytes: 3,
          is_text: 0,
          blob_b64: Buffer.from(Uint8Array.from([1, 2, 3])).toString("base64"),
          sha256: "binsha",
        },
      ],
      schema: {},
    });

    const localRoot = await makeTempRoot();
    const manifest = await hydrateWorkspace(client as never, "documents", localRoot, {
      workspaceConfig,
    });
    expect(new Uint8Array(await readFile(join(localRoot, "project", "blob.bin")))).toEqual(
      Uint8Array.from([1, 2, 3]),
    );

    await writeFile(join(localRoot, "project", "blob.bin"), Buffer.from([4, 5, 6]));
    const result = await syncWorkspace(client as never, "documents", localRoot, manifest, {
      workspaceConfig,
    });

    expect(result.modified).toContain("/project/blob.bin");
    const row = namespace.snapshotRows().find((entry) => entry.path === "/project/blob.bin");
    expect(Buffer.from(String(row?.blob_b64 ?? ""), "base64")).toEqual(Buffer.from([4, 5, 6]));
  });
});
