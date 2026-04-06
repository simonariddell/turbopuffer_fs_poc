import { describe, expect, it } from "vitest";

import {
  DEFAULT_WORKSPACE_CONFIG,
  archiveWorkspace,
  defaultWorkspaceConfig,
  loadSessionState,
  loadWorkspaceMetadata,
  loadWorkspaceConfigFile,
  mergeWorkspaceConfig,
  resolveUserPath,
  resolveWorkspaceConfig,
  saveSessionState,
  sessionStateDoc,
  workspaceCd,
  workspaceExists,
  workspaceInit,
  workspaceMetadataDoc,
  workspaceShow,
  WORKSPACE_METADATA_PATH,
  deleteWorkspace,
} from "../src/workspace.js";
import { FakeClient } from "./fakes.js";

describe("workspace", () => {
  it("returns default config", () => {
    expect(defaultWorkspaceConfig()).toEqual(DEFAULT_WORKSPACE_CONFIG);
  });

  it("merges deployment and bundle config", () => {
    const resolved = resolveWorkspaceConfig({
      deploymentConfig: { logs_dir: "/artifacts/logs" },
      bundleSpec: { workspace: { entrypoint: "/instructions/main.md" } },
    });
    expect(resolved.logs_dir).toBe("/artifacts/logs");
    expect(resolved.entrypoint).toBe("/instructions/main.md");
    expect(resolved.session_state).toBe("/state/session.json");
  });

  it("loads workspace config from json contents", () => {
    const loaded = loadWorkspaceConfigFile(
      JSON.stringify({
        logs_dir: "/custom/logs",
        project_dir: "/workspace",
      }),
    );
    expect(loaded.logs_dir).toBe("/custom/logs");
    expect(loaded.project_dir).toBe("/workspace");
  });

  it("builds session state docs", () => {
    const state = sessionStateDoc({
      mount: "documents",
      cwd: "/project",
      bundleId: "bundle-1",
    });
    expect(state.cwd).toBe("/project");
    expect(state.mount).toBe("documents");
    expect(state.bundle_id).toBe("bundle-1");
    expect(state.path).toBe("/state/session.json");
  });

  it("builds workspace metadata docs", () => {
    const metadata = workspaceMetadataDoc({
      mount: "documents",
      workspaceKind: "task",
      bundleId: "bundle-1",
      tags: ["alpha", "beta", "alpha"],
    });
    expect(metadata.mount).toBe("documents");
    expect(metadata.workspace_kind).toBe("task");
    expect(metadata.bundle_id).toBe("bundle-1");
    expect(metadata.path).toBe(WORKSPACE_METADATA_PATH);
    expect(metadata.tags).toEqual(["alpha", "beta"]);
  });

  it("resolves relative user paths against cwd", () => {
    expect(resolveUserPath("src/main.ts", { cwd: "/project" })).toBe("/project/src/main.ts");
    expect(resolveUserPath("../output/report.md", { cwd: "/project/src" })).toBe("/project/output/report.md");
    expect(resolveUserPath("/absolute/path", { cwd: "/project" })).toBe("/absolute/path");
    expect(resolveUserPath("", { cwd: "/project" })).toBe("/project");
    expect(resolveUserPath(".", { cwd: "/project" })).toBe("/project");
    expect(resolveUserPath("./", { cwd: "/project" })).toBe("/project");
    expect(resolveUserPath("../../../../logs", { cwd: "/project" })).toBe("/logs");
  });

  it("allows explicit overrides to win", () => {
    const merged = mergeWorkspaceConfig(
      DEFAULT_WORKSPACE_CONFIG,
      { project_dir: "/workspace" },
      { project_dir: "/bundle/project" },
      { project_dir: "/override" },
    );
    expect(merged.project_dir).toBe("/override");
  });

  it("initializes a workspace and persists session state", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();

    const summary = await workspaceInit(client as never, "documents", {
      workspaceConfig,
      cwd: "/project",
    });

    expect((summary.session as { cwd: string }).cwd).toBe("/project");

    const namespace = client.namespace("documents__fs");
    const writtenPaths = namespace.writeCalls.flatMap((payload) =>
      ((payload.upsert_rows as Array<Record<string, unknown>> | undefined) ?? []).map((row) => String(row.path)),
    );
    expect(writtenPaths).toContain(workspaceConfig.session_state);
    expect(writtenPaths).toContain(WORKSPACE_METADATA_PATH);
    expect(writtenPaths).toContain(workspaceConfig.logs_dir);
  });

  it("persists workspace metadata and reports workspace existence", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();

    expect(await workspaceExists(client as never, "documents", { workspaceConfig })).toBe(false);

    await workspaceInit(client as never, "documents", {
      workspaceConfig,
      cwd: "/project",
      workspaceKind: "task",
      bundleId: "bundle-2",
      taskId: "task-9",
      tags: ["alpha", "beta"],
    });

    expect(await workspaceExists(client as never, "documents", { workspaceConfig })).toBe(true);
    const metadata = await loadWorkspaceMetadata(client as never, "documents");
    expect(metadata?.workspace_kind).toBe("task");
    expect(metadata?.bundle_id).toBe("bundle-2");
    expect(metadata?.task_id).toBe("task-9");
    expect(metadata?.tags).toEqual(["alpha", "beta"]);
  });

  it("shows initialized and missing workspaces honestly", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();

    const missing = await workspaceShow(client as never, "documents", { workspaceConfig });
    expect(missing).toEqual({
      exists: false,
      workspace: workspaceConfig,
      metadata: null,
      session: null,
    });

    await workspaceInit(client as never, "documents", {
      workspaceConfig,
      cwd: "/project",
      workspaceKind: "agent",
    });

    const shown = await workspaceShow(client as never, "documents", { workspaceConfig });
    expect(shown.exists).toBe(true);
    expect(shown.session?.cwd).toBe("/project");
    expect(shown.metadata?.workspace_kind).toBe("agent");
  });

  it("loads and saves session state documents", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();

    await workspaceInit(client as never, "documents", {
      workspaceConfig,
      cwd: "/scratch",
    });

    const loaded = await loadSessionState(client as never, "documents", { workspaceConfig });
    const persisted = loaded.cwd === "/scratch"
      ? loaded
      : await saveSessionState(
          client as never,
          "documents",
          { cwd: "/scratch", mount: "documents" },
          { workspaceConfig },
        );

    expect(persisted.cwd).toBe("/scratch");
    expect(persisted.mount).toBe("documents");
  });

  it("preserves unrelated session metadata when saving and changing cwd", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();

    await workspaceInit(client as never, "documents", {
      workspaceConfig,
      cwd: "/project",
      bundleId: "bundle-7",
    });

    const saved = await saveSessionState(
      client as never,
      "documents",
      {
        cwd: "/project",
        mount: "documents",
        bundle_id: "bundle-7",
        future_metadata: { marker: true },
      },
      { workspaceConfig },
    );
    expect(saved.bundle_id).toBe("bundle-7");
    expect((saved as Record<string, unknown>).future_metadata).toEqual({ marker: true });

    const changed = await workspaceCd(client as never, "documents", ".", { workspaceConfig });
    expect(changed).toEqual({
      cwd: "/project",
      mount: "documents",
    });

    const reloaded = await loadSessionState(client as never, "documents", { workspaceConfig });
    expect(reloaded.bundle_id).toBe("bundle-7");
    expect((reloaded as Record<string, unknown>).future_metadata).toEqual({ marker: true });
  });

  it("validates workspaceCd targets", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();

    await workspaceInit(client as never, "documents", {
      workspaceConfig,
      cwd: "/project",
    });

    await expect(workspaceCd(client as never, "documents", "missing", { workspaceConfig })).rejects.toThrow(
      "FileNotFoundError:/project/missing",
    );

    await saveSessionState(
      client as never,
      "documents",
      { cwd: "/project", mount: "documents" },
      { workspaceConfig },
    );
    const namespace = client.namespace("documents__fs");
    await namespace.write({
      upsert_rows: [
        {
          id: "file-node",
          path: "/project/file.txt",
          parent: "/project",
          basename: "file.txt",
          kind: "file",
          ext: ".txt",
          mime: "text/plain",
          size_bytes: 5,
          is_text: 1,
          text: "hello",
          sha256: "abc",
        },
      ],
      schema: {},
    });

    await expect(workspaceCd(client as never, "documents", "file.txt", { workspaceConfig })).rejects.toThrow(
      "NotADirectoryError:/project/file.txt",
    );

    await expect(workspaceCd(client as never, "documents", ".", { workspaceConfig })).resolves.toEqual({
      cwd: "/project",
      mount: "documents",
    });
  });

  it("archives and deletes workspaces explicitly", async () => {
    const client = new FakeClient();
    const workspaceConfig = resolveWorkspaceConfig();

    await workspaceInit(client as never, "documents", {
      workspaceConfig,
      workspaceKind: "task",
    });

    const archived = await archiveWorkspace(client as never, "documents", { workspaceConfig });
    expect(archived.archived).toBe(true);
    expect((archived.metadata as { status: string }).status).toBe("archived");

    const shown = await workspaceShow(client as never, "documents", { workspaceConfig });
    expect(shown.exists).toBe(true);
    expect(shown.metadata?.status).toBe("archived");

    const deleted = await deleteWorkspace(client as never, "documents");
    expect(deleted).toEqual({
      mount: "documents",
      namespace: "documents__fs",
      deleted: true,
    });
  });
});
