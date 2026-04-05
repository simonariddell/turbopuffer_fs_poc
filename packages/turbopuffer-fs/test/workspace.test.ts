import { describe, expect, it } from "vitest";

import {
  DEFAULT_WORKSPACE_CONFIG,
  defaultWorkspaceConfig,
  loadWorkspaceConfigFile,
  mergeWorkspaceConfig,
  resolveUserPath,
  resolveWorkspaceConfig,
  sessionStateDoc,
} from "../src/workspace.js";

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

  it("resolves relative user paths against cwd", () => {
    expect(resolveUserPath("src/main.ts", { cwd: "/project" })).toBe("/project/src/main.ts");
    expect(resolveUserPath("../output/report.md", { cwd: "/project/src" })).toBe("/project/output/report.md");
    expect(resolveUserPath("/absolute/path", { cwd: "/project" })).toBe("/absolute/path");
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
});
