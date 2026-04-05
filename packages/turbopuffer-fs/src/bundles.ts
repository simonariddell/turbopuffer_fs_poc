import { readFile } from "node:fs/promises";
import { join as joinPath } from "node:path";

import { ingestDirectory } from "./ingest.js";
import { normalizePath } from "./paths.js";
import { resolveWorkspaceConfig, workspaceInit, type WorkspaceConfig } from "./workspace.js";
import { stat } from "./live.js";
import type { AnyObject } from "./types.js";

export type BundleSpec = {
  id?: string;
  title?: string;
  kind?: string;
  version?: number;
  entrypoint?: string;
  workspace?: Record<string, unknown>;
  allowed_outputs?: string[];
  expected_inputs?: string[];
  suggested_checks?: string[];
  tags?: string[];
} & AnyObject;

export async function loadBundleSpec(localRoot: string): Promise<BundleSpec> {
  const specPath = joinPath(localRoot, "bundle.json");
  return JSON.parse(await readFile(specPath, "utf8")) as BundleSpec;
}

export function bundleEntrypoint(spec: BundleSpec): string {
  return normalizePath(String(spec.entrypoint ?? "/TASK.md"));
}

export function listAllowedOutputs(spec: BundleSpec): string[] {
  return Array.isArray(spec.allowed_outputs)
    ? spec.allowed_outputs.map((item) => normalizePath(String(item)))
    : [];
}

export function bundleWorkspaceConfig(spec: BundleSpec): WorkspaceConfig {
  return resolveWorkspaceConfig({ bundleSpec: spec });
}

export async function bundleTaskPrompt(localRoot: string): Promise<string> {
  const spec = await loadBundleSpec(localRoot);
  const workspace = bundleWorkspaceConfig(spec);
  const taskText = await readFile(joinPath(localRoot, "TASK.md"), "utf8");
  const allowedOutputs = listAllowedOutputs(spec).map((path) => `- ${path}`).join("\n");
  return [
    "You are working inside a filesystem-shaped workspace backed by turbopuffer.",
    "",
    "Rules:",
    "- Use the filesystem interface for all persistent reads and writes.",
    "- Read /bundle.json and /TASK.md first.",
    `- Log every meaningful action to ${workspace.logs_dir}/run.jsonl.`,
    `- Write a final summary to ${workspace.logs_dir}/summary.md.`,
    "- Only write outputs to allowed output locations.",
    "",
    `Bundle ID: ${spec.id ?? "unknown"}`,
    `Entrypoint: ${bundleEntrypoint(spec)}`,
    `Session state file: ${workspace.session_state}`,
    "Allowed outputs:",
    allowedOutputs,
    "",
    "Task:",
    taskText,
  ].join("\n");
}

export async function validateBundleOutputs(
  client: Parameters<typeof stat>[0],
  mount: string,
  spec: BundleSpec,
): Promise<string[]> {
  const missing: string[] = [];
  for (const output of listAllowedOutputs(spec)) {
    const row = await stat(client, mount, output);
    if (row === null) {
      missing.push(output);
    }
  }
  return missing;
}

export async function seedBundle(
  client: Parameters<typeof stat>[0],
  mount: string,
  localRoot: string,
  options: { mountRoot?: string } = {},
): Promise<AnyObject> {
  const spec = await loadBundleSpec(localRoot);
  const workspace = bundleWorkspaceConfig(spec);
  const summary = await ingestDirectory(client, mount, localRoot, {
    mountRoot: options.mountRoot ?? "/",
  });
  await workspaceInit(client, mount, {
    workspaceConfig: workspace,
    cwd: workspace.project_dir,
    bundleId: spec.id ?? null,
  });
  return {
    ...summary,
    workspace,
    entrypoint: bundleEntrypoint(spec),
    allowedOutputs: listAllowedOutputs(spec),
  };
}
