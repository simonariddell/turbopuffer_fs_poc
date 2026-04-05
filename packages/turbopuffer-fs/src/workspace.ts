import { mkdir, mountNamespace, putText, readText, stat } from "./live.js";
import { normalizePath, parentPath } from "./paths.js";
import type { AnyObject } from "./types.js";

export const DEFAULT_WORKSPACE_CONFIG = {
  entrypoint: "/TASK.md",
  bundle_manifest: "/bundle.json",
  session_state: "/state/session.json",
  logs_dir: "/logs",
  output_dir: "/output",
  scratch_dir: "/scratch",
  project_dir: "/project",
  input_dir: "/input",
} as const satisfies Record<string, string>;

export type WorkspaceConfig = Record<keyof typeof DEFAULT_WORKSPACE_CONFIG, string> &
  Record<string, string>;

export function defaultWorkspaceConfig(): WorkspaceConfig {
  return { ...DEFAULT_WORKSPACE_CONFIG };
}

export function validateWorkspaceConfig(config: Record<string, unknown>): WorkspaceConfig {
  const validated: Record<string, string> = {};
  for (const [key, value] of Object.entries(config)) {
    validated[key] = normalizePath(String(value));
  }
  return validated as WorkspaceConfig;
}

export function mergeWorkspaceConfig(
  ...configs: Array<Record<string, unknown> | null | undefined>
): WorkspaceConfig {
  const merged: Record<string, string> = defaultWorkspaceConfig();
  for (const config of configs) {
    if (!config) continue;
    for (const [key, value] of Object.entries(config)) {
      if (value === undefined || value === null) continue;
      merged[key] = String(value);
    }
  }
  return validateWorkspaceConfig(merged);
}

export function loadWorkspaceConfigFile(contents: string): WorkspaceConfig {
  return validateWorkspaceConfig(JSON.parse(contents) as Record<string, unknown>);
}

export function resolveWorkspaceConfig(options: {
  deploymentConfig?: Record<string, unknown> | null;
  bundleSpec?: Record<string, unknown> | null;
  bundleWorkspace?: Record<string, unknown> | null;
  overrides?: Record<string, unknown> | null;
} = {}): WorkspaceConfig {
  const workspaceFromBundle =
    options.bundleWorkspace ?? ((options.bundleSpec?.workspace as Record<string, unknown> | undefined) ?? null);
  return mergeWorkspaceConfig(
    defaultWorkspaceConfig(),
    options.deploymentConfig ?? null,
    workspaceFromBundle,
    options.overrides ?? null,
  );
}

function nowIsoUtc(): string {
  return new Date().toISOString();
}

export function sessionStateDoc(options: {
  mount: string;
  cwd: string;
  config?: WorkspaceConfig;
  bundleId?: string | null;
}): AnyObject {
  const workspace = options.config ?? defaultWorkspaceConfig();
  const doc: AnyObject = {
    cwd: normalizePath(options.cwd),
    mount: options.mount,
    updated_at: nowIsoUtc(),
    path: workspace.session_state,
  };
  if (options.bundleId) {
    doc.bundle_id = options.bundleId;
  }
  return doc;
}

export async function loadSessionState(
  client: Parameters<typeof stat>[0],
  mount: string,
  options: { workspaceConfig: WorkspaceConfig },
): Promise<AnyObject> {
  const path = options.workspaceConfig.session_state;
  const existing = await stat(client, mount, path);
  if (existing === null) {
    return sessionStateDoc({
      mount,
      cwd: options.workspaceConfig.project_dir ?? "/",
      config: options.workspaceConfig,
    });
  }
  const text = await readText(client, mount, path);
  const payload = JSON.parse(String(text)) as AnyObject;
  payload.path = path;
  return payload;
}

export async function saveSessionState(
  client: Parameters<typeof stat>[0],
  mount: string,
  state: Record<string, unknown>,
  options: { workspaceConfig: WorkspaceConfig },
): Promise<AnyObject> {
  const payload: AnyObject = {
    cwd: normalizePath(String(state.cwd)),
    mount: String(state.mount ?? mount),
    updated_at: nowIsoUtc(),
  };
  if (state.bundle_id !== undefined) {
    payload.bundle_id = state.bundle_id;
  }
  const path = options.workspaceConfig.session_state;
  await putText(client, mount, path, JSON.stringify(payload, null, 2), {
    mime: "application/json",
  });
  return { ...payload, path };
}

export function resolveUserPath(userPath: string | null | undefined, options: { cwd: string }): string {
  if (userPath === null || userPath === undefined || userPath === "") {
    return normalizePath(options.cwd);
  }
  const raw = String(userPath);
  if (raw.startsWith("/")) {
    return normalizePath(raw);
  }
  if (raw === "." || raw === "./") {
    return normalizePath(options.cwd);
  }
  const segments = raw.split("/").filter((segment) => segment !== "" && segment !== ".");
  let current = normalizePath(options.cwd);
  for (const segment of segments) {
    if (segment === "..") {
      current = parentPath(current) ?? "/";
      continue;
    }
    current = normalizePath(`${current.replace(/\/$/, "")}/${segment}`);
  }
  return current;
}

export const resolveCliPath = resolveUserPath;

export async function workspaceInit(
  client: Parameters<typeof stat>[0],
  mount: string,
  options: {
    workspaceConfig: WorkspaceConfig;
    bundleId?: string | null;
    cwd?: string | null;
  },
): Promise<AnyObject> {
  const config = options.workspaceConfig;
  const created: string[] = [];
  for (const key of ["logs_dir", "output_dir", "scratch_dir", "project_dir", "input_dir"] as const) {
    await mkdir(client, mount, config[key]);
    created.push(config[key]);
  }

  const sessionParent = parentPath(config.session_state);
  if (sessionParent) {
    await mkdir(client, mount, sessionParent);
  }

  const cwd = options.cwd ? normalizePath(options.cwd) : config.project_dir ?? "/";
  const session = sessionStateDoc({
    mount,
    cwd,
    config,
    bundleId: options.bundleId ?? null,
  });
  await saveSessionState(client, mount, session, { workspaceConfig: config });

  return {
    mount,
    namespace: mountNamespace(mount),
    created,
    session_state: config.session_state,
    cwd,
    session,
  };
}
