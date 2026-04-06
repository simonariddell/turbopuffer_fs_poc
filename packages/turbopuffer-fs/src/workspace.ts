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

export const WORKSPACE_METADATA_PATH = "/state/workspace.json";
export const DEFAULT_WORKSPACE_KIND = "interactive";

export interface SessionState extends AnyObject {
  cwd: string;
  mount: string;
  updated_at: string;
  path: string;
  bundle_id?: string | null;
}

export interface WorkspaceMetadata extends AnyObject {
  path: string;
  mount: string;
  workspace_kind: string;
  created_at: string;
  updated_at: string;
  status: string;
  session_state: string;
  entrypoint: string;
  bundle_manifest: string;
  logs_dir: string;
  output_dir: string;
  scratch_dir: string;
  project_dir: string;
  input_dir: string;
  owner_id?: string | null;
  source_id?: string | null;
  work_item_id?: string | null;
  task_id?: string | null;
  bundle_id?: string | null;
  tags?: string[];
}

export interface WorkspaceShowResult extends AnyObject {
  exists: boolean;
  workspace: WorkspaceConfig;
  metadata: WorkspaceMetadata | null;
  session: SessionState | null;
}

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

function sanitizeTags(tags: unknown): string[] | undefined {
  if (!Array.isArray(tags)) {
    return undefined;
  }
  const values = [...new Set(tags.map((tag) => String(tag).trim()).filter((tag) => tag.length > 0))];
  return values.length > 0 ? values : undefined;
}

export function workspaceMetadataDoc(options: {
  mount: string;
  config?: WorkspaceConfig;
  workspaceKind?: string | null;
  status?: string | null;
  createdAt?: string | null;
  bundleId?: string | null;
  ownerId?: string | null;
  sourceId?: string | null;
  workItemId?: string | null;
  taskId?: string | null;
  tags?: string[] | null;
}): WorkspaceMetadata {
  const workspace = options.config ?? defaultWorkspaceConfig();
  const doc: WorkspaceMetadata = {
    path: WORKSPACE_METADATA_PATH,
    mount: options.mount,
    workspace_kind: options.workspaceKind ?? DEFAULT_WORKSPACE_KIND,
    created_at: options.createdAt ?? nowIsoUtc(),
    updated_at: nowIsoUtc(),
    status: options.status ?? "active",
    session_state: workspace.session_state,
    entrypoint: workspace.entrypoint,
    bundle_manifest: workspace.bundle_manifest,
    logs_dir: workspace.logs_dir,
    output_dir: workspace.output_dir,
    scratch_dir: workspace.scratch_dir,
    project_dir: workspace.project_dir,
    input_dir: workspace.input_dir,
  };
  if (options.bundleId !== undefined && options.bundleId !== null) {
    doc.bundle_id = options.bundleId;
  }
  if (options.ownerId !== undefined) {
    doc.owner_id = options.ownerId;
  }
  if (options.sourceId !== undefined) {
    doc.source_id = options.sourceId;
  }
  if (options.workItemId !== undefined) {
    doc.work_item_id = options.workItemId;
  }
  if (options.taskId !== undefined) {
    doc.task_id = options.taskId;
  }
  const tags = sanitizeTags(options.tags);
  if (tags) {
    doc.tags = tags;
  }
  return doc;
}

export function sessionStateDoc(options: {
  mount: string;
  cwd: string;
  config?: WorkspaceConfig;
  bundleId?: string | null;
}): SessionState {
  const workspace = options.config ?? defaultWorkspaceConfig();
  const doc: SessionState = {
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
): Promise<SessionState> {
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
  const payload = JSON.parse(String(text)) as SessionState;
  payload.path = path;
  return payload;
}

export async function saveSessionState(
  client: Parameters<typeof stat>[0],
  mount: string,
  state: Record<string, unknown>,
  options: { workspaceConfig: WorkspaceConfig },
): Promise<SessionState> {
  const payload = Object.fromEntries(
    Object.entries(state).filter(([key, value]) => key !== "path" && value !== undefined),
  ) as SessionState;
  payload.cwd = normalizePath(String(state.cwd));
  payload.mount = String(state.mount ?? mount);
  payload.updated_at = nowIsoUtc();
  if (payload.bundle_id === undefined) {
    delete payload.bundle_id;
  }
  const path = options.workspaceConfig.session_state;
  await putText(client, mount, path, JSON.stringify(payload, null, 2), {
    mime: "application/json",
  });
  return { ...payload, path } as SessionState;
}

export async function loadWorkspaceMetadata(
  client: Parameters<typeof stat>[0],
  mount: string,
): Promise<WorkspaceMetadata | null> {
  const existing = await stat(client, mount, WORKSPACE_METADATA_PATH);
  if (existing === null) {
    return null;
  }
  const text = await readText(client, mount, WORKSPACE_METADATA_PATH);
  const payload = JSON.parse(String(text)) as WorkspaceMetadata;
  payload.path = WORKSPACE_METADATA_PATH;
  return payload;
}

export async function saveWorkspaceMetadata(
  client: Parameters<typeof stat>[0],
  mount: string,
  metadata: Record<string, unknown>,
  options: { workspaceConfig: WorkspaceConfig },
): Promise<WorkspaceMetadata> {
  const workspace = options.workspaceConfig;
  const payload = Object.fromEntries(
    Object.entries(metadata).filter(([key, value]) => key !== "path" && value !== undefined),
  ) as WorkspaceMetadata;
  payload.mount = String(metadata.mount ?? mount);
  payload.path = WORKSPACE_METADATA_PATH;
  payload.workspace_kind = String(metadata.workspace_kind ?? DEFAULT_WORKSPACE_KIND);
  payload.created_at = typeof metadata.created_at === "string" ? metadata.created_at : nowIsoUtc();
  payload.updated_at = nowIsoUtc();
  payload.status = String(metadata.status ?? "active");
  payload.session_state = String(metadata.session_state ?? workspace.session_state);
  payload.entrypoint = String(metadata.entrypoint ?? workspace.entrypoint);
  payload.bundle_manifest = String(metadata.bundle_manifest ?? workspace.bundle_manifest);
  payload.logs_dir = String(metadata.logs_dir ?? workspace.logs_dir);
  payload.output_dir = String(metadata.output_dir ?? workspace.output_dir);
  payload.scratch_dir = String(metadata.scratch_dir ?? workspace.scratch_dir);
  payload.project_dir = String(metadata.project_dir ?? workspace.project_dir);
  payload.input_dir = String(metadata.input_dir ?? workspace.input_dir);
  const tags = sanitizeTags(metadata.tags);
  if (tags) {
    payload.tags = tags;
  } else {
    delete payload.tags;
  }
  await putText(client, mount, WORKSPACE_METADATA_PATH, JSON.stringify(payload, null, 2), {
    mime: "application/json",
  });
  return payload;
}

export async function workspaceExists(
  client: Parameters<typeof stat>[0],
  mount: string,
  options: { workspaceConfig: WorkspaceConfig },
): Promise<boolean> {
  const metadata = await loadWorkspaceMetadata(client, mount);
  if (metadata !== null) {
    return true;
  }
  const session = await loadSessionState(client, mount, options);
  return (await stat(client, mount, session.path)) !== null;
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

export async function workspaceShow(
  client: Parameters<typeof stat>[0],
  mount: string,
  options: { workspaceConfig: WorkspaceConfig },
): Promise<WorkspaceShowResult> {
  const exists = await workspaceExists(client, mount, options);
  if (!exists) {
    return {
      exists: false,
      workspace: options.workspaceConfig,
      metadata: null,
      session: null,
    };
  }
  const session = await loadSessionState(client, mount, options);
  const metadata =
    (await loadWorkspaceMetadata(client, mount)) ??
    workspaceMetadataDoc({
      mount,
      config: options.workspaceConfig,
      createdAt: session.updated_at,
      bundleId: session.bundle_id ?? null,
    });
  return {
    exists: true,
    workspace: options.workspaceConfig,
    metadata,
    session,
  };
}

export async function workspacePwd(
  client: Parameters<typeof stat>[0],
  mount: string,
  options: { workspaceConfig: WorkspaceConfig },
): Promise<string> {
  const state = await loadSessionState(client, mount, options);
  return String(state.cwd);
}

export async function workspaceCd(
  client: Parameters<typeof stat>[0],
  mount: string,
  targetPath: string,
  options: { workspaceConfig: WorkspaceConfig },
): Promise<{ cwd: string; mount: string }> {
  const session = await loadSessionState(client, mount, options);
  const resolved = resolveCliPath(targetPath, { cwd: String(session.cwd) });
  const target = await stat(client, mount, resolved);
  if (target === null) {
    throw new Error(`FileNotFoundError:${resolved}`);
  }
  if (target.kind !== "dir") {
    throw new Error(`NotADirectoryError:${resolved}`);
  }
  const saved = await saveSessionState(
    client,
    mount,
    { ...session, cwd: resolved, mount },
    options,
  );
  return {
    cwd: saved.cwd,
    mount,
  };
}

export async function workspaceInit(
  client: Parameters<typeof stat>[0],
  mount: string,
  options: {
    workspaceConfig: WorkspaceConfig;
    bundleId?: string | null;
    cwd?: string | null;
    workspaceKind?: string | null;
    ownerId?: string | null;
    sourceId?: string | null;
    workItemId?: string | null;
    taskId?: string | null;
    tags?: string[] | null;
    status?: string | null;
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
  const metadataParent = parentPath(WORKSPACE_METADATA_PATH);
  if (metadataParent) {
    await mkdir(client, mount, metadataParent);
  }

  const existingSessionRow = await stat(client, mount, config.session_state);
  const existingSession =
    existingSessionRow === null
      ? null
      : await loadSessionState(client, mount, { workspaceConfig: config });
  const existingMetadata = await loadWorkspaceMetadata(client, mount);

  const cwd = options.cwd
    ? normalizePath(options.cwd)
    : existingSession?.cwd
      ? normalizePath(String(existingSession.cwd))
      : config.project_dir ?? "/";
  const session = await saveSessionState(
    client,
    mount,
    {
      ...(existingSession ?? {}),
      cwd,
      mount,
      bundle_id: options.bundleId ?? existingSession?.bundle_id ?? null,
    },
    { workspaceConfig: config },
  );
  const metadata = await saveWorkspaceMetadata(
    client,
    mount,
    {
      ...(existingMetadata ?? {}),
      mount,
      workspace_kind: options.workspaceKind ?? existingMetadata?.workspace_kind ?? DEFAULT_WORKSPACE_KIND,
      created_at: existingMetadata?.created_at ?? session.updated_at,
      status: options.status ?? existingMetadata?.status ?? "active",
      owner_id: options.ownerId ?? existingMetadata?.owner_id,
      source_id: options.sourceId ?? existingMetadata?.source_id,
      work_item_id: options.workItemId ?? existingMetadata?.work_item_id,
      task_id: options.taskId ?? existingMetadata?.task_id,
      bundle_id: options.bundleId ?? existingMetadata?.bundle_id ?? session.bundle_id ?? null,
      tags: options.tags ?? existingMetadata?.tags,
    },
    { workspaceConfig: config },
  );

  return {
    mount,
    namespace: mountNamespace(mount),
    created,
    session_state: config.session_state,
    workspace_metadata: WORKSPACE_METADATA_PATH,
    cwd,
    session,
    metadata,
  };
}

export async function deleteWorkspace(
  client: Parameters<typeof stat>[0] & {
    namespace(namespace: string): { deleteAll(): Promise<void> };
  },
  mount: string,
): Promise<AnyObject> {
  const namespace = mountNamespace(mount);
  await client.namespace(namespace).deleteAll();
  return {
    mount,
    namespace,
    deleted: true,
  };
}

export async function archiveWorkspace(
  client: Parameters<typeof stat>[0],
  mount: string,
  options: { workspaceConfig: WorkspaceConfig },
): Promise<AnyObject> {
  const exists = await workspaceExists(client, mount, options);
  if (!exists) {
    return {
      mount,
      namespace: mountNamespace(mount),
      archived: false,
      metadata: null,
    };
  }
  const session = await loadSessionState(client, mount, options);
  const metadata =
    (await loadWorkspaceMetadata(client, mount)) ??
    workspaceMetadataDoc({
      mount,
      config: options.workspaceConfig,
      createdAt: session.updated_at,
      bundleId: session.bundle_id ?? null,
    });
  const saved = await saveWorkspaceMetadata(
    client,
    mount,
    {
      ...metadata,
      status: "archived",
      bundle_id: metadata.bundle_id ?? session.bundle_id ?? null,
    },
    options,
  );
  return {
    mount,
    namespace: mountNamespace(mount),
    archived: true,
    metadata: saved,
  };
}
