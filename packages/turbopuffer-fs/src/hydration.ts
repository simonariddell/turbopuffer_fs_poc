import { mkdir as fsMkdir, readdir as fsReaddir, readFile as fsReadFile, rm as fsRm, stat as fsStat, writeFile as fsWriteFile } from "node:fs/promises";
import { dirname, join, resolve as resolvePath } from "node:path";

import { directoryRow, rowFromBytes } from "./schema.js";
import { normalizePath } from "./paths.js";
import { find, loadSessionState, mkdir, putBytes, putText, readBytes, readText, rm, stat, type WorkspaceConfig } from "./index.js";
import type { AnyObject } from "./types.js";

export interface HydrationEntry extends AnyObject {
  path: string;
  kind: "file" | "dir";
  sha256?: string;
  mime?: string;
  size_bytes?: number;
  is_text?: number;
}

export interface HydrationManifest extends AnyObject {
  mount: string;
  hydrated_at: string;
  root: string;
  cwd: string;
  workspace_metadata_path: string;
  entries: Record<string, HydrationEntry>;
}

export interface SyncConflict extends AnyObject {
  path: string;
  reason: string;
}

export interface SyncWorkspaceResult extends AnyObject {
  mount: string;
  root: string;
  created: string[];
  modified: string[];
  deleted: string[];
  unchanged: string[];
  conflicts: SyncConflict[];
}

async function ensureParentDirectory(path: string): Promise<void> {
  await fsMkdir(dirname(path), { recursive: true });
}

function localPathFor(root: string, tpfsPath: string): string {
  if (tpfsPath === "/") {
    return root;
  }
  return join(root, tpfsPath.slice(1));
}

async function hydrateEntry(
  client: Parameters<typeof stat>[0],
  mount: string,
  localRoot: string,
  entry: HydrationEntry,
): Promise<void> {
  const localPath = localPathFor(localRoot, entry.path);
  if (entry.kind === "dir") {
    await fsMkdir(localPath, { recursive: true });
    return;
  }
  await ensureParentDirectory(localPath);
  if (Number(entry.is_text ?? 0) === 1) {
    const text = await readText(client, mount, entry.path);
    await fsWriteFile(localPath, String(text), "utf8");
    return;
  }
  const bytes = (await readBytes(client, mount, entry.path)) as Uint8Array;
  await fsWriteFile(localPath, Buffer.from(bytes));
}

async function scanLocalRecursive(root: string, localPath: string, rows: HydrationEntry[]): Promise<void> {
  const stats = await fsStat(localPath);
  const relative = localPath.slice(root.length).replace(/^\/+/, "");
  const mountPath = normalizePath(relative === "" ? "/" : `/${relative}`);
  if (stats.isDirectory()) {
    const directorySha = directoryRow(mountPath).sha256;
    rows.push({
      path: mountPath,
      kind: "dir",
      sha256: typeof directorySha === "string" ? directorySha : undefined,
      size_bytes: 0,
      is_text: 0,
    });
    const entries = (await fsReaddir(localPath)).sort();
    for (const entry of entries) {
      await scanLocalRecursive(root, resolvePath(localPath, entry), rows);
    }
    return;
  }
  const data = new Uint8Array(await fsReadFile(localPath));
  const row = rowFromBytes(mountPath, data);
  rows.push({
    path: mountPath,
    kind: String(row.kind) === "dir" ? "dir" : "file",
    sha256: typeof row.sha256 === "string" ? row.sha256 : undefined,
    mime: typeof row.mime === "string" ? row.mime : undefined,
    size_bytes: typeof row.size_bytes === "number" ? row.size_bytes : Number(row.size_bytes ?? 0),
    is_text: typeof row.is_text === "number" ? row.is_text : Number(row.is_text ?? 0),
  });
}

function mapEntries(entries: HydrationEntry[]): Map<string, HydrationEntry> {
  return new Map(entries.map((entry) => [entry.path, entry]));
}

function entriesRecord(entries: HydrationEntry[]): Record<string, HydrationEntry> {
  return Object.fromEntries(entries.map((entry) => [entry.path, entry]));
}

function hasChanged(remote: HydrationEntry | undefined, local: HydrationEntry | undefined): boolean {
  if (!remote || !local) {
    return remote !== local;
  }
  return (
    remote.kind !== local.kind ||
    remote.sha256 !== local.sha256 ||
    Number(remote.size_bytes ?? 0) !== Number(local.size_bytes ?? 0) ||
    Number(remote.is_text ?? 0) !== Number(local.is_text ?? 0)
  );
}

export async function hydrateWorkspace(
  client: Parameters<typeof stat>[0],
  mount: string,
  localRoot: string,
  options: { workspaceConfig: WorkspaceConfig } & { root?: string | null } ,
): Promise<HydrationManifest> {
  const root = normalizePath(options.root ?? "/");
  await fsMkdir(localRoot, { recursive: true });
  const rows = (await find(client, mount, root)) as Array<Record<string, unknown>>;
  const entries = rows.map((row) => ({
    path: String(row.path),
    kind: String(row.kind) === "dir" ? "dir" : "file",
    sha256: typeof row.sha256 === "string" ? row.sha256 : undefined,
    mime: typeof row.mime === "string" ? row.mime : undefined,
    size_bytes: Number(row.size_bytes ?? 0),
    is_text: Number(row.is_text ?? 0),
  })) as HydrationEntry[];
  for (const entry of entries) {
    await hydrateEntry(client, mount, localRoot, entry);
  }
  const session = await loadSessionState(client, mount, { workspaceConfig: options.workspaceConfig });
  return {
    mount,
    hydrated_at: new Date().toISOString(),
    root,
    cwd: String(session.cwd),
    workspace_metadata_path: "/state/workspace.json",
    entries: entriesRecord(entries),
  };
}

export async function syncWorkspace(
  client: Parameters<typeof stat>[0],
  mount: string,
  localRoot: string,
  manifest: HydrationManifest,
  _options?: { workspaceConfig?: WorkspaceConfig },
): Promise<SyncWorkspaceResult> {
  const currentRows = (await find(client, mount, manifest.root)) as Array<Record<string, unknown>>;
  const currentEntries = currentRows.map((row) => ({
    path: String(row.path),
    kind: String(row.kind) === "dir" ? "dir" : "file",
    sha256: typeof row.sha256 === "string" ? row.sha256 : undefined,
    mime: typeof row.mime === "string" ? row.mime : undefined,
    size_bytes: Number(row.size_bytes ?? 0),
    is_text: Number(row.is_text ?? 0),
  })) as HydrationEntry[];
  const localEntries: HydrationEntry[] = [];
  await scanLocalRecursive(resolvePath(localRoot), resolvePath(localRoot), localEntries);

  const snapshotEntries = Object.values(manifest.entries);
  const snapshotMap = mapEntries(snapshotEntries);
  const currentMap = mapEntries(currentEntries);
  const localMap = mapEntries(localEntries);

  const created: string[] = [];
  const modified: string[] = [];
  const deleted: string[] = [];
  const unchanged: string[] = [];
  const conflicts: SyncConflict[] = [];

  const candidatePaths = [...new Set([...localMap.keys(), ...snapshotMap.keys()])].sort();
  for (const path of candidatePaths) {
    if (path === "/") continue;
    const snapshot = snapshotMap.get(path);
    const current = currentMap.get(path);
    const local = localMap.get(path);
    const changedLocally = hasChanged(snapshot, local);
    if (!changedLocally) {
      unchanged.push(path);
      continue;
    }
    const changedRemotely = hasChanged(snapshot, current);
    if (changedRemotely) {
      conflicts.push({ path, reason: "remote_changed_since_hydration" });
      continue;
    }
    if (!local) {
      await rm(client, mount, path, current?.kind === "dir");
      deleted.push(path);
      continue;
    }
    const localFilePath = localPathFor(resolvePath(localRoot), path);
    if (local.kind === "dir") {
      if (!current) {
        await mkdir(client, mount, path);
        created.push(path);
      } else {
        unchanged.push(path);
      }
      continue;
    }
    const bytes = new Uint8Array(await fsReadFile(localFilePath));
    const remoteMime = typeof current?.mime === "string" ? current.mime : undefined;
    const effectiveIsText = Number(current?.is_text ?? snapshot?.is_text ?? local.is_text ?? 0);
    if (effectiveIsText === 1) {
      await putText(client, mount, path, Buffer.from(bytes).toString("utf8"), {
        mime: remoteMime ?? local.mime,
      });
    } else {
      await putBytes(client, mount, path, bytes, {
        mime: remoteMime ?? local.mime,
      });
    }
    if (!current) {
      created.push(path);
    } else {
      modified.push(path);
    }
  }

  return {
    mount,
    root: manifest.root,
    created,
    modified,
    deleted,
    unchanged,
    conflicts,
  };
}

export async function clearHydratedWorkspace(localRoot: string): Promise<void> {
  await fsRm(localRoot, { recursive: true, force: true });
}
