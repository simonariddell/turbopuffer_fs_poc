import { readdir, readFile, stat as fsStat } from "node:fs/promises";
import { resolve as resolvePath } from "node:path";

import { mountNamespace } from "./live.js";
import { joinPath, normalizePath } from "./paths.js";
import { directoryRow, rowFromBytes, upsertRowsPayload } from "./schema.js";
import type { AnyObject, FsRow } from "./types.js";

export function mountedPath(localRoot: string, localPath: string, mountRoot = "/"): string {
  const normalizedMountRoot = normalizePath(mountRoot);
  const relative = localPath.slice(localRoot.length).replace(/^\/+/, "");
  return relative === "" ? normalizedMountRoot : joinPath(normalizedMountRoot, relative);
}

async function scanRecursive(root: string, path: string, rows: FsRow[], mountRoot: string): Promise<void> {
  const stats = await fsStat(path);
  const mountPath = mountedPath(root, path, mountRoot);
  if (stats.isDirectory()) {
    rows.push(directoryRow(mountPath, Number(stats.mtimeMs * 1_000_000)));
    const entries = (await readdir(path)).sort();
    for (const entry of entries) {
      await scanRecursive(root, resolvePath(path, entry), rows, mountRoot);
    }
    return;
  }
  const data = new Uint8Array(await readFile(path));
  rows.push(
    rowFromBytes(
      mountPath,
      data,
      undefined,
      Number(stats.mtimeMs * 1_000_000),
      stats.size,
    ),
  );
}

export async function scanDirectory(localRoot: string, options: { mountRoot?: string } = {}): Promise<FsRow[]> {
  const root = resolvePath(localRoot);
  const stats = await fsStat(root);
  if (!stats.isDirectory()) {
    throw new Error(`NotADirectoryError:${root}`);
  }
  const rows: FsRow[] = [];
  await scanRecursive(root, root, rows, options.mountRoot ?? "/");
  return rows;
}

export function batched<T>(rows: T[], batchSize: number): T[][] {
  if (!Number.isInteger(batchSize) || batchSize < 1) {
    throw new Error("batchSize must be a positive integer");
  }
  const batches: T[][] = [];
  for (let index = 0; index < rows.length; index += batchSize) {
    batches.push(rows.slice(index, index + batchSize));
  }
  return batches;
}

export async function writeRows(
  client: { namespace(namespace: string): { write(payload: AnyObject): Promise<unknown> } },
  namespace: string,
  rows: FsRow[],
  options: { batchSize?: number } = {},
): Promise<unknown[]> {
  const handle = client.namespace(namespace);
  const batchSize = options.batchSize ?? 256;
  const responses: unknown[] = [];
  for (const batch of batched(rows, batchSize)) {
    responses.push(await handle.write(upsertRowsPayload(batch) as AnyObject));
  }
  return responses;
}

export async function ingestDirectory(
  client: { namespace(namespace: string): { write(payload: AnyObject): Promise<unknown> } },
  mount: string,
  localRoot: string,
  options: { mountRoot?: string; batchSize?: number } = {},
): Promise<AnyObject> {
  const rows = await scanDirectory(localRoot, { mountRoot: options.mountRoot });
  const namespace = mountNamespace(mount);
  const writes = await writeRows(client, namespace, rows, { batchSize: options.batchSize });
  return {
    mount,
    namespace,
    rowCount: rows.length,
    rows,
    writes,
  };
}
