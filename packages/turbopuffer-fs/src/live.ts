import Turbopuffer from "@turbopuffer/turbopuffer";

import {
  catPlan,
  findPlan,
  grepPlan,
  headPlan,
  lsPlan,
  mkdirPlan,
  putBytesPlan,
  putTextPlan,
  readBytesPlan,
  readTextPlan,
  rmPlan,
  statPlan,
  tailPlan,
} from "./plans.js";
import { run } from "./runtime.js";
export { ingestDirectory } from "./ingest.js";
import type { GrepOptions } from "./types.js";

export const MOUNT_SUFFIX = "__fs";

export function mountNamespace(mount: string): string {
  if (mount.length === 0) {
    throw new Error("mount must not be empty");
  }
  if (mount.includes("/")) {
    throw new Error(`mount must not contain '/': ${mount}`);
  }
  return `${mount}${MOUNT_SUFFIX}`;
}

export function makeClient(options: {
  apiKey?: string;
  region?: string;
  baseURL?: string;
}) {
  return new Turbopuffer({
    apiKey: options.apiKey,
    region: options.region,
    baseURL: options.baseURL,
  });
}

export async function listMounts(client: Turbopuffer, suffix = MOUNT_SUFFIX): Promise<string[]> {
  const response = await client.namespaces();
  const namespaces: Array<{ id: string }> = [];
  for await (const namespace of response) {
    namespaces.push({ id: namespace.id });
  }
  const mounts = namespaces
    .map((namespace) => namespace.id)
    .filter((id: string) => id.endsWith(suffix))
    .map((id: string) => id.slice(0, -suffix.length));
  mounts.sort();
  return mounts;
}

export async function runPlan(client: Turbopuffer, plan: import("./types.js").Plan): Promise<unknown> {
  return run(client, plan);
}

export function stat(client: Turbopuffer, mount: string, path: string) {
  return runPlan(client, statPlan(mountNamespace(mount), path));
}

export function ls(client: Turbopuffer, mount: string, path = "/", limit?: number) {
  return runPlan(client, lsPlan(mountNamespace(mount), path, limit));
}

export function find(
  client: Turbopuffer,
  mount: string,
  root = "/",
  options: { glob?: string; kind?: "file" | "dir"; ignoreCase?: boolean; limit?: number } = {},
) {
  return runPlan(
    client,
    findPlan(mountNamespace(mount), root, {
      glob: options.glob,
      kind: options.kind,
      ignoreCase: options.ignoreCase,
      limit: options.limit,
    }),
  );
}

export function cat(client: Turbopuffer, mount: string, path: string) {
  return runPlan(client, catPlan(mountNamespace(mount), path));
}

export function head(client: Turbopuffer, mount: string, path: string, n = 10) {
  return runPlan(client, headPlan(mountNamespace(mount), path, n));
}

export function tail(client: Turbopuffer, mount: string, path: string, n = 10) {
  return runPlan(client, tailPlan(mountNamespace(mount), path, n));
}

export function grep(
  client: Turbopuffer,
  mount: string,
  root: string,
  pattern: string,
  options: GrepOptions = {},
) {
  return runPlan(
    client,
    grepPlan(mountNamespace(mount), root, pattern, {
      ignoreCase: options.ignoreCase,
      glob: options.glob,
      limit: options.limit,
      mode: options.mode,
      multiline: options.multiline,
      dotAll: options.dotAll,
      lastAsPrefix: options.lastAsPrefix,
    }),
  );
}

export function search(
  client: Turbopuffer,
  mount: string,
  root: string,
  pattern: string,
  options: GrepOptions = {},
) {
  return grep(client, mount, root, pattern, options);
}

export function readText(client: Turbopuffer, mount: string, path: string) {
  return runPlan(client, readTextPlan(mountNamespace(mount), path));
}

export function readBytes(client: Turbopuffer, mount: string, path: string) {
  return runPlan(client, readBytesPlan(mountNamespace(mount), path));
}

export function mkdir(client: Turbopuffer, mount: string, path: string) {
  return runPlan(client, mkdirPlan(mountNamespace(mount), path));
}

export function putText(
  client: Turbopuffer,
  mount: string,
  path: string,
  text: string,
  options: { mime?: string } = {},
) {
  return runPlan(client, putTextPlan(mountNamespace(mount), path, text, options.mime));
}

export function putBytes(
  client: Turbopuffer,
  mount: string,
  path: string,
  data: Uint8Array,
  options: { mime?: string } = {},
) {
  return runPlan(client, putBytesPlan(mountNamespace(mount), path, data, options.mime));
}

export function rm(client: Turbopuffer, mount: string, path: string, recursive = false) {
  return runPlan(client, rmPlan(mountNamespace(mount), path, recursive));
}
