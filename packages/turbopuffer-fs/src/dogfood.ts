import { randomUUID } from "node:crypto";
import { mkdtemp, writeFile, mkdir as mkdirFs } from "node:fs/promises";
import { join as joinPath } from "node:path";
import { tmpdir } from "node:os";

import {
  find,
  grep,
  ls,
  makeClient,
  mkdir,
  mountNamespace,
  putBytes,
  putText,
  readBytes,
  readText,
  rm,
  stat,
} from "./live.js";
import { ingestDirectory } from "./ingest.js";
import { basename, normalizePath, parentPath } from "./paths.js";
import type { AnyObject } from "./types.js";

export type DogfoodOp = Record<string, unknown>;
export type ModelState = Record<string, Record<string, unknown>>;

const TEXT_PAYLOADS = [
  "hello world\n",
  "oauth token exchange\ncallback success\n",
  "alpha\nbeta\ngamma\n",
  "notes\nline two\nline three\n",
];

const BINARY_PAYLOADS = [
  Uint8Array.from([0, 1, 2]),
  Uint8Array.from([16, 32, 48, 64]),
  Uint8Array.from({ length: 16 }, (_, index) => index),
];

export function newModelState(): ModelState {
  return { "/": { kind: "dir" } };
}

function childrenOf(model: ModelState, path: string): string[] {
  const normalized = normalizePath(path);
  return Object.keys(model)
    .filter((candidate) => candidate !== normalized && parentPath(candidate) === normalized)
    .sort();
}

function descendantsOf(model: ModelState, path: string): string[] {
  const normalized = normalizePath(path);
  return Object.keys(model)
    .filter((candidate) => candidate === normalized || candidate.startsWith(`${normalized.replace(/\/$/, "")}/`))
    .sort();
}

function ensureParentDirs(model: ModelState, path: string): void {
  let current = parentPath(path);
  const stack: string[] = [];
  while (current !== null && !(current in model)) {
    stack.push(current);
    current = parentPath(current);
  }
  for (const directory of stack.reverse()) {
    model[directory] = { kind: "dir" };
  }
}

function copyModelFile(model: ModelState, src: string, dest: string): void {
  const source = model[src];
  if (!source || source.kind !== "file") {
    throw new Error(`FileNotFoundError:${src}`);
  }
  ensureParentDirs(model, dest);
  model[dest] = {
    ...source,
    text: source.text,
    bytes: source.bytes instanceof Uint8Array ? Uint8Array.from(source.bytes) : source.bytes,
  };
}

function copyModelDirectory(model: ModelState, src: string, dest: string): void {
  if (dest === src || dest.startsWith(`${src.replace(/\/$/, "")}/`)) {
    throw new Error(`InvalidCopyTarget:${src}->${dest}`);
  }
  const subtree = descendantsOf(model, src);
  for (const candidate of subtree) {
    const suffix = candidate === src ? "" : candidate.slice(src.length);
    const mapped = suffix === "" ? dest : normalizePath(`${dest.replace(/\/$/, "")}${suffix}`);
    if (model[candidate]?.kind === "dir") {
      ensureParentDirs(model, mapped);
      model[mapped] = { kind: "dir" };
      continue;
    }
    copyModelFile(model, candidate, mapped);
  }
}

export function applyModelOperation(model: ModelState, operation: DogfoodOp): void {
  const op = String(operation.op);
  const path = normalizePath(String(operation.path));
  if (op === "mkdir") {
    ensureParentDirs(model, path);
    model[path] = { kind: "dir" };
    return;
  }
  if (op === "put_text") {
    ensureParentDirs(model, path);
    const text = String(operation.text);
    model[path] = { kind: "file", is_text: 1, text, bytes: new TextEncoder().encode(text) };
    return;
  }
  if (op === "put_bytes") {
    ensureParentDirs(model, path);
    const data = operation.data as Uint8Array;
    model[path] = { kind: "file", is_text: 0, bytes: data };
    return;
  }
  if (op === "rm") {
    const recursive = Boolean(operation.recursive);
    if (!(path in model)) return;
    if (model[path].kind === "dir") {
      const children = childrenOf(model, path);
      if (children.length > 0 && !recursive) {
        throw new Error(`DirectoryNotEmptyError:${path}`);
      }
      for (const candidate of descendantsOf(model, path).reverse()) {
        if (candidate !== "/") delete model[candidate];
      }
      return;
    }
    delete model[path];
    return;
  }
  if (op === "cp") {
    const dest = normalizePath(String(operation.dest));
    const recursive = Boolean(operation.recursive);
    const source = model[path];
    if (!source) {
      throw new Error(`FileNotFoundError:${path}`);
    }
    if (source.kind === "dir") {
      if (!recursive) {
        throw new Error(`RecursiveRequiredError:${path}`);
      }
      copyModelDirectory(model, path, dest);
      return;
    }
    copyModelFile(model, path, dest);
    return;
  }
  if (op === "mv") {
    const dest = normalizePath(String(operation.dest));
    const source = model[path];
    if (!source) {
      throw new Error(`FileNotFoundError:${path}`);
    }
    if (source.kind === "dir") {
      copyModelDirectory(model, path, dest);
      for (const candidate of descendantsOf(model, path).reverse()) {
        if (candidate !== "/") delete model[candidate];
      }
      return;
    }
    copyModelFile(model, path, dest);
    delete model[path];
    return;
  }
  throw new Error(`unsupported model operation: ${op}`);
}

export function modelStat(model: ModelState, path: string): AnyObject | null {
  const normalized = normalizePath(path);
  if (!(normalized in model)) return null;
  return { path: normalized, ...model[normalized] };
}

export function modelLs(model: ModelState, path: string): AnyObject[] {
  const normalized = normalizePath(path);
  if (!(normalized in model)) throw new Error(`FileNotFoundError:${normalized}`);
  if (model[normalized].kind !== "dir") throw new Error(`NotADirectoryError:${normalized}`);
  return childrenOf(model, normalized).map((child) => ({ path: child, kind: model[child].kind }));
}

export function modelFind(model: ModelState, root: string): AnyObject[] {
  const normalized = normalizePath(root);
  if (!(normalized in model)) throw new Error(`FileNotFoundError:${normalized}`);
  if (model[normalized].kind === "file") {
    return [{ path: normalized, kind: "file" }];
  }
  return descendantsOf(model, normalized).map((child) => ({ path: child, kind: model[child].kind }));
}

function modelReadText(model: ModelState, path: string): string {
  const normalized = normalizePath(path);
  const row = model[normalized];
  if (!row) throw new Error(`FileNotFoundError:${normalized}`);
  if (row.kind === "dir") throw new Error(`IsADirectoryError:${normalized}`);
  if (Number(row.is_text ?? 0) !== 1) throw new Error(`ValueError:binary file: ${normalized}`);
  return String(row.text ?? "");
}

function modelReadBytes(model: ModelState, path: string): Uint8Array {
  const normalized = normalizePath(path);
  const row = model[normalized];
  if (!row) throw new Error(`FileNotFoundError:${normalized}`);
  if (row.kind === "dir") throw new Error(`IsADirectoryError:${normalized}`);
  return row.bytes as Uint8Array;
}

export function expectedGrepMatches(
  model: ModelState,
  options: { root: string; pattern: string; ignoreCase?: boolean; glob?: string | null },
): AnyObject[] {
  const needle = options.ignoreCase ? options.pattern.toLowerCase() : options.pattern;
  return modelFind(model, options.root).flatMap((row) => {
    const path = String(row.path);
    const state = model[path];
    if (state.kind !== "file" || Number(state.is_text ?? 0) !== 1) return [];
    if (options.glob && !basename(path).match(new RegExp(`^${options.glob.replace(/\*/g, ".*")}$`))) return [];
    return String(state.text ?? "")
      .split(/\r?\n/)
      .flatMap((line, index) => {
        const haystack = options.ignoreCase ? line.toLowerCase() : line;
        return haystack.includes(needle) ? [{ path, line_number: index + 1, line }] : [];
      });
  });
}

function existingDirs(model: ModelState): string[] {
  return Object.entries(model)
    .filter(([, row]) => row.kind === "dir")
    .map(([path]) => path)
    .sort();
}

function existingFiles(model: ModelState): string[] {
  return Object.entries(model)
    .filter(([, row]) => row.kind === "file")
    .map(([path]) => path)
    .sort();
}

function copyDestination(path: string, dest: string): string {
  return normalizePath(dest);
}

function randomName(rng: () => number, suffix = ""): string {
  const alphabet = "abcdefghijklmnopqrstuvwxyz";
  const length = 4 + Math.floor(rng() * 5);
  let stem = "";
  for (let index = 0; index < length; index += 1) {
    stem += alphabet[Math.floor(rng() * alphabet.length)];
  }
  return `${stem}${suffix}`;
}

function newPathUnder(model: ModelState, rng: () => number, suffix = ""): string {
  const parent = existingDirs(model)[Math.floor(rng() * existingDirs(model).length)];
  return normalizePath(`${parent.replace(/\/$/, "")}/${randomName(rng, suffix)}`);
}

function pickOperation(rng: () => number): string {
  const options: Array<[string, number]> = [
    ["mkdir", 10],
    ["put_text", 18],
    ["put_bytes", 12],
    ["stat", 8],
    ["ls", 8],
    ["find", 8],
    ["read_text", 8],
    ["read_bytes", 6],
    ["grep", 8],
    ["cp", 5],
    ["mv", 5],
    ["rm", 8],
    ["ingest", 2],
  ];
  const total = options.reduce((sum, [, weight]) => sum + weight, 0);
  let threshold = rng() * total;
  for (const [name, weight] of options) {
    threshold -= weight;
    if (threshold <= 0) return name;
  }
  return options[0]![0];
}

async function makeTempTree(seedText: string, binary: Uint8Array): Promise<string> {
  const root = await mkdtemp(joinPath(tmpdir(), "tpfs-dogfood-"));
  await writeFile(joinPath(root, "notes.txt"), seedText, "utf8");
  await mkdirFs(joinPath(root, "nested"));
  await writeFile(joinPath(root, "nested", "data.bin"), Buffer.from(binary));
  return root;
}

async function verifySampledState(client: Parameters<typeof stat>[0], mount: string, model: ModelState): Promise<void> {
  for (const path of Object.keys(model).slice(0, 5)) {
    const expected = modelStat(model, path);
    const actual = (await stat(client, mount, path)) as AnyObject | null;
    if (expected === null) {
      if (actual !== null) throw new Error(`unexpected path: ${path}`);
      continue;
    }
    if (!actual || actual.path !== path || actual.kind !== expected.kind) {
      throw new Error(`stat mismatch for ${path}`);
    }
  }
}

async function liveCopy(
  client: Parameters<typeof stat>[0],
  mount: string,
  src: string,
  dest: string,
  recursive: boolean,
): Promise<void> {
  const source = (await stat(client, mount, src)) as AnyObject | null;
  if (!source) {
    throw new Error(`FileNotFoundError:${src}`);
  }
  if (source.kind === "dir") {
    if (!recursive) {
      throw new Error(`RecursiveRequiredError:${src}`);
    }
    if (dest === src || dest.startsWith(`${src.replace(/\/$/, "")}/`)) {
      throw new Error(`InvalidCopyTarget:${src}->${dest}`);
    }
    const rows = (await find(client, mount, src)) as AnyObject[];
    for (const row of rows) {
      const rowPath = String(row.path);
      const suffix = rowPath === src ? "" : rowPath.slice(src.length);
      const mapped = suffix === "" ? dest : normalizePath(`${dest.replace(/\/$/, "")}${suffix}`);
      if (row.kind === "dir") {
        await mkdir(client, mount, mapped);
        continue;
      }
      const file = (await stat(client, mount, rowPath)) as AnyObject;
      if (Number(file.is_text ?? 0) === 1) {
        await putText(client, mount, mapped, String(await readText(client, mount, rowPath)), {
          mime: typeof file.mime === "string" ? file.mime : undefined,
        });
      } else {
        await putBytes(client, mount, mapped, (await readBytes(client, mount, rowPath)) as Uint8Array, {
          mime: typeof file.mime === "string" ? file.mime : undefined,
        });
      }
    }
    return;
  }
  if (Number(source.is_text ?? 0) === 1) {
    await putText(client, mount, dest, String(await readText(client, mount, src)), {
      mime: typeof source.mime === "string" ? source.mime : undefined,
    });
    return;
  }
  await putBytes(client, mount, dest, (await readBytes(client, mount, src)) as Uint8Array, {
    mime: typeof source.mime === "string" ? source.mime : undefined,
  });
}

async function liveMove(
  client: Parameters<typeof stat>[0],
  mount: string,
  src: string,
  dest: string,
): Promise<void> {
  const source = (await stat(client, mount, src)) as AnyObject | null;
  if (!source) {
    throw new Error(`FileNotFoundError:${src}`);
  }
  await liveCopy(client, mount, src, dest, source.kind === "dir");
  await rm(client, mount, src, source.kind === "dir");
}

export async function runDogfood(options: {
  apiKey?: string;
  region?: string;
  baseURL?: string;
  mountPrefix?: string;
  seed?: number;
  steps?: number;
  checkEvery?: number;
  keepOnFail?: boolean;
  keepAlways?: boolean;
  cleanup?: boolean;
} = {}): Promise<AnyObject> {
  const client = makeClient({
    apiKey: options.apiKey,
    region: options.region,
    baseURL: options.baseURL,
  });
  let seed = options.seed ?? 1;
  const rng = () => {
    seed = (seed * 1664525 + 1013904223) % 4294967296;
    return seed / 4294967296;
  };
  const mount = `${options.mountPrefix ?? "dogfood"}${(options.seed ?? 1).toString(16)}${randomUUID().slice(0, 6)}`;
  const namespace = mountNamespace(mount);
  const model = newModelState();
  const log: AnyObject[] = [];
  const counts: Record<string, number> = {};
  const steps = options.steps ?? 50;
  const checkEvery = options.checkEvery ?? 5;
  let checksRun = 0;
  let failed = false;

  try {
    for (let index = 0; index < steps; index += 1) {
      const opName = pickOperation(rng);
      counts[opName] = (counts[opName] ?? 0) + 1;
      if (opName === "mkdir") {
        const path = newPathUnder(model, rng);
        applyModelOperation(model, { op: "mkdir", path });
        await mkdir(client, mount, path);
        log.push({ index, op: opName, path });
      } else if (opName === "put_text") {
        const path = newPathUnder(model, rng, ".txt");
        const text = TEXT_PAYLOADS[Math.floor(rng() * TEXT_PAYLOADS.length)]!;
        applyModelOperation(model, { op: "put_text", path, text });
        await putText(client, mount, path, text);
        log.push({ index, op: opName, path });
      } else if (opName === "put_bytes") {
        const path = newPathUnder(model, rng, ".bin");
        const data = BINARY_PAYLOADS[Math.floor(rng() * BINARY_PAYLOADS.length)]!;
        applyModelOperation(model, { op: "put_bytes", path, data });
        await putBytes(client, mount, path, data);
        log.push({ index, op: opName, path });
      } else if (opName === "stat") {
        const path = Object.keys(model)[Math.floor(rng() * Object.keys(model).length)]!;
        await stat(client, mount, path);
        log.push({ index, op: opName, path });
      } else if (opName === "ls") {
        const path = existingDirs(model)[Math.floor(rng() * existingDirs(model).length)]!;
        await ls(client, mount, path);
        log.push({ index, op: opName, path });
      } else if (opName === "find") {
        const path = Object.keys(model)[Math.floor(rng() * Object.keys(model).length)]!;
        await find(client, mount, path);
        log.push({ index, op: opName, path });
      } else if (opName === "read_text") {
        const files = existingFiles(model).filter((path) => Number(model[path]?.is_text ?? 0) === 1);
        if (files.length > 0) {
          const path = files[Math.floor(rng() * files.length)]!;
          const actual = await readText(client, mount, path);
          if (actual !== modelReadText(model, path)) throw new Error(`read_text mismatch for ${path}`);
          log.push({ index, op: opName, path });
        }
      } else if (opName === "read_bytes") {
        const files = existingFiles(model);
        if (files.length > 0) {
          const path = files[Math.floor(rng() * files.length)]!;
          const actual = (await readBytes(client, mount, path)) as Uint8Array;
          if (Buffer.from(actual).toString("hex") !== Buffer.from(modelReadBytes(model, path)).toString("hex")) {
            throw new Error(`read_bytes mismatch for ${path}`);
          }
          log.push({ index, op: opName, path });
        }
      } else if (opName === "grep") {
        const dirs = existingDirs(model);
        const root = dirs[Math.floor(rng() * dirs.length)]!;
        const actual = await grep(client, mount, root, "oauth", { ignoreCase: true });
        const expected = expectedGrepMatches(model, { root, pattern: "oauth", ignoreCase: true });
        if (JSON.stringify(actual) !== JSON.stringify(expected)) throw new Error(`grep mismatch for ${root}`);
        log.push({ index, op: opName, root });
      } else if (opName === "cp") {
        const candidates = Object.keys(model).filter((path) => path !== "/");
        if (candidates.length > 0) {
          const path = candidates[Math.floor(rng() * candidates.length)]!;
          const dest = copyDestination(path, newPathUnder(model, rng, model[path]?.kind === "file" ? ".copy" : ""));
          const recursive = model[path]?.kind === "dir";
          applyModelOperation(model, { op: "cp", path, dest, recursive });
          await liveCopy(client, mount, path, dest, Boolean(recursive));
          log.push({ index, op: opName, path, dest, recursive });
        }
      } else if (opName === "mv") {
        const candidates = Object.keys(model).filter((path) => path !== "/");
        if (candidates.length > 0) {
          const path = candidates[Math.floor(rng() * candidates.length)]!;
          const dest = copyDestination(path, newPathUnder(model, rng, model[path]?.kind === "file" ? ".moved" : ""));
          applyModelOperation(model, { op: "mv", path, dest });
          await liveMove(client, mount, path, dest);
          log.push({ index, op: opName, path, dest });
        }
      } else if (opName === "rm") {
        const candidates = Object.keys(model).filter((path) => path !== "/");
        if (candidates.length > 0) {
          const path = candidates[Math.floor(rng() * candidates.length)]!;
          const recursive = model[path]?.kind === "dir";
          applyModelOperation(model, { op: "rm", path, recursive });
          await rm(client, mount, path, recursive);
          log.push({ index, op: opName, path, recursive });
        }
      } else if (opName === "ingest") {
        const tempRoot = await makeTempTree(
          TEXT_PAYLOADS[Math.floor(rng() * TEXT_PAYLOADS.length)]!,
          BINARY_PAYLOADS[Math.floor(rng() * BINARY_PAYLOADS.length)]!,
        );
        const mountRoot = newPathUnder(model, rng);
        applyModelOperation(model, { op: "mkdir", path: mountRoot });
        await ingestDirectory(client, mount, tempRoot, { mountRoot });
        log.push({ index, op: opName, mountRoot });
      }

      if ((index + 1) % checkEvery === 0) {
        await verifySampledState(client, mount, model);
        checksRun += 1;
      }
    }

    await verifySampledState(client, mount, model);
    checksRun += 1;
    return {
      seed: options.seed ?? 1,
      mount,
      namespace,
      steps,
      stepsCompleted: steps,
      checksRun,
      counts,
      log,
    };
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    if (!(options.cleanup === false || options.keepAlways || (failed && options.keepOnFail))) {
      try {
        const handle = client.namespace(namespace);
        await handle.deleteAll();
      } catch {
        // ignore cleanup failures
      }
    }
  }
}
