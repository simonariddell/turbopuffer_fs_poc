import { Buffer } from "node:buffer";

import type {
  BufferEncoding,
  CpOptions,
  FsStat,
  IFileSystem,
  MkdirOptions,
  RmOptions,
} from "just-bash";
import type { Turbopuffer } from "@turbopuffer/turbopuffer";
import {
  ancestorPaths,
  basename as tpfsBasename,
  find,
  ls,
  mkdir,
  putBytes,
  putText,
  readBytes,
  readText,
  resolveUserPath,
  rm,
  stat,
} from "@workspace/turbopuffer-fs";

import { invalidTpfsOperation, notYetImplemented, unsupportedByDesign } from "./errors.js";

type ReadFileOptions = { encoding?: BufferEncoding | null };
type WriteFileOptions = { encoding?: BufferEncoding };
type DirentEntry = {
  name: string;
  isFile: boolean;
  isDirectory: boolean;
  isSymbolicLink: boolean;
};

function encodeContent(content: string | Uint8Array, encoding: BufferEncoding = "utf8"): Uint8Array {
  if (content instanceof Uint8Array) {
    return content;
  }
  return new Uint8Array(Buffer.from(content, encoding === "utf-8" ? "utf8" : encoding));
}

function decodeContent(content: Uint8Array, encoding: BufferEncoding = "utf8"): string {
  return Buffer.from(content).toString(encoding === "utf-8" ? "utf8" : encoding);
}

function isBinaryEncoding(encoding: BufferEncoding): boolean {
  return ["base64", "hex"].includes(encoding);
}

function looksLikeTextString(content: string, encoding: BufferEncoding): boolean {
  if (isBinaryEncoding(encoding)) {
    return false;
  }
  for (let index = 0; index < content.length; index += 1) {
    const code = content.charCodeAt(index);
    if (code === 0) {
      return false;
    }
    if (code === 9 || code === 10 || code === 13) {
      continue;
    }
    if (code < 32) {
      return false;
    }
  }
  return true;
}

function childName(path: string): string {
  if (path === "/") return "/";
  return path.split("/").filter(Boolean).at(-1) ?? path;
}

function isSameOrDescendantPath(root: string, candidate: string): boolean {
  const prefix = `${root.replace(/\/$/, "")}/`;
  return candidate === root || candidate.startsWith(prefix);
}

function toFsStat(row: Record<string, unknown>): FsStat {
  const isDirectory = row.kind === "dir";
  return {
    isFile: !isDirectory,
    isDirectory,
    isSymbolicLink: false,
    mode: isDirectory ? 0o755 : 0o644,
    size: Number(row.size_bytes ?? 0),
    mtime: new Date(
      Number(row.source_mtime_ns ?? 0) > 0
        ? Math.floor(Number(row.source_mtime_ns) / 1_000_000)
        : Date.now(),
    ),
  };
}

export interface TpufFsAdapterOptions {
  client: Turbopuffer;
  mount: string;
  cwdProvider: () => Promise<string>;
  initialPaths?: string[];
}

export class TpufFsAdapter implements IFileSystem {
  private readonly client: Turbopuffer;
  private readonly mount: string;
  private readonly cwdProvider: () => Promise<string>;
  private readonly pathInventory: Set<string>;

  constructor(options: TpufFsAdapterOptions) {
    this.client = options.client;
    this.mount = options.mount;
    this.cwdProvider = options.cwdProvider;
    this.pathInventory = new Set(["/", ...(options.initialPaths ?? [])]);
  }

  private async resolveVirtualPath(target: string): Promise<string> {
    return resolveUserPath(target, { cwd: await this.cwdProvider() });
  }

  private rememberPath(path: string): void {
    for (const ancestor of ancestorPaths(path, true)) {
      this.pathInventory.add(ancestor);
    }
  }

  private forgetPath(path: string, recursive = false): void {
    if (recursive) {
      const prefix = path === "/" ? "/" : `${path.replace(/\/$/, "")}/`;
      for (const value of [...this.pathInventory]) {
        if (value === path || value.startsWith(prefix)) {
          this.pathInventory.delete(value);
        }
      }
      this.pathInventory.add("/");
      return;
    }
    this.pathInventory.delete(path);
    this.pathInventory.add("/");
  }

  private async resolveCopyDestination(src: string, dest: string): Promise<string> {
    const existing = (await stat(this.client, this.mount, dest)) as Record<string, unknown> | null;
    if (existing?.kind === "dir") {
      const name = tpfsBasename(src);
      return dest === "/" ? `/${name}` : `${dest.replace(/\/$/, "")}/${name}`;
    }
    return dest;
  }

  private async existingRow(path: string): Promise<Record<string, unknown> | null> {
    return (await stat(this.client, this.mount, path)) as Record<string, unknown> | null;
  }

  private async requireExistingRow(path: string, operation: "cp" | "mv"): Promise<Record<string, unknown>> {
    const row = await this.existingRow(path);
    if (!row) {
      throw new Error(`ENOENT: no such file or directory, ${operation} '${path}'`);
    }
    return row;
  }

  private async ensureNoClobberDest(
    dest: string,
    enabled: boolean,
    operation: "cp" | "mv",
  ): Promise<void> {
    if (!enabled) {
      return;
    }
    const row = await this.existingRow(dest);
    if (row !== null) {
      throw invalidTpfsOperation(
        operation,
        `${operation} with no-clobber cannot overwrite existing destination ${dest}.`,
        {
          alternatives: [
            `retry ${operation} with a new destination path`,
            `drop the no-clobber option if replacement is intended`,
          ],
          specSections: ["§8", "§12"],
        },
      );
    }
  }

  private async copyFileAbsolute(src: string, dest: string, source?: Record<string, unknown>): Promise<void> {
    const sourceRow = source ?? ((await stat(this.client, this.mount, src)) as Record<string, unknown> | null);
    if (!sourceRow || sourceRow.kind !== "file") {
      throw invalidTpfsOperation("cp", `cp expected a file source at ${src}.`, {
        specSections: ["§8", "§12"],
      });
    }
    const mime = typeof sourceRow.mime === "string" ? sourceRow.mime : undefined;
    if (Number(sourceRow.is_text ?? 0) === 1) {
      const text = await readText(this.client, this.mount, src);
      await putText(this.client, this.mount, dest, String(text), { mime });
      this.rememberPath(dest);
      return;
    }
    const bytes = (await readBytes(this.client, this.mount, src)) as Uint8Array;
    await putBytes(this.client, this.mount, dest, bytes, { mime });
    this.rememberPath(dest);
  }

  private async copyDirectoryAbsolute(src: string, dest: string): Promise<void> {
    if (isSameOrDescendantPath(src, dest)) {
      throw invalidTpfsOperation(
        "cp",
        `cp cannot copy a directory into itself or its own descendant (${src} -> ${dest}).`,
        {
          alternatives: ["copy the directory to a path outside the source subtree"],
          specSections: ["§8", "§12", "§13"],
        },
      );
    }
    const rows = (await find(this.client, this.mount, src)) as Array<Record<string, unknown>>;
    for (const row of rows) {
      const rowPath = String(row.path);
      const suffix = rowPath === src ? "" : rowPath.slice(src.length);
      const mapped = suffix === "" ? dest : `${dest.replace(/\/$/, "")}${suffix}`;
      if (row.kind === "dir") {
        await mkdir(this.client, this.mount, mapped);
        this.rememberPath(mapped);
        continue;
      }
      await this.copyFileAbsolute(rowPath, mapped);
    }
  }

  async readFile(path: string, options?: ReadFileOptions | BufferEncoding): Promise<string> {
    const resolved = await this.resolveVirtualPath(path);
    const encoding = typeof options === "string" ? options : options?.encoding ?? "utf8";
    const text = await readText(this.client, this.mount, resolved);
    return typeof text === "string"
      ? text
      : decodeContent(new Uint8Array(text as Uint8Array), encoding);
  }

  async readFileBuffer(path: string): Promise<Uint8Array> {
    const resolved = await this.resolveVirtualPath(path);
    return (await readBytes(this.client, this.mount, resolved)) as Uint8Array;
  }

  async writeFile(
    path: string,
    content: string | Uint8Array,
    options?: WriteFileOptions | BufferEncoding,
  ): Promise<void> {
    const resolved = await this.resolveVirtualPath(path);
    const encoding = typeof options === "string" ? options : options?.encoding ?? "utf8";
    if (content instanceof Uint8Array) {
      await putBytes(this.client, this.mount, resolved, content);
      this.rememberPath(resolved);
      return;
    }
    if (!looksLikeTextString(content, encoding)) {
      await putBytes(this.client, this.mount, resolved, encodeContent(content, encoding));
      this.rememberPath(resolved);
      return;
    }
    await putText(this.client, this.mount, resolved, content);
    this.rememberPath(resolved);
  }

  async appendFile(
    path: string,
    content: string | Uint8Array,
    options?: WriteFileOptions | BufferEncoding,
  ): Promise<void> {
    const resolved = await this.resolveVirtualPath(path);
    const existing = (await stat(this.client, this.mount, resolved)) as Record<string, unknown> | null;
    if (existing === null) {
      await this.writeFile(path, content, options);
      return;
    }
    if (existing.kind === "dir") {
      throw invalidTpfsOperation(
        "appendFile",
        `appendFile cannot target a directory path (${path}).`,
        {
          alternatives: [
            "append to a file path instead",
            "use mkdir for durable directory creation",
          ],
          specSections: ["§8.11", "§13"],
        },
      );
    }
    const current = await this.readFileBuffer(path);
    const encoding = typeof options === "string" ? options : options?.encoding ?? "utf8";
    const binaryEncoding = isBinaryEncoding(encoding) || !(content instanceof Uint8Array) && !looksLikeTextString(content, encoding);
    const shouldPromoteEmptyFileToText =
      Number(existing.is_text ?? 0) !== 1 &&
      Number(existing.size_bytes ?? 0) === 0 &&
      !(content instanceof Uint8Array) &&
      !binaryEncoding;
    const extra = encodeContent(content, encoding);
    const combined = new Uint8Array(current.length + extra.length);
    combined.set(current, 0);
    combined.set(extra, current.length);
    if (
      (Number(existing.is_text ?? 0) === 1 || shouldPromoteEmptyFileToText) &&
      !(content instanceof Uint8Array) &&
      !binaryEncoding
    ) {
      await putText(this.client, this.mount, resolved, decodeContent(combined, encoding));
      this.rememberPath(resolved);
      return;
    }
    await putBytes(this.client, this.mount, resolved, combined);
    this.rememberPath(resolved);
  }

  async exists(path: string): Promise<boolean> {
    const resolved = await this.resolveVirtualPath(path);
    return (await stat(this.client, this.mount, resolved)) !== null;
  }

  async stat(path: string): Promise<FsStat> {
    const resolved = await this.resolveVirtualPath(path);
    const row = (await stat(this.client, this.mount, resolved)) as Record<string, unknown> | null;
    if (!row) {
      throw new Error(`ENOENT: no such file or directory, stat '${path}'`);
    }
    return toFsStat(row);
  }

  async mkdir(path: string, _options?: MkdirOptions): Promise<void> {
    const resolved = await this.resolveVirtualPath(path);
    await mkdir(this.client, this.mount, resolved);
    this.rememberPath(resolved);
  }

  async readdir(path: string): Promise<string[]> {
    const entries = await this.readdirWithFileTypes(path);
    return entries.map((entry) => entry.name);
  }

  async readdirWithFileTypes(path: string): Promise<DirentEntry[]> {
    const resolved = await this.resolveVirtualPath(path);
    const rows = (await ls(this.client, this.mount, resolved)) as Array<Record<string, unknown>>;
    return rows.map((row) => {
      const isDirectory = row.kind === "dir";
      return {
        name: childName(String(row.path)),
        isFile: !isDirectory,
        isDirectory,
        isSymbolicLink: false,
      };
    });
  }

  async rm(path: string, options?: RmOptions): Promise<void> {
    const resolved = await this.resolveVirtualPath(path);
    await rm(this.client, this.mount, resolved, options?.recursive ?? false);
    this.forgetPath(resolved, options?.recursive ?? false);
  }

  async cp(srcInput: string, destInput: string, options?: CpOptions): Promise<void> {
    const src = await this.resolveVirtualPath(srcInput);
    const dest = await this.resolveVirtualPath(destInput);
    const source = await this.requireExistingRow(src, "cp");
    const resolvedDest = await this.resolveCopyDestination(src, dest);
    await this.ensureNoClobberDest(resolvedDest, false, "cp");

    if (source.kind === "dir") {
      if (!options?.recursive) {
        throw invalidTpfsOperation(
          "cp",
          `cp requires recursive:true when copying directories (${srcInput}).`,
          {
            alternatives: ["retry cp with recursive:true for directory copies"],
            specSections: ["§8", "§12"],
          },
        );
      }
      await this.copyDirectoryAbsolute(src, resolvedDest);
      return;
    }
    await this.copyFileAbsolute(src, resolvedDest, source);
  }

  async mv(srcInput: string, destInput: string): Promise<void> {
    const src = await this.resolveVirtualPath(srcInput);
    const dest = await this.resolveVirtualPath(destInput);
    if (src === "/") {
      throw invalidTpfsOperation("mv", "mv cannot target the root path.", {
        specSections: ["§8.12", "§13"],
      });
    }
    const source = await this.requireExistingRow(src, "mv");
    const resolvedDest = await this.resolveCopyDestination(src, dest);
    if (source.kind === "dir" && isSameOrDescendantPath(src, resolvedDest)) {
      throw invalidTpfsOperation(
        "mv",
        `mv cannot move a directory into itself or its own descendant (${srcInput} -> ${destInput}).`,
        {
          alternatives: ["move the directory to a path outside the source subtree"],
          specSections: ["§12", "§13"],
        },
      );
    }
    if (resolvedDest === src) {
      return;
    }
    if (source.kind === "dir") {
      await this.copyDirectoryAbsolute(src, resolvedDest);
      await rm(this.client, this.mount, src, true);
      this.forgetPath(src, true);
      return;
    }
    await this.copyFileAbsolute(src, resolvedDest, source);
    await rm(this.client, this.mount, src, false);
    this.forgetPath(src, false);
  }

  resolvePath(base: string, target: string): string {
    return resolveUserPath(target, { cwd: base });
  }

  getAllPaths(): string[] {
    return [...this.pathInventory].sort();
  }

  async chmod(_path: string, _mode: number): Promise<void> {
    throw unsupportedByDesign(
      "chmod",
      "tpfs does not implement a durable permission model.",
    );
  }

  async symlink(_target: string, _linkPath: string): Promise<void> {
    throw unsupportedByDesign(
      "symlink",
      "tpfs does not represent a durable symlink graph.",
    );
  }

  async link(_existingPath: string, _newPath: string): Promise<void> {
    throw unsupportedByDesign(
      "link",
      "tpfs does not represent hard-link identity or shared inode semantics.",
    );
  }

  async readlink(_path: string): Promise<string> {
    throw unsupportedByDesign(
      "readlink",
      "tpfs does not represent symlink targets because symlinks are unsupported.",
    );
  }

  async lstat(path: string): Promise<FsStat> {
    return this.stat(path);
  }

  async realpath(path: string): Promise<string> {
    return this.resolveVirtualPath(path);
  }

  async utimes(_path: string, _atime: Date, _mtime: Date): Promise<void> {
    throw unsupportedByDesign(
      "utimes",
      "tpfs does not expose a durable mutable timestamp API.",
    );
  }
}
