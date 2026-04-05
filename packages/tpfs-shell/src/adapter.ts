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
  ls,
  mkdir,
  putBytes,
  putText,
  readBytes,
  readText,
  resolveUserPath,
  rm,
  stat,
} from "../../turbopuffer-fs/src/index.js";

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

function childName(path: string): string {
  if (path === "/") return "/";
  return path.split("/").filter(Boolean).at(-1) ?? path;
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
}

export class TpufFsAdapter implements IFileSystem {
  private readonly client: Turbopuffer;
  private readonly mount: string;
  private readonly cwdProvider: () => Promise<string>;

  constructor(options: TpufFsAdapterOptions) {
    this.client = options.client;
    this.mount = options.mount;
    this.cwdProvider = options.cwdProvider;
  }

  private async resolveVirtualPath(target: string): Promise<string> {
    return resolveUserPath(target, { cwd: await this.cwdProvider() });
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
      return;
    }
    if (encoding === "base64" || encoding === "binary" || encoding === "hex") {
      await putBytes(this.client, this.mount, resolved, encodeContent(content, encoding));
      return;
    }
    await putText(this.client, this.mount, resolved, content);
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
      throw new Error(`EISDIR: illegal operation on a directory, append '${path}'`);
    }
    const current = await this.readFileBuffer(path);
    const extra = encodeContent(content, typeof options === "string" ? options : options?.encoding ?? "utf8");
    const combined = new Uint8Array(current.length + extra.length);
    combined.set(current, 0);
    combined.set(extra, current.length);
    if (Number(existing.is_text ?? 0) === 1 && !(content instanceof Uint8Array)) {
      await putText(this.client, this.mount, resolved, decodeContent(combined));
      return;
    }
    await putBytes(this.client, this.mount, resolved, combined);
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
  }

  async cp(_src: string, _dest: string, _options?: CpOptions): Promise<void> {
    throw new Error("ENOTSUP: cp is not yet supported");
  }

  async mv(_src: string, _dest: string): Promise<void> {
    throw new Error("ENOTSUP: mv is not yet supported");
  }

  resolvePath(base: string, target: string): string {
    return resolveUserPath(target, { cwd: base });
  }

  getAllPaths(): string[] {
    return [];
  }

  async chmod(_path: string, _mode: number): Promise<void> {
    throw new Error("ENOTSUP: chmod is not supported");
  }

  async symlink(_target: string, _linkPath: string): Promise<void> {
    throw new Error("ENOTSUP: symlink is not supported");
  }

  async link(_existingPath: string, _newPath: string): Promise<void> {
    throw new Error("ENOTSUP: hard links are not supported");
  }

  async readlink(_path: string): Promise<string> {
    throw new Error("ENOTSUP: readlink is not supported");
  }

  async lstat(path: string): Promise<FsStat> {
    return this.stat(path);
  }

  async realpath(path: string): Promise<string> {
    return this.resolveVirtualPath(path);
  }

  async utimes(_path: string, _atime: Date, _mtime: Date): Promise<void> {
    throw new Error("ENOTSUP: utimes is not supported");
  }
}
