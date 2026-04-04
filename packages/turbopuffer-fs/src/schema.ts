import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { basename, extension, normalizePath, parentPath, ancestorPaths, pathId } from "./paths.js";
import type { FsRow, FsSchema } from "./types.js";

const TEXT_EXTENSIONS = new Set([
  ".c",
  ".cfg",
  ".conf",
  ".cpp",
  ".css",
  ".csv",
  ".html",
  ".ini",
  ".java",
  ".js",
  ".json",
  ".jsx",
  ".md",
  ".py",
  ".r",
  ".rb",
  ".rs",
  ".rst",
  ".sh",
  ".sql",
  ".svg",
  ".toml",
  ".ts",
  ".tsx",
  ".txt",
  ".tsv",
  ".xml",
  ".yaml",
  ".yml",
]);

export const META_FIELDS = [
  "id",
  "path",
  "parent",
  "basename",
  "kind",
  "ext",
  "mime",
  "size_bytes",
  "is_text",
  "sha256",
  "source_mtime_ns",
  "source_size_bytes",
] as const;

export const CONTENT_FIELDS = [...META_FIELDS, "text", "blob_b64"] as const;

export function fsSchema(): FsSchema {
  return {
    path: { type: "string", regex: true, filterable: true },
    parent: "string",
    basename: { type: "string", regex: true, filterable: true },
    kind: "string",
    ext: "string",
    mime: "string",
    size_bytes: "uint",
    is_text: "uint",
    text: { type: "string", regex: true, filterable: true },
    blob_b64: { type: "string", filterable: false },
    sha256: "string",
    source_mtime_ns: "uint",
    source_size_bytes: "uint",
  };
}

export function metadataRow(row: FsRow): FsRow {
  return Object.fromEntries(
    META_FIELDS.filter((field) => field in row).map((field) => [field, row[field]]),
  ) as FsRow;
}

export function contentRow(row: FsRow): FsRow {
  return Object.fromEntries(
    CONTENT_FIELDS.filter((field) => field in row).map((field) => [field, row[field]]),
  ) as FsRow;
}

export function sha256Hex(data: Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

function inferMime(path: string, fallback: string): string {
  const ext = extension(path).toLowerCase();
  const table: Record<string, string> = {
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".csv": "text/csv",
    ".tsv": "text/tab-separated-values",
    ".json": "application/json",
    ".py": "text/x-python",
    ".js": "text/javascript",
    ".ts": "text/typescript",
    ".tsx": "text/typescript",
    ".html": "text/html",
    ".css": "text/css",
    ".xml": "application/xml",
    ".yaml": "application/yaml",
    ".yml": "application/yaml",
    ".toml": "application/toml",
    ".sql": "application/sql",
    ".svg": "image/svg+xml",
  };
  return table[ext] ?? fallback;
}

function withSourceMetadata(
  row: FsRow,
  sourceMtimeNs?: number,
  sourceSizeBytes?: number,
): FsRow {
  if (sourceMtimeNs !== undefined) {
    row.source_mtime_ns = sourceMtimeNs;
  }
  if (sourceSizeBytes !== undefined) {
    row.source_size_bytes = sourceSizeBytes;
  }
  return row;
}

function baseRow(path: string, params: {
  kind: "file" | "dir";
  mime: string;
  sizeBytes: number;
  isText: 0 | 1;
  sha256?: string;
}): FsRow {
  const normalized = normalizePath(path);
  const row: FsRow = {
    id: pathId(normalized),
    path: normalized,
    basename: basename(normalized),
    kind: params.kind,
    ext: params.kind === "dir" ? "" : extension(normalized),
    mime: params.mime,
    size_bytes: params.sizeBytes,
    is_text: params.isText,
  };
  const parent = parentPath(normalized);
  if (parent !== undefined) {
    row.parent = parent;
  }
  if (params.sha256 !== undefined) {
    row.sha256 = params.sha256;
  }
  return row;
}

export function directoryRow(path: string, sourceMtimeNs?: number): FsRow {
  return withSourceMetadata(
    baseRow(path, {
      kind: "dir",
      mime: "inode/directory",
      sizeBytes: 0,
      isText: 0,
    }),
    sourceMtimeNs,
    0,
  );
}

export function textRow(
  path: string,
  text: string,
  mime?: string,
  sourceMtimeNs?: number,
  sourceSizeBytes?: number,
): FsRow {
  const data = new TextEncoder().encode(text);
  const row = baseRow(path, {
    kind: "file",
    mime: mime ?? inferMime(path, "text/plain"),
    sizeBytes: data.length,
    isText: 1,
    sha256: sha256Hex(data),
  });
  row.text = text;
  return withSourceMetadata(row, sourceMtimeNs, sourceSizeBytes ?? data.length);
}

export function bytesRow(
  path: string,
  data: Uint8Array,
  mime?: string,
  sourceMtimeNs?: number,
  sourceSizeBytes?: number,
): FsRow {
  const row = baseRow(path, {
    kind: "file",
    mime: mime ?? inferMime(path, "application/octet-stream"),
    sizeBytes: data.length,
    isText: 0,
    sha256: sha256Hex(data),
  });
  row.blob_b64 = Buffer.from(data).toString("base64");
  return withSourceMetadata(row, sourceMtimeNs, sourceSizeBytes ?? data.length);
}

export function parentDirectoryRows(path: string): FsRow[] {
  return ancestorPaths(path, false).map((ancestor) => directoryRow(ancestor));
}

export function targetDirectoryRows(path: string): FsRow[] {
  return ancestorPaths(path, true).map((ancestor) => directoryRow(ancestor));
}

export function isProbablyText(path: string, data: Uint8Array): boolean {
  const ext = extension(path).toLowerCase();
  if (data.includes(0)) {
    return false;
  }
  if (TEXT_EXTENSIONS.has(ext)) {
    return true;
  }
  const mime = inferMime(path, "application/octet-stream");
  if (
    mime.startsWith("text/") ||
    ["application/json", "application/sql", "application/xml", "application/x-sh", "image/svg+xml"].includes(
      mime,
    )
  ) {
    return true;
  }
  try {
    new TextDecoder("utf-8", { fatal: true }).decode(data);
    return true;
  } catch {
    return false;
  }
}

export function rowFromBytes(
  path: string,
  data: Uint8Array,
  mime?: string,
  sourceMtimeNs?: number,
  sourceSizeBytes?: number,
): FsRow {
  if (isProbablyText(path, data)) {
    try {
      const text = new TextDecoder("utf-8", { fatal: true }).decode(data);
      return textRow(path, text, mime, sourceMtimeNs, sourceSizeBytes);
    } catch {
      return bytesRow(path, data, mime, sourceMtimeNs, sourceSizeBytes);
    }
  }
  return bytesRow(path, data, mime, sourceMtimeNs, sourceSizeBytes);
}

export function upsertRowsPayload(rows: FsRow[]): { upsert_rows: FsRow[]; schema: FsSchema } {
  return {
    upsert_rows: rows,
    schema: fsSchema(),
  };
}
