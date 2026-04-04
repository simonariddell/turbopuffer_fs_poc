import { createHash } from "node:crypto";
import path from "node:path/posix";

function requireText(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new TypeError(`${label} must be a string`);
  }
  if (value.length === 0) {
    throw new Error(`${label} must not be empty`);
  }
  if (value.includes("\u0000")) {
    throw new Error(`${label} must not contain NUL bytes`);
  }
  return value;
}

function normalizeCandidate(raw: string, label: string, allowGlob: boolean): string {
  if (!raw.startsWith("/")) {
    throw new Error(`${label} must be absolute: ${JSON.stringify(raw)}`);
  }
  if (raw === "/") {
    return "/";
  }

  const rawSegments = raw.split("/").filter(Boolean);
  if (rawSegments.some((segment) => segment === "." || segment === "..")) {
    throw new Error(`${label} must not contain '.' or '..' segments: ${JSON.stringify(raw)}`);
  }

  const normalized = path.normalize(`/${raw.replace(/^\/+/, "")}`);
  const normalizedSegments = normalized.split("/").filter(Boolean);
  if (normalizedSegments.some((segment) => segment === "..")) {
    throw new Error(`${label} must stay within root: ${JSON.stringify(raw)}`);
  }

  if (allowGlob) {
    return `/${rawSegments.join("/")}`;
  }
  if (normalized.length > 1 && normalized.endsWith("/")) {
    return normalized.replace(/\/+$/, "");
  }
  return normalized;
}

export function normalizePath(value: string): string {
  return normalizeCandidate(requireText(value, "path"), "path", false);
}

export function normalizeGlobPath(value: string): string {
  return normalizeCandidate(requireText(value, "glob pattern"), "glob pattern", true);
}

export function joinPath(root: string, tail: string): string {
  const rootValue = normalizePath(root);
  if (tail === "") {
    return rootValue;
  }
  const tailValue = requireText(tail, "tail");
  if (tailValue.startsWith("/")) {
    return normalizePath(tailValue);
  }
  return normalizePath(path.join(rootValue, tailValue));
}

export function joinGlob(root: string, pattern: string): string {
  const rootValue = normalizePath(root);
  const patternValue = requireText(pattern, "glob pattern");
  if (patternValue.startsWith("/")) {
    return normalizeGlobPath(patternValue);
  }
  return normalizeGlobPath(path.join(rootValue, patternValue));
}

export function parentPath(value: string): string | null {
  const normalized = normalizePath(value);
  if (normalized === "/") {
    return null;
  }
  return path.dirname(normalized);
}

export function basename(value: string): string {
  const normalized = normalizePath(value);
  return normalized === "/" ? "/" : path.basename(normalized);
}

export function extension(value: string): string {
  const name = basename(value);
  if (name === "/" || name === "" || name === "." || name === "..") {
    return "";
  }
  return path.extname(name);
}

export function ancestorPaths(value: string, options?: { includeSelf?: boolean } | boolean): string[] {
  const includeSelf =
    typeof options === "boolean" ? options : Boolean(options?.includeSelf);
  const normalized = normalizePath(value);
  if (normalized === "/") {
    return includeSelf ? ["/"] : [];
  }

  const parts = normalized.slice(1).split("/");
  const limit = includeSelf ? parts.length : parts.length - 1;
  if (limit < 1) {
    return ["/"];
  }

  const rows = ["/"];
  for (let index = 1; index <= limit; index += 1) {
    rows.push(`/${parts.slice(0, index).join("/")}`);
  }
  return rows;
}

export function pathId(value: string): string {
  const normalized = normalizePath(value);
  return createHash("sha256").update(normalized, "utf8").digest("hex");
}

export function andFilter<T>(...parts: Array<T | null | undefined>): T | ["And", T[]] | null {
  const kept = parts.filter((part): part is T => part !== null && part !== undefined);
  if (kept.length === 0) {
    return null;
  }
  if (kept.length === 1) {
    return kept[0]!;
  }
  return ["And", kept];
}

export function orFilter<T>(...parts: Array<T | null | undefined>): T | ["Or", T[]] | null {
  const kept = parts.filter((part): part is T => part !== null && part !== undefined);
  if (kept.length === 0) {
    return null;
  }
  if (kept.length === 1) {
    return kept[0]!;
  }
  return ["Or", kept];
}

export function pathsFilter(paths: Iterable<string>) {
  const values = [...new Set(Array.from(paths, (item) => normalizePath(item)))];
  return orFilter(...values.map((item) => ["path", "Eq", item] as const));
}

export function directChildrenFilter(value: string) {
  return ["parent", "Eq", normalizePath(value)] as const;
}

export function subtreeFilter(value: string) {
  const normalized = normalizePath(value);
  if (normalized === "/") {
    return null;
  }
  return orFilter(
    ["path", "Eq", normalized] as const,
    ["path", "Glob", `${normalized.replace(/\/$/, "")}/**`] as const,
  );
}

export function scopedGlobFilter(root: string, pattern: string | null | undefined, options?: { ignoreCase?: boolean }) {
  if (!pattern) {
    return null;
  }
  const op = options?.ignoreCase ? "IGlob" : "Glob";
  if (pattern.includes("/")) {
    return ["path", op, joinGlob(root, pattern)] as const;
  }
  return ["basename", op, pattern] as const;
}

export function textSubstringFilter(pattern: string, options?: { ignoreCase?: boolean }) {
  if (pattern === "") {
    return null;
  }
  const op = options?.ignoreCase ? "IGlob" : "Glob";
  return ["text", op, `*${pattern.replaceAll("*", "\\*").replaceAll("?", "\\?")}*`] as const;
}

export function withAfterFilter(filter: unknown, field: string, lastValue: string | null) {
  if (lastValue === null) {
    return filter;
  }
  return andFilter(filter as object, [field, "Gt", lastValue] as const);
}
