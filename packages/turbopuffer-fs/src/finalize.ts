import { Buffer } from "node:buffer";

import { finalizeBm25Grep, finalizeLiteralGrep, finalizeRegexGrep } from "./grep.js";
import type { AnyObject, ExecuteResults, RowLike, WriteResult } from "./types.js";
import { contentRow, metadataRow } from "./schema.js";

const rows = (results: ExecuteResults, name: string): RowLike[] => {
  const value = results[name] as AnyObject | undefined;
  return Array.isArray(value?.rows) ? (value.rows as RowLike[]) : [];
};

const row = (results: ExecuteResults, name: string): RowLike | null => rows(results, name)[0] ?? null;

const requireTarget = (results: ExecuteResults, path: string): RowLike => {
  const target = row(results, "target");
  if (!target) {
    throw new Error(`FileNotFoundError:${path}`);
  }
  return target;
};

const requireDirectory = (value: RowLike, path: string): RowLike => {
  if (value.kind !== "dir") {
    throw new Error(`NotADirectoryError:${path}`);
  }
  return value;
};

const requireFile = (value: RowLike, path: string): RowLike => {
  if (value.kind === "dir") {
    throw new Error(`IsADirectoryError:${path}`);
  }
  return value;
};

const requireText = (value: RowLike, path: string): string => {
  requireFile(value, path);
  if (Number(value.is_text ?? 0) !== 1) {
    throw new Error(`ValueError:path is a binary file: ${path}`);
  }
  return String(value.text ?? "");
};

export const contentText = (value: RowLike | null): string | null => {
  if (!value) {
    return null;
  }
  return requireText(value, String(value.path ?? ""));
};

export const contentBytes = (value: RowLike | null): Uint8Array | null => {
  if (!value) {
    return null;
  }
  requireFile(value, String(value.path ?? ""));
  if (Number(value.is_text ?? 0) === 1) {
    return Buffer.from(String(value.text ?? ""), "utf8");
  }
  const blob = value.blob_b64;
  if (blob === undefined || blob === null || blob === "") {
    return new Uint8Array();
  }
  return Buffer.from(String(blob), "base64");
};

const grepMatches = (value: RowLike, pattern: string, ignoreCase: boolean): AnyObject[] => {
  const text = String(value.text ?? "");
  const needle = ignoreCase ? pattern.toLowerCase() : pattern;
  return text.split(/\r?\n/).flatMap((line, index) => {
    const haystack = ignoreCase ? line.toLowerCase() : line;
    return haystack.includes(needle)
      ? [{ path: value.path, line_number: index + 1, line }]
      : [];
  });
};

export const FINALIZERS = {
  stat: (context: AnyObject, results: ExecuteResults) => {
    const value = row(results, "target");
    return value ? metadataRow(value) : null;
  },
  ls: (context: AnyObject, results: ExecuteResults) => {
    requireDirectory(requireTarget(results, String(context.path)), String(context.path));
    return rows(results, "children").map(metadataRow);
  },
  find: (context: AnyObject, results: ExecuteResults) => {
    const target = requireTarget(results, String(context.root));
    const matches = rows(results, "matches");
    if (target.kind === "file") {
      return matches.filter((value) => value.path === context.root).map(metadataRow);
    }
    return matches.map(metadataRow);
  },
  cat: (context: AnyObject, results: ExecuteResults) =>
    requireText(requireTarget(results, String(context.path)), String(context.path)),
  read_text: (context: AnyObject, results: ExecuteResults) =>
    requireText(requireTarget(results, String(context.path)), String(context.path)),
  read_bytes: (context: AnyObject, results: ExecuteResults) =>
    contentBytes(requireFile(requireTarget(results, String(context.path)), String(context.path))) ?? new Uint8Array(),
  head: (context: AnyObject, results: ExecuteResults) =>
    requireText(requireTarget(results, String(context.path)), String(context.path))
      .split(/\r?\n/)
      .slice(0, Number(context.n)),
  tail: (context: AnyObject, results: ExecuteResults) => {
    const lines = requireText(requireTarget(results, String(context.path)), String(context.path)).split(/\r?\n/);
    const count = Number(context.n);
    return count === 0 ? [] : lines.slice(-count);
  },
  grep: (context: AnyObject, results: ExecuteResults) => finalizeLiteralGrep(context, results as never),
  grep_regex: (context: AnyObject, results: ExecuteResults) => finalizeRegexGrep(context, results as never),
  grep_bm25: (context: AnyObject, results: ExecuteResults) => finalizeBm25Grep(context, results as never),
  write_summary: (_context: AnyObject, results: ExecuteResults) => {
    const { name: _name, ...value } = (results.write ?? {}) as WriteResult;
    return value;
  },
  write_target_meta: (context: AnyObject, results: ExecuteResults) => {
    const { name: _name, ...value } = (results.write ?? {}) as WriteResult;
    return {
      path: context.path,
      row: contentRow(context.targetRow as RowLike),
      write: value,
    };
  },
  rm: (context: AnyObject, results: ExecuteResults) => {
    const target = row(results, "target");
    if (!target) {
      return {
        path: context.path,
        recursive: Boolean(context.recursive),
        deleted: false,
        ids: [],
      };
    }
    const write = { ...((results.write ?? {}) as WriteResult) };
    const deletedIds = Array.isArray(write.deleted_ids)
      ? [...write.deleted_ids]
      : !Boolean(context.recursive) && target.id !== undefined
        ? [target.id]
        : [];
    const { name: _name, ...trimmedWrite } = write;
    return {
      path: context.path,
      recursive: Boolean(context.recursive),
      deleted: true,
      ids: deletedIds,
      write: trimmedWrite,
    };
  },
  mounts: (context: AnyObject, results: ExecuteResults) => {
    const suffix = String(context.suffix ?? "__fs");
    const names = Array.isArray((results.namespaces as AnyObject | undefined)?.namespaces)
      ? ((results.namespaces as AnyObject).namespaces as RowLike[])
      : [];
    return names
      .map((value) => String(value.id))
      .filter((value) => value.endsWith(suffix))
      .map((value) => value.slice(0, -suffix.length))
      .sort();
  },
} as const;

export type FinalizerName = keyof typeof FINALIZERS;

