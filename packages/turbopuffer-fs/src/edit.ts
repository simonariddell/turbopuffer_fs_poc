import { putText, readText, stat } from "./live.js";
import { normalizePath } from "./paths.js";
import { sha256Hex } from "./schema.js";
import type { AnyObject } from "./types.js";

export interface ReplaceTextInFileOptions {
  search: string | RegExp;
  replace: string;
  expectedMatches?: number;
  requireUnique?: boolean;
  ignoreCase?: boolean;
}

export interface ReplaceTextInFileResult extends AnyObject {
  path: string;
  matches: number;
  changed: boolean;
  before_text: string;
  after_text: string;
  before_sha256: string;
  after_sha256: string;
  mime?: string;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function matchCount(text: string, pattern: RegExp): number {
  return [...text.matchAll(pattern)].length;
}

function searchPattern(
  search: string | RegExp,
  options: { requireGlobal?: boolean; ignoreCase?: boolean } = {},
): RegExp {
  if (search instanceof RegExp) {
    const flags = new Set(search.flags.split(""));
    if (options.requireGlobal ?? true) {
      flags.add("g");
    }
    if (options.ignoreCase) {
      flags.add("i");
    }
    return new RegExp(search.source, [...flags].join(""));
  }
  const flags = `${options.ignoreCase ? "i" : ""}${options.requireGlobal ?? true ? "g" : ""}`;
  return new RegExp(escapeRegExp(search), flags);
}

export async function replaceTextInFile(
  client: Parameters<typeof stat>[0],
  mount: string,
  path: string,
  options: ReplaceTextInFileOptions,
): Promise<ReplaceTextInFileResult> {
  const normalizedPath = normalizePath(path);
  const target = await stat(client, mount, normalizedPath);
  if (target === null) {
    throw new Error(`FileNotFoundError:${normalizedPath}`);
  }
  if (target.kind === "dir") {
    throw new Error(`IsADirectoryError:${normalizedPath}`);
  }
  if (Number(target.is_text ?? 0) !== 1) {
    throw new Error(`ValueError:path is a binary file: ${normalizedPath}`);
  }

  const before = String(await readText(client, mount, normalizedPath));
  const pattern = searchPattern(options.search, { ignoreCase: options.ignoreCase });
  const matches = matchCount(before, pattern);
  const expectedMatches = options.requireUnique === false ? options.expectedMatches : (options.expectedMatches ?? 1);
  if (expectedMatches !== undefined && matches !== expectedMatches) {
    if (matches === 0) {
      throw new Error(`ReplaceTextNoMatchError:${normalizedPath}`);
    }
    throw new Error(`ReplaceTextMatchCountError:${normalizedPath}:expected ${expectedMatches},found ${matches}`);
  }
  if (options.requireUnique !== false && matches !== 1) {
    if (matches === 0) {
      throw new Error(`ReplaceTextNoMatchError:${normalizedPath}`);
    }
    throw new Error(`ReplaceTextMatchCountError:${normalizedPath}:expected 1,found ${matches}`);
  }

  const after = before.replace(pattern, options.replace);
  if (after === before) {
    return {
      path: normalizedPath,
      matches,
      changed: false,
      before_text: before,
      after_text: after,
      before_sha256: sha256Hex(new TextEncoder().encode(before)),
      after_sha256: sha256Hex(new TextEncoder().encode(after)),
      mime: typeof target.mime === "string" ? target.mime : undefined,
    };
  }

  await putText(client, mount, normalizedPath, after, {
    mime: typeof target.mime === "string" ? target.mime : undefined,
  });

  return {
    path: normalizedPath,
    matches,
    changed: true,
    before_text: before,
    after_text: after,
    before_sha256: sha256Hex(new TextEncoder().encode(before)),
    after_sha256: sha256Hex(new TextEncoder().encode(after)),
    mime: typeof target.mime === "string" ? target.mime : undefined,
  };
}
