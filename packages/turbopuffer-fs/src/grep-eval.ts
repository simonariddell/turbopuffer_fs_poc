import { Buffer } from "node:buffer";
import path from "node:path/posix";
import { performance } from "node:perf_hooks";

import { buildGrepPlan, finalizeBm25Grep, finalizeLiteralGrep, finalizeRegexGrep } from "./grep.js";
import { bytesRow, textRow } from "./schema.js";
import type { GrepMode, GrepOptions, GrepResult, RowLike } from "./types.js";

export interface GrepEvalDocument {
  path: string;
  text?: string;
  bytes?: Uint8Array;
}

export interface GrepEvalMetrics {
  mode: GrepMode;
  candidateCount: number;
  candidateTextBytes: number;
  finalCount: number;
  durationMs: number;
}

export interface GrepEvalResult {
  plan: ReturnType<typeof buildGrepPlan>;
  metrics: GrepEvalMetrics;
  result: GrepResult;
}

function normalized(pathValue: string): string {
  return pathValue === "/" ? "/" : `/${pathValue.replace(/^\/+/, "").replace(/\/+$/, "")}`.replace(/\/+/g, "/");
}

function isUnderRoot(root: string, candidate: string): boolean {
  return root === "/" || candidate === root || candidate.startsWith(`${root.replace(/\/$/, "")}/`);
}

function simpleGlobMatch(pattern: string | null | undefined, candidatePath: string): boolean {
  if (!pattern) return true;
  const target = pattern.includes("/") ? candidatePath : path.basename(candidatePath);
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*").replace(/\?/g, ".");
  return new RegExp(`^${escaped}$`).test(target);
}

function literalIncludes(text: string, pattern: string, ignoreCase: boolean): boolean {
  return ignoreCase
    ? text.toLowerCase().includes(pattern.toLowerCase())
    : text.includes(pattern);
}

function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .split(/[^a-z0-9_]+/)
    .filter((token) => token.length > 0);
}

function bm25Score(text: string, query: string): number {
  const docTokens = tokenize(text);
  const queryTokens = tokenize(query);
  const counts = new Map<string, number>();
  for (const token of docTokens) {
    counts.set(token, (counts.get(token) ?? 0) + 1);
  }
  return queryTokens.reduce((sum, token) => sum + (counts.get(token) ?? 0), 0);
}

function corpusRows(documents: GrepEvalDocument[]): RowLike[] {
  return documents.map((document) =>
    document.text !== undefined
      ? textRow(document.path, document.text)
      : bytesRow(document.path, document.bytes ?? new Uint8Array()),
  );
}

function candidateRows(rows: RowLike[], root: string, pattern: string, options: GrepOptions): RowLike[] {
  const mode = options.mode ?? "literal";
  const ignoreCase = options.ignoreCase ?? false;
  const scoped = rows.filter((row) =>
    row.kind === "file" &&
    Number(row.is_text ?? 0) === 1 &&
    isUnderRoot(root, String(row.path)) &&
    simpleGlobMatch(options.glob ?? null, String(row.path)),
  );
  if (mode === "literal") {
    return scoped.filter((row) => literalIncludes(String(row.text ?? ""), pattern, ignoreCase));
  }
  if (mode === "bm25") {
    return scoped
      .map((row) => ({ ...row, $dist: bm25Score(String(row.text ?? ""), pattern) }))
      .filter((row) => Number(row.$dist ?? 0) > 0)
      .sort((left, right) => Number(right.$dist ?? 0) - Number(left.$dist ?? 0))
      .slice(0, options.limit ?? 100);
  }
  return scoped;
}

export function runGrepEval(
  documents: GrepEvalDocument[],
  root: string,
  pattern: string,
  options: GrepOptions = {},
): GrepEvalResult {
  const rows = corpusRows(documents);
  const plan = buildGrepPlan("documents__fs", root, pattern, options);
  const candidates = candidateRows(rows, normalized(root), pattern, options);
  const metricsStart = performance.now();
  const result =
    (options.mode ?? "literal") === "regex"
      ? finalizeRegexGrep(plan.context, {
          target: { rows: [{ path: normalized(root), kind: "dir" }] },
          candidates: { rows: candidates },
        })
      : (options.mode ?? "literal") === "bm25"
        ? finalizeBm25Grep(plan.context, {
            target: { rows: [{ path: normalized(root), kind: "dir" }] },
            candidates: { rows: candidates },
          })
        : finalizeLiteralGrep(plan.context, {
            target: { rows: [{ path: normalized(root), kind: "dir" }] },
            candidates: { rows: candidates },
          });
  const durationMs = performance.now() - metricsStart;
  return {
    plan,
    metrics: {
      mode: options.mode ?? "literal",
      candidateCount: candidates.length,
      candidateTextBytes: candidates.reduce(
        (sum, row) => sum + Buffer.byteLength(String(row.text ?? ""), "utf8"),
        0,
      ),
      finalCount: result.length,
      durationMs,
    },
    result,
  };
}
