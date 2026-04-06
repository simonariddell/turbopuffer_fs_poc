import { planGrepQuery } from "./grep-planner.js";
import type {
  AnyObject,
  GrepLineMatch,
  GrepOptions,
  GrepResult,
  GrepSearchHit,
  Plan,
  RowLike,
} from "./types.js";

export function buildGrepPlan(namespace: string, root: string, pattern: string, options: GrepOptions = {}): Plan {
  return planGrepQuery(namespace, root, pattern, options).plan;
}

const rows = (results: Record<string, AnyObject>, name: string): RowLike[] => {
  const value = results[name];
  return Array.isArray(value?.rows) ? (value.rows as RowLike[]) : [];
};

const row = (results: Record<string, AnyObject>, name: string): RowLike | null => rows(results, name)[0] ?? null;

const requireTarget = (results: Record<string, AnyObject>, path: string): RowLike => {
  const target = row(results, "target");
  if (!target) {
    throw new Error(`FileNotFoundError:${path}`);
  }
  return target;
};

function matchLiteralLines(value: RowLike, pattern: string, ignoreCase: boolean): GrepLineMatch[] {
  const text = String(value.text ?? "");
  const needle = ignoreCase ? pattern.toLowerCase() : pattern;
  return text.split(/\r?\n/).flatMap((line, index) => {
    const haystack = ignoreCase ? line.toLowerCase() : line;
    return haystack.includes(needle)
      ? [{ kind: "line_match", path: String(value.path), line_number: index + 1, line }]
      : [];
  });
}

function buildRegex(pattern: string, options: { ignoreCase: boolean; multiline: boolean; dotAll: boolean }): RegExp {
  let flags = "u";
  if (options.ignoreCase) flags += "i";
  if (options.multiline) flags += "m";
  if (options.dotAll) flags += "s";
  return new RegExp(pattern, flags);
}

function matchRegexLines(
  value: RowLike,
  pattern: string,
  options: { ignoreCase: boolean; multiline: boolean; dotAll: boolean },
): GrepLineMatch[] {
  const text = String(value.text ?? "");
  const expression = buildRegex(pattern, options);
  return text.split(/\r?\n/).flatMap((line, index) =>
    expression.test(line)
      ? [{ kind: "line_match", path: String(value.path), line_number: index + 1, line }]
      : [],
  );
}

function snippetForQuery(text: string, query: string, ignoreCase: boolean): string {
  const lines = text.split(/\r?\n/);
  const needle = ignoreCase ? query.toLowerCase() : query;
  const matchingLine = lines.find((line) => {
    const haystack = ignoreCase ? line.toLowerCase() : line;
    return haystack.includes(needle);
  });
  if (matchingLine) {
    return matchingLine.slice(0, 240);
  }
  return text.slice(0, 240);
}

export function finalizeLiteralGrep(context: AnyObject, results: Record<string, AnyObject>): GrepResult {
  requireTarget(results, String(context.root));
  return rows(results, "candidates").flatMap((value) =>
    matchLiteralLines(value, String(context.pattern ?? ""), Boolean(context.ignoreCase ?? context.ignore_case)),
  );
}

export function finalizeRegexGrep(context: AnyObject, results: Record<string, AnyObject>): GrepResult {
  requireTarget(results, String(context.root));
  return rows(results, "candidates").flatMap((value) =>
    matchRegexLines(value, String(context.pattern ?? ""), {
      ignoreCase: Boolean(context.ignoreCase ?? context.ignore_case),
      multiline: Boolean(context.multiline),
      dotAll: Boolean(context.dotAll),
    }),
  );
}

export function finalizeBm25Grep(context: AnyObject, results: Record<string, AnyObject>): GrepResult {
  requireTarget(results, String(context.root));
  return rows(results, "candidates").map((value) => ({
    kind: "search_hit",
    mode: "bm25",
    path: String(value.path),
    score: Number(value.$dist ?? 0),
    snippet: snippetForQuery(
      String(value.text ?? ""),
      String(context.pattern ?? ""),
      Boolean(context.ignoreCase ?? context.ignore_case),
    ),
  })) as GrepSearchHit[];
}
