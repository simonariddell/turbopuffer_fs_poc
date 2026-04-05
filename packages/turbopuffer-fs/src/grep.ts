import { andFilter, normalizePath, scopedGlobFilter, subtreeFilter, textSubstringFilter } from "./paths.js";
import { META_FIELDS } from "./schema.js";
import type {
  AnyObject,
  GrepLineMatch,
  GrepMode,
  GrepOptions,
  GrepResult,
  GrepSearchHit,
  Plan,
  PlanStep,
  QueryStep,
  RowLike,
} from "./types.js";

const DEFAULT_GREP_LIMIT = 100;

const limitValue = (limit?: number | null): number | undefined => {
  if (limit == null) return undefined;
  const value = Number(limit);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error("limit must be a positive integer");
  }
  return value;
};

const queryStep = (
  name: string,
  payload: Record<string, unknown>,
  options: {
    paginate?: boolean;
    limit?: number | null;
    pageSize?: number;
    orderField?: string;
  } = {},
): PlanStep => {
  const step: QueryStep = {
    kind: "query",
    name,
    payload: { ...payload },
  };
  if (options.paginate) {
    const effectiveLimit = limitValue(options.limit);
    const pageSize = options.pageSize ?? 256;
    step.paginate = true;
    step.limit = effectiveLimit;
    step.pageSize = Math.min(pageSize, effectiveLimit ?? pageSize);
    step.orderField = options.orderField ?? "path";
  }
  return step;
};

const plan = (
  namespace: string,
  steps: PlanStep[],
  finalize: string,
  context: Record<string, unknown> = {},
): Plan => ({
  namespace,
  steps,
  finalize,
  context,
});

const lookupPayload = (path: string, fields: readonly string[]) => ({
  filters: ["path", "Eq", normalizePath(path)],
  rank_by: ["path", "asc"],
  limit: 1,
  include_attributes: fields,
});

const orderedPayload = (filters: unknown, fields: readonly string[]) => ({
  filters,
  rank_by: ["path", "asc"],
  include_attributes: fields,
});

const grepMode = (options: GrepOptions): GrepMode => options.mode ?? "literal";

function literalCandidateFilters(root: string, pattern: string, options: GrepOptions): unknown {
  const ignoreCase = options.ignoreCase ?? false;
  return andFilter<unknown>(
    ["kind", "Eq", "file"],
    ["is_text", "Eq", 1],
    subtreeFilter(root),
    scopedGlobFilter(root, options.glob ?? null, { ignoreCase }),
    textSubstringFilter(pattern, { ignoreCase }),
  );
}

function regexCandidateFilters(root: string, options: GrepOptions): unknown {
  const ignoreCase = options.ignoreCase ?? false;
  return andFilter<unknown>(
    ["kind", "Eq", "file"],
    ["is_text", "Eq", 1],
    subtreeFilter(root),
    scopedGlobFilter(root, options.glob ?? null, { ignoreCase }),
  );
}

function bm25RankBy(pattern: string, options: GrepOptions): unknown {
  return options.lastAsPrefix
    ? ["text", "BM25", pattern, { last_as_prefix: true }]
    : ["text", "BM25", pattern];
}

export function buildGrepPlan(namespace: string, root: string, pattern: string, options: GrepOptions = {}): Plan {
  if (pattern === "") {
    throw new Error("pattern must not be empty");
  }
  const value = normalizePath(root);
  const mode = grepMode(options);
  const limit = limitValue(options.limit) ?? DEFAULT_GREP_LIMIT;
  if (mode === "bm25") {
    return plan(
      namespace,
      [
        queryStep("target", lookupPayload(value, META_FIELDS)),
        queryStep("candidates", {
          filters: regexCandidateFilters(value, options),
          rank_by: bm25RankBy(pattern, options),
          top_k: limit,
          include_attributes: ["path", "text"],
        }),
      ],
      "grep_bm25",
      {
        root: value,
        pattern,
        mode,
        ignoreCase: options.ignoreCase ?? false,
        glob: options.glob ?? null,
        limit,
      },
    );
  }
  if (mode === "regex") {
    return plan(
      namespace,
      [
        queryStep("target", lookupPayload(value, META_FIELDS)),
        queryStep("candidates", orderedPayload(regexCandidateFilters(value, options), ["path", "text"]), {
          paginate: true,
          limit,
        }),
      ],
      "grep_regex",
      {
        root: value,
        pattern,
        mode,
        ignoreCase: options.ignoreCase ?? false,
        glob: options.glob ?? null,
        limit,
        multiline: options.multiline ?? false,
        dotAll: options.dotAll ?? false,
      },
    );
  }
  return plan(
    namespace,
    [
      queryStep("target", lookupPayload(value, META_FIELDS)),
      queryStep("candidates", orderedPayload(literalCandidateFilters(value, pattern, options), ["path", "text"]), {
        paginate: true,
        limit,
      }),
    ],
    "grep",
    {
      root: value,
      pattern,
      mode,
      ignoreCase: options.ignoreCase ?? false,
      glob: options.glob ?? null,
      limit,
    },
  );
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
