import type { AnyObject, GrepMode, GrepOptions, GrepPlannerArtifact, GrepPlannerStage } from "./types.js";
import { andFilter, normalizePath, scopedGlobFilter, subtreeFilter, textSubstringFilter } from "./paths.js";

const DEFAULT_GREP_LIMIT = 100;
const DEFAULT_PAGE_SIZE = 256;

function limitValue(limit?: number | null): number {
  if (limit == null) return DEFAULT_GREP_LIMIT;
  const value = Number(limit);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error("limit must be a positive integer");
  }
  return value;
}

function modeOf(options: GrepOptions): GrepMode {
  return options.mode ?? "regex";
}

function normalizeRequest(root: string, pattern: string, options: GrepOptions) {
  if (pattern === "") {
    throw new Error("pattern must not be empty");
  }
  return {
    root: normalizePath(root),
    pattern,
    mode: modeOf(options),
    glob: options.glob ?? null,
    limit: limitValue(options.limit),
    ignoreCase: options.ignoreCase ?? false,
    multiline: options.multiline ?? false,
    dotAll: options.dotAll ?? false,
    lastAsPrefix: options.lastAsPrefix ?? false,
  } satisfies Required<Omit<GrepOptions, "glob">> & {
    root: string;
    pattern: string;
    glob: string | null;
  };
}

function stableStringify(value: unknown): string {
  if (value === null) return "null";
  if (value === undefined) return "undefined";
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(", ")}]`;
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).sort(([left], [right]) =>
      left.localeCompare(right),
    );
    return `{${entries.map(([key, item]) => `${JSON.stringify(key)}: ${stableStringify(item)}`).join(", ")}}`;
  }
  return JSON.stringify(value);
}

function lookupPayload(path: string, fields: readonly string[]) {
  return {
    filters: ["path", "Eq", path],
    rank_by: ["path", "asc"],
    limit: 1,
    include_attributes: [...fields],
  };
}

function regexScopeFilters(
  root: string,
  options: { glob: string | null; ignoreCase: boolean },
): unknown {
  return andFilter<unknown>(
    ["kind", "Eq", "file"],
    ["is_text", "Eq", 1],
    subtreeFilter(root),
    scopedGlobFilter(root, options.glob, { ignoreCase: options.ignoreCase }),
  );
}

function literalScopeFilters(
  root: string,
  pattern: string,
  options: { glob: string | null; ignoreCase: boolean },
): unknown {
  return andFilter<unknown>(
    regexScopeFilters(root, options),
    textSubstringFilter(pattern, { ignoreCase: options.ignoreCase }),
  );
}

function bm25RankBy(pattern: string, lastAsPrefix: boolean): unknown {
  return lastAsPrefix
    ? ["text", "BM25", pattern, { last_as_prefix: true }]
    : ["text", "BM25", pattern];
}

function strategyFor(mode: GrepMode): GrepPlannerStage["strategy"] {
  switch (mode) {
    case "bm25":
      return "bm25_ranked_direct";
    case "literal":
      return "literal_prefilter_then_exact_lines";
    case "regex":
    default:
      return "regex_scope_then_exact_lines";
  }
}

export function planGrepQuery(namespace: string, root: string, pattern: string, options: GrepOptions = {}): GrepPlannerArtifact {
  const request = normalizeRequest(root, pattern, options);
  const targetPayload = lookupPayload(request.root, [
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
  ]);

  const candidatePayload =
    request.mode === "bm25"
      ? {
          filters: regexScopeFilters(request.root, request),
          rank_by: bm25RankBy(request.pattern, request.lastAsPrefix),
          top_k: request.limit,
          include_attributes: ["path", "text"],
        }
      : {
          filters:
            request.mode === "literal"
              ? literalScopeFilters(request.root, request.pattern, request)
              : regexScopeFilters(request.root, request),
          rank_by: ["path", "asc"],
          include_attributes: ["path", "text"],
          limit: request.limit,
        };

  const followupQueries =
    request.mode === "bm25"
      ? []
      : [
          {
            name: "candidate_text_fetch_template",
            payload: {
              rank_by: ["path", "asc"],
              include_attributes: ["path", "text"],
              limit: Math.min(request.limit, DEFAULT_PAGE_SIZE),
              filters: ["path", "In", ["$CANDIDATE_PATHS"]],
            },
          },
        ];

  const finalization =
    request.mode === "bm25"
      ? "ranked_bm25_hits"
      : request.mode === "literal"
        ? "exact_literal_lines"
        : "exact_regex_lines";

  const stage: GrepPlannerStage = {
    strategy: strategyFor(request.mode),
    candidateQuery: candidatePayload as AnyObject,
    candidateQueryText: stableStringify(candidatePayload),
    followupQueries: followupQueries.map((query) => ({
      ...query,
      payloadText: stableStringify(query.payload),
    })),
    finalization,
  };

  return {
    request,
    plan: {
      namespace,
      steps: [
        { kind: "query", name: "target", payload: targetPayload },
        {
          kind: "query",
          name: "candidates",
          payload: candidatePayload,
          ...(request.mode === "bm25"
            ? {}
            : {
                paginate: true,
                limit: request.limit,
                pageSize: Math.min(request.limit, DEFAULT_PAGE_SIZE),
                orderField: "path",
              }),
        },
      ],
      finalize:
        request.mode === "bm25"
          ? "grep_bm25"
          : request.mode === "literal"
            ? "grep"
            : "grep_regex",
      context: {
        root: request.root,
        pattern: request.pattern,
        mode: request.mode,
        glob: request.glob,
        limit: request.limit,
        ignoreCase: request.ignoreCase,
        multiline: request.multiline,
        dotAll: request.dotAll,
        lastAsPrefix: request.lastAsPrefix,
      },
    },
    stage,
    planText: [
      `mode=${request.mode}`,
      `strategy=${stage.strategy}`,
      `scope=${stableStringify({
        root: request.root,
        glob: request.glob,
        ignoreCase: request.ignoreCase,
      })}`,
      `candidate=${stage.candidateQueryText}`,
      ...stage.followupQueries.map((query) => `followup:${query.name}=${query.payloadText}`),
      `finalization=${stage.finalization}`,
    ].join("\n"),
  };
}
