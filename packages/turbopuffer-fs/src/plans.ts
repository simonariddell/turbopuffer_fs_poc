import {
  andFilter,
  ancestorPaths,
  directChildrenFilter,
  normalizePath,
  pathsFilter,
  scopedGlobFilter,
  subtreeFilter,
  textSubstringFilter,
} from "./paths.js";
import {
  bytesRow,
  CONTENT_FIELDS,
  directoryRow,
  META_FIELDS,
  parentDirectoryRows,
  targetDirectoryRows,
  textRow,
  upsertRowsPayload,
} from "./schema.js";
import { buildGrepPlan } from "./grep.js";
import type { Plan, PlanStep, FsRow } from "./types.js";

export const DEFAULT_PAGE_SIZE = 256;

const limitValue = (limit?: number | null): number | undefined => {
  if (limit == null) return undefined;
  const value = Number(limit);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error("limit must be a positive integer");
  }
  return value;
};

const lineCount = (n: number): number => {
  const value = Number(n);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error("n must be non-negative");
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
  const step: PlanStep = {
    kind: "query",
    name,
    payload: { ...payload },
  };
  if (options.paginate) {
    const effectiveLimit = limitValue(options.limit);
    const pageSize = options.pageSize ?? DEFAULT_PAGE_SIZE;
    step.paginate = true;
    step.limit = effectiveLimit;
    step.pageSize = Math.min(pageSize, effectiveLimit ?? pageSize);
    step.orderField = options.orderField ?? "path";
  }
  return step;
};

const writeStep = (name: string, payload: Record<string, unknown>): PlanStep => ({
  kind: "write",
  name,
  payload: { ...payload },
});

const assertStep = (name: string, check: string): PlanStep => ({
  kind: "assert",
  name,
  check,
});

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

export const statPlan = (namespace: string, path: string): Plan =>
  plan(namespace, [queryStep("target", lookupPayload(path, META_FIELDS))], "stat", {
    path: normalizePath(path),
  });

export const lsPlan = (namespace: string, path = "/", limit?: number | null): Plan => {
  const value = normalizePath(path);
  return plan(
    namespace,
    [
      queryStep("target", lookupPayload(value, META_FIELDS)),
      queryStep("children", orderedPayload(directChildrenFilter(value), META_FIELDS), {
        paginate: true,
        limit,
      }),
    ],
    "ls",
    { path: value, limit: limitValue(limit) },
  );
};

export const findPlan = (
  namespace: string,
  root = "/",
  options: {
    glob?: string | null;
    kind?: string | null;
    ignoreCase?: boolean;
    limit?: number | null;
  } = {},
): Plan => {
  const value = normalizePath(root);
  const filters = andFilter<unknown>(
    subtreeFilter(value),
    options.kind ? ["kind", "Eq", options.kind] : null,
    scopedGlobFilter(value, options.glob ?? null, { ignoreCase: options.ignoreCase ?? false }),
  );
  return plan(
    namespace,
    [
      queryStep("target", lookupPayload(value, META_FIELDS)),
      queryStep("matches", orderedPayload(filters, META_FIELDS), {
        paginate: true,
        limit: options.limit,
      }),
    ],
    "find",
    {
      root: value,
      glob: options.glob ?? null,
      kind: options.kind ?? null,
      ignoreCase: options.ignoreCase ?? false,
      limit: limitValue(options.limit),
    },
  );
};

export const catPlan = (namespace: string, path: string): Plan =>
  plan(namespace, [queryStep("target", lookupPayload(path, CONTENT_FIELDS))], "cat", {
    path: normalizePath(path),
  });

export const readTextPlan = (namespace: string, path: string): Plan =>
  plan(namespace, [queryStep("target", lookupPayload(path, CONTENT_FIELDS))], "read_text", {
    path: normalizePath(path),
  });

export const readBytesPlan = (namespace: string, path: string): Plan =>
  plan(namespace, [queryStep("target", lookupPayload(path, CONTENT_FIELDS))], "read_bytes", {
    path: normalizePath(path),
  });

export const headPlan = (namespace: string, path: string, n = 10): Plan =>
  plan(namespace, [queryStep("target", lookupPayload(path, CONTENT_FIELDS))], "head", {
    path: normalizePath(path),
    n: lineCount(n),
  });

export const tailPlan = (namespace: string, path: string, n = 10): Plan =>
  plan(namespace, [queryStep("target", lookupPayload(path, CONTENT_FIELDS))], "tail", {
    path: normalizePath(path),
    n: lineCount(n),
  });

export const grepPlan = (
  namespace: string,
  root: string,
  pattern: string,
  options: {
    mode?: "literal" | "regex" | "bm25";
    ignoreCase?: boolean;
    glob?: string | null;
    limit?: number | null;
    multiline?: boolean;
    dotAll?: boolean;
    lastAsPrefix?: boolean;
  } = {},
): Plan => {
  return buildGrepPlan(namespace, root, pattern, options);
};

export const mkdirPlan = (namespace: string, path: string): Plan => {
  const value = normalizePath(path);
  const directoryPaths = ancestorPaths(value, true);
  return plan(
    namespace,
    [
      queryStep("existing", {
        ...orderedPayload(pathsFilter(directoryPaths), META_FIELDS),
        limit: directoryPaths.length,
      }),
      assertStep("validate", "mkdir_preconditions"),
      writeStep("write", upsertRowsPayload(targetDirectoryRows(value))),
    ],
    "write_target_meta",
    {
      path: value,
      parentPaths: ancestorPaths(value, false),
      targetRow: directoryRow(value),
    },
  );
};

export const putTextPlan = (
  namespace: string,
  path: string,
  text: string,
  mime?: string | null,
): Plan => {
  const value = normalizePath(path);
  if (value === "/") throw new Error("cannot write text to root directory");
  const checkPaths = [...ancestorPaths(value, false), value];
  const target = textRow(value, text, mime ?? undefined);
  return plan(
    namespace,
    [
      queryStep("existing", {
        ...orderedPayload(pathsFilter(checkPaths), META_FIELDS),
        limit: checkPaths.length,
      }),
      assertStep("validate", "put_preconditions"),
      writeStep("write", upsertRowsPayload([...parentDirectoryRows(value), target])),
    ],
    "write_target_meta",
    {
      path: value,
      parentPaths: ancestorPaths(value, false),
      targetRow: target,
    },
  );
};

export const putBytesPlan = (
  namespace: string,
  path: string,
  data: Uint8Array,
  mime?: string | null,
): Plan => {
  const value = normalizePath(path);
  if (value === "/") throw new Error("cannot write bytes to root directory");
  const checkPaths = [...ancestorPaths(value, false), value];
  const target = bytesRow(value, data, mime ?? undefined);
  return plan(
    namespace,
    [
      queryStep("existing", {
        ...orderedPayload(pathsFilter(checkPaths), META_FIELDS),
        limit: checkPaths.length,
      }),
      assertStep("validate", "put_preconditions"),
      writeStep("write", upsertRowsPayload([...parentDirectoryRows(value), target])),
    ],
    "write_target_meta",
    {
      path: value,
      parentPaths: ancestorPaths(value, false),
      targetRow: target,
    },
  );
};

export const rmPlan = (namespace: string, path: string, recursive = false): Plan => {
  const value = normalizePath(path);
  if (value === "/") throw new Error("rm('/') is not supported");
  const steps: PlanStep[] = [queryStep("target", lookupPayload(value, META_FIELDS))];
  if (recursive) {
    steps.push(
      queryStep("delete_targets", orderedPayload(subtreeFilter(value), ["id", "path", "kind"]), {
        paginate: true,
      }),
    );
  } else {
    steps.push(
      queryStep("child_probe", {
        filters: directChildrenFilter(value),
        rank_by: ["path", "asc"],
        limit: 1,
        include_attributes: ["path"],
      }),
    );
  }
  steps.push(assertStep("validate", "rm_preconditions"));
  steps.push(
    writeStep(
      "write",
      recursive
        ? {
            delete_rows_from: "delete_targets",
            delete_batch_size: DEFAULT_PAGE_SIZE,
            return_affected_ids: true,
          }
        : {
            delete_rows_from: "target",
            delete_batch_size: 1,
            return_affected_ids: true,
          },
    ),
  );
  return plan(namespace, steps, "rm", { path: value, recursive });
};

export const upsertRowsPlan = (namespace: string, rows: FsRow[]): Plan =>
  plan(namespace, [writeStep("write", upsertRowsPayload(rows))], "write_summary", {
    rows,
  });
