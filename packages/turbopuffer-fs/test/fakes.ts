import type { AnyObject } from "../src/types.js";

function globToRegExp(pattern: string, ignoreCase = false): RegExp {
  let source = "^";
  for (let index = 0; index < pattern.length; index += 1) {
    const char = pattern[index]!;
    const next = pattern[index + 1];
    if (char === "*" && next === "*") {
      source += ".*";
      index += 1;
      continue;
    }
    if (char === "*") {
      source += "[^/]*";
      continue;
    }
    if (char === "?") {
      source += ".";
      continue;
    }
    source += /[\\^$+?.()|[\]{}]/.test(char) ? `\\${char}` : char;
  }
  source += "$";
  return new RegExp(source, ignoreCase ? "i" : undefined);
}

function matchesFilter(row: AnyObject, filter: unknown): boolean {
  if (!filter) {
    return true;
  }
  if (Array.isArray(filter) && filter[0] === "And" && Array.isArray(filter[1])) {
    return filter[1].every((part) => matchesFilter(row, part));
  }
  if (Array.isArray(filter) && filter[0] === "Or" && Array.isArray(filter[1])) {
    return filter[1].some((part) => matchesFilter(row, part));
  }
  if (!Array.isArray(filter) || filter.length < 3) {
    return true;
  }
  const [field, op, value] = filter;
  const candidate = row[String(field)];
  switch (op) {
    case "Eq":
      return candidate === value;
    case "Gt":
      return String(candidate ?? "") > String(value ?? "");
    case "Glob":
      return globToRegExp(String(value)).test(String(candidate ?? ""));
    case "IGlob":
      return globToRegExp(String(value), true).test(String(candidate ?? ""));
    default:
      return true;
  }
}

export class ModelLike {
  readonly payload: unknown;

  constructor(payload: unknown) {
    this.payload = payload;
  }

  model_dump(): unknown {
    return this.payload;
  }
}

export class FakeQueryResponse {
  readonly rows: AnyObject[];
  readonly billing: AnyObject;
  readonly performance: AnyObject;
  readonly aggregations: unknown;
  readonly aggregation_groups: unknown;

  constructor(options: {
    rows?: AnyObject[];
    billing?: AnyObject;
    performance?: AnyObject;
    aggregations?: unknown;
    aggregationGroups?: unknown;
  } = {}) {
    this.rows = [...(options.rows ?? [])];
    this.billing = options.billing ?? { units: 1 };
    this.performance = options.performance ?? { latency_ms: 1 };
    this.aggregations = options.aggregations;
    this.aggregation_groups = options.aggregationGroups;
  }
}

export class FakeWriteResponse {
  readonly payload: AnyObject;

  constructor(payload: AnyObject = {}) {
    this.payload = {
      status: "OK",
      message: "ok",
      rows_affected: 0,
      ...payload,
    };
  }

  model_dump(): AnyObject {
    return { ...this.payload };
  }
}

export class FakeNamespaceList implements AsyncIterable<{ id: string }> {
  readonly namespaces: Array<{ id: string }>;
  readonly next_cursor: string | null;

  constructor(namespaces: Array<{ id: string }>, nextCursor: string | null = null) {
    this.namespaces = namespaces.map((namespace) => ({ ...namespace }));
    this.next_cursor = nextCursor;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<{ id: string }> {
    for (const namespace of this.namespaces) {
      yield { ...namespace };
    }
  }
}

type QueryResponseFactory = (payload: AnyObject) => unknown;
type WriteResponseFactory = (payload: AnyObject) => unknown;

export class FakeNamespace {
  readonly name: string;
  readonly queryCalls: AnyObject[] = [];
  readonly writeCalls: AnyObject[] = [];
  private readonly queryResponses: Array<unknown | QueryResponseFactory>;
  private readonly writeResponses: Array<unknown | WriteResponseFactory>;
  private readonly rows: Map<string, AnyObject> = new Map();

  constructor(
    name: string,
    options: {
      queryResponses?: Array<unknown | QueryResponseFactory>;
      writeResponses?: Array<unknown | WriteResponseFactory>;
      initialRows?: AnyObject[];
    } = {},
  ) {
    this.name = name;
    this.queryResponses = [...(options.queryResponses ?? [])];
    this.writeResponses = [...(options.writeResponses ?? [])];
    for (const row of options.initialRows ?? []) {
      if (row.path !== undefined) {
        this.rows.set(String(row.path), { ...row });
      }
    }
  }

  async query(payload: AnyObject): Promise<unknown> {
    this.queryCalls.push({ ...payload });
    const response = this.queryResponses.shift();
    if (typeof response === "function") {
      return (response as QueryResponseFactory)({ ...payload });
    }
    if (response !== undefined) {
      return response;
    }
    const includeAttributes = Array.isArray(payload.include_attributes)
      ? new Set(payload.include_attributes.map((value) => String(value)))
      : null;
    const rankBy = Array.isArray(payload.rank_by) ? payload.rank_by : null;
    const limit = typeof payload.limit === "number" ? payload.limit : undefined;
    let rows = [...this.rows.values()].filter((row) => matchesFilter(row, payload.filters));
    if (rankBy?.[0] === "path" && rankBy?.[1] === "asc") {
      rows = rows.sort((left, right) => String(left.path).localeCompare(String(right.path)));
    }
    if (limit !== undefined) {
      rows = rows.slice(0, limit);
    }
    const projected = includeAttributes
      ? rows.map((row) =>
          Object.fromEntries(
            Object.entries(row).filter(([key]) => key === "id" || includeAttributes.has(key)),
          ))
      : rows;
    return new FakeQueryResponse({ rows: projected });
  }

  async write(payload: AnyObject): Promise<unknown> {
    this.writeCalls.push({ ...payload });
    const response = this.writeResponses.shift();
    if (typeof response === "function") {
      return (response as WriteResponseFactory)({ ...payload });
    }
    if (response !== undefined) {
      return response;
    }
    const deletes = Array.isArray(payload.deletes) ? payload.deletes : [];
    const upserts = Array.isArray(payload.upsert_rows) ? payload.upsert_rows : [];
    for (const row of upserts) {
      const typedRow = row as AnyObject;
      if (typedRow.path !== undefined) {
        this.rows.set(String(typedRow.path), { ...typedRow });
      }
    }
    if (deletes.length > 0) {
      const ids = new Set(deletes.map((value) => String(value)));
      for (const [path, row] of [...this.rows.entries()]) {
        if (ids.has(String(row.id))) {
          this.rows.delete(path);
        }
      }
    }
    return new FakeWriteResponse({
      rows_affected: deletes.length + upserts.length,
      rows_deleted: deletes.length || undefined,
      rows_upserted: upserts.length || undefined,
      deleted_ids: deletes.length > 0 ? deletes : undefined,
      upserted_ids: upserts.length > 0 ? upserts.map((row) => (row as AnyObject).id) : undefined,
    });
  }

  async deleteAll(): Promise<void> {
    this.rows.clear();
    return undefined;
  }

  snapshotRows(): AnyObject[] {
    return [...this.rows.values()].map((row) => ({ ...row }));
  }
}

export class FakeClient {
  readonly namespaceCalls: string[] = [];
  readonly namespaceListCalls: AnyObject[] = [];
  private readonly namespaceHandles: Map<string, FakeNamespace>;
  private readonly namespaceLists: Array<FakeNamespaceList>;

  constructor(options: {
    namespaces?: Record<string, FakeNamespace>;
    namespaceIds?: string[];
    namespaceLists?: FakeNamespaceList[];
  } = {}) {
    this.namespaceHandles = new Map(Object.entries(options.namespaces ?? {}));
    this.namespaceLists = options.namespaceLists
      ? [...options.namespaceLists]
      : options.namespaceIds
        ? [new FakeNamespaceList(options.namespaceIds.map((id) => ({ id })))]
        : [];
  }

  namespace(name: string): FakeNamespace {
    this.namespaceCalls.push(name);
    const existing = this.namespaceHandles.get(name);
    if (existing) {
      return existing;
    }
    const created = new FakeNamespace(name);
    this.namespaceHandles.set(name, created);
    return created;
  }

  async namespaces(payload: AnyObject = {}): Promise<FakeNamespaceList> {
    this.namespaceListCalls.push({ ...payload });
    return this.namespaceLists.shift() ?? new FakeNamespaceList([]);
  }
}

export class NotFoundError extends Error {
  readonly status = 404;

  constructor(message = "namespace not found") {
    super(message);
    this.name = "NotFoundError";
  }
}
