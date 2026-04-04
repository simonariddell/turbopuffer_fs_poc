import { Turbopuffer } from "@turbopuffer/turbopuffer";

import { runCheck } from "./checks.js";
import { FINALIZERS } from "./finalize.js";
import { withAfterFilter } from "./paths.js";
import type {
  AnyObject,
  ExecuteResults,
  ExecuteResult,
  NamespacesResult,
  Plan,
  QueryResult as PlanQueryResult,
  PlanStep,
  QueryPage,
  QueryStep,
  QueryResult,
  WriteResult,
  WriteStep,
} from "./types.js";

export function toPlain(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value;
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return value.map(toPlain);
  if (typeof value === "object") {
    if ("model_dump" in (value as Record<string, unknown>) && typeof (value as { model_dump: () => unknown }).model_dump === "function") {
      return toPlain((value as { model_dump: () => unknown }).model_dump());
    }
    if ("toJSON" in (value as Record<string, unknown>) && typeof (value as { toJSON: () => unknown }).toJSON === "function") {
      return toPlain((value as { toJSON: () => unknown }).toJSON());
    }
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [key, toPlain(item)]),
    );
  }
  return value;
}

function rowsOf(response: unknown): AnyObject[] {
  const plain = toPlain(response);
  if (!plain || typeof plain !== "object") {
    return [];
  }
  const rows = (plain as Record<string, unknown>).rows;
  return Array.isArray(rows) ? (rows as AnyObject[]) : [];
}

function normalizeQueryResult(name: string, response: unknown, pages?: QueryPage[]): QueryResult {
  const plain = (toPlain(response) ?? {}) as Record<string, unknown>;
  const result: QueryResult = {
    name,
    rows: rowsOf(response),
    billing: plain.billing,
    performance: plain.performance,
    aggregations: plain.aggregations,
    aggregationGroups: plain.aggregation_groups,
  };
  if (pages) {
    result.pages = pages;
    result.pageCount = pages.length;
  }
  return result;
}

function normalizeWriteResult(name: string, response: unknown): WriteResult {
  const plain = toPlain(response);
  if (plain && typeof plain === "object" && !Array.isArray(plain)) {
    return { ...(plain as Record<string, unknown>), name } as WriteResult;
  }
  return { name, response: plain } as WriteResult;
}

function namespaceId(value: unknown): string {
  if (value && typeof value === "object" && "id" in (value as Record<string, unknown>)) {
    return String((value as Record<string, unknown>).id);
  }
  throw new TypeError(`namespace value has no id: ${String(value)}`);
}

function normalizeNamespaces(name: string, response: unknown): NamespacesResult {
  const plain = toPlain(response);
  if (plain && typeof plain === "object" && !Array.isArray(plain)) {
    const namespaces = Array.isArray((plain as Record<string, unknown>).namespaces)
      ? ((plain as Record<string, unknown>).namespaces as unknown[])
      : [];
    return {
      name,
      namespaces: namespaces.map((item) => ({ id: namespaceId(item) })),
      nextCursor: ((plain as Record<string, unknown>).next_cursor as string | undefined) ?? null,
    };
  }
  const items = Array.isArray(plain) ? plain : [];
  return {
    name,
    namespaces: items.map((item) => ({ id: namespaceId(item) })),
    nextCursor: null,
  };
}

async function deleteBatches(
  namespaceHandle: ReturnType<Turbopuffer["namespace"]>,
  step: WriteStep,
  results: Record<string, AnyObject>,
): Promise<WriteResult> {
  const payload = { ...step.payload } as Record<string, unknown>;
  const sourceName = String(payload.delete_rows_from);
  const batchSize = Number(payload.delete_batch_size ?? 256);
  const deleteRows = ((results[sourceName] as Record<string, unknown> | undefined)?.rows ?? []) as AnyObject[];
  const deleteIds = deleteRows
    .map((row) => row.id)
    .filter((value) => value !== undefined && value !== null)
    .map((value) => String(value));

  if (deleteIds.length === 0) {
    return {
      name: step.name,
      status: "OK",
      message: "no rows matched delete request",
      rows_affected: 0,
      rows_deleted: 0,
      deleted_ids: [],
      writes: [],
    };
  }

  const writePayload = Object.fromEntries(
    Object.entries(payload).filter(([key]) => key !== "delete_rows_from" && key !== "delete_batch_size"),
  );

  const writes: WriteResult[] = [];
  for (let index = 0; index < deleteIds.length; index += batchSize) {
    const batch = deleteIds.slice(index, index + batchSize);
    const response = await namespaceHandle.write({ ...writePayload, deletes: batch });
    writes.push(normalizeWriteResult(step.name, response));
  }

  return {
    name: step.name,
    status: "OK",
    message: "ok",
    rows_affected: writes.reduce((total, item) => total + Number(item.rows_affected ?? 0), 0),
    rows_deleted: writes.reduce((total, item) => total + Number(item.rows_deleted ?? 0), 0),
    deleted_ids: writes.flatMap((item) => (Array.isArray(item.deleted_ids) ? item.deleted_ids : [])),
    writes,
  };
}

async function paginateOrderedQuery(
  namespaceHandle: ReturnType<Turbopuffer["namespace"]>,
  step: QueryStep,
): Promise<QueryResult> {
  const payload = { ...step.payload } as Record<string, unknown>;
  const pageSize = Number(step.pageSize ?? 256);
  const limit = step.limit === null || step.limit === undefined ? null : Number(step.limit);
  const orderField = String(step.orderField ?? "path");
  let lastValue: string | null = null;
  let remaining = limit;
  const rows: AnyObject[] = [];
  const pages: QueryPage[] = [];

  while (true) {
    const currentPayload: Record<string, unknown> = { ...payload };
    currentPayload.filters = withAfterFilter(payload.filters ?? null, orderField, lastValue);
    currentPayload.limit = remaining === null ? pageSize : Math.min(pageSize, remaining);

    let response: unknown;
    try {
      response = await namespaceHandle.query(currentPayload as never);
    } catch (error) {
      if ((error as { name?: string }).name === "NotFoundError") {
        break;
      }
      throw error;
    }
    const page = normalizeQueryResult(step.name, response);
    pages.push(page as QueryPage);
    const pageRows = page.rows;
    if (pageRows.length === 0) {
      break;
    }
    rows.push(...pageRows);
    if (remaining !== null) {
      remaining -= pageRows.length;
      if (remaining <= 0) {
        break;
      }
    }
    if (pageRows.length < Number(currentPayload.limit)) {
      break;
    }
    lastValue = String(pageRows[pageRows.length - 1]?.[orderField]);
  }

  return {
    name: step.name,
    rows,
    pages,
    pageCount: pages.length,
  };
}

export async function runStep(
  client: Turbopuffer,
  namespaceHandle: ReturnType<Turbopuffer["namespace"]> | null,
  step: PlanStep,
  context: Record<string, unknown>,
  results: ExecuteResults,
): Promise<AnyObject> {
  const kind = step.kind;
  if (kind === "query") {
    if (!namespaceHandle) {
      throw new Error("query step requires a namespace handle");
    }
    if (step.paginate) {
      return paginateOrderedQuery(namespaceHandle, step);
    }
    try {
      const response = await namespaceHandle.query(step.payload as never);
      return normalizeQueryResult(step.name, response);
    } catch (error) {
      if ((error as { name?: string }).name === "NotFoundError") {
        return normalizeQueryResult(step.name, { rows: [] });
      }
      throw error;
    }
  }
  if (kind === "write") {
    if (!namespaceHandle) {
      throw new Error("write step requires a namespace handle");
    }
    if ("delete_rows_from" in step.payload) {
      return deleteBatches(namespaceHandle, step as WriteStep, results);
    }
    const response = await namespaceHandle.write(step.payload as never);
    return normalizeWriteResult(step.name, response);
  }
  if (kind === "namespaces") {
    const response = await client.namespaces((step.payload ?? {}) as never);
    return normalizeNamespaces(step.name, response);
  }
  if (kind === "assert") {
    runCheck(String(step.check), context as Record<string, unknown>, results as ExecuteResults);
    return { name: step.name, status: "ok" };
  }
  throw new Error(`unsupported plan step kind: ${String(kind)}`);
}

export async function executePlan(client: Turbopuffer, plan: Plan): Promise<ExecuteResult> {
  const context = { ...(plan.context ?? {}) } as Record<string, unknown>;
  const results = {} as ExecuteResults;
  const needsNamespace = plan.steps.some((step) => step.kind === "query" || step.kind === "write");
  const namespaceHandle = needsNamespace ? client.namespace(plan.namespace) : null;
  for (const step of plan.steps) {
    const result = (await runStep(client, namespaceHandle, step, context, results)) as ExecuteResults[string];
    results[step.name] = result;
  }
  return { plan, results };
}

export async function finalizePlan(plan: Plan, executed: ExecuteResult): Promise<unknown> {
  const finalizer = FINALIZERS[plan.finalize as keyof typeof FINALIZERS];
  return finalizer({ ...(plan.context ?? {}) }, executed.results as never);
}

export async function run(client: Turbopuffer, plan: Plan): Promise<unknown> {
  const executed = await executePlan(client, plan);
  return finalizePlan(plan, executed);
}
