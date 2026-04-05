import { describe, expect, it } from "vitest";

import { executePlan, paginateOrderedQuery, toPlain } from "../src/runtime.js";
import type { QueryResult } from "../src/types.js";
import {
  FakeClient,
  FakeNamespace,
  FakeNamespaceList,
  FakeQueryResponse,
  FakeWriteResponse,
  ModelLike,
  NotFoundError,
} from "./fakes.js";

describe("runtime", () => {
  it("converts model-like values to plain objects", () => {
    expect(toPlain(new ModelLike({ a: [1, 2] }))).toEqual({ a: [1, 2] });
  });

  it("paginates ordered queries with after filter", async () => {
    const namespace = new FakeNamespace("docs__fs", {
      queryResponses: [
        new FakeQueryResponse({ rows: [{ path: "/a" }, { path: "/b" }] }),
        new FakeQueryResponse({ rows: [{ path: "/c" }] }),
      ],
    });

    const result = await paginateOrderedQuery(namespace as never, {
      kind: "query",
      name: "matches",
      payload: { filters: ["kind", "Eq", "file"], rank_by: ["path", "asc"] },
      paginate: true,
      pageSize: 2,
    });

    expect(result.rows.map((row: Record<string, unknown>) => row.path)).toEqual(["/a", "/b", "/c"]);
    expect(namespace.queryCalls[1]?.filters).toEqual(["And", [["kind", "Eq", "file"], ["path", "Gt", "/b"]]]);
  });

  it("normalizes query, write, and namespaces results", async () => {
    const client = new FakeClient({
      namespaces: {
        docs__fs: new FakeNamespace("docs__fs", {
          queryResponses: [new FakeQueryResponse({ rows: [{ path: "/a", kind: "file" }] })],
          writeResponses: [new FakeWriteResponse({ status: "OK", rows_affected: 1, message: "ok" })],
        }),
      },
      namespaceLists: [new FakeNamespaceList([{ id: "docs__fs" }, { id: "logs" }])],
    });

    const executed = await executePlan(client as never, {
      namespace: "docs__fs",
      finalize: "write_summary",
      context: {},
      steps: [
        {
          kind: "query",
          name: "target",
          payload: { filters: ["path", "Eq", "/a"], limit: 1 },
        },
        {
          kind: "write",
          name: "write",
          payload: { deletes: ["id-1"] },
        },
        {
          kind: "namespaces",
          name: "ns",
          payload: {},
        },
      ],
    });

    expect((executed.results.target as QueryResult).rows[0]?.path).toBe("/a");
    expect(executed.results.write.status).toBe("OK");
    expect((executed.results.ns as { namespaces: Array<{ id: string }> }).namespaces).toEqual([
      { id: "docs__fs" },
      { id: "logs" },
    ]);
  });

  it("batches delete_rows_from writes", async () => {
    const namespace = new FakeNamespace("docs__fs", {
      queryResponses: [new FakeQueryResponse({ rows: [{ id: "id-1" }, { id: "id-2" }, { id: "id-3" }] })],
    });
    const client = new FakeClient({
      namespaces: {
        docs__fs: namespace,
      },
    });

    const executed = await executePlan(client as never, {
      namespace: "docs__fs",
      finalize: "write_summary",
      context: {},
      steps: [
        {
          kind: "query",
          name: "delete_targets",
          payload: { filters: ["path", "Glob", "/notes/**"], rank_by: ["path", "asc"] },
        },
        {
          kind: "write",
          name: "write",
          payload: {
            delete_rows_from: "delete_targets",
            delete_batch_size: 2,
            return_affected_ids: true,
          },
        },
      ],
    });

    expect(namespace.writeCalls).toEqual([
      { deletes: ["id-1", "id-2"], return_affected_ids: true },
      { deletes: ["id-3"], return_affected_ids: true },
    ]);
    expect(executed.results.write.deleted_ids).toEqual(["id-1", "id-2", "id-3"]);
  });

  it("treats missing namespace reads as empty", async () => {
    const client = new FakeClient({
      namespaces: {
        docs__fs: new FakeNamespace("docs__fs", {
          queryResponses: [() => {
            throw new NotFoundError();
          }],
        }),
      },
    });

    const executed = await executePlan(client as never, {
      namespace: "docs__fs",
      finalize: "stat",
      context: {},
      steps: [
        {
          kind: "query",
          name: "target",
          payload: { filters: ["path", "Eq", "/a"], limit: 1 },
        },
      ],
    });

    expect(executed.results.target.rows).toEqual([]);
  });
});
