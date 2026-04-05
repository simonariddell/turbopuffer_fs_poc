import { describe, expect, it } from "vitest";

import {
  findPlan,
  grepPlan,
  lsPlan,
  mkdirPlan,
  putBytesPlan,
  putTextPlan,
  readBytesPlan,
  readTextPlan,
  rmPlan,
  statPlan,
} from "../src/plans.js";
import type { QueryStep, WriteStep } from "../src/types.js";

const asQueryStep = <T extends { kind: string }>(step: T): QueryStep => step as unknown as QueryStep;
const asWriteStep = <T extends { kind: string }>(step: T): WriteStep => step as unknown as WriteStep;

describe("plans", () => {
  it("builds stat plan shape", () => {
    const plan = statPlan("documents__fs", "/notes/todo.txt");
    const step = asQueryStep(plan.steps[0]!);
    expect(plan.namespace).toBe("documents__fs");
    expect(plan.finalize).toBe("stat");
    expect(step.kind).toBe("query");
    expect(step.payload.filters).toEqual(["path", "Eq", "/notes/todo.txt"]);
  });

  it("builds ls plan with target and paginated children", () => {
    const plan = lsPlan("documents__fs", "/notes", 50);
    const children = asQueryStep(plan.steps[1]!);
    expect(plan.steps.map((step) => step.name)).toEqual(["target", "children"]);
    expect(children).toMatchObject({
      paginate: true,
      limit: 50,
    });
    expect(children.payload.filters).toEqual(["parent", "Eq", "/notes"]);
  });

  it("builds find plan with subtree, glob, kind, and limit", () => {
    const plan = findPlan("documents__fs", "/notes", {
      glob: "*.md",
      kind: "file",
      ignoreCase: true,
      limit: 10,
    });
    const matches = asQueryStep(plan.steps[1]!);
    const filters = matches.payload.filters as [string, unknown[]];
    expect(plan.finalize).toBe("find");
    expect(filters[0]).toBe("And");
    expect(filters[1]).toContainEqual(["kind", "Eq", "file"]);
    expect(filters[1]).toContainEqual(["basename", "IGlob", "*.md"]);
    expect(matches).toMatchObject({
      paginate: true,
      limit: 10,
    });
  });

  it("builds grep plan with coarse text filter", () => {
    const plan = grepPlan("documents__fs", "/notes", "oauth", {
      ignoreCase: true,
      glob: "*.md",
    });
    const candidates = asQueryStep(plan.steps[1]!);
    const filters = (candidates.payload.filters as [string, unknown[]])[1];
    expect(filters).toContainEqual(["kind", "Eq", "file"]);
    expect(filters).toContainEqual(["is_text", "Eq", 1]);
    expect(filters).toContainEqual(["basename", "IGlob", "*.md"]);
    expect(filters).toContainEqual(["text", "IGlob", "*oauth*"]);
  });

  it("builds regex grep plans without coarse text substring filters", () => {
    const plan = grepPlan("documents__fs", "/notes", "oauth.*token", {
      mode: "regex",
      ignoreCase: true,
      glob: "*.md",
      limit: 25,
    });
    const candidates = asQueryStep(plan.steps[1]!);
    const filters = (candidates.payload.filters as [string, unknown[]])[1];
    expect(plan.finalize).toBe("grep_regex");
    expect(filters).toContainEqual(["kind", "Eq", "file"]);
    expect(filters).toContainEqual(["is_text", "Eq", 1]);
    expect(filters).toContainEqual(["basename", "IGlob", "*.md"]);
    expect(filters).not.toContainEqual(["text", "IGlob", "*oauth.*token*"]);
    expect(candidates).toMatchObject({
      paginate: true,
      limit: 25,
    });
  });

  it("builds bm25 grep plans with ranked top-k queries", () => {
    const plan = grepPlan("documents__fs", "/notes", "oauth token", {
      mode: "bm25",
      limit: 7,
      lastAsPrefix: true,
    });
    const candidates = asQueryStep(plan.steps[1]!);
    expect(plan.finalize).toBe("grep_bm25");
    expect(candidates.payload.top_k).toBe(7);
    expect(candidates.payload.rank_by).toEqual(["text", "BM25", "oauth token", { last_as_prefix: true }]);
    expect(candidates.payload.include_attributes).toEqual(["path", "text"]);
  });

  it("builds read plans with correct finalizers", () => {
    expect(readTextPlan("documents__fs", "/a.txt").finalize).toBe("read_text");
    expect(readBytesPlan("documents__fs", "/a.txt").finalize).toBe("read_bytes");
  });

  it("builds mkdir plan with ancestor directory rows", () => {
    const plan = mkdirPlan("documents__fs", "/a/b");
    const existing = asQueryStep(plan.steps[0]!);
    const write = asWriteStep(plan.steps[2]!);
    expect(plan.steps.map((step) => step.kind)).toEqual(["query", "assert", "write"]);
    expect(existing.payload.limit).toBe(3);
    const rows = write.payload.upsert_rows as Array<{ path: string }>;
    expect(rows.map((row) => row.path)).toEqual(["/", "/a", "/a/b"]);
  });

  it("builds put text and bytes plans with parent rows", () => {
    const textPlan = putTextPlan("documents__fs", "/a/b.txt", "hello");
    const textRows = asWriteStep(textPlan.steps[2]!).payload.upsert_rows as Array<Record<string, unknown>>;
    expect(textRows.map((row) => row.path)).toEqual(["/", "/a", "/a/b.txt"]);
    expect(textRows.at(-1)?.text).toBe("hello");
    expect(textRows.at(-1)?.is_text).toBe(1);

    const bytesPlan = putBytesPlan("documents__fs", "/a/data.bin", Uint8Array.from([0, 1]));
    const target = (asWriteStep(bytesPlan.steps[2]!).payload.upsert_rows as Array<Record<string, unknown>>).at(-1);
    expect(target?.is_text).toBe(0);
    expect(target).toHaveProperty("blob_b64");
  });

  it("builds rm plans for recursive and non-recursive modes", () => {
    const nonRecursive = rmPlan("documents__fs", "/notes", false);
    expect(nonRecursive.steps.map((step) => step.name)).toEqual(["target", "child_probe", "validate", "write"]);
    expect(asWriteStep(nonRecursive.steps.at(-1)!).payload.delete_rows_from).toBe("target");

    const recursive = rmPlan("documents__fs", "/notes", true);
    expect(recursive.steps[1]?.name).toBe("delete_targets");
    expect(asWriteStep(recursive.steps.at(-1)!).payload.delete_rows_from).toBe("delete_targets");
  });
});
