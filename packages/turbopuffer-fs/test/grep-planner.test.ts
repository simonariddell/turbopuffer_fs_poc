import { describe, expect, it } from "vitest";

import { planGrepQuery } from "../src/grep-planner.js";

describe("grep planner", () => {
  it("defaults to regex semantics and produces a stable textual plan", () => {
    const planned = planGrepQuery("documents__fs", "/project", "oauth.*token", {
      glob: "*.md",
      ignoreCase: true,
    });

    expect(planned.request.mode).toBe("regex");
    expect(planned.stage.strategy).toBe("regex_scope_then_exact_lines");
    expect(planned.stage.candidateQueryText).toContain('"filters"');
    expect(planned.planText).toContain("mode=regex");
    expect(planned.planText).toContain("strategy=regex_scope_then_exact_lines");
    expect(planned.planText).toContain("followup:candidate_text_fetch_template");
  });

  it("uses literal prefilter strategy explicitly", () => {
    const planned = planGrepQuery("documents__fs", "/notes", "oauth token", {
      mode: "literal",
      ignoreCase: true,
      glob: "*.txt",
      limit: 25,
    });

    expect(planned.request.mode).toBe("literal");
    expect(planned.stage.strategy).toBe("literal_prefilter_then_exact_lines");
    expect(planned.stage.candidateQueryText).toContain('"IGlob"');
    expect(planned.stage.candidateQueryText).toContain('"*oauth token*"');
    expect(planned.stage.finalization).toBe("exact_literal_lines");
  });

  it("uses ranked direct bm25 strategy explicitly", () => {
    const planned = planGrepQuery("documents__fs", "/", "oauth token", {
      mode: "bm25",
      lastAsPrefix: true,
      limit: 7,
    });

    expect(planned.stage.strategy).toBe("bm25_ranked_direct");
    expect(planned.stage.candidateQueryText).toContain('"BM25"');
    expect(planned.stage.candidateQueryText).toContain('"last_as_prefix"');
    expect(planned.stage.followupQueries).toEqual([]);
    expect(planned.stage.finalization).toBe("ranked_bm25_hits");
  });
});
