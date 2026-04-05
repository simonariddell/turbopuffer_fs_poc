import { describe, expect, it } from "vitest";

import { runGrepEval, type GrepEvalDocument } from "../src/grep-eval.js";

const corpus: GrepEvalDocument[] = [
  { path: "/notes/auth.md", text: "oauth token exchange\ncallback success\n" },
  { path: "/notes/mixed.md", text: "OAuth done\nsecondary line\n" },
  { path: "/src/app.ts", text: "const token = 'oauth';\nexport const value = 1;\n" },
  { path: "/logs/app.log", text: "INFO auth start\nERROR oauth failure\n" },
  { path: "/bin/data.bin", bytes: Uint8Array.from([0, 1, 2]) },
];

describe("grep eval harness", () => {
  it("captures a literal grep baseline with exact line hits", () => {
    const result = runGrepEval(corpus, "/", "oauth", { mode: "literal", ignoreCase: true });

    expect(result.metrics.mode).toBe("literal");
    expect(result.metrics.candidateCount).toBeGreaterThan(0);
    expect(result.metrics.candidateTextBytes).toBeGreaterThan(0);
    expect(result.metrics.finalCount).toBe(4);
    expect(result.result).toEqual([
      { kind: "line_match", path: "/notes/auth.md", line_number: 1, line: "oauth token exchange" },
      { kind: "line_match", path: "/notes/mixed.md", line_number: 1, line: "OAuth done" },
      { kind: "line_match", path: "/src/app.ts", line_number: 1, line: "const token = 'oauth';" },
      { kind: "line_match", path: "/logs/app.log", line_number: 2, line: "ERROR oauth failure" },
    ]);
  });

  it("shows regex candidate expansion relative to literal grep", () => {
    const literal = runGrepEval(corpus, "/", "oauth", { mode: "literal", ignoreCase: true });
    const regex = runGrepEval(corpus, "/", "^oauth.*$", {
      mode: "regex",
      ignoreCase: true,
    });

    expect(regex.metrics.candidateCount).toBeGreaterThanOrEqual(literal.metrics.candidateCount);
    expect(regex.metrics.candidateTextBytes).toBeGreaterThanOrEqual(literal.metrics.candidateTextBytes);
    expect(regex.result).toEqual([
      { kind: "line_match", path: "/notes/auth.md", line_number: 1, line: "oauth token exchange" },
      { kind: "line_match", path: "/notes/mixed.md", line_number: 1, line: "OAuth done" },
    ]);
  });

  it("returns ranked bm25-style search hits", () => {
    const result = runGrepEval(corpus, "/", "oauth token", {
      mode: "bm25",
      ignoreCase: true,
      limit: 2,
      lastAsPrefix: true,
    });

    expect(result.metrics.mode).toBe("bm25");
    expect(result.metrics.candidateCount).toBe(2);
    expect(result.metrics.finalCount).toBe(2);
    expect(result.result).toEqual([
      expect.objectContaining({
        kind: "search_hit",
        mode: "bm25",
        path: "/notes/auth.md",
        score: 2,
      }),
      expect.objectContaining({
        kind: "search_hit",
        mode: "bm25",
        path: "/src/app.ts",
        score: 2,
      }),
    ]);
  });
});
