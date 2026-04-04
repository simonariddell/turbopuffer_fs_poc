import { describe, expect, it } from "vitest";

import {
  ancestorPaths,
  basename,
  extension,
  joinGlob,
  joinPath,
  normalizeGlobPath,
  normalizePath,
  pathId,
  scopedGlobFilter,
  subtreeFilter,
} from "../src/paths.js";

describe("paths", () => {
  it("normalizes root and repeated slashes", () => {
    expect(normalizePath("/")).toBe("/");
    expect(normalizePath("//a///b/")).toBe("/a/b");
  });

  it("preserves glob tokens for glob paths", () => {
    expect(normalizeGlobPath("/a/**/b/*.txt")).toBe("/a/**/b/*.txt");
  });

  it("computes basename, extension, and ancestors", () => {
    expect(basename("/a/b/c.txt")).toBe("c.txt");
    expect(extension("/a/b/c.txt")).toBe(".txt");
    expect(ancestorPaths("/a/b/c.txt", { includeSelf: false })).toEqual(["/", "/a", "/a/b"]);
    expect(ancestorPaths("/a/b/c.txt", { includeSelf: true })).toEqual(["/", "/a", "/a/b", "/a/b/c.txt"]);
  });

  it("joins paths and globs", () => {
    expect(joinPath("/a", "b/c.txt")).toBe("/a/b/c.txt");
    expect(joinGlob("/a", "**/*.txt")).toBe("/a/**/*.txt");
  });

  it("builds subtree and scoped glob filters", () => {
    expect(subtreeFilter("/")).toBeNull();
    expect(subtreeFilter("/a")).toEqual(["Or", [["path", "Eq", "/a"], ["path", "Glob", "/a/**"]]]);
    expect(scopedGlobFilter("/a", "*.txt")).toEqual(["basename", "Glob", "*.txt"]);
  });

  it("keeps path ids stable", () => {
    expect(pathId("/a/b")).toBe(pathId("/a//b/"));
  });
});
