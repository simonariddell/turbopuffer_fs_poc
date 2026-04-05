import { describe, expect, it } from "vitest";

import {
  applyModelOperation,
  expectedGrepMatches,
  modelFind,
  modelLs,
  newModelState,
} from "../src/dogfood.js";

describe("dogfood model", () => {
  it("models mkdir and ls", () => {
    const state = newModelState();
    applyModelOperation(state, { op: "mkdir", path: "/notes" });
    applyModelOperation(state, { op: "mkdir", path: "/notes/archive" });
    expect(modelLs(state, "/notes").map((row) => row.path)).toEqual(["/notes/archive"]);
  });

  it("models text and bytes files", () => {
    const state = newModelState();
    applyModelOperation(state, { op: "put_text", path: "/notes/todo.txt", text: "hello\nworld\n" });
    applyModelOperation(state, { op: "put_bytes", path: "/bin/data.bin", data: Uint8Array.from([0, 1]) });
    expect(modelFind(state, "/").map((row) => row.path)).toEqual([
      "/",
      "/bin",
      "/bin/data.bin",
      "/notes",
      "/notes/todo.txt",
    ]);
  });

  it("matches grep expectations", () => {
    const state = newModelState();
    applyModelOperation(state, {
      op: "put_text",
      path: "/notes/a.txt",
      text: "oauth token\nother\nOAuth done\n",
    });
    expect(expectedGrepMatches(state, { root: "/", pattern: "oauth", ignoreCase: true })).toEqual([
      { path: "/notes/a.txt", line_number: 1, line: "oauth token" },
      { path: "/notes/a.txt", line_number: 3, line: "OAuth done" },
    ]);
  });

  it("models cp and mv for files and directories", () => {
    const state = newModelState();
    applyModelOperation(state, { op: "put_text", path: "/notes/a.txt", text: "hello\n" });
    applyModelOperation(state, { op: "mkdir", path: "/notes/archive" });

    applyModelOperation(state, { op: "cp", path: "/notes/a.txt", dest: "/notes/b.txt", recursive: false });
    applyModelOperation(state, { op: "mv", path: "/notes/archive", dest: "/notes/renamed" });

    expect(modelFind(state, "/").map((row) => row.path)).toEqual([
      "/",
      "/notes",
      "/notes/a.txt",
      "/notes/b.txt",
      "/notes/renamed",
    ]);
  });

  it("rejects non-recursive non-empty directory delete", () => {
    const state = newModelState();
    applyModelOperation(state, { op: "put_text", path: "/notes/a.txt", text: "hello\n" });
    expect(() => applyModelOperation(state, { op: "rm", path: "/notes", recursive: false })).toThrowError(
      "DirectoryNotEmptyError:/notes",
    );
  });

  it("rejects invalid directory copies", () => {
    const state = newModelState();
    applyModelOperation(state, { op: "mkdir", path: "/notes" });

    expect(() =>
      applyModelOperation(state, { op: "cp", path: "/notes", dest: "/notes/archive", recursive: true }),
    ).toThrowError("InvalidCopyTarget:/notes->/notes/archive");
  });
});
