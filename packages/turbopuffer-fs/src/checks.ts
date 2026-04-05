import type { AnyObject, ExecuteResults, RowLike } from "./types.js";

const rowsFor = (results: ExecuteResults, name: string): RowLike[] =>
  ((results[name] as Record<string, unknown> | undefined)?.rows as RowLike[] | undefined) ?? [];

const firstRow = (results: ExecuteResults, name: string): RowLike | null =>
  rowsFor(results, name)[0] ?? null;

const rowsByPath = (rows: RowLike[]): Record<string, RowLike> =>
  Object.fromEntries(rows.map((row) => [String(row.path), row]));

function checkMkdirPreconditions(
  context: Record<string, unknown>,
  results: ExecuteResults,
): void {
  const path = String(context.path);
  const existing = rowsByPath(rowsFor(results, "existing"));
  const parentPaths = (context.parentPaths as string[] | undefined) ?? [];
  for (const ancestor of parentPaths) {
    const row = existing[ancestor];
    if (row && row.kind !== "dir") {
      throw new Error(`NotADirectoryError:${ancestor}`);
    }
  }
  const target = existing[path];
  if (target && target.kind !== "dir") {
    throw new Error(`FileExistsError:${path}`);
  }
}

function checkPutPreconditions(
  context: Record<string, unknown>,
  results: ExecuteResults,
): void {
  const path = String(context.path);
  const existing = rowsByPath(rowsFor(results, "existing"));
  const parentPaths = (context.parentPaths as string[] | undefined) ?? [];
  for (const ancestor of parentPaths) {
    const row = existing[ancestor];
    if (row && row.kind !== "dir") {
      throw new Error(`NotADirectoryError:${ancestor}`);
    }
  }
  const target = existing[path];
  if (target && target.kind === "dir") {
    throw new Error(`IsADirectoryError:${path}`);
  }
}

function checkRmPreconditions(
  context: Record<string, unknown>,
  results: ExecuteResults,
): void {
  const path = String(context.path);
  const recursive = Boolean(context.recursive);
  const target = firstRow(results, "target");
  if (!target || recursive) {
    return;
  }
  if (target.kind === "dir" && rowsFor(results, "child_probe").length > 0) {
    throw new Error(`DirectoryNotEmptyError:${path}`);
  }
}

const CHECKS = {
  mkdir_preconditions: checkMkdirPreconditions,
  put_preconditions: checkPutPreconditions,
  rm_preconditions: checkRmPreconditions,
} as const;

export function runCheck(
  name: string,
  context: Record<string, unknown>,
  results: ExecuteResults,
): void {
  const check = CHECKS[name as keyof typeof CHECKS];
  if (!check) {
    throw new Error(`UnknownCheck:${name}`);
  }
  check(context, results);
}
