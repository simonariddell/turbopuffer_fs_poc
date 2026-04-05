export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type AnyObject = Record<string, unknown>;
export type RowLike = AnyObject;
export type Row = RowLike;
export type FsRow = RowLike;
export type FsSchema = Record<string, unknown>;

export type GrepMode = "literal" | "regex" | "bm25";

export interface GrepOptions {
  mode?: GrepMode;
  glob?: string | null;
  limit?: number | null;
  ignoreCase?: boolean;
  multiline?: boolean;
  dotAll?: boolean;
  lastAsPrefix?: boolean;
}

export interface GrepLineMatch extends AnyObject {
  kind: "line_match";
  path: string;
  line_number: number;
  line: string;
}

export interface GrepSearchHit extends AnyObject {
  kind: "search_hit";
  mode: "bm25";
  path: string;
  score: number;
  snippet?: string;
}

export type GrepResult = GrepLineMatch[] | GrepSearchHit[];

export interface QueryStep {
  kind: "query";
  name: string;
  payload: AnyObject;
  paginate?: boolean;
  limit?: number | null;
  pageSize?: number;
  orderField?: string;
}

export interface WriteStep {
  kind: "write";
  name: string;
  payload: AnyObject;
}

export interface AssertStep {
  kind: "assert";
  name: string;
  check: string;
}

export interface NamespacesStep {
  kind: "namespaces";
  name: string;
  payload?: AnyObject;
}

export type PlanStep = QueryStep | WriteStep | AssertStep | NamespacesStep;

export interface Plan {
  namespace: string;
  steps: PlanStep[];
  finalize: string;
  context: AnyObject;
}

export interface QueryResult {
  name: string;
  rows: RowLike[];
  billing?: unknown;
  performance?: unknown;
  aggregations?: unknown;
  aggregationGroups?: unknown;
  pages?: QueryPage[];
  pageCount?: number;
  [key: string]: unknown;
}

export type QueryPage = QueryResult;

export type WriteResult = AnyObject & { name: string };

export interface NamespacesResult {
  name: string;
  namespaces: { id: string }[];
  nextCursor?: string | null;
  [key: string]: unknown;
}

export interface StatusResult {
  name: string;
  status: string;
  [key: string]: unknown;
}

export type StepResult = QueryResult | WriteResult | NamespacesResult | StatusResult;
export type ExecuteResults = Record<string, StepResult>;

export interface ExecuteResult {
  plan: Plan;
  results: ExecuteResults;
}
