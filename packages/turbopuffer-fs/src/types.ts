export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type AnyObject = Record<string, unknown>;
export type RowLike = AnyObject;
export type Row = RowLike;
export type FsRow = RowLike;
export type FsSchema = Record<string, unknown>;

export type QueryStep = {
  kind: "query";
  name: string;
  payload: AnyObject;
  paginate?: boolean;
  limit?: number | null;
  pageSize?: number;
  orderField?: string;
};

export type WriteStep = {
  kind: "write";
  name: string;
  payload: AnyObject;
};

export type AssertStep = {
  kind: "assert";
  name: string;
  check: string;
};

export type NamespacesStep = {
  kind: "namespaces";
  name: string;
  payload?: AnyObject;
};

export type PlanStep = QueryStep | WriteStep | AssertStep | NamespacesStep;

export type Plan = {
  namespace: string;
  steps: PlanStep[];
  finalize: string;
  context: AnyObject;
};

export type QueryResult = {
  name: string;
  rows: RowLike[];
  billing?: unknown;
  performance?: unknown;
  aggregations?: unknown;
  aggregationGroups?: unknown;
  pages?: QueryPage[];
  pageCount?: number;
};

export type QueryPage = QueryResult;

export type WriteResult = AnyObject & {
  name: string;
};

export type NamespacesResult = {
  name: string;
  namespaces: { id: string }[];
  nextCursor?: string | null;
};

export type StepResult = QueryResult | WriteResult | NamespacesResult | { name: string; status: string };
export type ExecuteResults = Record<string, StepResult & AnyObject>;

export type ExecuteResult = {
  plan: Plan;
  results: ExecuteResults;
};
