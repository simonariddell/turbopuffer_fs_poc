export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type AnyObject = Record<string, unknown>;
export type RowLike = AnyObject;
export type Row = RowLike;
export type FsRow = RowLike;
export type FsSchema = Record<string, unknown>;

export type GrepMode = "literal" | "regex" | "bm25";

export type GrepStrategy =
  | "regex_remote_candidates_then_local_finalize"
  | "regex_scope_only_then_local_finalize"
  | "literal_substring_candidates_then_local_finalize"
  | "bm25_ranked_direct";

export interface GrepOptions {
  mode?: GrepMode;
  glob?: string | null;
  limit?: number | null;
  ignoreCase?: boolean;
  multiline?: boolean;
  dotAll?: boolean;
  lastAsPrefix?: boolean;
}

export interface GrepRequest {
  namespace: string;
  root: string;
  pattern: string;
  options: GrepOptions;
}

export interface GrepCandidateQueryPlan extends AnyObject {
  name: string;
  payload: AnyObject;
  paginate?: boolean;
  limit?: number | null;
  pageSize?: number;
  orderField?: string;
}

export interface GrepScopePlan extends AnyObject {
  namespace: string;
  normalizedRoot: string;
  filters: AnyObject;
  whereText: string;
}

export interface GrepPlanArtifact extends AnyObject {
  request: GrepRequest;
  mode: GrepMode;
  strategy: GrepStrategy;
  scope: GrepScopePlan;
  candidateQuery: GrepCandidateQueryPlan;
  candidateQueryText: string;
  finalize: string;
  finalizeMode: "exact_lines" | "ranked_hits";
  context: AnyObject;
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

export type GrepPlannerStrategy =
  | "regex_scope_then_exact_lines"
  | "literal_prefilter_then_exact_lines"
  | "bm25_ranked_direct";

export type GrepFinalizationMode =
  | "exact_regex_lines"
  | "exact_literal_lines"
  | "ranked_bm25_hits";

export interface GrepFollowupQuery {
  name: string;
  payload: AnyObject;
  payloadText: string;
}

export interface GrepPlannerStage {
  strategy: GrepPlannerStrategy;
  candidateQuery: AnyObject;
  candidateQueryText: string;
  followupQueries: GrepFollowupQuery[];
  finalization: GrepFinalizationMode;
}

export interface GrepPlannerArtifact {
  request: {
    root: string;
    pattern: string;
    mode: GrepMode;
    glob: string | null;
    limit: number;
    ignoreCase: boolean;
    multiline: boolean;
    dotAll: boolean;
    lastAsPrefix: boolean;
  };
  plan: Plan;
  stage: GrepPlannerStage;
  planText: string;
}

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
