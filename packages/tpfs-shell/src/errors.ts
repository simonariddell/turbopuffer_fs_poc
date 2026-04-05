export type TpfsErrorCode =
  | "TPFS_UNSUPPORTED_BY_DESIGN"
  | "TPFS_NOT_YET_IMPLEMENTED"
  | "TPFS_INVALID_OPERATION";

export interface TpfsErrorOptions {
  code: TpfsErrorCode;
  operation: string;
  reason: string;
  alternatives?: string[];
  specSections?: string[];
}

function formatSections(sections: string[] | undefined): string {
  if (!sections || sections.length === 0) {
    return "SPEC.tpfs.md";
  }
  return `SPEC.tpfs.md ${sections.join(", ")}`;
}

function formatAlternatives(alternatives: string[] | undefined): string {
  if (!alternatives || alternatives.length === 0) {
    return "";
  }
  return ` Alternatives: ${alternatives.join("; ")}.`;
}

function formatMessage(options: TpfsErrorOptions): string {
  const supportText =
    options.code === "TPFS_UNSUPPORTED_BY_DESIGN"
      ? "unsupported by design"
      : options.code === "TPFS_NOT_YET_IMPLEMENTED"
        ? "not yet implemented"
        : "invalid under tpfs semantics";
  return [
    `${options.code}: ${options.operation} is ${supportText}.`,
    "tpfs is a turbopuffer-backed, durable, non-POSIX-complete filesystem-shaped runtime.",
    options.reason,
    `See ${formatSections(options.specSections)}.`,
    formatAlternatives(options.alternatives),
  ]
    .filter((part) => part.length > 0)
    .join(" ");
}

export class TpfsSpecError extends Error {
  readonly code: TpfsErrorCode;
  readonly operation: string;
  readonly alternatives: string[];
  readonly specSections: string[];

  constructor(options: TpfsErrorOptions) {
    super(formatMessage(options));
    this.name = "TpfsSpecError";
    this.code = options.code;
    this.operation = options.operation;
    this.alternatives = [...(options.alternatives ?? [])];
    this.specSections = [...(options.specSections ?? [])];
  }
}

export function unsupportedByDesign(
  operation: string,
  reason: string,
  options: {
    alternatives?: string[];
    specSections?: string[];
  } = {},
): TpfsSpecError {
  return new TpfsSpecError({
    code: "TPFS_UNSUPPORTED_BY_DESIGN",
    operation,
    reason,
    alternatives: options.alternatives,
    specSections: options.specSections ?? ["§12", "§13"],
  });
}

export function notYetImplemented(
  operation: string,
  reason: string,
  options: {
    alternatives?: string[];
    specSections?: string[];
  } = {},
): TpfsSpecError {
  return new TpfsSpecError({
    code: "TPFS_NOT_YET_IMPLEMENTED",
    operation,
    reason,
    alternatives: options.alternatives,
    specSections: options.specSections ?? ["§12"],
  });
}

export function invalidTpfsOperation(
  operation: string,
  reason: string,
  options: {
    alternatives?: string[];
    specSections?: string[];
  } = {},
): TpfsSpecError {
  return new TpfsSpecError({
    code: "TPFS_INVALID_OPERATION",
    operation,
    reason,
    alternatives: options.alternatives,
    specSections: options.specSections ?? ["§8", "§13", "§14"],
  });
}
