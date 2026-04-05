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

export function fileNotFound(path: string, operation: string): TpfsSpecError {
  return invalidTpfsOperation(
    operation,
    `The required path does not exist in the durable tpfs namespace: ${path}.`,
    {
      alternatives: [
        "Create the path durably before retrying",
        "Use exists/stat first when the path may be absent",
      ],
      specSections: ["§8", "§11", "§14"],
    },
  );
}

export function notADirectory(path: string, operation: string): TpfsSpecError {
  return invalidTpfsOperation(
    operation,
    `The path is not a directory in the durable tpfs model: ${path}.`,
    {
      alternatives: [
        "Target a directory path",
        "Use stat to inspect kind before calling directory-only operations",
      ],
      specSections: ["§8", "§13", "§14"],
    },
  );
}

export function isADirectory(path: string, operation: string): TpfsSpecError {
  return invalidTpfsOperation(
    operation,
    `The path is a directory where a file operation was requested: ${path}.`,
    {
      alternatives: [
        "Use a file target instead",
        "Use directory-aware operations for directory nodes",
      ],
      specSections: ["§8", "§13", "§14"],
    },
  );
}

export function directoryNotEmpty(path: string, operation: string): TpfsSpecError {
  return invalidTpfsOperation(
    operation,
    `The directory contains durable child entries and the requested operation requires an empty directory: ${path}.`,
    {
      alternatives: [
        "Retry with recursive semantics when appropriate",
        "Remove or move child entries first",
      ],
      specSections: ["§7.4", "§8.12", "§14"],
    },
  );
}
