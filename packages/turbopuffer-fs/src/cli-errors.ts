import type { AnyObject } from "./types.js";

export interface CliErrorShape extends AnyObject {
  error: string;
  code: string;
  message: string;
  details?: Record<string, unknown>;
}

function codeAndDetailsFromKnownMessage(message: string): Pick<CliErrorShape, "error" | "code" | "details"> {
  if (message.startsWith("FileNotFoundError:")) {
    const path = message.slice("FileNotFoundError:".length);
    return {
      error: "FileNotFoundError",
      code: "FileNotFoundError",
      details: { path },
    };
  }
  if (message.startsWith("IsADirectoryError:")) {
    const path = message.slice("IsADirectoryError:".length);
    return {
      error: "IsADirectoryError",
      code: "IsADirectoryError",
      details: { path },
    };
  }
  if (message.startsWith("NotADirectoryError:")) {
    const path = message.slice("NotADirectoryError:".length);
    return {
      error: "NotADirectoryError",
      code: "NotADirectoryError",
      details: { path },
    };
  }
  if (message.startsWith("ReplaceTextNoMatchError:")) {
    const path = message.slice("ReplaceTextNoMatchError:".length);
    return {
      error: "ReplaceTextNoMatchError",
      code: "ReplaceTextNoMatchError",
      details: { path },
    };
  }
  if (message.startsWith("ReplaceTextMatchCountError:")) {
    const [, remainder] = message.split(":", 2);
    const [path, tail] = remainder.split(":expected ");
    const [expectedRaw, foundRaw] = tail.split(",found ");
    return {
      error: "ReplaceTextMatchCountError",
      code: "ReplaceTextMatchCountError",
      details: {
        path,
        expected_matches: Number(expectedRaw),
        found_matches: Number(foundRaw),
      },
    };
  }
  if (message.startsWith("NonTextFileError:")) {
    const path = message.slice("NonTextFileError:".length);
    return {
      error: "NonTextFileError",
      code: "NonTextFileError",
      details: { path },
    };
  }
  return {
    error: "Error",
    code: "UnknownError",
  };
}

export function normalizeCliError(error: unknown): CliErrorShape {
  if (error && typeof error === "object") {
    const known = error as Record<string, unknown>;
    if (
      typeof known.error === "string" &&
      typeof known.code === "string" &&
      typeof known.message === "string"
    ) {
      return {
        error: known.error,
        code: known.code,
        message: known.message,
        details:
          known.details && typeof known.details === "object"
            ? (known.details as Record<string, unknown>)
            : undefined,
      };
    }
  }
  const message = error instanceof Error ? String(error.message) : String(error);
  const known = codeAndDetailsFromKnownMessage(message);
  return {
    error: known.error,
    code: known.code,
    message,
    details: known.details,
  };
}

export function cliErrorEnvelope(
  error: unknown,
  options: { defaultCode?: string } = {},
): { error: CliErrorShape } {
  const normalized = normalizeCliError(error);
  if (normalized.code === "UnknownError" && options.defaultCode) {
    return {
      error: {
        ...normalized,
        code: options.defaultCode,
      },
    };
  }
  return { error: normalized };
}

export function parseBooleanFlag(value: string | boolean | undefined): boolean {
  if (value === true) {
    return true;
  }
  if (typeof value !== "string") {
    return false;
  }
  const normalized = value.trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}
