import { readText, putText } from "../../turbopuffer-fs/src/index.js";
import type Turbopuffer from "@turbopuffer/turbopuffer";

export type CommandLogEntry = {
  timestamp: string;
  command: string;
  cwd_before: string;
  cwd_after: string;
  exit_code: number;
  stdout_preview: string;
  stderr_preview: string;
};

export async function appendCommandLog(
  client: Turbopuffer,
  mount: string,
  logPath: string,
  entry: CommandLogEntry,
): Promise<void> {
  const line = `${JSON.stringify(entry)}\n`;
  try {
    const existing = (await readText(client, mount, logPath)) as string;
    await putText(client, mount, logPath, `${existing}${line}`, {
      mime: "application/jsonl",
    });
  } catch {
    await putText(client, mount, logPath, line, { mime: "application/jsonl" });
  }
}
