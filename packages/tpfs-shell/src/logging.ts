import { readText, stat, putText } from "@workspace/turbopuffer-fs";
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
  const existingRow = await stat(client, mount, logPath);
  if (existingRow === null) {
    await putText(client, mount, logPath, line, { mime: "application/jsonl" });
    return;
  }
  try {
    const existing = (await readText(client, mount, logPath)) as string;
    await putText(client, mount, logPath, `${existing}${line}`, {
      mime: "application/jsonl",
    });
  } catch (error) {
    throw new Error(`failed to append durable command log at ${logPath}: ${String((error as Error).message)}`);
  }
}
