import { createBootContext, runShellCommand } from "@workspace/tpfs-shell";

type RunnerInput = {
  mount: string;
  apiKey?: string;
  region?: string;
  baseURL?: string;
  commands: string[];
};

type CommandResult = {
  command: string;
  exitCode: number;
  stdout: string;
  stderr: string;
  cwdAfter: string;
};

type RunnerOutput = {
  mount: string;
  logPath: string;
  finalCwd: string;
  results: CommandResult[];
};

function readInput(): RunnerInput {
  const raw = process.env.TPFS_RUNNER_INPUT;
  if (!raw) {
    throw new Error("TPFS_RUNNER_INPUT is required");
  }
  const parsed = JSON.parse(raw) as Partial<RunnerInput>;
  if (typeof parsed.mount !== "string" || parsed.mount.length === 0) {
    throw new Error("TPFS_RUNNER_INPUT.mount is required");
  }
  if (!Array.isArray(parsed.commands) || parsed.commands.some((value) => typeof value !== "string")) {
    throw new Error("TPFS_RUNNER_INPUT.commands must be an array of strings");
  }
  return {
    mount: parsed.mount,
    apiKey: parsed.apiKey,
    region: parsed.region,
    baseURL: parsed.baseURL,
    commands: parsed.commands,
  };
}

async function main(): Promise<void> {
  const input = readInput();
  const context = await createBootContext({
    mount: input.mount,
    apiKey: input.apiKey,
    region: input.region,
    baseURL: input.baseURL,
  });

  const results: CommandResult[] = [];
  for (const command of input.commands) {
    const result = await runShellCommand(context, command);
    results.push({
      command,
      exitCode: result.exitCode,
      stdout: result.stdout,
      stderr: result.stderr,
      cwdAfter: String(context.session.cwd),
    });
  }

  const output: RunnerOutput = {
    mount: input.mount,
    logPath: context.logPath,
    finalCwd: String(context.session.cwd),
    results,
  };
  const json = JSON.stringify(output, null, 2);
  process.stdout.write(`${json}\n`);
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.stack ?? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
