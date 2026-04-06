import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { Daytona } from "@daytonaio/sdk";

type CliOptions = {
  mount: string;
  sandboxName?: string;
  image?: string;
  keepSandbox: boolean;
  commands: string[];
};

function parseArgs(argv: string[]): CliOptions {
  let mount = process.env.TPFS_MOUNT ?? "";
  let sandboxName: string | undefined;
  let image: string | undefined;
  let keepSandbox = false;
  const commands: string[] = [];

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token) {
      continue;
    }
    if (token === "--mount") {
      mount = argv[index + 1] ?? "";
      index += 1;
      continue;
    }
    if (token === "--sandbox-name") {
      sandboxName = argv[index + 1];
      index += 1;
      continue;
    }
    if (token === "--image") {
      image = argv[index + 1];
      index += 1;
      continue;
    }
    if (token === "--keep-sandbox") {
      keepSandbox = true;
      continue;
    }
    commands.push(token);
  }

  if (!mount) {
    throw new Error(
      "missing mount; pass --mount <name> or set TPFS_MOUNT",
    );
  }
  if (commands.length === 0) {
    throw new Error(
      "missing commands; pass one or more shell commands as trailing arguments",
    );
  }

  return {
    mount,
    sandboxName,
    image,
    keepSandbox,
    commands,
  };
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`missing required environment variable ${name}`);
  }
  return value;
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const scriptDir = dirname(fileURLToPath(import.meta.url));
  const runnerPath = join(scriptDir, "runner.js");
  const runnerCode = await readFile(runnerPath, "utf8");

  const runnerInput = {
    mount: options.mount,
    apiKey: requiredEnv("TURBOPUFFER_API_KEY"),
    region: requiredEnv("TURBOPUFFER_REGION"),
    baseURL: process.env.TURBOPUFFER_BASE_URL,
    commands: options.commands,
  };

  const sandboxLabelSuffix = Math.random().toString(16).slice(2, 10);
  const sandboxName = options.sandboxName ?? `tpfs-${options.mount}-${sandboxLabelSuffix}`;

  const daytona = new Daytona();
  const sandbox = options.image
    ? await daytona.create(
        {
          name: sandboxName,
          language: "javascript",
          image: options.image,
          envVars: { TPFS_MOUNT: options.mount },
        },
        { timeout: 120 },
      )
    : await daytona.create(
        {
          name: sandboxName,
          language: "javascript",
          envVars: { TPFS_MOUNT: options.mount },
        },
        { timeout: 120 },
      );

  try {
    console.log(`Created sandbox ${sandbox.name} (${sandbox.id})`);

    const response = await sandbox.process.codeRun(
      runnerCode,
      {
        env: {
          TPFS_RUNNER_INPUT: JSON.stringify(runnerInput),
        },
      },
      120,
    );

    process.stdout.write(response.result);
    if (response.exitCode !== 0) {
      throw new Error(`runner exited with code ${response.exitCode}`);
    }
  } finally {
    if (options.keepSandbox) {
      console.log(`Kept sandbox ${sandbox.name} (${sandbox.id}) alive for inspection`);
    } else {
      await sandbox.delete(120);
      console.log(`Deleted sandbox ${sandbox.name} (${sandbox.id})`);
    }
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.stack ?? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
