import { readFile as readFileNode, writeFile as writeFileNode } from "node:fs/promises";

import {
  bundleConfig,
  bundleEntrypoint,
  bundleTaskPrompt,
  listAllowedOutputs,
  loadBundleSpec,
  seedBundle,
  validateBundleOutputs,
} from "./bundles.js";
import { runDogfood } from "./dogfood.js";
import {
  cat,
  exists,
  find,
  grep,
  ingestDirectory,
  listMounts,
  ls,
  makeClient,
  mkdir,
  putBytes,
  putText,
  readBytes,
  readText,
  rm,
  search,
  stat,
} from "./live.js";
import {
  archiveWorkspace as archiveWorkspaceCore,
  loadSessionState,
  resolveWorkspaceConfig,
  resolveCliPath,
  workspaceExists as workspaceExistsCore,
  workspaceInit,
  workspaceShow as workspaceShowCore,
  saveSessionState,
  deleteWorkspace as deleteWorkspaceCore,
} from "./workspace.js";

export interface CliIO {
  stdout(text: string): void;
  stderr(text: string): void;
  stdinText(): Promise<string>;
  stdinBytes(): Promise<Uint8Array>;
}

export const defaultCliIO: CliIO = {
  stdout: (text) => process.stdout.write(text),
  stderr: (text) => process.stderr.write(text),
  stdinText: async () => {
    const chunks: Buffer[] = [];
    for await (const chunk of process.stdin) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks).toString("utf8");
  },
  stdinBytes: async () => {
    const chunks: Buffer[] = [];
    for await (const chunk of process.stdin) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return new Uint8Array(Buffer.concat(chunks));
  },
};

type CommonOptions = {
  apiKey?: string;
  region?: string;
  baseURL?: string;
  workspaceConfigPath?: string;
};

type ParsedArgs = CommonOptions & {
  command: string;
  positionals: string[];
  flags: Record<string, string | boolean>;
};

function jsonDump(value: unknown): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}

function parseArgs(argv: string[]): ParsedArgs {
  const flags: Record<string, string | boolean> = {};
  const positionals: string[] = [];
  let command: string | null = null;
  const common: CommonOptions = {};

  const consumeValue = (index: number, flag: string): [string, number] => {
    const value = argv[index + 1];
    if (!value) {
      throw new Error(`missing value for ${flag}`);
    }
    return [value, index + 1];
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index]!;
    if (token === "--api-key") {
      [common.apiKey, index] = consumeValue(index, token);
      continue;
    }
    if (token === "--region") {
      [common.region, index] = consumeValue(index, token);
      continue;
    }
    if (token === "--base-url") {
      [common.baseURL, index] = consumeValue(index, token);
      continue;
    }
    if (token === "--workspace-config") {
      [common.workspaceConfigPath, index] = consumeValue(index, token);
      continue;
    }
    if (token.startsWith("--")) {
      const key = token.slice(2);
      const next = argv[index + 1];
      if (next && !next.startsWith("--")) {
        flags[key] = next;
        index += 1;
      } else {
        flags[key] = true;
      }
      continue;
    }
    if (command === null) {
      command = token;
      continue;
    }
    positionals.push(token);
  }

  if (!command) {
    throw new Error("missing command");
  }

  return { ...common, command, positionals, flags };
}

function clientFromParsed(args: ParsedArgs) {
  return makeClient({
    apiKey: args.apiKey,
    region: args.region,
    baseURL: args.baseURL,
  });
}

async function loadTextInput(parsed: ParsedArgs, io: CliIO): Promise<string> {
  if (typeof parsed.flags.text === "string") {
    return parsed.flags.text;
  }
  if (typeof parsed.flags.file === "string") {
    return readFileNode(parsed.flags.file, "utf8");
  }
  if (parsed.flags.stdin) {
    return io.stdinText();
  }
  throw new Error("expected one of --text, --file, or --stdin");
}

async function loadBytesInput(parsed: ParsedArgs, io: CliIO): Promise<Uint8Array> {
  if (typeof parsed.flags.file === "string") {
    return new Uint8Array(await readFileNode(parsed.flags.file));
  }
  if (parsed.flags.stdin) {
    return io.stdinBytes();
  }
  throw new Error("expected one of --file or --stdin");
}

export async function workspaceShow(
  client: ReturnType<typeof makeClient>,
  mount: string,
  workspaceConfig: ReturnType<typeof resolveWorkspaceConfig>,
) {
  return workspaceShowCore(client, mount, { workspaceConfig });
}

export async function workspaceExists(
  client: ReturnType<typeof makeClient>,
  mount: string,
  workspaceConfig: ReturnType<typeof resolveWorkspaceConfig>,
) {
  return { mount, exists: await workspaceExistsCore(client, mount, { workspaceConfig }) };
}

export async function workspacePwd(
  client: ReturnType<typeof makeClient>,
  mount: string,
  workspaceConfig: ReturnType<typeof resolveWorkspaceConfig>,
) {
  const state = await loadSessionState(client, mount, { workspaceConfig });
  return String(state.cwd);
}

export async function workspaceCd(
  client: ReturnType<typeof makeClient>,
  mount: string,
  targetPath: string,
  workspaceConfig: ReturnType<typeof resolveWorkspaceConfig>,
) {
  const cwd = await workspacePwd(client, mount, workspaceConfig);
  const resolved = resolveCliPath(targetPath, { cwd });
  const target = (await stat(client, mount, resolved)) as Record<string, unknown> | null;
  if (target === null) {
    throw new Error(`FileNotFoundError:${resolved}`);
  }
  if (String(target.kind) !== "dir") {
    throw new Error(`NotADirectoryError:${resolved}`);
  }
  const saved = await saveSessionState(
    client,
    mount,
    { cwd: resolved, mount },
    { workspaceConfig },
  );
  return {
    cwd: saved.cwd,
    mount,
  };
}

export async function workspaceDelete(
  client: ReturnType<typeof makeClient> & { namespace(namespace: string): { deleteAll(): Promise<void> } },
  mount: string,
) {
  return deleteWorkspaceCore(client, mount);
}

export async function workspaceArchive(
  client: ReturnType<typeof makeClient>,
  mount: string,
  workspaceConfig: ReturnType<typeof resolveWorkspaceConfig>,
) {
  return archiveWorkspaceCore(client, mount, { workspaceConfig });
}

export async function runCli(argv: string[], io: CliIO = defaultCliIO): Promise<number> {
  let parsed: ParsedArgs;
  try {
    parsed = parseArgs(argv);
  } catch (error) {
    io.stderr(jsonDump({ error: "ParseError", message: String((error as Error).message) }));
    return 2;
  }

  const client = clientFromParsed(parsed);
  const bundleRoot = typeof parsed.flags["bundle-root"] === "string" ? parsed.flags["bundle-root"] : undefined;
  const workspaceConfig = resolveWorkspaceConfig({
    deploymentConfig: parsed.workspaceConfigPath ? JSON.parse(await readFileNode(parsed.workspaceConfigPath, "utf8")) as Record<string, unknown> : undefined,
    bundleSpec: bundleRoot ? await loadBundleSpec(bundleRoot) : undefined,
  });

  try {
    let result: unknown;
    switch (parsed.command) {
      case "mounts":
        result = await listMounts(client);
        break;
      case "workspace-show": {
        const mount = parsed.positionals[0] ?? "documents";
        result = await workspaceShow(client, mount, workspaceConfig);
        break;
      }
      case "workspace-exists": {
        const mount = parsed.positionals[0] ?? "documents";
        result = await workspaceExists(client, mount, workspaceConfig);
        break;
      }
      case "workspace-init": {
        const mount = parsed.positionals[0] ?? "documents";
        result = await workspaceInit(client, mount, {
          workspaceConfig,
          workspaceKind: typeof parsed.flags.kind === "string" ? parsed.flags.kind : undefined,
          bundleId: typeof parsed.flags["bundle-id"] === "string" ? parsed.flags["bundle-id"] : undefined,
          ownerId: typeof parsed.flags["owner-id"] === "string" ? parsed.flags["owner-id"] : undefined,
          sourceId: typeof parsed.flags["source-id"] === "string" ? parsed.flags["source-id"] : undefined,
          workItemId: typeof parsed.flags["work-item-id"] === "string" ? parsed.flags["work-item-id"] : undefined,
          taskId: typeof parsed.flags["task-id"] === "string" ? parsed.flags["task-id"] : undefined,
          tags:
            typeof parsed.flags.tag === "string"
              ? String(parsed.flags.tag).split(",").map((tag) => tag.trim()).filter(Boolean)
              : undefined,
        });
        break;
      }
      case "workspace-delete": {
        const mount = parsed.positionals[0] ?? "documents";
        result = await workspaceDelete(client as ReturnType<typeof makeClient> & {
          namespace(namespace: string): { deleteAll(): Promise<void> };
        }, mount);
        break;
      }
      case "workspace-archive": {
        const mount = parsed.positionals[0] ?? "documents";
        result = await workspaceArchive(client, mount, workspaceConfig);
        break;
      }
      case "pwd": {
        const mount = parsed.positionals[0] ?? "documents";
        result = { cwd: await workspacePwd(client, mount, workspaceConfig), mount };
        break;
      }
      case "cd": {
        const mount = parsed.positionals[0] ?? "documents";
        const targetPath = parsed.positionals[1];
        if (!targetPath) throw new Error("cd requires a path");
        result = await workspaceCd(client, mount, targetPath, workspaceConfig);
        break;
      }
      case "stat": {
        const [mount, path] = parsed.positionals;
        if (!mount || !path) throw new Error("stat requires <mount> <path>");
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        result = await stat(client, mount, resolveCliPath(path, { cwd }));
        break;
      }
      case "exists": {
        const [mount, path] = parsed.positionals;
        if (!mount || !path) throw new Error("exists requires <mount> <path>");
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        const resolved = resolveCliPath(path, { cwd });
        result = { path: resolved, exists: await exists(client, mount, resolved) };
        break;
      }
      case "ls": {
        const mount = parsed.positionals[0];
        if (!mount) throw new Error("ls requires <mount>");
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        const path = parsed.positionals[1] ?? ".";
        const limit = typeof parsed.flags.limit === "string" ? Number(parsed.flags.limit) : undefined;
        result = await ls(client, mount, resolveCliPath(path, { cwd }), limit);
        break;
      }
      case "find": {
        const mount = parsed.positionals[0];
        if (!mount) throw new Error("find requires <mount>");
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        const root = parsed.positionals[1] ?? ".";
        result = await find(client, mount, resolveCliPath(root, { cwd }), {
          glob: typeof parsed.flags.glob === "string" ? parsed.flags.glob : undefined,
          kind: typeof parsed.flags.kind === "string" ? (parsed.flags.kind as "file" | "dir") : undefined,
          ignoreCase: Boolean(parsed.flags["ignore-case"]),
          limit: typeof parsed.flags.limit === "string" ? Number(parsed.flags.limit) : undefined,
        });
        break;
      }
      case "cat":
      case "read-text": {
        const [mount, path] = parsed.positionals;
        if (!mount || !path) throw new Error(`${parsed.command} requires <mount> <path>`);
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        const resolved = resolveCliPath(path, { cwd });
        result = parsed.command === "cat"
          ? await cat(client, mount, resolved)
          : await readText(client, mount, resolved);
        break;
      }
      case "read-bytes": {
        const [mount, path] = parsed.positionals;
        if (!mount || !path) throw new Error("read-bytes requires <mount> <path>");
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        const resolved = resolveCliPath(path, { cwd });
        const data = (await readBytes(client, mount, resolved)) as Uint8Array;
        if (typeof parsed.flags.out === "string") {
          await writeFileNode(parsed.flags.out, Buffer.from(data));
          result = { path: resolved, out: parsed.flags.out, bytesWritten: data.length };
        } else {
          result = { path: resolved, sizeBytes: data.length, blobB64: Buffer.from(data).toString("base64") };
        }
        break;
      }
      case "grep":
      case "search": {
        const [mount, root, rawPattern] = parsed.positionals;
        if (!mount || !root) throw new Error(`${parsed.command} requires <mount> <root> <pattern-or-query>`);
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        const mode = typeof parsed.flags.mode === "string" ? parsed.flags.mode : "literal";
        const pattern = rawPattern ?? (typeof parsed.flags.query === "string" ? parsed.flags.query : undefined);
        if (!pattern) throw new Error(`${parsed.command} requires a pattern or query`);
        const runner = parsed.command === "search" ? search : grep;
        result = await runner(client, mount, resolveCliPath(root, { cwd }), pattern, {
          mode: mode as "literal" | "regex" | "bm25",
          glob: typeof parsed.flags.glob === "string" ? parsed.flags.glob : undefined,
          ignoreCase: Boolean(parsed.flags["ignore-case"]),
          limit: typeof parsed.flags.limit === "string" ? Number(parsed.flags.limit) : undefined,
          multiline: Boolean(parsed.flags.multiline),
          dotAll: Boolean(parsed.flags.dotall),
          lastAsPrefix: Boolean(parsed.flags["last-as-prefix"]),
        });
        break;
      }
      case "mkdir": {
        const [mount, path] = parsed.positionals;
        if (!mount || !path) throw new Error("mkdir requires <mount> <path>");
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        result = await mkdir(client, mount, resolveCliPath(path, { cwd }));
        break;
      }
      case "put-text": {
        const [mount, path] = parsed.positionals;
        if (!mount || !path) throw new Error("put-text requires <mount> <path>");
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        result = await putText(
          client,
          mount,
          resolveCliPath(path, { cwd }),
          await loadTextInput(parsed, io),
          { mime: typeof parsed.flags.mime === "string" ? parsed.flags.mime : undefined },
        );
        break;
      }
      case "put-bytes": {
        const [mount, path] = parsed.positionals;
        if (!mount || !path) throw new Error("put-bytes requires <mount> <path>");
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        result = await putBytes(
          client,
          mount,
          resolveCliPath(path, { cwd }),
          await loadBytesInput(parsed, io),
          { mime: typeof parsed.flags.mime === "string" ? parsed.flags.mime : undefined },
        );
        break;
      }
      case "rm": {
        const [mount, path] = parsed.positionals;
        if (!mount || !path) throw new Error("rm requires <mount> <path>");
        const cwd = await workspacePwd(client, mount, workspaceConfig);
        result = await rm(client, mount, resolveCliPath(path, { cwd }), Boolean(parsed.flags.recursive));
        break;
      }
      case "ingest": {
        const [mount, localRoot] = parsed.positionals;
        if (!mount || !localRoot) throw new Error("ingest requires <mount> <local_root>");
        result = await ingestDirectory(client, mount, localRoot, {
          mountRoot: typeof parsed.flags["mount-root"] === "string" ? parsed.flags["mount-root"] : "/",
          batchSize: typeof parsed.flags["batch-size"] === "string" ? Number(parsed.flags["batch-size"]) : 256,
        });
        break;
      }
      case "bundle-show": {
        const [localRoot] = parsed.positionals;
        if (!localRoot) throw new Error("bundle-show requires <local_root>");
        const spec = await loadBundleSpec(localRoot);
        result = {
          spec,
          entrypoint: bundleEntrypoint(spec),
          allowedOutputs: listAllowedOutputs(spec),
          workspace: bundleConfig(spec),
        };
        break;
      }
      case "bundle-seed": {
        const [mount, localRoot] = parsed.positionals;
        if (!mount || !localRoot) throw new Error("bundle-seed requires <mount> <local_root>");
        result = await seedBundle(client, mount, localRoot, {
          mountRoot: typeof parsed.flags["mount-root"] === "string" ? parsed.flags["mount-root"] : "/",
        });
        break;
      }
      case "bundle-validate": {
        const [mount, localRoot] = parsed.positionals;
        if (!mount || !localRoot) throw new Error("bundle-validate requires <mount> <local_root>");
        const spec = await loadBundleSpec(localRoot);
        const missing = await validateBundleOutputs(client, mount, spec);
        result = { missing, ok: missing.length === 0 };
        break;
      }
      case "bundle-prompt": {
        const [localRoot] = parsed.positionals;
        if (!localRoot) throw new Error("bundle-prompt requires <local_root>");
        result = { prompt: await bundleTaskPrompt(localRoot) };
        break;
      }
      case "dogfood":
        result = await runDogfood({
          apiKey: parsed.apiKey,
          region: parsed.region,
          baseURL: parsed.baseURL,
          mountPrefix: typeof parsed.flags["mount-prefix"] === "string" ? parsed.flags["mount-prefix"] : "dogfood",
          seed: typeof parsed.flags.seed === "string" ? Number(parsed.flags.seed) : 1,
          steps: typeof parsed.flags.steps === "string" ? Number(parsed.flags.steps) : 50,
          checkEvery: typeof parsed.flags["check-every"] === "string" ? Number(parsed.flags["check-every"]) : 5,
          keepOnFail: Boolean(parsed.flags["keep-on-fail"]),
          keepAlways: Boolean(parsed.flags["keep-always"]),
          cleanup: !Boolean(parsed.flags["no-cleanup"]),
        });
        break;
      default:
        throw new Error(`unsupported command: ${parsed.command}`);
    }

    io.stdout(jsonDump(result));
    return 0;
  } catch (error) {
    io.stderr(jsonDump({ error: (error as Error).name, message: String((error as Error).message) }));
    return 1;
  }
}

export async function main(argv = process.argv.slice(2), io: CliIO = defaultCliIO): Promise<number> {
  return runCli(argv, io);
}

if (process.argv[1] && process.argv[1].endsWith("cli.js")) {
  void main().then((code) => {
    process.exitCode = code;
  });
}
