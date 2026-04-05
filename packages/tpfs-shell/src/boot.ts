import { readFile } from "node:fs/promises";

import type Turbopuffer from "@turbopuffer/turbopuffer";
import {
  loadSessionState,
  makeClient,
  resolveWorkspaceConfig,
  saveSessionState,
  stat,
  type WorkspaceConfig,
  workspaceInit,
} from "@workspace/turbopuffer-fs";

import { TpufFsAdapter } from "./adapter.js";

export interface ShellBootOptions {
  mount: string;
  apiKey?: string;
  region?: string;
  baseURL?: string;
  workspaceConfigPath?: string;
  bundleSpec?: Record<string, unknown> | null;
}

export interface ShellBootContext {
  client: Turbopuffer;
  mount: string;
  workspaceConfig: WorkspaceConfig;
  session: Record<string, unknown>;
  fs: TpufFsAdapter;
  persistSession: (cwd: string) => Promise<void>;
  reloadSession: () => Promise<Record<string, unknown>>;
  logPath: string;
}

export async function createBootContext(options: ShellBootOptions): Promise<ShellBootContext> {
  const client = makeClient({
    apiKey: options.apiKey,
    region: options.region,
    baseURL: options.baseURL,
  });
  const deploymentConfig = options.workspaceConfigPath
    ? (JSON.parse(await readFile(options.workspaceConfigPath, "utf8")) as Record<string, unknown>)
    : undefined;
  const workspaceConfig = resolveWorkspaceConfig({
    deploymentConfig,
    bundleSpec: options.bundleSpec ?? undefined,
  });
  const sessionDoc = await stat(client, options.mount, workspaceConfig.session_state);
  let session: Record<string, unknown>;
  if (sessionDoc === null) {
    const initialized = await workspaceInit(client, options.mount, { workspaceConfig });
    session = initialized.session as Record<string, unknown>;
  } else {
    session = await loadSessionState(client, options.mount, { workspaceConfig });
  }
  const fs = new TpufFsAdapter({
    client,
    mount: options.mount,
    cwdProvider: async () => String(session.cwd),
  });

  const reloadSession = async (): Promise<Record<string, unknown>> => {
    session = await loadSessionState(client, options.mount, { workspaceConfig });
    return session;
  };

  return {
    client,
    mount: options.mount,
    workspaceConfig,
    session,
    fs,
    persistSession: async (cwd: string) => {
      const saved = await saveSessionState(
        client,
        options.mount,
        { ...session, cwd, mount: options.mount },
        { workspaceConfig },
      );
      session = { ...session, ...saved };
    },
    reloadSession,
    logPath: `${workspaceConfig.logs_dir}/run.jsonl`,
  };
}

export const bootShell = createBootContext;
