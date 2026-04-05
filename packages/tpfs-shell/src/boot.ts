import type Turbopuffer from "@turbopuffer/turbopuffer";

import {
  loadSessionState,
  makeClient,
  resolveWorkspaceConfig,
  saveSessionState,
  type WorkspaceConfig,
  workspaceInit,
} from "../../turbopuffer-fs/src/index.js";

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
  logPath: string;
}

export async function createBootContext(options: ShellBootOptions): Promise<ShellBootContext> {
  const client = makeClient({
    apiKey: options.apiKey,
    region: options.region,
    baseURL: options.baseURL,
  });
  const workspaceConfig = resolveWorkspaceConfig({
    deploymentConfig: undefined,
    bundleSpec: options.bundleSpec ?? undefined,
  });
  let session = await loadSessionState(client, options.mount, { workspaceConfig });
  if (!session.path) {
    const initialized = await workspaceInit(client, options.mount, { workspaceConfig });
    session = initialized.session as Record<string, unknown>;
  }
  const fs = new TpufFsAdapter({
    client,
    mount: options.mount,
    cwdProvider: async () => String((await loadSessionState(client, options.mount, { workspaceConfig })).cwd),
  });
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
        { cwd, mount: options.mount },
        { workspaceConfig },
      );
      session = { ...session, cwd: saved.cwd };
    },
    logPath: `${workspaceConfig.logs_dir}/run.jsonl`,
  };
}

export const bootShell = createBootContext;
