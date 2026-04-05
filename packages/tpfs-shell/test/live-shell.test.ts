import { afterAll, describe, expect, it } from "vitest";

import { loadSessionState, mountNamespace, readText } from "@workspace/turbopuffer-fs";

import { createBootContext } from "../src/boot.js";
import { runShellCommand } from "../src/shell.js";

const enabled =
  process.env.TURBOPUFFER_FS_LIVE === "1" &&
  Boolean(process.env.TURBOPUFFER_API_KEY) &&
  Boolean(process.env.TURBOPUFFER_REGION);

const describeLive = enabled ? describe : describe.skip;

describeLive("tpfs-shell live", () => {
  const mounts = [
    `shelllive${Math.random().toString(16).slice(2, 10)}`,
    `shelllive${Math.random().toString(16).slice(2, 10)}`,
    `shelllive${Math.random().toString(16).slice(2, 10)}`,
  ];
  const namespaces = mounts.map((mount) => mountNamespace(mount));

  afterAll(async () => {
    if (!enabled) return;
    const { makeClient } = await import("@workspace/turbopuffer-fs");
    const client = makeClient({
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
    });
    for (const namespace of namespaces) {
      try {
        await client.namespace(namespace).deleteAll();
      } catch {
        // ignore cleanup failure
      }
    }
  });

  it("persists cwd and logs across shell restarts", async () => {
    const mount = mounts[0]!;
    const first = await createBootContext({
      mount,
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
    });

    const pwd0 = await runShellCommand(first, "pwd");
    expect(pwd0.stdout.trim()).toBe("/project");

    await runShellCommand(first, "mkdir notes");
    await runShellCommand(first, "cd notes");
    const pwd1 = await runShellCommand(first, "pwd");
    expect(pwd1.stdout.trim()).toBe("/project/notes");

    const second = await createBootContext({
      mount,
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
    });

    const resumedPwd = await runShellCommand(second, "pwd");
    expect(resumedPwd.stdout.trim()).toBe("/project/notes");

    const logText = await readText(second.client, mount, second.logPath);
    expect(logText).toContain('"command":"pwd"');
    expect(logText).toContain('"command":"cd notes"');
    expect(logText).toContain('"command":"mkdir notes"');
  }, 20000);

  it("preserves bundle session metadata when cwd changes", async () => {
    const mount = mounts[1]!;
    const first = await createBootContext({
      mount,
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
      bundleSpec: { id: "bundle-123", workspace: { project_dir: "/project" } },
    });

    await runShellCommand(first, "mkdir bundle-dir");
    await runShellCommand(first, "cd bundle-dir");

    const persisted = await loadSessionState(first.client, mount, {
      workspaceConfig: first.workspaceConfig,
    });

    expect(persisted.cwd).toBe("/project/bundle-dir");
    expect(persisted.bundle_id).toBe("bundle-123");
  }, 20000);

  it("supports durable cp and mv through the shell adapter", async () => {
    const mount = mounts[2]!;
    const context = await createBootContext({
      mount,
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
    });

    await runShellCommand(context, "mkdir docs");
    await runShellCommand(context, "echo hello > docs/source.txt");
    await runShellCommand(context, "cp docs/source.txt docs/copied.txt");
    await runShellCommand(context, "mv docs/copied.txt docs/moved.txt");

    const copied = await readText(context.client, mount, "/project/docs/moved.txt");
    expect(String(copied).trim()).toBe("hello");

    const source = await readText(context.client, mount, "/project/docs/source.txt");
    expect(String(source).trim()).toBe("hello");
  }, 30000);
});
