import { afterAll, describe, expect, it } from "vitest";

import { mountNamespace, readText } from "@workspace/turbopuffer-fs";

import { createBootContext } from "../src/boot.js";
import { runShellCommand } from "../src/shell.js";

const enabled =
  process.env.TURBOPUFFER_FS_LIVE === "1" &&
  Boolean(process.env.TURBOPUFFER_API_KEY) &&
  Boolean(process.env.TURBOPUFFER_REGION);

const describeLive = enabled ? describe : describe.skip;

describeLive("tpfs-shell live", () => {
  const mount = `shelllive${Math.random().toString(16).slice(2, 10)}`;
  const namespace = mountNamespace(mount);

  afterAll(async () => {
    if (!enabled) return;
    const { makeClient } = await import("@workspace/turbopuffer-fs");
    const client = makeClient({
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
    });
    try {
      await client.namespace(namespace).deleteAll();
    } catch {
      // ignore cleanup failure
    }
  });

  it("persists cwd and logs across shell restarts", async () => {
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
});
