import { afterAll, describe, expect, it } from "vitest";
import { join } from "node:path";

import {
  loadBundleSpec,
  makeClient,
  mountNamespace,
  readText,
  seedBundle,
  validateBundleOutputs,
} from "@workspace/turbopuffer-fs";

import { createBootContext } from "../src/boot.js";
import { runShellCommand } from "../src/shell.js";

const enabled =
  process.env.TURBOPUFFER_FS_LIVE === "1" &&
  Boolean(process.env.TURBOPUFFER_API_KEY) &&
  Boolean(process.env.TURBOPUFFER_REGION);

const describeLive = enabled ? describe : describe.skip;

describeLive("tpfs-shell task bundle harness", () => {
  const mount = `bundlelive${Math.random().toString(16).slice(2, 10)}`;
  const namespace = mountNamespace(mount);
  const bundleRoot = join("/workspace/examples/task-bundles", "csv-cleaning-v1");

  afterAll(async () => {
    if (!enabled) return;
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

  it("seeds a bundle, runs shell commands, and validates outputs", async () => {
    const client = makeClient({
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
    });
    const spec = await loadBundleSpec(bundleRoot);

    const seedSummary = await seedBundle(client, mount, bundleRoot);
    expect(seedSummary.entrypoint).toBe("/TASK.md");
    expect(seedSummary.allowedOutputs).toContain("/output/sales.cleaned.csv");

    const context = await createBootContext({
      mount,
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
      bundleSpec: spec,
    });

    const pwd = await runShellCommand(context, "pwd");
    expect(pwd.stdout.trim()).toBe("/project");

    const taskText = await readText(context.client, mount, "/TASK.md");
    expect(taskText).toContain("Clean `/data/dirty_sales.csv`.");

    await runShellCommand(context, "cp /data/dirty_sales.csv /output/sales.cleaned.csv");
    await runShellCommand(context, "echo '# Cleaning report' > /output/cleaning_report.md");
    await runShellCommand(context, "echo 'done' > /logs/summary.md");

    const missing = await validateBundleOutputs(context.client, mount, spec);
    expect(missing).toEqual([]);

    const cleaned = await readText(context.client, mount, "/output/sales.cleaned.csv");
    expect(cleaned).toContain("Order ID");

    const summary = await readText(context.client, mount, "/logs/summary.md");
    expect(summary).toContain("done");
  }, 60000);
});
