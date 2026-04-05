import { join } from "node:path";
import { describe, expect, it } from "vitest";

import {
  bundleEntrypoint,
  bundleTaskPrompt,
  bundleWorkspaceConfig,
  listAllowedOutputs,
  loadBundleSpec,
} from "../src/bundles.js";

const bundlePath = (name: string) => join("/workspace/examples/task-bundles", name);

describe("bundles", () => {
  it("loads a bundle spec", async () => {
    const spec = await loadBundleSpec(bundlePath("csv-cleaning-v1"));
    expect(spec.id).toBe("csv-cleaning-v1");
  });

  it("derives bundle helpers", async () => {
    const spec = await loadBundleSpec(bundlePath("code-maintenance-v1"));
    expect(bundleEntrypoint(spec)).toBe("/TASK.md");
    expect(listAllowedOutputs(spec)).toContain("/logs/run.jsonl");
    expect(bundleWorkspaceConfig(spec).session_state).toBe("/state/session.json");
  });

  it("renders a task prompt", async () => {
    const prompt = await bundleTaskPrompt(bundlePath("csv-analysis-v1"));
    expect(prompt).toContain("Allowed outputs:");
    expect(prompt).toContain("Bundle ID: csv-analysis-v1");
  });
});
