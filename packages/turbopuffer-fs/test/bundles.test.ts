import { join } from "node:path";
import { describe, expect, it } from "vitest";

import {
  bundleEntrypoint,
  bundleTaskPrompt,
  bundleWorkspaceConfig,
  listAllowedOutputs,
  loadBundleSpec,
  validateBundleOutputs,
} from "../src/bundles.js";
import { FakeClient, FakeNamespace, FakeQueryResponse } from "./fakes.js";

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

  it("validates missing bundle outputs", async () => {
    const spec = await loadBundleSpec(bundlePath("csv-cleaning-v1"));
    const expectedOutputs = listAllowedOutputs(spec);
    const client = new FakeClient({
      namespaces: {
        documents__fs: new FakeNamespace("documents__fs", {
          queryResponses: expectedOutputs.map(() => new FakeQueryResponse({ rows: [] })),
        }),
      },
    });

    await expect(validateBundleOutputs(client as never, "documents", spec)).resolves.toEqual(expectedOutputs);
  });
});
