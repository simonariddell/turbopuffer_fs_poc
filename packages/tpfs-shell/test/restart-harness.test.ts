import { afterAll, describe, expect, it } from "vitest";

import {
  loadSessionState,
  mountNamespace,
  readBytes,
  readText,
  stat,
} from "@workspace/turbopuffer-fs";

import { createBootContext } from "../src/boot.js";
import { runShellCommand } from "../src/shell.js";

const enabled =
  process.env.TURBOPUFFER_FS_LIVE === "1" &&
  Boolean(process.env.TURBOPUFFER_API_KEY) &&
  Boolean(process.env.TURBOPUFFER_REGION);

const describeLive = enabled ? describe : describe.skip;

type RestartScenario = {
  name: string;
  mount: string;
  beforeRestart: string[];
  afterRestart?: string[];
  verify: (mount: string) => Promise<void>;
};

describeLive("tpfs restart harness", () => {
  const mounts = [
    `restarth${Math.random().toString(16).slice(2, 10)}`,
    `restarth${Math.random().toString(16).slice(2, 10)}`,
    `restarth${Math.random().toString(16).slice(2, 10)}`,
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

  async function runScenario(scenario: RestartScenario): Promise<void> {
    const first = await createBootContext({
      mount: scenario.mount,
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
    });

    for (const command of scenario.beforeRestart) {
      const result = await runShellCommand(first, command);
      expect(result.exitCode).toBe(0);
    }

    const second = await createBootContext({
      mount: scenario.mount,
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
    });

    for (const command of scenario.afterRestart ?? []) {
      const result = await runShellCommand(second, command);
      expect(result.exitCode).toBe(0);
    }

    await scenario.verify(scenario.mount);
  }

  const scenarios: RestartScenario[] = [
    {
      name: "cwd and appended text survive restart",
      mount: mounts[0]!,
      beforeRestart: [
        "mkdir work",
        "cd work",
        "echo first > notes.txt",
        "echo second >> notes.txt",
      ],
      verify: async (mount) => {
        const { makeClient } = await import("@workspace/turbopuffer-fs");
        const client = makeClient({
          apiKey: process.env.TURBOPUFFER_API_KEY,
          region: process.env.TURBOPUFFER_REGION,
          baseURL: process.env.TURBOPUFFER_BASE_URL,
        });
        const session = await loadSessionState(client, mount, {
          workspaceConfig: {
            entrypoint: "/TASK.md",
            bundle_manifest: "/bundle.json",
            session_state: "/state/session.json",
            logs_dir: "/logs",
            output_dir: "/output",
            scratch_dir: "/scratch",
            project_dir: "/project",
            input_dir: "/input",
          },
        });
        expect(session.cwd).toBe("/project/work");
        const text = await readText(client, mount, "/project/work/notes.txt");
        expect(String(text)).toBe("first\nsecond\n");
      },
    },
    {
      name: "copy then move survives restart",
      mount: mounts[1]!,
      beforeRestart: [
        "mkdir docs",
        "echo hello > docs/source.txt",
        "cp docs/source.txt docs/copied.txt",
        "mv docs/copied.txt docs/archive.txt",
      ],
      verify: async (mount) => {
        const { makeClient } = await import("@workspace/turbopuffer-fs");
        const client = makeClient({
          apiKey: process.env.TURBOPUFFER_API_KEY,
          region: process.env.TURBOPUFFER_REGION,
          baseURL: process.env.TURBOPUFFER_BASE_URL,
        });
        expect(await readText(client, mount, "/project/docs/source.txt")).toBe("hello\n");
        expect(await readText(client, mount, "/project/docs/archive.txt")).toBe("hello\n");
        const copied = await stat(client, mount, "/project/docs/copied.txt");
        expect(copied).toBeNull();
      },
    },
    {
      name: "binary writes and recursive delete survive restart",
      mount: mounts[2]!,
      beforeRestart: [
        "mkdir assets",
        "printf '\\x41\\x42\\x43' > assets/blob.bin",
        "mkdir assets/sub",
        "echo temp > assets/sub/temp.txt",
        "rm -r assets/sub",
      ],
      verify: async (mount) => {
        const { makeClient } = await import("@workspace/turbopuffer-fs");
        const client = makeClient({
          apiKey: process.env.TURBOPUFFER_API_KEY,
          region: process.env.TURBOPUFFER_REGION,
          baseURL: process.env.TURBOPUFFER_BASE_URL,
        });
        const bytes = await readBytes(client, mount, "/project/assets/blob.bin");
        expect(Array.from(bytes as Uint8Array)).toEqual([65, 66, 67]);
        const deleted = await stat(client, mount, "/project/assets/sub");
        expect(deleted).toBeNull();
      },
    },
  ];

  for (const scenario of scenarios) {
    it(scenario.name, async () => {
      await runScenario(scenario);
    }, 30000);
  }
});
