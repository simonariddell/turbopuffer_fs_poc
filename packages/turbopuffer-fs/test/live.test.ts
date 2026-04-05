import { afterAll, describe, expect, it } from "vitest";

import {
  find,
  grep,
  listMounts,
  ls,
  makeClient,
  mountNamespace,
  putBytes,
  putText,
  readBytes,
  readText,
  rm,
  stat,
} from "../src/live.js";
import { runDogfood } from "../src/dogfood.js";

const enabled =
  process.env.TURBOPUFFER_FS_LIVE === "1" &&
  Boolean(process.env.TURBOPUFFER_API_KEY) &&
  Boolean(process.env.TURBOPUFFER_REGION);

const describeLive = enabled ? describe : describe.skip;

describeLive("live turbopuffer", () => {
  const client = makeClient({
    apiKey: process.env.TURBOPUFFER_API_KEY,
    region: process.env.TURBOPUFFER_REGION,
    baseURL: process.env.TURBOPUFFER_BASE_URL,
  });
  const mount = `tslive${Math.random().toString(16).slice(2, 10)}`;
  const namespace = mountNamespace(mount);

  afterAll(async () => {
    try {
      await client.namespace(namespace).deleteAll();
    } catch {
      // ignore cleanup failures
    }
  });

  it("runs a round trip through the live wrapper", async () => {
    await putText(client, mount, "/notes/hello.txt", "hello\noauth token\n");
    await putBytes(client, mount, "/bin/data.bin", Uint8Array.from([0, 1, 2]));

    const mounts = await listMounts(client);
    expect(mounts).toContain(mount);

    const textStat = (await stat(client, mount, "/notes/hello.txt")) as Record<string, unknown>;
    expect(textStat.path).toBe("/notes/hello.txt");

    const children = (await ls(client, mount, "/")) as Array<Record<string, unknown>>;
    expect(new Set(children.map((row) => row.path))).toEqual(new Set(["/bin", "/notes"]));

    const matches = (await find(client, mount, "/notes", { glob: "*.txt" })) as Array<Record<string, unknown>>;
    expect(matches.map((row) => row.path)).toEqual(["/notes/hello.txt"]);

    expect(await readText(client, mount, "/notes/hello.txt")).toBe("hello\noauth token\n");
    expect(Buffer.from((await readBytes(client, mount, "/bin/data.bin")) as Uint8Array)).toEqual(
      Buffer.from([0, 1, 2]),
    );

    const grepMatches = (await grep(client, mount, "/", "oauth", { ignoreCase: true })) as Array<Record<string, unknown>>;
    expect(grepMatches).toEqual([{ path: "/notes/hello.txt", line_number: 2, line: "oauth token" }]);

    await rm(client, mount, "/notes", true);
    await rm(client, mount, "/bin", true);
  });

  it("runs a short live dogfood sequence", async () => {
    const summary = (await runDogfood({
      apiKey: process.env.TURBOPUFFER_API_KEY,
      region: process.env.TURBOPUFFER_REGION,
      baseURL: process.env.TURBOPUFFER_BASE_URL,
      steps: 12,
      seed: 7,
      mountPrefix: "tsdogfood",
      keepOnFail: false,
      keepAlways: false,
      cleanup: true,
    })) as Record<string, unknown>;
    expect(summary.stepsCompleted).toBe(12);
    expect(Number(summary.checksRun)).toBeGreaterThan(0);
  });
});
