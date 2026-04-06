import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";

const packageRoot = dirname(fileURLToPath(import.meta.url));
const distDir = resolve(packageRoot, "dist");

await mkdir(distDir, { recursive: true });

await build({
  absWorkingDir: packageRoot,
  bundle: false,
  entryPoints: ["src/host.ts"],
  format: "esm",
  logLevel: "info",
  outdir: "dist",
  platform: "node",
  sourcemap: true,
  target: "node20",
  tsconfig: "tsconfig.json",
});

await build({
  absWorkingDir: packageRoot,
  bundle: true,
  entryPoints: ["src/runner.ts"],
  format: "cjs",
  logLevel: "info",
  outdir: "dist",
  platform: "node",
  sourcemap: true,
  target: "node20",
  tsconfig: "tsconfig.json",
});
