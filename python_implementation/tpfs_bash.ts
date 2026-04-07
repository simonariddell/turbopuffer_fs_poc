/**
 * tpfs_bash.ts — just-bash ↔ tpfs.py bridge
 *
 * Implements the just-bash IFileSystem interface by delegating every
 * filesystem operation to `python3 tpfs.py --json ...`.  This lets you
 * spin up a just-bash shell where `ls`, `cat`, `cp`, `grep`, etc. all
 * operate on the turbopuffer-backed filesystem — no local disk needed.
 *
 * Usage:
 *   import { createTpfsBash } from "./tpfs_bash.js";
 *
 *   const bash = await createTpfsBash({
 *     mount: "agent-demo",
 *     apiKey: process.env.TURBOPUFFER_API_KEY!,
 *     region: "aws-us-west-2",
 *   });
 *
 *   const result = await bash.exec("ls /project");
 *   console.log(result.stdout);
 *
 *   await bash.exec("cat solver.py");
 *   await bash.exec('echo "hello" > /project/test.txt');
 */

import { Buffer } from "node:buffer";
import { execFileSync } from "node:child_process";
import { resolve as pathResolve } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { Bash } from "just-bash";
import type {
  BufferEncoding,
  CpOptions,
  DirentEntry,
  FsStat,
  IFileSystem,
  MkdirOptions,
  RmOptions,
} from "just-bash";

// ── Configuration ───────────────────────────────────────────────────────────

export interface TpfsBashOptions {
  /** Mount name (default: "demo") */
  mount?: string;
  /** Turbopuffer API key */
  apiKey: string;
  /** Turbopuffer region (default: "aws-us-west-2") */
  region?: string;
  /** Path to tpfs.py (default: auto-detected relative to this file) */
  tpfsPath?: string;
  /** Path to python3 binary (default: "python3") */
  pythonPath?: string;
  /** Additional environment variables for the bash shell */
  env?: Record<string, string>;
  /** Initial working directory for bash (default: loaded from tpfs session) */
  cwd?: string;
  /** Initialize workspace if it doesn't exist (default: true) */
  autoInit?: boolean;
}

// ── tpfs.py CLI caller ──────────────────────────────────────────────────────

/**
 * Calls `python3 tpfs.py --json <args>` and returns the parsed JSON result.
 *
 * Throws on non-zero exit codes with the stderr error message.
 */
// Capture a snapshot of process.env at module load time, before
// just-bash's defense-in-depth patches block enumeration.
const _hostEnv: Record<string, string> = {};
for (const key of Object.keys(process.env)) {
  if (process.env[key] !== undefined) {
    _hostEnv[key] = process.env[key]!;
  }
}

function tpfsExec(
  pythonPath: string,
  tpfsPath: string,
  mount: string,
  apiKey: string,
  region: string,
  args: string[],
  input?: string,
): unknown {
  const fullArgs = [
    tpfsPath,
    "--json",
    "--mount", mount,
    "--api-key", apiKey,
    "--region", region,
    ...args,
  ];

  try {
    const stdout = execFileSync(pythonPath, fullArgs, {
      encoding: "utf8",
      input,
      env: { ..._hostEnv, PYTHONUNBUFFERED: "1" },
      maxBuffer: 50 * 1024 * 1024, // 50MB for large files
      stdio: ["pipe", "pipe", "pipe"],
    });
    if (!stdout.trim()) return null;
    return JSON.parse(stdout);
  } catch (err: unknown) {
    const error = err as { stderr?: string; status?: number; message?: string };
    // Try to parse structured error from stderr
    const stderr = error.stderr ?? "";
    let parsed: { error?: string; message?: string } | null = null;
    try {
      parsed = JSON.parse(stderr);
    } catch {
      // stderr is not JSON
    }
    const rawMsg = parsed?.message || stderr.trim() || error.message || "tpfs.py failed";
    const msg = typeof rawMsg === "string" && rawMsg.length > 0 ? rawMsg : "tpfs.py failed";
    const code = parsed?.error ?? "Error";

    // Map tpfs error types to standard Node-like errors
    if (code === "FileNotFoundError" || msg.includes("FileNotFoundError")) {
      const e = new Error(`ENOENT: no such file or directory: ${msg}`);
      (e as NodeJS.ErrnoException).code = "ENOENT";
      throw e;
    }
    if (code === "IsADirectoryError" || msg.includes("IsADirectoryError")) {
      const e = new Error(`EISDIR: illegal operation on a directory: ${msg}`);
      (e as NodeJS.ErrnoException).code = "EISDIR";
      throw e;
    }
    if (code === "NotADirectoryError" || msg.includes("NotADirectoryError")) {
      const e = new Error(`ENOTDIR: not a directory: ${msg}`);
      (e as NodeJS.ErrnoException).code = "ENOTDIR";
      throw e;
    }
    throw new Error(msg);
  }
}

// ── Path resolution ─────────────────────────────────────────────────────────

/**
 * Simple POSIX path resolution (no host filesystem consultation).
 */
function posixResolve(base: string, target: string): string {
  if (!target || target === "." || target === "./") return base;
  if (target.startsWith("/")) return posixNormalize(target);
  const segments = target.split("/");
  let current = base;
  for (const seg of segments) {
    if (seg === "" || seg === ".") continue;
    if (seg === "..") {
      const lastSlash = current.lastIndexOf("/");
      current = lastSlash > 0 ? current.slice(0, lastSlash) : "/";
      continue;
    }
    current = current === "/" ? `/${seg}` : `${current}/${seg}`;
  }
  return current;
}

function posixNormalize(path: string): string {
  if (path === "/") return "/";
  const parts = path.split("/").filter(Boolean);
  const result: string[] = [];
  for (const p of parts) {
    if (p === ".") continue;
    if (p === "..") {
      result.pop();
      continue;
    }
    result.push(p);
  }
  return "/" + result.join("/");
}

// ── IFileSystem implementation ──────────────────────────────────────────────

class TpfsPyFs implements IFileSystem {
  private readonly py: string;
  private readonly tpfs: string;
  private readonly mount: string;
  private readonly apiKey: string;
  private readonly region: string;
  private pathCache: string[] = [];

  constructor(options: {
    pythonPath: string;
    tpfsPath: string;
    mount: string;
    apiKey: string;
    region: string;
  }) {
    this.py = options.pythonPath;
    this.tpfs = options.tpfsPath;
    this.mount = options.mount;
    this.apiKey = options.apiKey;
    this.region = options.region;
  }

  private call(args: string[], input?: string): unknown {
    return tpfsExec(this.py, this.tpfs, this.mount, this.apiKey, this.region, args, input);
  }

  /** Refresh the path cache from turbopuffer. */
  refreshPaths(): void {
    try {
      const rows = this.call(["find", "/"]) as Array<{ path: string }>;
      this.pathCache = rows.map(r => r.path).sort();
      if (!this.pathCache.includes("/")) {
        this.pathCache.unshift("/");
      }
    } catch {
      this.pathCache = ["/"];
    }
  }

  private addPath(path: string): void {
    if (!this.pathCache.includes(path)) {
      this.pathCache.push(path);
      this.pathCache.sort();
    }
    // Also add parent paths
    let current = path;
    while (current !== "/") {
      const slash = current.lastIndexOf("/");
      current = slash > 0 ? current.slice(0, slash) : "/";
      if (!this.pathCache.includes(current)) {
        this.pathCache.push(current);
        this.pathCache.sort();
      }
    }
  }

  private removePath(path: string, recursive: boolean): void {
    if (recursive) {
      const prefix = path === "/" ? "/" : path + "/";
      this.pathCache = this.pathCache.filter(
        p => p !== path && !p.startsWith(prefix)
      );
    } else {
      this.pathCache = this.pathCache.filter(p => p !== path);
    }
  }

  // ── Read operations ─────────────────────────────────────────────────────

  async readFile(
    path: string,
    _options?: { encoding?: BufferEncoding | null } | BufferEncoding,
  ): Promise<string> {
    const result = this.call(["cat", path]);
    if (result && typeof result === "object" && "text" in (result as Record<string, unknown>)) {
      return (result as { text: string }).text;
    }
    return String(result ?? "");
  }

  async readFileBuffer(path: string): Promise<Uint8Array> {
    const result = this.call(["read-bytes", path]) as { base64: string } | null;
    if (!result?.base64) return new Uint8Array();
    // Decode base64 to Uint8Array
    const binaryString = atob(result.base64);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }
    return bytes;
  }

  async writeFile(
    path: string,
    content: string | Uint8Array,
    _options?: { encoding?: BufferEncoding } | BufferEncoding,
  ): Promise<void> {
    if (content instanceof Uint8Array) {
      // Binary path: base64-encode and use write-bytes
      const b64 = Buffer.from(content).toString("base64");
      this.call(["write-bytes", path, "--stdin-base64"], b64);
      this.addPath(path);
      return;
    }
    // Text path: use put with stdin
    this.call(["put", path, "--stdin"], content);
    this.addPath(path);
  }

  async appendFile(
    path: string,
    content: string | Uint8Array,
    _options?: { encoding?: BufferEncoding } | BufferEncoding,
  ): Promise<void> {
    let existing = "";
    try {
      existing = await this.readFile(path);
    } catch {
      // File doesn't exist yet — that's fine for append
    }
    const extra = content instanceof Uint8Array
      ? new TextDecoder().decode(content)
      : content;
    await this.writeFile(path, existing + extra);
  }

  async exists(path: string): Promise<boolean> {
    try {
      const result = this.call(["stat", path]);
      return result !== null;
    } catch {
      return false;
    }
  }

  async stat(path: string): Promise<FsStat> {
    let result: Record<string, unknown> | null;
    try {
      result = this.call(["stat", path]) as Record<string, unknown> | null;
    } catch (err) {
      // If the stat command fails, the path doesn't exist
      const e = new Error(`ENOENT: no such file or directory, stat '${path}'`);
      (e as NodeJS.ErrnoException).code = "ENOENT";
      throw e;
    }
    if (!result) {
      const e = new Error(`ENOENT: no such file or directory, stat '${path}'`);
      (e as NodeJS.ErrnoException).code = "ENOENT";
      throw e;
    }
    const isDir = result.kind === "dir";
    return {
      isFile: !isDir,
      isDirectory: isDir,
      isSymbolicLink: false,
      mode: isDir ? 0o755 : 0o644,
      size: Number(result.size_bytes ?? 0),
      mtime: new Date(),
    };
  }

  async mkdir(path: string, _options?: MkdirOptions): Promise<void> {
    this.call(["mkdir", path]);
    this.addPath(path);
  }

  async readdir(path: string): Promise<string[]> {
    const entries = await this.readdirWithFileTypes(path);
    return entries.map(e => e.name);
  }

  async readdirWithFileTypes(path: string): Promise<DirentEntry[]> {
    const rows = this.call(["ls", path]) as Array<Record<string, unknown>>;
    return rows.map(row => {
      const isDir = row.kind === "dir";
      return {
        name: String(row.basename ?? row.path ?? ""),
        isFile: !isDir,
        isDirectory: isDir,
        isSymbolicLink: false,
      };
    });
  }

  async rm(path: string, options?: RmOptions): Promise<void> {
    const args = ["rm", path];
    if (options?.recursive) args.push("-r");
    try {
      this.call(args);
    } catch (err) {
      if (options?.force) return;
      throw err;
    }
    this.removePath(path, options?.recursive ?? false);
  }

  async cp(src: string, dest: string, options?: CpOptions): Promise<void> {
    const args = ["cp", src, dest];
    if (options?.recursive) args.push("-r");
    const result = this.call(args) as Record<string, unknown> | null;
    // Update cache from result payload, not input args
    const actualPath = result?.path ?? dest;
    this.addPath(String(actualPath));
    // For recursive ops, refresh entire cache
    if (options?.recursive) {
      this.refreshPaths();
    }
  }

  async mv(src: string, dest: string): Promise<void> {
    const result = this.call(["mv", src, dest]) as Record<string, unknown> | null;
    const actualPath = result?.path ?? dest;
    // Check if source was a directory (recursive move)
    const isRecursive = result && "files_copied" in result;
    this.removePath(src, !!isRecursive);
    this.addPath(String(actualPath));
    if (isRecursive) {
      this.refreshPaths();
    }
  }

  // ── Path operations ─────────────────────────────────────────────────────

  resolvePath(base: string, target: string): string {
    return posixResolve(base, target);
  }

  /**
   * Return the current path inventory.
   *
   * **Cache semantics**: This list is seeded at adapter creation via
   * ``refreshPaths()`` and updated for mutations performed through
   * this adapter instance.  It may lag concurrent out-of-band writers
   * until the next ``refreshPaths()`` call.  Glob expansion in
   * just-bash relies on this cache and is therefore best-effort.
   */
  getAllPaths(): string[] {
    return [...this.pathCache];
  }

  // ── Unsupported operations (fail explicitly) ──────────────────────────

  async chmod(_path: string, _mode: number): Promise<void> {
    throw new Error(
      "TPFS_UNSUPPORTED: chmod is not supported. " +
      "tpfs does not implement a durable permission model."
    );
  }

  async symlink(_target: string, _linkPath: string): Promise<void> {
    throw new Error(
      "TPFS_UNSUPPORTED: symlink is not supported. " +
      "tpfs does not represent a durable symlink graph."
    );
  }

  async link(_existingPath: string, _newPath: string): Promise<void> {
    throw new Error(
      "TPFS_UNSUPPORTED: hard links are not supported. " +
      "tpfs does not represent hard-link identity."
    );
  }

  async readlink(_path: string): Promise<string> {
    throw new Error(
      "TPFS_UNSUPPORTED: readlink is not supported. " +
      "tpfs does not support symlinks."
    );
  }

  async lstat(path: string): Promise<FsStat> {
    // No symlink distinction in tpfs
    return this.stat(path);
  }

  async realpath(path: string): Promise<string> {
    // No symlinks — realpath is just normalization
    return posixNormalize(path);
  }

  async utimes(_path: string, _atime: Date, _mtime: Date): Promise<void> {
    throw new Error(
      "TPFS_UNSUPPORTED: utimes is not supported. " +
      "tpfs does not expose a durable mutable timestamp API."
    );
  }
}

// ── Factory ─────────────────────────────────────────────────────────────────

/**
 * Create a TpfsPyFs adapter backed by tpfs.py.
 */
export function createTpfsAdapter(options: TpfsBashOptions): TpfsPyFs {
  const pythonPath = options.pythonPath ?? "python3";
  const tpfsPath = options.tpfsPath ?? join(dirname(fileURLToPath(import.meta.url)), "tpfs.py");
  const mount = options.mount ?? "demo";
  const region = options.region ?? "aws-us-west-2";

  const fs = new TpfsPyFs({
    pythonPath,
    tpfsPath,
    mount,
    apiKey: options.apiKey,
    region,
  });

  return fs;
}

/**
 * Create a just-bash shell backed entirely by turbopuffer via tpfs.py.
 *
 * Usage:
 *   const bash = await createTpfsBash({
 *     apiKey: process.env.TURBOPUFFER_API_KEY!,
 *     mount: "agent-demo",
 *   });
 *   const bash = await createTpfsBash({
 *     apiKey: process.env.TURBOPUFFER_API_KEY!,
 *     mount: "agent-demo",
 *   });
 *   const result = await bash.exec("ls /project");
 *   console.log(result.stdout);
 *
 * Durable CWD:
 *   After each exec(), the wrapper checks if bash's cwd changed.
 *   If so, it persists the new cwd via `tpfs cd <newPwd>`.
 *   A fresh shell created on the same mount will start from that cwd.
 */

// ── DurableBash wrapper ─────────────────────────────────────────────────────

interface TpfsExecConfig {
  pythonPath: string;
  tpfsPath: string;
  mount: string;
  apiKey: string;
  region: string;
}

/**
 * Wraps a just-bash Bash instance to persist cwd changes durably
 * via tpfs cd after each exec().
 */
class DurableBash {
  private bash: Bash;
  private durableCwd: string;
  private readonly config: TpfsExecConfig;
  readonly fs: TpfsPyFs;

  constructor(
    bash: Bash,
    initialCwd: string,
    config: TpfsExecConfig,
    fs: TpfsPyFs,
  ) {
    this.bash = bash;
    this.durableCwd = initialCwd;
    this.config = config;
    this.fs = fs;
  }

  /** Execute a command with durable cwd tracking. */
  async exec(command: string): Promise<{ stdout: string; stderr: string; exitCode: number }> {
    const result = await this.bash.exec(command);

    // Check if cwd changed after exec
    const newCwd = this.bash.getCwd();
    if (newCwd && newCwd !== this.durableCwd) {
      // Persist the new cwd durably
      try {
        tpfsExec(
          this.config.pythonPath,
          this.config.tpfsPath,
          this.config.mount,
          this.config.apiKey,
          this.config.region,
          ["cd", newCwd],
        );
        this.durableCwd = newCwd;
      } catch {
        // If cd fails (e.g. path doesn't exist in tpfs), keep old cwd
      }
    }

    return result;
  }

  /** Get the current durable working directory. */
  getCwd(): string {
    return this.durableCwd;
  }
}

export async function createTpfsBash(options: TpfsBashOptions): Promise<DurableBash> {
  const pythonPath = options.pythonPath ?? "python3";
  const tpfsPath = options.tpfsPath ?? join(dirname(fileURLToPath(import.meta.url)), "tpfs.py");
  const mount = options.mount ?? "demo";
  const region = options.region ?? "aws-us-west-2";

  const config: TpfsExecConfig = { pythonPath, tpfsPath, mount, apiKey: options.apiKey, region };

  // Initialize workspace if needed (init is idempotent — safe to call always)
  if (options.autoInit !== false) {
    tpfsExec(pythonPath, tpfsPath, mount, options.apiKey, region, ["init"]);
  }

  // Get current working directory from tpfs session
  let cwd = options.cwd ?? "/";
  try {
    const pwdResult = tpfsExec(
      pythonPath, tpfsPath, mount, options.apiKey, region, ["pwd"],
    ) as { cwd?: string } | null;
    if (pwdResult?.cwd) {
      cwd = pwdResult.cwd;
    }
  } catch {
    // Fall back to provided cwd or "/"
  }

  // Create the filesystem adapter
  const fs = createTpfsAdapter(options);
  fs.refreshPaths();

  // Create the bash shell
  const bash = new Bash({
    fs,
    cwd,
    env: {
      HOME: "/",
      USER: "agent",
      SHELL: "/bin/tpfs-bash",
      PWD: cwd,
      OLDPWD: cwd,
      MOUNT: mount,
      ...options.env,
    },
  });

  return new DurableBash(bash, cwd, config, fs);
}

// ── CLI demo ────────────────────────────────────────────────────────────────

async function main() {
  const apiKey = process.env.TURBOPUFFER_API_KEY;
  if (!apiKey) {
    console.error("Set TURBOPUFFER_API_KEY environment variable");
    process.exit(1);
  }

  const mount = process.argv[2] ?? "demo";
  console.log(`Creating just-bash shell backed by turbopuffer (mount: ${mount})...\n`);

  const durableBash = await createTpfsBash({
    apiKey,
    mount,
    region: process.env.TURBOPUFFER_REGION ?? "aws-us-west-2",
    tpfsPath: join(dirname(fileURLToPath(import.meta.url)), "tpfs.py"),
  });

  // Run demo commands — each one goes through tpfs.py → turbopuffer
  const commands = [
    "pwd",
    "ls /",
    "ls /project",
    'echo "# Agent Notes" > /project/notes.md',
    'echo "- All state lives in turbopuffer" >> /project/notes.md',
    'echo "- No local disk needed" >> /project/notes.md',
    "cat /project/notes.md",
    "cp /project/notes.md /output/notes.md",
    "ls /output",
  ];

  for (const cmd of commands) {
    console.log(`$ ${cmd}`);
    const result = await durableBash.exec(cmd);
    if (result.stdout) process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
    if (result.exitCode !== 0) console.log(`(exit ${result.exitCode})`);
    console.log();
  }
}

// Run demo if executed directly
const isMain = process.argv[1] && (
  process.argv[1].endsWith("tpfs_bash.ts") ||
  process.argv[1].endsWith("tpfs_bash.js")
);
if (isMain) {
  main().catch(console.error);
}
