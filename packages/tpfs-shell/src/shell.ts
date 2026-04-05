import { Bash, type ExecResult } from "just-bash";

import { appendCommandLog } from "./logging.js";
import { createBootContext, type ShellBootContext, type ShellBootOptions } from "./boot.js";

function cwdFromExecResult(result: ExecResult, fallback: string): string {
  const candidate = (result as ExecResult & { cwd?: string }).cwd;
  return typeof candidate === "string" && candidate.length > 0 ? candidate : fallback;
}

export async function createBash(context: ShellBootContext): Promise<Bash> {
  return new Bash({
    fs: context.fs,
    cwd: String(context.session.cwd),
    env: {
      HOME: "/",
      USER: "agent",
      SHELL: "/bin/tpfs-shell",
      PWD: String(context.session.cwd),
      OLDPWD: String(context.session.cwd),
    },
  });
}

export async function runShellCommand(
  context: ShellBootContext,
  command: string,
): Promise<ExecResult> {
  const cwdBefore = String(context.session.cwd);
  const trimmed = command.trim();

  if (trimmed === "pwd") {
    const result: ExecResult = { stdout: `${cwdBefore}\n`, stderr: "", exitCode: 0 };
    await appendCommandLog(context.client, context.mount, context.logPath, {
      timestamp: new Date().toISOString(),
      command,
      cwd_before: cwdBefore,
      cwd_after: cwdBefore,
      exit_code: result.exitCode,
      stdout_preview: result.stdout.slice(0, 2000),
      stderr_preview: result.stderr.slice(0, 2000),
    });
    return result;
  }

  if (trimmed === "cd" || trimmed.startsWith("cd ")) {
    const targetRaw = trimmed === "cd" ? "." : trimmed.slice(3).trim();
    const target = context.fs.resolvePath(cwdBefore, targetRaw);
    const stat = await context.fs.stat(target);
    if (!stat.isDirectory) {
      const result: ExecResult = {
        stdout: "",
        stderr: `cd: not a directory: ${targetRaw}\n`,
        exitCode: 1,
      };
      await appendCommandLog(context.client, context.mount, context.logPath, {
        timestamp: new Date().toISOString(),
        command,
        cwd_before: cwdBefore,
        cwd_after: cwdBefore,
        exit_code: result.exitCode,
        stdout_preview: "",
        stderr_preview: result.stderr.slice(0, 2000),
      });
      return result;
    }
    await context.persistSession(target);
    context.session.cwd = target;
    const result: ExecResult = { stdout: "", stderr: "", exitCode: 0 };
    await appendCommandLog(context.client, context.mount, context.logPath, {
      timestamp: new Date().toISOString(),
      command,
      cwd_before: cwdBefore,
      cwd_after: target,
      exit_code: result.exitCode,
      stdout_preview: "",
      stderr_preview: "",
    });
    return result;
  }

  const bash = await createBash(context);
  const result = await bash.exec(command);
  const cwdAfter = cwdFromExecResult(result, bash.getCwd());

  if (cwdAfter !== cwdBefore) {
    await context.persistSession(cwdAfter);
    context.session.cwd = cwdAfter;
  }

  await appendCommandLog(
    context.client,
    context.mount,
    context.logPath,
    {
      timestamp: new Date().toISOString(),
      command,
      cwd_before: cwdBefore,
      cwd_after: cwdAfter,
      exit_code: result.exitCode,
      stdout_preview: result.stdout.slice(0, 2000),
      stderr_preview: result.stderr.slice(0, 2000),
    },
  );

  return result;
}

export async function runShellScript(
  options: ShellBootOptions,
  commands: string[],
): Promise<ExecResult[]> {
  const context = await createBootContext(options);
  const results: ExecResult[] = [];
  for (const command of commands) {
    results.push(await runShellCommand(context, command));
  }
  return results;
}
