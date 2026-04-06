# Daytona + TPFS shell example

 This package demonstrates option A: run an agent-oriented shell inside a Daytona
 sandbox where filesystem operations are backed directly by turbopuffer via TPFS,
 without hydrating or syncing a local workspace tree.

 ## What this does

- creates a Daytona sandbox
- passes Turbopuffer credentials and a TPFS mount name as environment variables
- executes a bundled TPFS runner through Daytona's JavaScript `process.codeRun(...)`
- runs commands through `@workspace/tpfs-shell`
- prints each command's stdout/stderr/exit code as JSON

All durable filesystem state goes through TPFS. The Daytona sandbox filesystem is
not used for workspace hydration or command execution state.

 ## Prerequisites

 Environment variables on the host machine:

 - `DAYTONA_API_KEY`
 - optional `DAYTONA_API_URL`
 - optional `DAYTONA_TARGET`
 - `TURBOPUFFER_API_KEY`
 - `TURBOPUFFER_REGION`
 - optional `TURBOPUFFER_BASE_URL`

 ## Build

 ```bash
 pnpm --filter @workspace/daytona-tpfs-example build
 ```

 ## Run

 ```bash
 pnpm --filter @workspace/daytona-tpfs-example start -- \
   --mount demo-mount \
   "pwd" \
   "ls" \
   "mkdir notes" \
   "cd notes" \
   "echo hello > hello.txt" \
   "cat hello.txt"
 ```

 Example with persistent sandbox left running:

 ```bash
 pnpm --filter @workspace/daytona-tpfs-example start -- \
   --mount demo-mount \
   --keep-sandbox \
   "pwd" \
   "ls /project"
 ```

 ## Notes

 - TPFS is not POSIX-complete. Commands that rely on symlinks, chmod, atomic
   rename, or other unsupported semantics may fail or behave differently.
 - This example uses the `just-bash` shell model exposed by `tpfs-shell`, not the
   Daytona sandbox filesystem APIs.
- The only code executed inside the sandbox is the bundled TPFS runner passed via
  Daytona's JavaScript code execution API.
 - The mount name is the durable workspace identity. Reuse the same mount to
   observe persisted TPFS state across sandbox runs.
