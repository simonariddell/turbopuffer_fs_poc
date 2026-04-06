# turbopuffer-fs

`turbopuffer-fs` is now a **TypeScript-first** filesystem-shaped compiler/runtime
over turbopuffer, plus a durable `just-bash` shell integration for
agent-oriented workflows.

The design is intentionally literal:

1. compile filesystem intent into explicit turbopuffer plans,
2. execute those plans with the real turbopuffer client,
3. apply small pure finalizers locally,
4. persist all critical shell/session state back through turbopuffer.

The result is closer to an object-store-backed durable workspace than a local
POSIX filesystem:

- backend-first
- filesystem illusion second
- no sidecar metadata database
- no local durability daemon
- no hidden manifest store
- no FUSE

## Architecture

The repository is a pnpm workspace with two TypeScript packages:

- `packages/turbopuffer-fs`
  - core filesystem compiler/runtime
  - plan builders, runtime execution, finalizers, schema, workspace/session helpers,
    bundle support, ingest, dogfood harness, and CLI
- `packages/tpfs-shell`
  - durable `just-bash` shell runtime layered on the core package
  - restartable cwd/session state
  - durable command logging
  - filesystem adapter for shell execution

## Durability model

One mount maps to one turbopuffer namespace:

- mount `documents`
- namespace `documents__fs`

One normalized absolute path maps to one document.

Directories are explicit documents.
Files are explicit documents.

Text files store full text in `text`.
Binary files store full bytes in `blob_b64`.

Critical invariant:

> No critical state may live ephemerally between the machine and turbopuffer.

That means:

- workspace files live in turbopuffer-backed docs
- session state lives in `/state/session.json`
- workspace metadata lives in `/state/workspace.json`
- command logs live in `/logs/run.jsonl`
- bundle/task artifacts live in the same durable namespace

If a machine dies, another machine should be able to recover from turbopuffer
alone.

## Content model and size semantics

TPFS uses a deliberately simple whole-object content model:

- text files are stored in full in `text`
- binary files are stored in full in `blob_b64`
- writes are whole-file overwrites, not random-write patches
- append is implemented as durable read/modify/write
- hydration/sync also operates on whole-file content

Text vs binary behavior is intentionally predictable:

- text and binary classification is inferred from bytes, file extension, and
  MIME heuristics
- callers may also provide an explicit MIME override for writes
- binary files round-trip as bytes
- text reads fail explicitly on binary targets

TPFS does not currently advertise chunked-write or random-write semantics.
Likewise, the current contract does not define a universal hard file-size cap.
If a deployment introduces size limits, TPFS should fail explicitly rather than
silently truncating content.

## Installation

```bash
pnpm install
```

## Workspace commands

```bash
pnpm typecheck
pnpm test
pnpm --filter @workspace/turbopuffer-fs typecheck
pnpm --filter @workspace/turbopuffer-fs test
pnpm --filter @workspace/tpfs-shell test
```

## Core TypeScript API

The core package exports:

- path helpers
- schema/row builders
- pure plan builders
- runtime execution
- finalizers
- live wrappers
- workspace/session helpers
- workspace metadata/lifecycle helpers
- edit helpers
- hydration/sync helpers
- ingest helpers
- bundle helpers
- dogfood harness
- CLI

Representative API:

```ts
import {
  makeClient,
  statPlan,
  lsPlan,
  findPlan,
  grepPlan,
  stat,
  ls,
  find,
  grep,
  putText,
  readText,
  workspaceInit,
  workspaceShow,
  workspaceExists,
  replaceTextInFile,
  hydrateWorkspace,
  syncWorkspace,
  loadSessionState,
} from "@workspace/turbopuffer-fs";
```

Example:

```ts
import { makeClient, putText, ls, readText } from "@workspace/turbopuffer-fs";

const client = makeClient({
  apiKey: process.env.TURBOPUFFER_API_KEY,
  region: process.env.TURBOPUFFER_REGION,
  baseURL: process.env.TURBOPUFFER_BASE_URL,
});

await putText(client, "documents", "/notes/hello.txt", "hello turbopuffer\n");
console.log(await ls(client, "documents", "/notes"));
console.log(await readText(client, "documents", "/notes/hello.txt"));
```

## CLI

The TypeScript core ships the canonical `tpfs` CLI:

```bash
pnpm --filter @workspace/turbopuffer-fs build
pnpm exec tpfs --region aws-us-west-2 mounts
pnpm exec tpfs --region aws-us-west-2 ls documents /notes
pnpm exec tpfs --region aws-us-west-2 put-text documents /notes/todo.txt --stdin
pnpm exec tpfs --region aws-us-west-2 grep documents / oauth --ignore-case
pnpm exec tpfs --region aws-us-west-2 grep documents / "oauth.*token" --mode regex --ignore-case
pnpm exec tpfs --region aws-us-west-2 search documents / "oauth token" --mode bm25
```

Use `--api-key`, `--region`, or `--base-url` explicitly, or rely on the
corresponding environment variables in your shell.

`grep` is now mode-aware:

- `--mode literal` for exact substring grep
- `--mode regex` for exact regex line matching
- `--mode bm25` for ranked lexical retrieval

`search` is currently a first-class alias over the same grep/search engine.

## Durable shell runtime

`packages/tpfs-shell` layers `just-bash` over the durable filesystem runtime.

The shell runtime is designed so that:

- `just-bash` provides shell semantics
- `turbopuffer-fs` provides durable filesystem semantics
- `cwd` is persisted in `/state/session.json`
- command logs are appended to `/logs/run.jsonl`
- restarts hydrate from turbopuffer-backed state instead of local disk state

Supported phase-1 shell semantics include:

- `pwd`
- `cd`
- `ls`
- `cat`
- `cp`
- `mv`
- durable file reads and writes
- durable directory creation and removal

Unsupported features fail explicitly instead of pretending to exist:

- symlinks
- hard links
- chmod
- `utimes`
- full POSIX transactional semantics

## Deployment-configurable workspaces

Agent workspaces can use deployment or bundle-specific conventions instead of a
hard-coded layout. The default workspace profile is:

```json
{
  "entrypoint": "/TASK.md",
  "bundle_manifest": "/bundle.json",
  "session_state": "/state/session.json",
  "logs_dir": "/logs",
  "output_dir": "/output",
  "scratch_dir": "/scratch",
  "project_dir": "/project",
  "input_dir": "/input"
}
```

These paths are conventions stored inside the same filesystem-backed namespace.
Durable session state such as `pwd` / `cd` lives in the configured session-state
document, so an agent can restart on another node and continue from durable
state.

Workspace identity is also first-class. By default, TPFS persists workspace
metadata to `/state/workspace.json`, including workspace kind, creation/update
timestamps, lifecycle status, and canonical workspace paths. This lets higher
level agent runtimes distinguish:

- a mount that has never been initialized
- an active durable workspace
- an archived workspace

CLI examples:

```bash
pnpm exec tpfs workspace-init documents
pnpm exec tpfs workspace-exists documents
pnpm exec tpfs workspace-show documents
pnpm exec tpfs workspace-archive documents
pnpm exec tpfs workspace-delete documents
pnpm exec tpfs cd documents /project
pnpm exec tpfs pwd documents
pnpm exec tpfs ls documents
pnpm exec tpfs cat documents src/file.ts
```

Task bundles can override parts of the workspace profile via a `workspace`
section in `bundle.json`.

## Agent-safe editing

TPFS now includes a small text-edit helper for deterministic read/edit/write
loops:

```ts
import { replaceTextInFile } from "@workspace/turbopuffer-fs";

await replaceTextInFile(client, "documents", "/project/notes.txt", {
  search: "world",
  replace: "tpfs",
});
```

The helper:

- reads through TPFS
- requires a text file
- fails explicitly on zero matches or ambiguous multi-match replacements
- rewrites the file durably through TPFS
- returns simple change metadata including match count and before/after hashes

## Hydration and sync

TPFS also exposes an explicit hydration/sync workflow for agent runtimes that
need a disposable local execution mirror:

```ts
import { hydrateWorkspace, syncWorkspace } from "@workspace/turbopuffer-fs";

const manifest = await hydrateWorkspace(client, "documents", "/tmp/tpfs-sandbox", {
  workspaceConfig,
});

// ...run tools locally inside /tmp/tpfs-sandbox...

const result = await syncWorkspace(client, "documents", "/tmp/tpfs-sandbox", manifest);
```

Current sync semantics are intentionally simple and conservative:

- hydrate produces a snapshot manifest
- sync detects created, modified, deleted, and unchanged paths
- sync skips unchanged paths
- sync reports conflicts when a touched remote path changed after hydration
- binary files round-trip as bytes

Local sandboxes are execution mirrors only; TPFS remains the durable source of
truth.

## Dogfood harness

There is also a seeded live dogfood runner for exercising the wrapper the way an
agent would:

```bash
pnpm --filter @workspace/turbopuffer-fs test -- --runInBand
```

Live dogfood validation is enabled when:

- `TURBOPUFFER_FS_LIVE=1`
- `TURBOPUFFER_API_KEY` is set
- `TURBOPUFFER_REGION` is set

The dogfood path:

- creates a fresh mount/namespace
- performs randomized filesystem-shaped operations
- maintains a local shadow model
- checks invariants as it goes
- cleans up unless configured otherwise

## Examples

The `examples/task-bundles/` tree contains workload fixtures and task bundles
used for bundle seeding, dogfooding, and shell-oriented recovery workflows.
These example files may include Python project content as task fixture material,
but they are not the library implementation surface.

## Non-goals

This project intentionally does not implement:

- FUSE
- kernel callbacks
- POSIX-complete semantics
- random writes
- permissions
- hard links
- background sync
- hidden local truth
- external metadata stores

The goal is a small, auditable, durable filesystem-shaped runtime over
turbopuffer, not a heavyweight VFS framework.
