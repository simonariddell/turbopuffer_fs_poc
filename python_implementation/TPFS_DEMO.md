# tpfs — A Turbopuffer-Backed Filesystem

**One Python file. One CLI. Every file lives in turbopuffer.**

`tpfs.py` is a general filesystem proof-of-concept for **regular files and
directories**, demonstrating how to build a durable filesystem abstraction
over [turbopuffer](https://turbopuffer.com). Every file and directory is a
document in a turbopuffer namespace. An agent with zero local disk can boot,
write code, search, edit, die, and reboot on another machine — recovering all
state from turbopuffer alone.

---

## Scope

### Supported in this pass

- **Regular files**: text and binary
- **Directories**: explicit documents
- **Durable cwd/session**: persisted in `/state/session.json`
- **Hydrate/sync**: with conflict detection via conditional writes
- **Recursive cp / mv / rm**: for directory subtrees
- **grep / find / tree / ls / stat**: full filesystem inspection
- **just-bash bridge**: preserves durable cwd across shell invocations
- **Binary round-trips**: base64 transport through CLI and bridge

### Explicit non-goals for this pass

- Symlinks
- Hard links
- chmod / chown / ACLs
- Extended attributes (xattrs)
- File locking
- mmap
- Giant files beyond the current single-document storage ceiling

---

## Architecture

### The Data Model

```
┌─────────────────────────────────────────────────────────┐
│                     turbopuffer                         │
│                                                         │
│  Namespace: "agent-demo__fs"                            │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Document: id=sha256("/project/solver.py")       │   │
│  │    path:       "/project/solver.py"              │   │
│  │    parent:     "/project"                        │   │
│  │    basename:   "solver.py"                       │   │
│  │    kind:       "file"                            │   │
│  │    is_text:    1                                  │   │
│  │    version:    3                                  │   │
│  │    text:       "import numpy as np\n..."          │   │
│  │    mime:       "text/x-python"                    │   │
│  │    sha256:     "d88b0e4f..."                      │   │
│  │    size_bytes: 1058                               │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Document: id=sha256("/project")                 │   │
│  │    path:       "/project"                        │   │
│  │    kind:       "dir"                             │   │
│  │    mime:       "inode/directory"                  │   │
│  └──────────────────────────────────────────────────┘   │
│  ...                                                    │
└─────────────────────────────────────────────────────────┘
```

**Key design choices:**

| Concept | Implementation |
|---------|---------------|
| **Mount** | A logical workspace name. Mount `"demo"` maps to namespace `"demo__fs"`. |
| **Document ID** | `SHA-256(normalized_absolute_path)`. Deterministic, collision-free, fits the 64-byte max string ID. |
| **Directories** | Explicit documents with `kind="dir"`. Not inferred from children. |
| **Text files** | Content stored in `text` field. Full-text-search enabled (BM25, `word_v3`). |
| **Binary files** | Content stored in `blob_b64` (base64-encoded). |
| **Version** | Monotonic `uint` on files for optimistic concurrency (conditional writes). |
| **Session state** | `cwd` persisted at `/state/session.json` — survives machine death. |
| **Workspace metadata** | Stored at `/state/workspace.json` — lifecycle, config, schema version. |

### Schema (v2)

Every document in the namespace shares this schema:

```python
FS_SCHEMA = {
    "path":      {"type": "string", "filterable": True, "glob": True},
    "parent":    "string",
    "basename":  {"type": "string", "filterable": True, "glob": True},
    "kind":      "string",           # "file" or "dir"
    "ext":       "string",           # ".py", ".md", etc.
    "mime":      "string",           # "text/x-python", etc.
    "size_bytes": "uint",
    "is_text":   "uint",             # 1 for text, 0 for binary
    "version":   "uint",             # files only; monotonic for conditional writes
    "text": {                        # Full text content (text files only)
        "type": "string",
        "full_text_search": {
            "tokenizer": "word_v3",
            "remove_stopwords": False,
            "stemming": False,
        },
        # text is NOT filterable — avoids documented 4 KiB filterable ceiling
    },
    "blob_b64":  {"type": "string", "filterable": False},  # Base64 (binary files)
    "sha256":    "string",           # Content hash
}
```

**Schema v2 changes from v1:**
- `path` and `basename` get explicit `glob: True` (not relying on backwards compat)
- `text` drops `filterable: True` (4 KiB ceiling makes it unsafe for general file bodies)
- New `version: uint` field for optimistic concurrency on files
- Migration: use a fresh mount with `schema_version = 2` in workspace metadata

### Current single-file size ceilings

turbopuffer documents max attribute value size 8 MiB and max document size 64 MiB.
Because binary content is base64-encoded, the effective limits are:

| Type | Max size |
|------|----------|
| Text file | ~8 MiB (UTF-8 bytes) |
| Binary file | ~5.9 MiB (raw bytes → ~8 MiB base64) |

These limits are enforced with explicit errors in `put_text()` and `put_bytes()`.

### Durability Model

> **Success means durable.** An operation does not return success until the
> write has been committed through turbopuffer.

There is no local cache, no writeback buffer, no hidden metadata store. If a
machine dies after a successful write, another machine can read that data
immediately. Session state (`cwd`) is itself a document in the same namespace.

### Sync Correctness

Sync uses turbopuffer's conditional write primitives for conflict safety:

- **New files**: `upsert_condition=["id", "Eq", None]` — only succeeds if doc doesn't exist
- **Modified files**: `patch_condition=["version", "Lt", {"$ref_new": "version"}]` — version check
- **Deleted files**: `delete_condition=["version", "Eq", expected_version]` — exact match
- **Conflicts**: reported precisely via `return_affected_ids`

---

## Installation

```bash
pip install turbopuffer click
```

Set your API key:
```bash
export TURBOPUFFER_API_KEY="your-key-here"
export TURBOPUFFER_REGION="aws-us-west-2"  # optional, this is the default
```

Verify it works:
```bash
python tpfs.py --help
```

---

## Command Reference

| Command | Usage | Description |
|---------|-------|-------------|
| `init` | `tpfs init` | Initialize workspace (idempotent — won't reset existing session) |
| `mounts` | `tpfs mounts` | List all filesystem mounts |
| `pwd` | `tpfs pwd` | Print durable working directory |
| `cd` | `tpfs cd <path>` | Change durable working directory |
| `ls` | `tpfs ls [path]` | List directory contents |
| `stat` | `tpfs stat <path>` | Show file/dir metadata |
| `cat` | `tpfs cat <path>` | Print text file contents |
| `read-bytes` | `tpfs read-bytes <path>` | Read file as base64 JSON |
| `write-bytes` | `tpfs write-bytes <path> --stdin-base64` | Write binary file from base64 |
| `head` | `tpfs head <path> [-n N]` | First N lines |
| `tail` | `tpfs tail <path> [-n N]` | Last N lines |
| `tree` | `tpfs tree [path] [--depth D]` | Pretty directory tree |
| `find` | `tpfs find [root] [--glob G] [--kind K]` | Recursive find |
| `grep` | `tpfs grep <pattern> [root] [--mode M]` | Search files |
| `mkdir` | `tpfs mkdir <path>` | Create directory (and parents) |
| `put` | `tpfs put <path> --text T` | Write a text file |
| `rm` | `tpfs rm <path> [-r]` | Remove file or directory |
| `cp` | `tpfs cp <src> <dst> [-r]` | Copy file or directory |
| `mv` | `tpfs mv <src> <dst>` | Move file or directory |
| `touch` | `tpfs touch <path>` | Create empty file |
| `wc` | `tpfs wc <path>` | Count lines/words/chars/bytes |
| `replace` | `tpfs replace <path> --search S --replace R` | Search & replace |
| `hydrate` | `tpfs hydrate <local_dir> [--root R]` | Pull workspace to local disk |
| `sync` | `tpfs sync <local_dir> --manifest M` | Push local changes back |
| `ingest` | `tpfs ingest <local_dir> [--mount-root R]` | Upload local dir to turbopuffer |
| `log` | `tpfs log` | Show durable command log |
| `delete-mount` | `tpfs delete-mount` | Delete entire mount |

**Global options:** `--mount/-m` (default: `demo`), `--api-key`, `--region`, `--json`

---

## How Grep Works

`tpfs grep` supports three search modes:

### Literal Mode (`--mode literal`, default)

Locally authoritative. Fetches text files in scope page-by-page (filtered by
kind, is_text, subtree, optional glob), then does exact substring matching
locally per line. The `limit` parameter caps actual line matches, not candidate
documents.

### Regex Mode (`--mode regex`)

Locally authoritative. Same paging strategy as literal, but uses compiled regex
for line matching. Best for patterns like `def.*test` or `import\s+numpy`.

### BM25 Mode (`--mode bm25`)

Uses turbopuffer's built-in BM25 scoring on the `text` field with pinned
`word_v3` tokenizer. Returns documents ranked by relevance score with snippet
extraction.

---

## JSON Error Envelope

When `--json` is set, errors emit structured JSON to stderr:

```json
{"error": {"type": "FileNotFoundError", "message": "/foo/bar"}}
```

Stable error types: `FileNotFoundError`, `IsADirectoryError`,
`NotADirectoryError`, `FileExistsError`, `ValueError`, `OSError`.

---

## just-bash Integration

`tpfs_bash.ts` bridges [just-bash](https://github.com/vercel-labs/just-bash)
to `tpfs.py`, implementing the `IFileSystem` interface so that standard bash
commands (`ls`, `cat`, `echo >`, `cp`, `rm`, etc.) all operate on the
turbopuffer-backed filesystem transparently.

### Durable CWD

The bridge wraps `Bash` in a `DurableBash` class that:
1. Tracks a `durableCwd` variable (initialized from tpfs session)
2. After each `exec()`, checks if `bash.getCwd()` changed
3. If changed, persists the new cwd via `tpfs cd <newPwd>`

This means `cd /output` in one shell instance persists for a fresh shell
created on the same mount.

### Binary Support

- `readFileBuffer()` calls `read-bytes` and decodes base64 → `Uint8Array`
- `writeFile(Uint8Array)` calls `write-bytes` with base64 encoding
- Text writes still use the `put --stdin` path

### Path Cache

The adapter's `getAllPaths()` is backed by a local cache seeded at creation
via `refreshPaths()`. The cache is updated for mutations through the active
adapter instance but may lag concurrent out-of-band writers until the next
`refreshPaths()` call. Glob expansion in just-bash is therefore best-effort.

---

## What This Proves

1. **turbopuffer is a viable backend for filesystem-shaped state.** The query
   model (filters, Glob, BM25) maps naturally to filesystem operations. Ordered
   pagination gives you `ls`. Subtree filters give you `find`. Full-text search
   gives you `grep`.

2. **Agents don't need local disk.** Session state, project files, search
   indexes — it all lives in turbopuffer. A machine can die and be replaced
   without losing anything.

3. **Conditional writes enable safe sync.** Version-based optimistic concurrency
   via `patch_condition` and `delete_condition` prevents silent overwrites when
   remote state changes during sync.

4. **The abstraction is thin.** `tpfs.py` is a single file with no framework,
   no ORM, no intermediate compilation step. Each filesystem operation is a
   direct turbopuffer API call.
