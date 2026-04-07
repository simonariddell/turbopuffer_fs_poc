# tpfs — A Turbopuffer-Backed Filesystem

**One Python file. One CLI. Every file lives in turbopuffer.**

`tpfs.py` is a proof-of-concept demonstrating how to build a complete, durable
filesystem abstraction over [turbopuffer](https://turbopuffer.com). Every file
and directory is a document in a turbopuffer namespace. An agent with zero local
disk can boot, write code, search, edit, die, and reboot on another machine —
recovering all state from turbopuffer alone.

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
| **Document ID** | `SHA-256(normalized_absolute_path)`. Deterministic, collision-free. |
| **Directories** | Explicit documents with `kind="dir"`. Not inferred from children. |
| **Text files** | Content stored in `text` field. Full-text-search enabled (BM25). |
| **Binary files** | Content stored in `blob_b64` (base64-encoded). |
| **Session state** | `cwd` persisted at `/state/session.json` — survives machine death. |
| **Workspace metadata** | Stored at `/state/workspace.json` — lifecycle, config, identity. |

### Schema

Every document in the namespace shares this schema:

```python
{
    "path":       {"type": "string", "filterable": True},
    "parent":     "string",
    "basename":   {"type": "string", "filterable": True},
    "kind":       "string",           # "file" or "dir"
    "ext":        "string",           # ".py", ".md", etc.
    "mime":       "string",           # "text/x-python", etc.
    "size_bytes": "uint",
    "is_text":    "uint",             # 1 for text, 0 for binary
    "text":       {                   # Full text content (text files only)
        "type": "string",
        "filterable": True,
        "full_text_search": {
            "tokenizer": "word_v3",
            "remove_stopwords": False,
            "stemming": False,
        },
    },
    "blob_b64":   {"type": "string", "filterable": False},  # Base64 (binary files)
    "sha256":     "string",           # Content hash
}
```

### Durability Model

> **Success means durable.** An operation does not return success until the
> write has been committed through turbopuffer.

There is no local cache, no writeback buffer, no hidden metadata store. If a
machine dies after a successful write, another machine can read that data
immediately. Session state (`cwd`) is itself a document in the same namespace.

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
| `init` | `tpfs init` | Initialize workspace with standard dirs |
| `mounts` | `tpfs mounts` | List all filesystem mounts |
| `pwd` | `tpfs pwd` | Print durable working directory |
| `cd` | `tpfs cd <path>` | Change durable working directory |
| `ls` | `tpfs ls [path]` | List directory contents |
| `stat` | `tpfs stat <path>` | Show file/dir metadata |
| `cat` | `tpfs cat <path>` | Print file contents |
| `head` | `tpfs head <path> [-n N]` | First N lines |
| `tail` | `tpfs tail <path> [-n N]` | Last N lines |
| `tree` | `tpfs tree [path] [--depth D]` | Pretty directory tree |
| `find` | `tpfs find [root] [--glob G] [--kind K]` | Recursive find |
| `grep` | `tpfs grep <pattern> [root] [--mode M]` | Search files |
| `mkdir` | `tpfs mkdir <path>` | Create directory (and parents) |
| `put` | `tpfs put <path> --text T` | Write a text file |
| `rm` | `tpfs rm <path> [-r]` | Remove file or directory |
| `cp` | `tpfs cp <src> <dst>` | Copy a file |
| `mv` | `tpfs mv <src> <dst>` | Move a file |
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

`tpfs grep` supports three search modes, each with a different strategy for
combining remote turbopuffer queries with local result refinement:

### Literal Mode (`--mode literal`, default)

**Two-phase search.** Phase 1 queries turbopuffer with a `text Glob` filter
(`*pattern*`) to narrow the candidate set remotely. Phase 2 iterates over each
candidate document locally, splitting text into lines and doing exact substring
matching to produce `{path, line_number, line}` results.

The remote filter may over-approximate (Glob escaping edge cases), but local
matching is always exact. This is the most efficient mode for simple substring
searches because turbopuffer's attribute index eliminates non-matching documents
before they're transferred.

### Regex Mode (`--mode regex`)

**Remote scope, local matching.** Fetches all text files in the subtree (with
optional glob filter), then compiles the regex locally and tests each line. No
remote text filter is applied because arbitrary regex cannot be approximated by
turbopuffer's Glob operator.

Best for pattern searches like `def.*test` or `import\s+numpy`.

### BM25 Mode (`--mode bm25`)

**Full-text ranked retrieval.** Uses turbopuffer's built-in BM25 scoring on the
`text` field. Returns documents ranked by relevance score, with snippet
extraction (the most relevant line from each document).

Best for natural-language queries like "quadratic equation solver" where you
want ranked results rather than exact line matches.

---

## Demo: Agent With No Disk

This is a complete terminal session showing an agent that boots on fresh compute
with zero local storage, uses turbopuffer as its entire filesystem, and
demonstrates that state survives machine death.

All output below is from actual API calls against a live turbopuffer instance.

### Act 1 — Agent boots on fresh compute

```
$ tpfs init
✓ Workspace initialized
  mount:     agent-demo
  namespace: agent-demo__fs
  created:   /state /logs /output /scratch /project /input
  cwd:       /project

$ tpfs pwd
/project
```

The workspace is initialized. Six standard directories are created, session
state is written to `/state/session.json`, and `cwd` is set to `/project`.
All of this state lives in turbopuffer — there is no local disk.

### Act 2 — Agent writes project files

```
$ tpfs put /project/solver.py --text '...'
✓ /project/solver.py  (1.0K)

$ tpfs put /project/tests/test_solver.py --text '...'
✓ /project/tests/test_solver.py  (836B)

$ tpfs put /project/README.md --text '...'
✓ /project/README.md  (413B)
```

Each `put` creates the file document and ensures all parent directories exist
as explicit directory documents. The `tests/` directory was created automatically.

### Act 3 — Agent navigates and inspects

```
$ tpfs tree /
/
├── input/
├── logs/
├── output/
├── project/
│   ├── README.md
│   ├── solver.py
│   └── tests/
│       └── test_solver.py
├── scratch/
└── state/
    ├── session.json
    └── workspace.json

$ tpfs ls
  README.md  413B  text/markdown
  solver.py  1.0K  text/x-python
  tests/

$ tpfs cat solver.py
"""Quadratic solver v0.1 — a numpy-based root finder."""
import numpy as np
from typing import Tuple

def solve_quadratic(a: float, b: float, c: float) -> Tuple[complex, complex]:
    """Solve ax^2 + bx + c = 0 using the quadratic formula.
    ...
    """
    discriminant = b**2 - 4*a*c
    sqrt_disc = np.sqrt(complex(discriminant))
    x1 = (-b + sqrt_disc) / (2*a)
    x2 = (-b - sqrt_disc) / (2*a)
    return x1, x2
...

$ tpfs stat solver.py
  path: /project/solver.py
  kind: file
  mime: text/x-python
  size_bytes: 1058
  is_text: 1
  sha256: d88b0e4f20f53dbf984346b5eb10440ba1a354f10461e41ad1e86c3be8339dd0
  ext: .py
```

Note that `ls` and `cat` resolve paths relative to the durable `cwd` (`/project`),
just like a real shell.

### Act 4 — Agent searches the codebase

**Literal grep** — find all import statements:
```
$ tpfs grep "import" /
/project/README.md:8: from solver import solve_quadratic
/project/solver.py:2: import numpy as np
/project/solver.py:3: from typing import Tuple
/project/tests/test_solver.py:2: import numpy as np
/project/tests/test_solver.py:3: from solver import solve_quadratic, solve_batch
```

**Regex grep** — find function definitions matching a pattern:
```
$ tpfs grep "def.*solve" /project --mode regex
/project/solver.py:5: def solve_quadratic(a: float, b: float, c: float) -> Tuple[complex, complex]:
/project/solver.py:17: def solve_batch(coefficients: np.ndarray) -> np.ndarray:
```

**BM25 full-text search** — semantic ranked retrieval:
```
$ tpfs grep "quadratic equation roots" / --mode bm25
/project/README.md  score=3.3754
    # Quadratic Solver
/project/solver.py  score=1.6882
    """Quadratic solver v0.1 — a numpy-based root finder."""
/project/tests/test_solver.py  score=1.1508
    """Tests for the quadratic solver module."""
```

**Find with glob:**
```
$ tpfs find / --glob "*.py"
/project/solver.py
/project/tests/test_solver.py
```

### Act 5 — Agent edits a file

```
$ tpfs replace /project/solver.py --search "v0.1" --replace "v0.2"
✓ /project/solver.py: 1 match(es) replaced

$ tpfs head /project/solver.py -n 1
"""Quadratic solver v0.2 — a numpy-based root finder."""

$ tpfs wc /project/solver.py
  35 lines  142 words  1056 chars  1058 bytes  /project/solver.py
```

### Act 6 — Machine dies. New machine boots.

At this point, imagine the VM is terminated. A new machine spins up with a fresh
environment, the same API key, and no local state. Let's see what happens:

```
$ tpfs pwd
/project

$ tpfs head solver.py -n 1
"""Quadratic solver v0.2 — a numpy-based root finder."""

$ tpfs tree /project
/project
├── README.md
├── solver.py
└── tests/
    └── test_solver.py
```

**Everything is recovered.** The working directory, all files, the edit from
v0.1→v0.2 — all of it survived machine death because it was durably written to
turbopuffer. The new machine didn't need any local disk, any sidecar database,
or any sync daemon. It just read from turbopuffer.

### Act 6 — Hydrate → run tools locally → sync back

This is the bridge between "filesystem in the cloud" and "actually running
code." Hydrate pulls the workspace to a local directory so you can run real
tools (pytest, linters, compilers). Sync pushes changes back.

**Pull workspace to local disk:**
```
$ tpfs hydrate /tmp/sandbox --root /project --manifest-out /tmp/manifest.json
✓ Hydrated
  local:  /tmp/sandbox
  root:   /project
  files:  3
  dirs:   2
  manifest: /tmp/manifest.json
```

Now `/tmp/sandbox` contains real files — you can `cd` into it and run `pytest`,
`npm test`, `cargo build`, whatever your project needs.

**Agent modifies code locally:**
```
  • Modified solver.py (v0.2 → v0.3, added discriminant function)
  • Created REVIEW.md
  • Deleted README.md
```

**Push changes back to turbopuffer:**
```
$ tpfs sync /tmp/sandbox --manifest /tmp/manifest.json
✓ Synced
  created:   1
  modified:  1
  deleted:   2
  unchanged: 2
```

Sync compares local files against the hydration snapshot. It detects what was
created, modified, and deleted — and pushes only the changes. If someone else
modified a file in turbopuffer since hydration, sync reports a conflict instead
of silently overwriting.

**Verify the changes landed:**
```
$ tpfs tree /project
/project
├── REVIEW.md
└── solver.py

$ tpfs head /project/solver.py -n 1
"""Quadratic solver v0.3 — upgraded locally with new features."""
```

The local sandbox is disposable. Turbopuffer is the durable truth.

---

## Running the Full Demo

A self-contained demo script runs all seven acts end-to-end:

```bash
export TURBOPUFFER_API_KEY="..."
./demo.sh                   # uses mount "solver-agent"
./demo.sh my-custom-mount   # uses custom mount name
```

The script initializes a workspace, writes files, searches, edits, hydrates to
local disk, modifies locally, syncs back, and verifies recovery — all in about
25 seconds. It cleans up after itself.

---

## just-bash Integration

`tpfs_bash.ts` bridges [just-bash](https://github.com/vercel-labs/just-bash)
to `tpfs.py`, implementing the `IFileSystem` interface so that standard bash
commands (`ls`, `cat`, `echo >`, `cp`, `rm`, etc.) all operate on the
turbopuffer-backed filesystem transparently.

### How It Works

```
  just-bash                    tpfs_bash.ts                 tpfs.py
  ┌──────────┐                ┌──────────────┐            ┌──────────┐
  │ ls /     │──readdir("/")──│ TpfsPyFs     │──exec──────│ --json   │──→ turbopuffer
  │ cat foo  │──readFile()────│ implements   │  python3   │ ls /     │
  │ echo > f │──writeFile()───│ IFileSystem  │  tpfs.py   │ cat foo  │
  │ cp a b   │──cp()──────────│              │            │ put ...  │
  └──────────┘                └──────────────┘            └──────────┘
```

Each `IFileSystem` method calls `python3 tpfs.py --json <command>` via
`execFileSync`. The agent types normal bash — it never knows there's no disk.

### Usage

```typescript
import { createTpfsBash } from "./tpfs_bash.js";

const bash = await createTpfsBash({
  apiKey: process.env.TURBOPUFFER_API_KEY!,
  mount: "agent-demo",
});

// These are real bash commands — backed entirely by turbopuffer
await bash.exec("ls /project");
await bash.exec('echo "hello" > /project/hello.txt');
await bash.exec("cat /project/hello.txt");
await bash.exec("cp /project/hello.txt /output/hello.txt");
```

### Demo Output

```
$ pwd
/project

$ ls /
input
logs
output
project
scratch
state

$ ls /project
README.md
solver.py

$ echo "# Agent Notes" > /project/notes.md
$ echo "- All state lives in turbopuffer" >> /project/notes.md

$ cat /project/notes.md
# Agent Notes
- All state lives in turbopuffer

$ cp /project/notes.md /output/notes.md
$ ls /output
notes.md
```

Every command above went through: just-bash → `TpfsPyFs` adapter →
`python3 tpfs.py --json` → turbopuffer API. No local disk was touched.

---

## JSON Output

Every command supports `--json` for machine-consumable output:

```
$ tpfs --json stat /project/solver.py
{
  "id": "ef9c6fdbc5ef189b65bc49b78ef48554a0994170203b79b0263254a51f036821",
  "basename": "solver.py",
  "ext": ".py",
  "is_text": 1,
  "kind": "file",
  "mime": "text/x-python",
  "parent": "/project",
  "path": "/project/solver.py",
  "sha256": "...",
  "size_bytes": 1058
}
```

This makes `tpfs.py` usable as a stable machine-consumable bridge for
downstream runtimes (agent frameworks, CI pipelines, notebook kernels) that
need durable workspace semantics without reimplementing the storage layer.

---

## Compatibility

`tpfs.py` uses the same schema, document ID scheme (`SHA-256(path)`), namespace
naming convention (`{mount}__fs`), and workspace layout as the TypeScript
`turbopuffer-fs` implementation in this repository. Data written by either
implementation is readable by the other.

---

## What This Proves

1. **turbopuffer is a viable backend for filesystem-shaped state.** The query
   model (filters, Glob, BM25) maps naturally to filesystem operations. Ordered
   pagination gives you `ls`. Subtree filters give you `find`. Full-text search
   gives you `grep`.

2. **Agents don't need local disk.** Session state, project files, search
   indexes — it all lives in turbopuffer. A machine can die and be replaced
   without losing anything.

3. **The abstraction is thin.** `tpfs.py` is a single file with no framework,
   no ORM, no intermediate compilation step. Each filesystem operation is a
   direct turbopuffer API call. The mapping from POSIX semantics to document
   queries is straightforward and inspectable.

4. **grep is interesting.** The three-mode design (literal with remote
   narrowing, regex with local matching, BM25 for ranked retrieval) shows how
   a document database's query capabilities can be composed into familiar
   developer tools.
