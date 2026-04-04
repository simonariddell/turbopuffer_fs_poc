# turbopuffer-fs

`turbopuffer-fs` is a small Python library that exposes a filesystem-shaped
interface over turbopuffer without pretending turbopuffer is a local POSIX
filesystem.

The design is intentionally literal:

1. compile filesystem intent into explicit turbopuffer plans,
2. execute those plans with the real turbopuffer client,
3. apply small pure post-processing steps locally.

The result is closer to an object-store filesystem view than a kernel
filesystem:

- backend-first
- filesystem illusion second
- no sidecar metadata database
- no chunking
- no daemon
- no FUSE

## Model

One mount maps to one turbopuffer namespace:

- mount `documents`
- namespace `documents__fs`

One normalized absolute path maps to one document.

Directories are explicit documents.
Files are explicit documents.

Text files store full text in `text`.
Binary files store full bytes in `blob_b64`.

## Architecture

The package is split into explicit layers:

- `turbopuffer_fs.fs`
  - pure filesystem intent -> plan dicts
- `turbopuffer_fs.runtime`
  - plan dicts -> turbopuffer API calls
- `turbopuffer_fs.post`
  - raw rows -> final filesystem-ish results
- `turbopuffer_fs.live`
  - tiny convenience wrappers

The plans are plain dicts so you can inspect exactly what queries and writes
will happen.

## Installation

```bash
python3 -m pip install turbopuffer-fs
```

For development:

```bash
python3 -m pip install -e ".[dev]"
```

## Quick start

```python
from turbopuffer_fs import make_client, put_text, ls, read_text

client = make_client()

put_text(client, "documents", "/notes/hello.txt", "hello turbopuffer\n")
print(ls(client, "documents", "/notes"))
print(read_text(client, "documents", "/notes/hello.txt"))
```

## Public API

### Pure plan functions

```python
from turbopuffer_fs import (
    stat_plan,
    ls_plan,
    find_plan,
    cat_plan,
    head_plan,
    tail_plan,
    grep_plan,
    read_text_plan,
    read_bytes_plan,
    mkdir_plan,
    put_text_plan,
    put_bytes_plan,
    rm_plan,
    upsert_rows_plan,
)
```

### Live wrappers

```python
from turbopuffer_fs import (
    make_client,
    mount_namespace,
    list_mounts,
    stat,
    ls,
    find,
    cat,
    head,
    tail,
    grep,
    read_text,
    read_bytes,
    mkdir,
    put_text,
    put_bytes,
    rm,
    ingest_directory,
)
```

## Semantics

- paths are normalized absolute POSIX-like paths
- `stat(path)` returns the document row or `None`
- `ls(path)` returns direct children and raises on missing paths or file targets
- `find(root)` is recursive and ordered by path
- `cat(path)` and `read_text(path)` only work for text files
- `read_bytes(path)` works for both text and binary files
- `grep(...)` is literal substring grep only
- `mkdir(path)` creates explicit directory documents and ensures parents exist
- `put_text(...)` / `put_bytes(...)` are whole-file overwrites
- `rm(path, recursive=False)` deletes files or directories with explicit
  recursive behavior

## Examples

### Inspect a plan

```python
from turbopuffer_fs import stat_plan

plan = stat_plan("documents__fs", "/notes/taxes.csv")
print(plan)
```

### List a directory

```python
from turbopuffer_fs import ls

rows = ls(client, "documents", "/notes")
for row in rows:
    print(row["path"], row["kind"])
```

### Read text and bytes

```python
from turbopuffer_fs import read_text, read_bytes

text = read_text(client, "documents", "/notes/taxes.csv")
data = read_bytes(client, "documents", "/photos/kid.jpg")
```

### Literal grep

```python
from turbopuffer_fs import grep

matches = grep(
    client,
    "documents",
    "/notes",
    "oauth",
    ignore_case=True,
    glob="*.md",
)
```

### Put and remove files

```python
from turbopuffer_fs import put_text, rm

put_text(client, "documents", "/notes/todo.txt", "finish taxes\n")
rm(client, "documents", "/notes/todo.txt")
```

### Ingest a local tree

```python
from turbopuffer_fs import ingest_directory

ingest_directory(client, "documents", "./local-docs", mount_root="/archive")
```

## CLI

The package also ships with a tiny JSON-first CLI:

```bash
tpfs --region aws-us-west-2 mounts
tpfs --region aws-us-west-2 ls documents /notes
tpfs --region aws-us-west-2 put-text documents /notes/todo.txt --stdin
tpfs --region aws-us-west-2 grep documents / oauth --ignore-case
```

Use `--api-key`, `--region`, or `--base-url` explicitly, or rely on the
corresponding environment variables you export in your shell.

## Dogfooding harness

There is also a seeded live dogfood runner for exercising the wrapper the way an
agent would:

```bash
python3 -m turbopuffer_fs.dogfood \
  --api-key "$TURBOPUFFER_API_KEY" \
  --region "$TURBOPUFFER_REGION" \
  --seed 7 \
  --steps 50 \
  --check-every 5
```

This runner:
- creates a fresh mount/namespace
- performs randomized filesystem-shaped operations
- maintains a local shadow model
- checks invariants as it goes
- can emit replayable failure artifacts with `--artifact-dir`

## Non-goals

This library intentionally does not implement:

- FUSE
- kernel callbacks
- POSIX-complete semantics
- append or random writes
- permissions
- hard links
- caching
- chunking
- background sync
- regex grep
- external metadata stores

The goal is a readable filesystem-shaped query compiler/runtime over
turbopuffer, not a framework.
