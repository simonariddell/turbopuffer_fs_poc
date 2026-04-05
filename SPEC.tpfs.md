# tpfs specification

## 1. Scope

This document defines the normative behavioral contract for `tpfs`.

`tpfs` is a turbopuffer-backed, filesystem-shaped durable storage and shell
execution model. This document is intended for agents and implementations. It
is not an onboarding guide.

This specification covers:

- the persistent data model
- the path model
- the durability model
- core filesystem operation semantics
- shell/runtime integration semantics
- restart/recovery semantics
- unsupported feature semantics
- agent reasoning constraints

This specification does not define:

- POSIX completeness
- kernel, FUSE, or local-disk semantics
- hidden local overlay behavior
- multi-file transactional guarantees beyond what is explicitly stated here

---

## 2. Normative language

The key words MUST, MUST NOT, REQUIRED, SHALL, SHALL NOT, SHOULD, SHOULD NOT,
RECOMMENDED, MAY, and OPTIONAL in this document are to be interpreted as
normative requirements.

---

## 3. Core invariants

### 3.1 Durable truth

- All critical persisted state MUST live in turbopuffer-backed documents.
- A local VM, local disk, local sqlite database, local metadata sidecar, or
  writeback cache MUST NOT be treated as durable truth.

### 3.2 Success means durable

- An operation MUST NOT return success for a claimed persistent effect until
  that effect has been durably committed through turbopuffer.
- If durability is not yet achieved, the implementation MUST return failure or
  keep the operation incomplete.

### 3.3 Ephemeral compute is allowed

- In-memory parsing, buffering, shell interpretation, temporary reductions, and
  transient execution state MAY exist ephemerally.
- Any state required by another machine after restart MUST be durably written
  before success is returned.

### 3.4 Restartability

- A new VM MUST be able to reconstruct all required persistent workspace state
  from turbopuffer alone.
- Restartability MUST NOT depend on process-local state from a prior machine.

### 3.5 Honest architecture

- The implementation MUST preserve the conceptual layering:
  - filesystem intent → explicit plan
  - plan → runtime execution
  - raw rows → finalization
- Implementations MUST NOT collapse these layers into opaque side effects that
  make durable behavior uninspectable.

---

## 4. Entity model

### 4.1 Mount

- A mount is a logical workspace identifier.
- A mount MUST map to exactly one turbopuffer namespace.
- Namespace naming MUST be deterministic.
- Current implementation mapping:
  - `mount = "documents"`
  - `namespace = "documents__fs"`

### 4.2 Path

- A normalized absolute path identifies one logical filesystem node.
- One normalized absolute path MUST map to at most one logical document row.

### 4.3 Directory document

- Directories are explicit documents.
- A directory document MUST have `kind = "dir"`.

### 4.4 File document

- Files are explicit documents.
- A file document MUST have `kind = "file"`.

### 4.5 Text file representation

- Text file contents MUST be stored in the `text` field.
- Text files MUST have `is_text = 1`.

### 4.6 Binary file representation

- Binary file contents MUST be stored in the `blob_b64` field.
- Binary files MUST have `is_text = 0`.

### 4.7 Session state document

- Session state MUST be durably persisted as a document stored at the configured
  `session_state` path.
- Default path:
  - `/state/session.json`

Required fields:

- `cwd`
- `mount`
- `updated_at`
- `path`

Optional fields MAY include:

- `bundle_id`
- future workspace/session metadata

### 4.8 Command log document

- Durable command logs MUST be persisted as append-style text at the configured
  log path.
- Default log file:
  - `/logs/run.jsonl`

Each log entry MUST contain at least:

- `timestamp`
- `command`
- `cwd_before`
- `cwd_after`
- `exit_code`
- `stdout_preview`
- `stderr_preview`

---

## 5. Path model

### 5.1 Path requirements

- Implementations MUST operate on normalized absolute POSIX-like paths.
- Relative paths MUST be resolved only against an explicit cwd input.
- Implementations MUST NOT treat unnormalized paths as distinct durable nodes.

### 5.2 Root

- The root path is `/`.
- `/` is a valid directory path.
- `/` MUST NOT be removable.
- `/` MUST NOT be writable as a file target.

### 5.3 Normalization

Implementations MUST:

- require an absolute root when operating on normalized storage paths
- collapse repeated separators
- reject or normalize trivial relative markers where appropriate
- preserve logical identity for equivalent absolute paths

Implementations MUST NOT:

- allow path traversal outside root
- allow path semantics that depend on local host filesystem rules

### 5.4 Relative path resolution

When resolving a user path against cwd:

- `""`, `null`, `undefined`, `"."`, and `"./"` MUST resolve to cwd
- absolute paths MUST remain absolute
- `..` segments MUST resolve lexically against cwd and MUST clamp at root

### 5.5 Glob paths

- Glob filters MAY use path or basename-based matching.
- Glob syntax MUST be treated as filter syntax, not as a distinct durable path.

---

## 6. Workspace model

### 6.1 Workspace config

Implementations MUST support a durable workspace configuration model.

Default config:

- `entrypoint = /TASK.md`
- `bundle_manifest = /bundle.json`
- `session_state = /state/session.json`
- `logs_dir = /logs`
- `output_dir = /output`
- `scratch_dir = /scratch`
- `project_dir = /project`
- `input_dir = /input`

### 6.2 Config resolution order

An implementation SHOULD resolve workspace config in this order:

1. defaults
2. deployment config
3. bundle workspace config
4. explicit overrides

Later layers SHOULD override earlier layers.

### 6.3 Workspace initialization

When initializing a fresh mount:

- required workspace directories MUST be created durably
- the session-state parent directory MUST exist durably
- the initial session-state document MUST be durably written
- if a bundle id is known at initialization time, it MUST be persisted into the
  initial session state

---

## 7. Durability model

### 7.1 Persistent categories

The following categories are durable state:

- workspace files
- directory structure
- outputs
- command logs
- session state
- bundle metadata required after restart

### 7.2 Non-durable categories

The following categories are not required to survive restart unless explicitly
  persisted:

- process-local variables
- in-memory parser/interpreter state
- shell object instances
- temporary buffers

### 7.3 Acknowledgment boundary

For any operation that claims to change durable state:

- success MUST imply the durable write has completed
- failure MAY occur after partial commit
- implementations MUST NOT pretend full rollback exists unless explicitly
  implemented

### 7.4 Partial-commit semantics

- Multi-step operations MAY have partial-commit semantics.
- If an operation has partial-commit semantics, the implementation MUST NOT
  describe it as atomic.
- Logging and return values SHOULD make partial execution debuggable.

---

## 8. Core operation contracts

This section defines filesystem-shaped operation semantics.

### 8.1 `stat(path)`

Purpose:

- return metadata for the target node

Preconditions:

- `path` MUST be normalized/normalizable

Postconditions:

- returns metadata for the path if present
- returns `null` if absent

Durability:

- read-only

### 8.2 `ls(path)`

Purpose:

- return direct children of a directory

Preconditions:

- target MUST exist
- target MUST be a directory

Failure:

- MUST fail for missing target
- MUST fail for file target

Postconditions:

- returns direct children only
- MUST NOT recursively descend

Durability:

- read-only

### 8.3 `find(root, options)`

Purpose:

- return recursive matches under a root

Semantics:

- if root is a directory, returns descendants and/or self according to query
  semantics
- if root is a file, result MUST be constrained to that file when present

Filtering MAY include:

- glob
- kind
- ignore-case matching
- limit

Durability:

- read-only

### 8.4 `cat(path)` / `readText(path)`

Purpose:

- return full text contents of a text file

Preconditions:

- target MUST exist
- target MUST be a file
- target MUST be text

Failure:

- MUST fail for missing path
- MUST fail for directory target
- MUST fail for binary target

### 8.5 `readBytes(path)`

Purpose:

- return file contents as bytes

Semantics:

- for text files, UTF-8 bytes of the text payload MUST be returned
- for binary files, decoded binary bytes MUST be returned

Failure:

- MUST fail for directory target
- MUST fail for missing target

### 8.6 `head(path, n)` / `tail(path, n)`

Purpose:

- return leading or trailing text lines from a text file

Preconditions:

- same as text-read semantics

Failure:

- MUST fail on non-text files

### 8.7 `grep(root, pattern, options)`

Purpose:

- return grep/search results over candidate text files

Semantics:

- `grep` is the primary search entrypoint
- implementations MAY also expose `search(...)` as a first-class alias over the
  same engine
- `grep` behavior is mode-dependent

Required modes:

- `mode = "literal"`
  - exact literal substring grep
  - returns exact line hits
- `mode = "regex"`
  - exact regex grep
  - returns exact line hits
- `mode = "bm25"`
  - ranked lexical retrieval
  - returns ranked search hits, not exact grep lines

Search pipeline:

- candidate-stage remote search MAY over-approximate
- final-stage exact line matching MUST remain exact for exact modes
- ranked modes MAY return ranked search hits instead of line hits

Filtering MAY include:

- glob
- kind
- ignore-case matching
- limit
- mode-specific search arguments

Output contract:

- exact modes return:
  - `path`
  - `line_number`
  - `line`
- ranked modes return:
  - `path`
  - `score`
  - optional `snippet`
  - optional mode-specific metadata

Failure:

- empty pattern/query SHOULD fail
- missing root SHOULD fail

### 8.8 `mkdir(path)`

Purpose:

- create a directory and ensure required parents exist

Semantics:

- explicit directory documents MUST be written
- missing parents MUST be materialized as directory docs

Failure:

- MUST fail if an ancestor exists as a file
- MUST fail if target exists as a file

Durability:

- success MUST imply durable directory document writes

### 8.9 `putText(path, text)`

Purpose:

- whole-file overwrite of a text file

Semantics:

- target parents MUST exist durably after success
- file contents MUST be replaced, not partially patched

Failure:

- MUST fail if target path is `/`
- MUST fail if an ancestor is a file
- MUST fail if target exists as a directory

Durability:

- success MUST imply durable file write

### 8.10 `putBytes(path, bytes)`

Purpose:

- whole-file overwrite of a binary file

Semantics and failure:

- same structural rules as `putText`

### 8.11 `appendFile(path, content)`

Purpose:

- append content to an existing file or create a new file if absent

Current contract:

- existing file bytes are read
- appended bytes are concatenated in memory
- resulting file is fully rewritten

Consequences:

- append is not a native remote partial-write primitive
- append behaves as read-modify-write

Failure:

- MUST fail when target exists as a directory

Durability:

- success MUST imply the rewritten full file is durable

### 8.12 `rm(path, recursive)`

Purpose:

- delete a file or directory

Semantics:

- `rm("/")` MUST fail
- non-recursive directory delete MUST fail when the directory is non-empty
- recursive delete MAY be implemented as a paginated delete of all matching row
  ids

Durability:

- success MUST imply requested deletes have been durably applied

Atomicity:

- recursive delete MUST NOT be assumed atomic across all deleted rows

### 8.13 `exists(path)`

Purpose:

- boolean existence probe

Semantics:

- returns `true` iff `stat(path)` is non-null

### 8.14 `readdir(path)` / `readdirWithFileTypes(path)`

Purpose:

- expose `ls()` to shell/runtime consumers

Semantics:

- `readdir()` returns direct-child names
- `readdirWithFileTypes()` returns direct-child names with directory/file flags
- symlink flags MUST be false in the current model

### 8.15 `resolvePath(base, target)`

Purpose:

- lexical path resolution

Semantics:

- MUST resolve relative `target` against `base`
- MUST NOT consult host filesystem state

### 8.16 `realpath(path)`

Purpose:

- canonicalized path resolution

Current semantics:

- because symlinks are unsupported, `realpath()` is path normalization and cwd
  resolution only

### 8.17 `lstat(path)`

Current semantics:

- identical to `stat(path)` because symlink distinction is unsupported

### 8.18 `getAllPaths()`

Purpose:

- return a sorted durable path inventory for shell/path-enumeration consumers

Current semantics:

- inventory is seeded from a durable recursive enumeration at adapter boot
- inventory is updated for mutations performed through the active adapter
- inventory includes `/`

Constraints:

- consumers MUST NOT assume this inventory reflects concurrent out-of-band
  mutations performed through a different adapter instance until the adapter is
  re-booted or refreshed

### 8.19 `cp(src, dest, options)`

Purpose:

- durably copy a file or directory subtree

Semantics:

- file copy MUST read the durable source and durably write the destination
- directory copy MUST require `recursive = true`
- directory copy MUST materialize destination directories explicitly
- copying a directory into itself or its descendant MUST fail
- destination-path resolution MAY treat an existing directory destination as a
  parent container and append the source basename

Durability:

- success MUST imply the destination copy is durably written

Atomicity:

- recursive copy MUST NOT be assumed atomic across the full subtree

### 8.20 `mv(src, dest)`

Purpose:

- durably move a file or directory subtree

Current semantics:

- file move is implemented as durable copy then durable delete
- directory move is implemented as durable subtree copy then recursive delete
- moving `/` MUST fail
- moving a directory into itself or its descendant MUST fail

Durability:

- success MUST imply the destination exists durably and the source delete has
  completed durably for the completed operation

Atomicity:

- `mv` MUST NOT be assumed equivalent to POSIX atomic rename
- partial-commit behavior is possible because move is implemented as multiple
  durable operations

---

## 9. Runtime execution model

### 9.1 Plan execution

Implementations MUST preserve the following separation:

- plan construction
- runtime execution
- result finalization

### 9.2 Query execution

- Query steps MUST execute against the mount namespace.
- Missing namespaces MAY be treated as empty query results where first-write or
  empty-workspace semantics require it.

### 9.3 Ordered pagination

- Ordered recursive scans SHOULD be implemented via repeated ordered queries
  plus a `path > lastPath` style after-filter.

### 9.4 Finalization

- Finalizers MUST translate raw rows into filesystem-shaped results.
- Finalizers MUST enforce type/path expectations such as text-vs-binary checks.

---

## 10. Shell integration contract

### 10.1 Source of truth for cwd

- Durable cwd MUST be stored in the session-state document.
- `just-bash` internal cwd MUST NOT be treated as the long-term authoritative
  source of truth by itself.

### 10.2 Boot behavior

On shell boot:

- workspace config MUST be resolved
- session state MUST be loaded from turbopuffer if present
- otherwise the workspace MUST be initialized durably
- shell cwd MUST start from durable session cwd

### 10.3 Command execution contract

For each command:

1. durable session state is the starting context
2. filesystem operations execute through the turbopuffer-backed adapter
3. if cwd changes, updated session state MUST be durably persisted
4. command log entry MUST be durably appended
5. only then MAY the command be considered durably acknowledged

### 10.4 Special-cased shell builtins

Implementations MAY special-case shell commands such as:

- `pwd`
- `cd`

If special-cased:

- their persistent effects MUST still obey this specification

### 10.5 Post-command cwd

- If shell execution changes cwd, the durable session-state document MUST be
  updated before success is returned.
- The logged `cwd_after` value MUST match the persisted post-command cwd.

### 10.6 Session metadata preservation

- Updating cwd MUST NOT erase unrelated session metadata.
- Persisted session writes MUST preserve fields such as `bundle_id` unless the
  implementation explicitly intends to remove them.

---

## 11. Restart and recovery contract

### 11.1 Required recoverable state

A new VM booting against the same mount MUST be able to recover:

- workspace files
- directory structure
- session cwd
- command logs
- bundle/session metadata needed for continuation

### 11.2 Not required to recover automatically

The following are not required to survive unless separately persisted:

- interpreter-local temporary shell variables
- arbitrary prior process memory
- non-durable transient execution context

### 11.3 Cross-VM continuity

If VM A returns success for a durable mutation and then dies, VM B MUST be able
to observe that mutation by reading turbopuffer-backed state alone.

---

## 12. Unsupported and partial features

### 12.1 Current unsupported operations

The following operations are currently unsupported in the adapter:

- `chmod`
- `symlink`
- `link`
- `readlink`
- `utimes`

### 12.2 Current partial operations

The following operations are partial or constrained:

- `appendFile`
  - implemented as durable read-modify-write, not native append
- `realpath`
  - no symlink graph semantics
- `lstat`
  - no distinction from `stat`
- `getAllPaths`
  - inventory is adapter-scoped and not guaranteed to reflect concurrent
    out-of-band mutations without refresh/reboot
- `cp`
  - recursive subtree copy is non-atomic
- `mv`
  - implemented as copy+delete rather than atomic rename

### 12.3 Required unsupported-operation behavior

For unsupported features, implementations SHOULD:

- fail explicitly
- identify the operation as unsupported or not yet implemented
- avoid implying POSIX equivalence
- avoid silent emulation that violates durability invariants

### 12.4 Forbidden false claims

Implementations MUST NOT claim:

- symlink support when no durable link graph exists
- hard-link support when no shared inode/link model exists
- chmod/permission semantics when no durable permission model exists
- atomic multi-file commit semantics unless explicitly implemented

---

## 13. Non-POSIX differences

Agents and implementations MUST treat `tpfs` as non-POSIX-complete.

Current differences include:

- no symlink graph
- no hard links
- no permission model
- no hidden local authoritative writable overlay
- no random-write primitive
- append implemented as read-modify-write
- no guaranteed multi-file atomic commit
- object/document durability model instead of kernel filesystem semantics

---

## 14. Agent reasoning rules

Agents interacting with `tpfs` MUST:

- treat turbopuffer-backed state as the only durable truth
- treat success as the durable acknowledgment boundary
- expect explicit unsupported operations instead of implicit POSIX emulation
- use supported operations for any state that must survive restart

Agents MUST NOT assume:

- local VM disk is authoritative durable state
- a shell process alone preserves cwd across VM replacement
- unsupported operations can be safely approximated without spec support
- rename/copy/link semantics are POSIX-complete unless explicitly defined
- multi-file operations are atomic unless explicitly stated

Agents SHOULD:

- prefer supported whole-file operations
- rely on session-state persistence for cwd continuity
- rely on durable logs for execution history
- reason about recursive delete and append as potentially multi-step durable
  operations

---

## 15. Conformance

An implementation conforms to this specification iff it:

- preserves the core invariants in Section 3
- uses the entity/path/workspace model defined here
- only returns success for durable acknowledged effects
- preserves durable session/log behavior across restart
- does not claim unsupported semantics it does not implement
- preserves session metadata across cwd updates
- exposes unsupported features honestly

---

## 16. Implementation status snapshot

Current repository status at the time of writing:

- TypeScript core exists and is canonical
- durable shell integration exists
- Python product surface has been removed
- local and live test suites pass
- live shell boot preserves bundle session metadata on fresh mounts

This section is descriptive, not normative.
