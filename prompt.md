# Bottom-up crate documentation

Generate documentation for the Materialize codebase bottom-up, writing markdown files into `doc/developer/generated/`.
Do **not** modify any source files.

## Output structure

Mirror the `src/` tree under `doc/developer/generated/`:

```
doc/developer/generated/
  <crate-name>/
    _crate.md            # crate-level summary
    <module>.md           # one per top-level module
    <module>/
      _module.md          # submodule summary (synthesized from children)
      <child>.md          # one per child source file
      ...
```

* Each `.md` file documents the corresponding `.rs` source file or module directory.
* `_crate.md` documents the crate root (`lib.rs` or `main.rs`).
* `_module.md` documents a directory-based module (its `mod.rs` or parent declaration).
* Only create files for modules/files that carry meaningful logic — skip empty re-export-only files, generated code, and vendored code.

## Process (per crate)

Work in strict bottom-up order:

### 1. Leaf source files

For each `.rs` file in the crate:

1. Read the file.
2. Create a corresponding `.md` with:
   * **Header**: `# <crate>::<module path>` (e.g., `# mz-expr::scalar::func`)
   * **Summary**: 1-3 sentences — what this file provides, its key types/functions, how it fits into the parent module.
3. Log the action in `log.md`.

### 2. Directory modules

After all children of a directory are documented:

1. Read the child `.md` files you just wrote, plus the module's `mod.rs`.
2. Create `_module.md` synthesized from children:
   * What the module provides as a whole.
   * Key types/traits it exposes.
   * How its children relate to each other.
3. Log in `log.md`.

### 3. Crate root

After all modules are done:

1. Read child module docs and `lib.rs`/`main.rs`.
2. Create `_crate.md`:
   * Purpose of the crate in one sentence.
   * Overview of its module structure.
   * Key dependencies and downstream consumers (check `Cargo.toml`).
3. Log in `log.md`.

## Revision tracking

Each generated `.md` file should include a YAML front-matter block recording the git revision of the source file it was based on:

```markdown
---
source: src/ore/src/retry.rs
revision: 82d92a7fad
---
```

This enables future sessions to detect stale docs: if `git log --oneline <revision>..HEAD -- <source>` shows changes, the doc needs updating.

**Done**: All 88 existing docs backfilled with revision front-matter (2026-03-16).

## Writing rules

* Keep descriptions succinct — prefer one clear sentence over a vague paragraph.
* Use active voice, present tense ("Provides …", "Defines …", "Implements …").
* Only make claims based on the source code. Do not hallucinate.
* When a file is trivial or a thin re-export wrapper, note that and move on — no need for a full write-up.
* Do not document individual struct fields or function signatures in detail; focus on the role and relationships.

## Scope control

Each session documents one "area" — a batch of related small crates, or a single larger crate.
After finishing the batch, update the progress table, append to `log.md`, and stop.
The next session picks up where the previous one left off.

### Suggested next batches

1. ~~**`ore`** — done~~
2. ~~**Near-leaf crates** — done~~ (`audit-log`, `dyncfg`, `segment`, `http-util`, `auth`, `aws-util`, `pgtest`, `rocksdb-types`, `proto`, `tracing`, `ssh-util`)
3. ~~**Second-tier crates** — done~~ (`dyncfg-file`, `server-core`, `lowertest`, `sql-lexer`, `postgres-client`, `rocksdb`, `metrics`)
4. ~~**Backfill** — done~~

## Progress

| Crate | Status |
|-------|--------|
| `build-info` | done |
| `build-tools` | done |
| `tls-util` | done |
| `pgrepr-consts` | done |
| `ore-build` | done |
| `ore-proc` | done |
| `lowertest-derive` | done |
| `alloc` | done |
| `alloc-default` | done |
| `ore` | done |
| `aws-util` | done |
| `rocksdb-types` | done |
| `proto` | done |
| `ssh-util` | done |
| `tracing` | done |
| `http-util` | done |
| `auth` | done |
| `pgtest` | done |
| `audit-log` | done |
| `dyncfg` | done |
| `segment` | done |
| `postgres-client` | done |
| `rocksdb` | done |
| `metrics` | done |
| `dyncfg-file` | done |
| `server-core` | done |
| `lowertest` | done |
| `sql-lexer` | done |
| `authenticator` | done |
| `expr-derive-impl` | done |
| `license-keys` | done |
| `orchestrator-process` | done |
| `pgcopy` | done |
| `prof-http` | done |
| `secrets` | done |
| `orchestrator-kubernetes` | done |
| `prof` | done |
| `walkabout` | done |
| `kafka-util` | done |
| `sql-server-util` | done |
| `adapter-types` | done |
| `cloud-api` | done |
| `pgwire-common` | done |
| `postgres-util` | done |
| `aws-secrets-controller` | done |
| `cloud-provider` | done |
| `durable-cache` | done |
| `dyncfg-launchdarkly` | done |
| `dyncfgs` | done |
| `expr-derive` | done |
| `expr-test-util` | done |
| `foundationdb` | done |
| `metabase` | done |
| `oidc-mock` | done |
| `orchestrator` | done |
| `orchestrator-tracing` | done |
| `persist-proc` | done |
| `regexp` | done |
| `s3-datagen` | done |
| `arrow-util` | done |
| `cluster` | done |
| `cluster-client` | done |
| `lsp-server` | done |
| `pgtz` | done |
| `sql-pretty` | done |
| `ccsr` | done |
| `expr-parser` | done |
| `catalog-debug` | done |
| `materialized` | done |
| `npm` | done |
| `repr-test-util` | done |
| `workspace-hack` | done |
| `controller-types` | done |
| `clusterd` | done |
| `controller` | done |
| `balancerd` | done |
| `pgwire` | done |
| `sqllogictest` | done |
| `frontegg-auth` | done |
| `catalog-protos` | done |
| `fivetran-destination` | done |
| `orchestratord` | done |
| `service` | done |
| `mz-debug` | done |
| `timestamp-oracle` | done |
| `txn-wal` | done |
| `frontegg-client` | done |
| `mysql-util` | done |
| `storage-client` | done |
| `cloud-resources` | done |
| `interchange` | done |
| `storage-controller` | done |
| `persist-cli` | done |
| `pgrepr` | done |
| `avro` | done |
| `environmentd` | done |
| `mz` | done |
| `storage-operators` | done |
| `persist-types` | done |
| `compute-client` | done |
| `sql-parser` | done |
| `persist` | done |
| `compute-types` | done |
| `timely-util` | done |
| `frontegg-mock` | done |
| `persist-client` | done |
| `storage-types` | done |
| `catalog` | done |
| `compute` | done |
| `repr` | done |

## Tracking files

* `prompt.md` — this file (instructions + progress tracking)
* `log.md` — append-only log of every file written or skipped
