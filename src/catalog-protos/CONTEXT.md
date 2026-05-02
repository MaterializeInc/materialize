# mz-catalog-protos

The durable-schema layer for the Materialize catalog. Owns the protobuf type
definitions that `mz-catalog` persists to Persist, plus the frozen per-version
snapshots used in the catalog upgrade chain.

## Structure

| Path | LOC | What it owns |
|---|---|---|
| `src/objects.rs` | ~8,500 | Current schema (v81); generated from proto, symlinked to `objects_v81.rs` |
| `src/objects_v74.rs` – `src/objects_v81.rs` | ~24,000 | Frozen snapshots — one file per supported catalog version |
| `src/serialization.rs` | ~740 | Hand-written `RustType` adapters for orphan-rule types from `mz-repr`, `mz-sql`, `mz-compute-types`, `mz-storage-types` |
| `src/audit_log.rs` | ~900 | `RustType` adapters for `mz-audit-log` event types |
| `build.rs` | ~130 | MD5-hash guard: fails the build if any frozen snapshot is mutated |
| `objects_hashes.json` | — | Persisted hashes for all `objects*.rs` files |

## Key interfaces

- **`CATALOG_VERSION = 81`** — current schema version; bump this when schema changes.
- **`MIN_CATALOG_VERSION = 74`** — oldest version we can migrate from; deleting older snapshots is safe once this advances.
- **`objects::*`** — the live proto types imported by `mz-catalog` for all durable writes/reads.
- **`RustType<Proto>`** — the `mz-proto` trait bridging Rust domain types to proto wire types; implemented in `serialization` and `audit_log` to satisfy orphan rules.

## Seam

`mz-catalog-protos` is the Adapter between the domain types scattered across
`mz-repr`, `mz-sql`, `mz-compute-types`, `mz-storage-types`, `mz-audit-log`
and the single protobuf wire schema used for catalog persistence. It does not
implement any catalog logic; it is purely a schema + conversion layer.

## Architecture notes

- The hash-guard build script enforces snapshot immutability at compile time:
  mutating a frozen file without first copying it to a new version is a build
  error, not a silent data corruption risk.
- `objects.rs` and `objects_v{CATALOG_VERSION}.rs` must be byte-identical;
  `test_assert_current_snapshot` enforces this in CI.
- `serialization.rs` exists solely because of Rust orphan rules: `impl
  RustType<objects::RoleId> for mz_repr::RoleId` cannot live in `mz-catalog`
  (neither the trait nor the type is local there).
- The frozen snapshots are almost entirely generated code — ARCH_REVIEW is not
  warranted for the snapshot files themselves.

## What to bubble up to `src/CONTEXT.md`

- `mz-catalog-protos` is the canonical durable-schema seam; `mz-catalog`
  reads/writes through it exclusively and no other production crate touches the
  proto types directly.
- Catalog schema versioning is enforced structurally (frozen files + MD5
  guard), not by convention.
- The orphan-rule split (`serialization.rs`, `audit_log.rs`) is a recurring
  pattern in this part of the codebase; it is load-bearing and not cleanable
  without upstream trait or type consolidation.

## Cross-references

- Generated developer docs: `doc/developer/generated/catalog-protos/`.
- Primary consumer: `src/catalog/`.
