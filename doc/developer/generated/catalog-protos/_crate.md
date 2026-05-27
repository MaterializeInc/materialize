---
source: src/catalog-protos/src/lib.rs
revision: 7f722d60df
---

# mz-catalog-protos

Provides all Rust types durably persisted in the Materialize catalog, along with `RustType` conversion impls bridging those types to protobuf.
The crate exposes the current schema as `objects` (currently v85) plus frozen snapshots `objects_v74` through `objects_v85` used for migrations.
`CATALOG_VERSION` (85) and `MIN_CATALOG_VERSION` (74) constants bound the supported migration range; the build script validates file hashes to prevent accidental mutation of snapshots.
The crate has an optional `proptest` feature; `derive(Arbitrary)` on catalog types is compiled only when the `test` cfg or the `proptest` feature is enabled.
Key dependencies are `mz-proto`, `mz-repr`, `mz-sql`, `mz-audit-log`, `mz-compute-types`, and `mz-storage-types`; the primary consumer is `mz-catalog`.
