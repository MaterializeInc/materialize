---
source: src/catalog-protos/src/lib.rs
revision: 70b83779a4
---

# mz-catalog-protos

Provides all Rust types durably persisted in the Materialize catalog.
The crate exposes the current schema as `objects` (currently v92) plus frozen snapshots `objects_v74` through `objects_v92` used for migrations.
`CATALOG_VERSION` (92) and `MIN_CATALOG_VERSION` (74) constants bound the supported migration range; the build script validates file hashes to prevent accidental mutation of snapshots.
A `From<String> for objects::StringWrapper` impl is defined in `lib.rs` for convenience.
The crate has an optional `proptest` feature; `derive(Arbitrary)` on catalog types is compiled only when the `test` cfg or the `proptest` feature is enabled.
Key dependencies are `mz-proto`, `mz-repr`, `mz-sql`, `mz-audit-log`, `mz-compute-types`, and `mz-storage-types`; the primary consumer is `mz-catalog`.
