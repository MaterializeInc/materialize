# Scoped feature flags — durable persistence (step 2) implementation notes

These are implementation notes for **step 2** of the scoped feature flags design
(`20260609_scoped_feature_flags.md`, section *"Storage: a durable cache, written
solely by the sync loop"*). Steps 0, 1, and the persistence-ready
re-architecture (working copy in the coordinator) are already implemented on
this branch.

> Status: **not yet implemented.** This branch adds only the in-memory working
> copy (`ScopedParameters` in the coordinator) plus *inert stubs* for the durable
> `Transaction` accessors (`get/upsert/remove_replica_system_config` in
> `src/catalog/src/durable/transaction.rs`, and the `ReplicaSystemConfiguration`
> object type in `src/catalog/src/durable/objects.rs`). The durable collection
> itself is intentionally deferred, because completing it requires running the
> catalog migration tooling and test suite (golden encoding files) — see
> *"Why this isn't on the branch yet"*.

## Goal

Persist scoped overrides so they survive an `environmentd` restart and an LD
outage; fall back to the environment-wide value only on a *cold cache* (an
object never yet evaluated against LD). The sync loop is the **sole writer**.

## Schema decision

The design writes the durable key as `(Cluster(ClusterId) | Replica(ReplicaId),
parameter_name) -> value`. We implement this as **one durable collection per
scope** rather than a single collection with a sum-typed key, because the durable
proto model is flat (no oneof ergonomics) and each collection then mirrors the
existing `system_configurations` collection almost verbatim:

- `replica_system_configurations`, keyed by `(ReplicaId, name) -> String` —
  added in step 2 (this note).
- `cluster_system_configurations`, keyed by `(ClusterId, name) -> String` —
  added in step 3 (cluster-coherent), identical recipe with `ClusterId`.

Both are the per-object analog of `system_configurations` (the `ALTER SYSTEM`
durable collection). Object ids are never reused
(`USER_CLUSTER_ID_ALLOC_KEY`), so entries for a dropped object are inert and GC
is hygiene-only (lazy prune of ids absent from the catalog).

## Version-bump recipe (catalog snapshot + migration)

Adding a durable collection changes `src/catalog-protos/src/objects.rs`, which
requires a catalog version bump. The migration is a **no-op** because the change
only *adds* a `StateUpdateKind` variant and message types (JSON-compatible — the
same situation as `v84_to_v85`). Steps (mirroring the header doc in
`src/catalog/src/durable/upgrade.rs`):

1. `objects_v85.rs` already exists and equals the current `objects.rs`.
2. Bump `CATALOG_VERSION` 85 → 86 in `src/catalog-protos/src/lib.rs`; add
   `pub mod objects_v86;`.
3. Make the proto changes in `src/catalog-protos/src/objects.rs` (below).
4. Copy the new `objects.rs` to `src/catalog-protos/src/objects_v86.rs`
   verbatim (the snapshots are standalone module copies; v85 and the current
   `objects.rs` are byte-identical today).
5. Update `src/catalog-protos/objects_hashes.json`: set `objects.rs` to its new
   md5 (`md5sum src/catalog-protos/src/objects.rs`). `build.rs` auto-persists the
   hash for the new `objects_v86.rs`, but `objects.rs`'s changed hash makes the
   build hard-fail until updated by hand.
6. Add `v86` to the second group of the `objects!` macro in
   `src/catalog/src/durable/upgrade.rs`.
7. Create `src/catalog/src/durable/upgrade/v85_to_v86.rs` — a no-op migration
   (`Vec::new()`), like `v84_to_v85.rs`. Add `mod v85_to_v86;` and a `85 => {
   run_versioned_upgrade(..., v85_to_v86::upgrade).await }` arm to `run_upgrade`.
8. Generate the encoding golden files:
   `cargo test --package mz-catalog --lib durable::upgrade::tests::generate_missing_encodings -- --ignored`
   then run the catalog test suite to validate round-trips.

## The ~27 collection sites (mirror `system_configurations` / `ServerConfiguration`)

For `replica_system_configurations`, keyed by `(ReplicaId, name) -> value`. The
proto `ReplicaId` enum (`System(u64) | User(u64)`) already exists and has a
`RustType` impl (used by `ClusterReplicaKey`).

**`src/catalog-protos/src/objects.rs`** (proto types):
- `ReplicaSystemConfigurationKey { replica_id: ReplicaId, name: String }`
- `ReplicaSystemConfigurationValue { value: String }`
- `StateUpdateKind::ReplicaSystemConfiguration(ReplicaSystemConfiguration)`
- wrapper `ReplicaSystemConfiguration { key, value }`
  (then mirror all four into `objects_v86.rs`).

**`src/catalog/src/durable/objects.rs`**:
- `ReplicaSystemConfiguration { replica_id, name, value }` (added on this branch)
  + a `DurableType` impl (`Key = ReplicaSystemConfigurationKey`, `Value =
  ReplicaSystemConfigurationValue`).
- add the `Snapshot.replica_system_configurations` field.

**`src/catalog/src/durable/objects/serialization.rs`**:
- `RustType<proto::ReplicaSystemConfigurationKey>` and `…Value` impls (the key
  reuses the existing `ReplicaId <-> proto::ReplicaId` `RustType`).

**`src/catalog/src/durable/objects/state_update.rs`**:
- `StateUpdateKind::ReplicaSystemConfiguration(key, value)` variant.
- arms in `collection_type()`, `into_proto_owned()`, `from_proto()`.
- `TransactionBatch` destructure + `from_batch` + `.chain(...)`.

**`src/catalog/src/durable/transaction.rs`**:
- `Transaction.replica_system_configurations:
  TableTransaction<ReplicaSystemConfigurationKey, ReplicaSystemConfigurationValue>`.
- `Transaction::new` Snapshot destructure + `TableTransaction::new(...)`.
- replace the **stub** accessors (added on this branch) with real
  `set(...)`/`delete(...)`/`items()` bodies.
- `TransactionBatch.replica_system_configurations` field + `is_empty()` +
  `pending()` + `current_items_proto()`.

**`src/catalog/src/durable/persist.rs`**: snapshot-apply arm + trace-building arm.

**`src/catalog/src/durable/debug.rs`**: `CollectionType` variant,
`collection_impl!` invocation, `Trace` field, `Trace::new()` init.

**`src/catalog/src/memory/objects.rs`**: `StateUpdateKind` and
`BootstrapStateUpdateKind` variants + the two conversion arms.

## Wiring (step 2b, against the real accessors)

Once the collection exists, replace the stub bodies and wire the coordinator:

- **Startup (bootstrap):** read `get_replica_system_configurations()` into
  `Coordinator::scoped_system_parameters.replica`, then call
  `reconcile_scoped_system_parameters` so values are in effect *before* the
  first LD sync. (Today the working copy starts empty.)
- **Reconcile (sole writer):** in `reconcile_scoped_system_parameters`, after
  replacing the working copy, persist the diff against the durable collection
  via a catalog transaction — `upsert` changed entries, `remove` ones LD no
  longer serves (the sync loop already sends the *complete* desired state, so
  the coordinator diffs old-vs-new working copy). Prefer a dedicated
  `catalog::Op` (mirroring `Op::UpdateSystemConfiguration`) so it flows through
  `catalog_transact`.
- **LD slow/unavailable:** keep serving the loaded values (no revert).
- **Cold cache:** empty working copy ⇒ env-wide fallback (already the behavior).
- **Lazy GC:** on startup and each reconcile, drop persisted entries whose object
  id is absent from the catalog.

## Why this isn't on the branch yet

The version bump regenerates encoding **golden files** (step 8) and is validated
by the catalog migration test suite. That tooling can't run in the current
remote environment (the toolchain only builds with `--ignore-rust-version`, and
the catalog test binary exhausts the container disk). A durable catalog
migration that compiles but whose golden files/round-trip tests are unrun is a
production hazard, so the collection is deferred to a session with the proper
toolchain, where steps 1–8 above are mechanical and the migration is a no-op.
