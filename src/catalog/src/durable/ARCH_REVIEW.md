# Architecture review — `catalog::durable`

Scope: `src/catalog/src/durable/` and all subdirs (≈ 13,781 LOC).

## 1. `Transaction` is a 4,117-LOC god struct with 19 parallel collection fields

**File:** `src/catalog/src/durable/transaction.rs:80–114`

**Problem**
`Transaction<'a>` carries 19 `TableTransaction<K,V>` fields, each with
its own CRUD method block. Adding a new catalog collection requires: (a) a new
field, (b) a new method group in `Transaction`, (c) new variants in
`Snapshot` and `TransactionBatch`, (d) new serialization in
`objects/serialization.rs`, and (e) new key-value types in `objects.rs`.
All five sites must be touched in lockstep. This is the *parallel predicates*
pattern: the same concept ("a catalog collection exists") is expressed across
five locations rather than one. The 103 public methods on `Transaction` are
the symptom.

**Solution sketch**
Extract `TableTransaction<K,V>` (already private) into a named trait
`CatalogCollection` with associated `Key`/`Value` types and standard
`get`, `set`, `delete`, `pending_updates` methods. Store collections in
an indexed registry rather than named fields — `Transaction` becomes a
thin wrapper over `BTreeMap<CollectionId, Box<dyn CatalogCollection>>`.
`TransactionBatch` becomes `Vec<CollectionUpdate>`. The `commit_transaction`
path is unchanged (still one `compare_and_append`).

**Benefits**
- Locality: a new collection is one new struct + one registry entry.
- Leverage: the commit/rollback logic in `Transaction::commit` and
  `TableTransaction::pending_updates` lives once.
- Depth: `Transaction` goes from 4K LOC to ~200 LOC; each collection is
  independently testable.

**Risk**
`TableTransaction` carries per-collection `uniqueness_violation` callbacks
(closure-typed `fn(a: &V, b: &V) -> bool`). A trait-object-based registry
needs these to be either per-type associated constants or runtime-registered
validators. Not a blocker, but requires care.

## 2. Upgrade chain: `v78_to_v79.rs` (868 LOC) is an outlier

**File:** `src/catalog/src/durable/upgrade/v78_to_v79.rs`

The proto→serde migration step is 5× larger than any other single step.
If a future migration reaches similar complexity, the "add one file" model
still works but a test scaffold for round-trip fidelity against a
representative production snapshot would reduce regression risk.
No structural change warranted; flag for test investment.

## 3. (Honest skip) Trait hierarchy (`OpenableDurableCatalogState` → `ReadOnlyDurableCatalogState` → `DurableCatalogState`)

Three traits, one concrete Implementation. The hierarchy is correct and
proportionate: it exists to enable read-only catalog consumers without
granting mutation rights. Deletion test fails — removing any tier removes
a real capability (read-only cursors, boot-only access). Keep.

## 4. (Honest skip) `TableTransaction` is private

`TableTransaction` is defined at `transaction.rs:2925` as a private
`struct`. Extracting it to its own module would not deepen the Interface —
it is already fully encapsulated behind `Transaction`'s public methods.
No action.

## What should bubble up to `src/catalog/CONTEXT.md`

- The durable/in-memory seam: `UpdateFrom<durable::*>` in `memory::objects` is
  the boundary; durable key-value types must not leak to adapter consumers.
- `Transaction` god-struct risk (Item 1 above) is the main structural concern in
  this crate.
- Upgrade chain mechanics: `CATALOG_VERSION = 81`, minimum = 74; each step
  is frozen-type isolated; `mz-catalog-protos` owns the version constant.
