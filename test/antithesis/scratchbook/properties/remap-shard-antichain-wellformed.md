# remap-shard-antichain-wellformed

## Summary

At every Materialize timestamp `t`, the contents of the source's remap shard accumulated to `t` form a well-formed `Antichain<KafkaTimestamp>`. Each source-time element has multiplicity exactly 1; for multi-partition Kafka sources, there is one element per partition range with no overlaps.

## Origin

This invariant is stated explicitly in the `ReclockOperator` doc comment (`src/storage/src/source/reclock.rs:31-34`):

> "The `ReclockOperator` will always maintain the invariant that for any time `IntoTime` the remap collection accumulates into an Antichain where each `FromTime` timestamp has frequency `1`. In other words the remap collection describes a well formed `Antichain<FromTime>` as it is marching forwards."

## Code paths

- `src/storage/src/source/reclock.rs:118-169` — `ReclockOperator::mint`. Each call:
  1. Emits retractions (`-1`) of the prior `source_upper`.
  2. Emits insertions (`+1`) of the new `source_upper`.
  3. Calls `compare_and_append` on the remap shard.
  4. On `UpperMismatch`, `sync()` and retry.
- `src/storage/src/source/reclock.rs:124` — `assert!(!new_into_upper.less_equal(&binding_ts))` guards the mint precondition.
- `src/storage/src/source/reclock.rs:321` — `assert!(prev < RB::before(pid))` guards the partition-range ordering.
- `src/storage/src/source/reclock/compat.rs:144` — `assert!` on persist handle state.
- `src/storage/src/source/reclock/compat.rs:306` — `panic!("compare_and_append failed: {invalid_use}")` for genuinely invalid CaS calls.

## Antithesis form

Two complementary checks:

1. **SUT-side** inside `ReclockOperator::sync` / `mint`, after every update: walk the local accumulated state and `assert_always!(antichain_wellformed, "reclock: remap shard accumulates to well-formed antichain")` — every source-time element has multiplicity 1. This is the tightest expression of the invariant.

2. **Workload-side** as a periodic SQL probe: select the remap shard's contents (via `mz_internal` introspection views if available) and verify the well-formed property externally. This catches the case where the SUT-side check is correct but the durable persist state diverges.

## What goes wrong on violation

A malformed remap antichain corrupts every subsequent restart's resume frontier. The source either skips data (resume frontier too far ahead), re-reads data (too far back), or panics in downstream operators that depend on well-formed antichains (e.g., the as_of computation in commit `e3805ad790`).

## Antithesis angle

- Concurrent reclock writers across restart: kill the storage worker mid-mint, restart, the new worker must `sync()` the durable state and re-mint from there. If `sync()` is wrong, the new worker may insert without retracting, breaking multiplicity.
- Partition adds/removes interleaved with mints: the partition-range encoding in `RangeBound<PartitionId>` is the part that has to stay consistent across discovery and binding.
- `compare_and_append` retry loop interactions: the historical bug at reclock.rs:160-166 was retried correctly, but the cached upper drift (commit `e3805ad790`) bypassed it.

## Open question (resolved)

Q: Can the in-memory `source_upper` and the persisted remap state ever diverge enough that the operator emits a malformed update batch?

A: The `MutableAntichain<FromTime>` in `ReclockOperator::source_upper` is the source of truth for what *should* be persisted next. `mint()` constructs the update batch by diffing the new desired upper against the current `source_upper`. The retraction-insertion structure is what preserves the antichain-multiplicity invariant. The only divergence path is if `sync()` after `UpperMismatch` reads a state inconsistent with what `source_upper` thinks — i.e., a true persist corruption. The assertion at compat.rs:144 is meant to catch this.

## Existing instrumentation

The `assert!` and `panic!` calls at reclock.rs:124, :321 and compat.rs:144, :306 exist. None of them check the *accumulated antichain* property directly — they check local invariants. The recommended new assertion is a `assert_always!` over the in-memory accumulator that runs at every state transition.

## Provenance

Surfaced by: Data Integrity, Distributed Coordination. Foundational invariant for the entire reclocking subsystem.
