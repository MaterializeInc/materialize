# Migrate renderer from RowRowSpine → FactRowRowSpine

## Status: Tier 5 complete (closed out)

All tiers have landed.
`ArrangementFlavor` and `JoinedFlavor` expose `Local` + `Trace` only; the old `FactLocal` variant was merged into the renamed `Local`.
`TraceBundle::oks` now stores `PaddedTrace<RowRowAgent>` where `RowRowAgent` is an alias for the factorized spine (`FactValSpine<Row, Row, T, R>` from `mz_timely_util::columnar::factorized`).
The dyncfg `enable_compute_factorized_arrangement` is gone.
There is one local-arrangement flavor, backed by the factorized trie spine, and nothing else.

## Goal (original)

Move every `ArrangementFlavor::Local` / `JoinedFlavor::Local` producer and
consumer over to their `FactLocal` equivalents.
Once done, delete the original `Local` path, rename `FactLocal` → `Local`, drop the `enable_compute_factorized_arrangement` dyncfg.
The codebase ends up with a single local-arrangement flavor backed by the factorized trie spine, and nothing else.

## Constraints

* `RowRowSpine` cursor yields `Key<'a> = DatumSeq<'a>`, `Val<'a> = DatumSeq<'a>`.
* `FactRowRowSpine` cursor yields `Key<'a> = &'a RowRef`, `Val<'a> = &'a RowRef`.
* `DatumSeq` and `&RowRef` both impl `ToDatumIter` — the canonical escape hatch.
* `Diff` is returned by value on the Fact path (`LayoutExt::DiffGat<'a> = Diff`),
  but by reference on the RowRow path (`= &'a Diff`). Consumers that destructure
  `(t, d)` from cursor or merge methods need to use owned semantics.
* `Row` *is* `Columnar` (container = `Rows`, ref = `&RowRef`). `DataflowError` is
  not. Any `KeyValSpine<DataflowError, …>` must stay on `RowValSpine`; don't
  touch `RowErr*`.
* The `reduce`-style reduction pipeline (`mz_reduce_abelian` etc.) requires a
  `Builder::Input` that implements `InternalMerge`. `KVUpdates` does not — so
  reduce output stays on `RowValBuilder` / `RowValSpine`; only the *input* trace
  to the reduce is migratable.

## Inventory of consumers

Files that pattern-match on `ArrangementFlavor::Local`, in approximate order of
migration difficulty:

### Tier 1 — trivial (key-only peek/consumption) — DONE
* `src/compute/src/render/context.rs`
  * `as_collection` (line 271)
  * `flat_map` (line 327)
  * `scope` / `enter_region` / `leave_region` (trivial forwarding)
  * `ensure_collections` producer (line 869) — currently flagged
* `src/compute/src/render.rs`
  * Hydration logging (line 1421) — already passes through FactLocal via
    stream-only handling.

### Tier 2 — cursor walks with `Key<'_>` / `Val<'_>` — DONE
* `src/compute/src/render.rs:733` — `export_index` installs the trace into a
  `TraceBundle`. `TraceBundle` is currently generic over the trace type
  (`RowRowAgent`). Needs one of:
  * Variant `TraceBundle::Fact(FactRowRowAgent, ErrAgent)`, or
  * Change `TraceBundle` to hold the Fact trace directly.
  Knock-on: `compute_state.traces` (the peek path) must handle both. Peek
  cursor code in `src/compute/src/server/peek.rs` assumes `DatumSeq`.
* `src/compute/src/render.rs:818` — `export_index_iterative`. Same pattern as
  above but inside an iterative scope; also constructs a `RowRowBuilder` via
  `mz_arrange::<RowRowBatcher, RowRowBuilder, _>` to re-arrange after
  `leave(outer)`. Needs a Fact-variant rearrange using
  `FactRowRowBatcher/FactRowRowBuilder/FactRowRowSpine`.
* `src/compute/src/render/threshold.rs:91` — `threshold_arrangement` takes
  a builder type parameter. Needs a Fact-flavored variant or a builder switch
  based on input flavor.

### Tier 3 — reduce / top_k output — DONE
* `src/compute/src/render/reduce.rs:181` — constructs an `ArrangementFlavor::Local`
  from a `reduce::reduce` or `reduce_abelian` call. The *output* of reduce is
  built by `RowValBuilder<Row, T, R>`. Its `Input` type is a columnation stack
  which is `InternalMerge`-able — this is why `reduce` currently uses
  `RowValSpine`. Migrating means teaching `reduce` to emit a `FactLocal`:
  * If reduce still builds into `RowRowSpine`, wrap the result in a
    `Local`→`FactLocal` conversion (as_collection + re-arrange) — kills
    factorization upside.
  * Real migration: add a Fact-compatible reduce output path (likely a
    reduce-abelian variant that materializes keyed `Row` rows and feeds them
    to a `FactRowRowColBatcher`). Significant work.
* `src/compute/src/render/top_k.rs` and friends (if they construct `Local`) —
  same pattern.

### Tier 4 — joins — DONE
* `src/compute/src/render/join/linear_join.rs`
  * `JoinedFlavor::Local` (line 186) — the streamed-side arrangement. Migrate
    to `FactLocal`. `differential_join_inner` already accepts any
    `TraceReader` where `Key<'a>: ToDatumIter` + matching key types, so it's
    a type swap.
  * Cross-product match (line 412+) — all four quadrants (Local/Trace ×
    Local/Trace) currently in place. Migrate `JoinedFlavor::Local` →
    `FactLocal`, and on the inner match accept `ArrangementFlavor::FactLocal`
    as the lookup. Trace side (importing) is separate; we can keep a mixed
    path until imports are migrated too.
  * `stage_joined` construction on line 386 calls
    `mz_arrange_core::<_, Col2ValBatcher, RowRowBuilder, RowRowSpine>` to
    pre-arrange the streamed side. Swap to
    `FactRowRowColBatcher/FactRowRowBuilder/FactRowRowSpine`.
* `src/compute/src/render/join/delta_join.rs:88` — imports arrangements and
  enters them into the delta-join region. The stored type is
  `Result<Arranged<_, RowRowAgent>, Arranged<_, RowRowEnter>>` —
  migrate the `Ok` side to `FactRowRowAgent`, which touches the
  `build_delta_join` consumer too.

### Tier 5 — imported / exported traces (`ArrangementFlavor::Trace`) — DONE
* `compute_state.traces` (`TraceBundle` storage) and the peek path both
  assume `RowRowAgent`-shaped traces. Migrating `Trace` means changing the
  cross-dataflow storage format and all peek/subscribe consumers. This
  probably wants its own plan once the Local path is done.

See `.claude/plans/factorized-tier5.md` for the task-by-task breakdown that
was executed.
Summary of what landed:

* Peek paths (`compute_state.rs`, `compute_state/peek_result_iterator.rs`)
  loosened their `DiffGat` bound from `= &'a Diff` to `Copy + Into<Diff>`,
  so both owned and by-reference layouts satisfy the trait.
* `TraceBundle::oks` flipped from `PaddedTrace<RowRowAgent>` (old, DatumSeq-
  yielding) to `PaddedTrace<FactRowRowAgent>`.
  Bridges in `export_index` / `export_index_iterative` were deleted; the
  Fact arrangement installs directly.
* Delta-join's lookup slot now holds
  `Result<Arranged<FactRowRowAgent>, Arranged<FactRowRowEnter>>`.
* Linear-join's pre-arrange unconditionally emits Fact; the dyncfg branch is
  gone.
  `JoinedFlavor::Local` was renamed to reuse the Fact slot.
* `ArrangementFlavor::Local` was deleted; `FactLocal` was renamed to `Local`.
  All match sites (`context.rs`, `reduce.rs`, `threshold.rs`, `linear_join.rs`,
  `delta_join.rs`, `render.rs`) collapsed to `{Local, Trace}`.
* `FactRowRow*` aliases were renamed to `RowRow*` in `typedefs.rs`; the old
  `DatumContainer`-backed `RowRowLayout` / `RowRowSpine` / `RowRowBatcher` /
  `RowRowBuilder` are gone from `row_spine.rs`.
  `RowValLayout` (and therefore `DatumContainer`) is still kept for reduce's
  internal input arrangement and for `RowSpine` (key-only).
* The `ENABLE_COMPUTE_FACTORIZED_ARRANGEMENT` dyncfg was removed from
  `src/compute-types/src/dyncfgs.rs` and its registrations in
  `misc/python/materialize/parallel_workload/action.py` and
  `misc/python/materialize/mzcompose/__init__.py`.

## Migration order (one PR per tier)

1. **Tier 1** — DONE. Flipped the flag default to `true` once Tier 2 was merged
   and arrange sites only produced `FactLocal`. Deleted `ArrangementFlavor::Local`
   at the end of Tier 5.
2. **Tier 2** — DONE. Added Fact-aware `TraceBundle` handling; swapped
   threshold's builder. `export_index_iterative` got a Fact rearrange.
3. **Tier 3** — DONE. Reduce pipeline migrated via a Fact-output reduce path.
4. **Tier 4** — DONE. Joins swapped: `JoinedFlavor::Local` and the cross-product
   arms. Bridges at the join boundaries were later deleted in Tier 5.
5. **Tier 5** — DONE. Imported traces migrated: `TraceBundle`, peek path,
   subscribe, delta-join lookup.
6. **Cleanup** — DONE. Deleted `ArrangementFlavor::Local`, renamed `FactLocal`
   → `Local`, dropped `FactRowRow*` aliases in favor of `RowRow*`, dropped the
   dyncfg.

## Verification per step

After each tier:
* `cargo check -p mz-compute` + workspace.
* `cargo test -p mz-compute --test lib` (datadriven reduce / top_k / join).
* `bin/sqllogictest -- test/sqllogictest/transform/`.
* A minimal `mzcompose` sanity run on a MV that exercises the tier's
  consumer.
* For Tier 3+ also run a microbenchmark (spines_row.rs example, or a
  targeted MV) to confirm we're not regressing.

## Open questions (resolved)

* Reduce pipeline: is a Fact-output reduce worth the complexity, or do we
  accept an `as_collection` → re-arrange bridge at that boundary and keep
  reduce on `RowValSpine` forever? *Resolved:* reduce's output is on
  FactLocal via a DatumSeqRow-style path; its internal input arrangement
  stays on `RowValSpine`.
* `DataflowError` is not Columnar.
  Error spines stay on `RowValSpine` / `ErrSpine` — unchanged.

## Reduce block: DatumSeq vs &RowRef (historical)

Attempted straight swap of reduce output (`RowRowSpine` → `FactRowRowSpine`)
and hit `reduce_abelian`'s bound `T2::Key<'a> = T1::Key<'a>`. Reduce's
internal pre-arrange is `RowValSpine<V>` (Key<'a> = `DatumSeq<'a>`), but
`FactRowRowSpine`'s cursor yields `&'a RowRef`. Types don't unify.

Two resolutions considered:

1. **Bridge** (easy, runtime cost): at each `reduce → downstream-that-wants-FactLocal`
   boundary, `as_collection` + re-arrange into `FactRowRowSpine`. One extra
   arrangement pass per reduce output. Keeps `ArrangementFlavor::Local` as
   a permanent second variant.
2. **DatumSeq-keyed Fact spine** (user-proposed): a `DatumSeqRow(Row)` newtype
   whose `Columnar::Container::Borrow::Ref<'a> = DatumSeq<'a>`.

Path (2) was taken.
`DatumSeq` continues to live in `mz_repr::row` — that's the new normal.

## Deferred / out-of-scope (still deferred)

* Migrating the error spine to Fact — `DataflowError` isn't `Columnar`, so
  the error spine stays on `ColumnationStack`.
* Migrating `RowValSpine` (reduce's internal input arrangement) to Fact —
  would need a Columnar value type for the accumulator tuples; not clearly
  a win, skipped.
* Migrating `RowSpine` (key-only, value `()`) — same: probably not worth the
  churn. `DatumContainer` is retained to serve these remaining spines.
