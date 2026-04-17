# Migrate renderer from RowRowSpine → FactRowRowSpine

## Goal

Move every `ArrangementFlavor::Local` / `JoinedFlavor::Local` producer and
consumer over to their `FactLocal` equivalents. Once done, delete the original
`Local` path, rename `FactLocal` → `Local`, drop the
`enable_compute_factorized_arrangement` dyncfg. The codebase ends up with a
single local-arrangement flavor backed by the factorized trie spine, and
nothing else.

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

### Tier 1 — trivial (key-only peek/consumption)
* `src/compute/src/render/context.rs`
  * `as_collection` (line 271)
  * `flat_map` (line 327)
  * `scope` / `enter_region` / `leave_region` (trivial forwarding)
  * `ensure_collections` producer (line 869) — currently flagged
* `src/compute/src/render.rs`
  * Hydration logging (line 1421) — already passes through FactLocal via
    stream-only handling.

### Tier 2 — cursor walks with `Key<'_>` / `Val<'_>`
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

### Tier 3 — reduce / top_k output
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

### Tier 4 — joins
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

### Tier 5 — imported / exported traces (`ArrangementFlavor::Trace`)
* `compute_state.traces` (`TraceBundle` storage) and the peek path both
  assume `RowRowAgent`-shaped traces. Migrating `Trace` means changing the
  cross-dataflow storage format and all peek/subscribe consumers. This
  probably wants its own plan once the Local path is done.

## Migration order (one PR per tier)

1. **Tier 1** — already largely done; just flip the flag default to `true`
   once Tier 2 is merged and arrange sites only produce `FactLocal`. Delete
   `ArrangementFlavor::Local` at the end.
2. **Tier 2** — add Fact-aware `TraceBundle` variant; swap threshold's
   builder. `export_index_iterative` gets a Fact rearrange.
3. **Tier 3** — reduce pipeline. Biggest unknown; design a Fact-output reduce
   or bridge. May end up staying on `RowValSpine` for the *output* and
   converting to `FactLocal` only when feeding further Tier 4 stages.
4. **Tier 4** — joins. Once threshold / reduce land in Tier 2–3, swap
   `JoinedFlavor::Local` and the cross-product arms. Remove the mixed-key
   `unreachable!()` arms added in the plumbing commit.
5. **Tier 5** — imported traces. Migrate `TraceBundle`, peek path, subscribe.
   Larger cross-component change.
6. **Cleanup** — delete `ArrangementFlavor::Local`, rename `FactLocal` →
   `Local`, drop `FactRowRow*` aliases in favor of renamed `RowRow*`, drop
   the dyncfg.

## Verification per step

After each tier:
* `cargo check -p mz-compute` + workspace.
* `cargo test -p mz-compute --test lib` (datadriven reduce / top_k / join).
* `bin/sqllogictest -- test/sqllogictest/transform/`.
* A minimal `mzcompose` sanity run on a MV that exercises the tier's
  consumer.
* For Tier 3+ also run a microbenchmark (spines_row.rs example, or a
  targeted MV) to confirm we're not regressing.

## Open questions

* Reduce pipeline: is a Fact-output reduce worth the complexity, or do we
  accept an `as_collection` → re-arrange bridge at that boundary and keep
  reduce on `RowValSpine` forever? Decide before Tier 3.
* `DataflowError` is not Columnar. Error spines stay on `RowValSpine` /
  `ErrSpine` — this is already the case in the existing plumbing.

## Reduce block: DatumSeq vs &RowRef

Attempted straight swap of reduce output (`RowRowSpine` → `FactRowRowSpine`)
and hit `reduce_abelian`'s bound `T2::Key<'a> = T1::Key<'a>`. Reduce's
internal pre-arrange is `RowValSpine<V>` (Key<'a> = `DatumSeq<'a>`), but
`FactRowRowSpine`'s cursor yields `&'a RowRef`. Types don't unify.

Two resolutions:

1. **Bridge** (easy, runtime cost): at each `reduce → downstream-that-wants-FactLocal`
   boundary, `as_collection` + re-arrange into `FactRowRowSpine`. One extra
   arrangement pass per reduce output. Keeps `ArrangementFlavor::Local` as
   a permanent second variant.
2. **DatumSeq-keyed Fact spine** (user-proposed): a `DatumSeqRow(Row)` newtype
   whose `Columnar::Container::Borrow::Ref<'a> = DatumSeq<'a>`. Implementation:
   * Add `DatumSeq::from_bytes(&[u8]) -> DatumSeq<'_>` ctor in `row_spine.rs`.
   * New `DatumSeqRows<BC, VC>` container newtype around `Rows<BC, VC>` —
     same storage, but its `Borrow` impl yields `DatumSeq<'a>` from `Index::get`.
   * `impl Columnar for DatumSeqRow` with `Container = DatumSeqRows`.
   * Swap `FactRowRowSpine` → `FactValSpine<DatumSeqRow, DatumSeqRow, T, R>`
     (or define `FactRowRowDsSpine` alongside).
   Cost: a few hundred lines mirroring `row.rs`'s `columnar` module; one
   followup to rewire all Fact types. Buys a zero-copy unification with the
   existing RowVal internals and unblocks reduce migration without a runtime
   arrangement.

Preferred path: **(2)**. Deferred as its own followup; tracked here.
