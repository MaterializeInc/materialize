# MV Sink Correction Buffer V3

## The Problem

The MV sink's `CorrectionV2` buffer (in `src/compute/src/sink/correction_v2.rs`) maintains
chains of chunks backed by columnation (`TimelyStack`). Three interrelated performance
problems limit its scalability:

**P1: Time advancement during merges.** Consolidation requires advancing all times to the
`since` frontier so that updates at different original times can cancel. The current
`advance_by` method handles this by splitting each cursor into one sub-cursor per distinct
time ≤ `since`. With K chains containing T distinct timestamps each, this produces up to
K×T cursors, making merges O(N log(K×T)) instead of O(N log K).

**P2: Chunks are too small.** `TimelyStack` targets `BUFFER_SIZE_BYTES = 8192` bytes for
its index vector (from `timely_container`). For `(Row, Timestamp, Diff)` at ~48 byte index
entries, each chunk holds ~170 elements. This leads to many small allocations, excessive
chunk management overhead, and poor lgalloc utilization. Target: ~2 MiB per chunk.

**P3: Many timestamps in flight.** MVs downstream of temporal filters can have corrections
at thousands of distinct future times. Because `advance_by` splits by time during every
merge—including merges triggered by `insert_inner` to restore the chain invariant—a steady
stream of updates at different timestamps creates pathological merge cascades.

## Success Criteria

- Merge operations never perform time-based cursor splitting (eliminates P1/P3).
- Chunks target ~2 MiB contiguous allocations with lgalloc/spill-to-disk support (fixes P2).
- Many timestamps in flight are handled in O(log(time_range)) buckets, not O(distinct_timestamps) chains.
- The existing V2 implementation remains available as a fallback, controlled by a feature flag.
- Row-typed corrections use the new design; DataflowError corrections reuse V2.

## Out of Scope

- Implementing `Columnar` for `DataflowError` (complex nested enum with hand-written `Columnation`).
- Changes to the `mint` or `append` operators. Only the `write` operator's correction buffer changes.
- Multi-dimensional timestamp support. The `BucketChain` requires a total order, which the MV sink uses.

## Solution Proposal

### Architecture Overview

```
CorrectionV3<Row> {
    compacted: ChainList<Row>,         // updates at times ≤ since, sorted by data
    future: BucketChain<Storage<Row>>, // updates at times > since, bucketed by time
    stage_compacted: Stage<Row>,       // staging buffer for compacted inserts
    since: Antichain<Timestamp>,
    metrics: ...
}
```

Three key departures from V2:

1. **Two groups** (compacted + future) eliminate `advance_by` splitting entirely.
2. **`BucketChain`** from `src/timely-util/src/temporal.rs` replaces the future chain-of-chains.
3. **Columnar `Column<C>`** replaces `TimelyStack`-based chunks, giving 2 MiB contiguous
   allocations with lgalloc/spill-to-disk support.

### Two-Group Architecture

The `since` frontier divides corrections into two fundamentally different groups:

- **Compacted** (times ≤ since): All times collapse to `since_ts` after advancement. The
  meaningful ordering is **by data only**—time is irrelevant.
- **Future** (times > since): Times are unaffected by advancement. Standard **(time, data)
  ordering** is correct and no splitting is ever needed during merges.

By physically separating these groups, we eliminate the need for `advance_by` splitting
entirely. Merges within each group are straightforward and never need to reason about time
advancement.

### The Compacted Group

Contains all corrections at times ≤ `since_ts`. Maintains a chain-of-chains with
proportionality factor 3, sorted **by data only**.

**Why data-only ordering works:** All times ≤ since collapse to `since_ts`. Two updates
`(d, t1, +1)` and `(d, t2, -1)` where both `t1, t2 ≤ since` represent the same logical
datum at the same effective time. Sorting by data makes them adjacent, so they cancel
during consolidation.

**Timestamp handling:** Timestamps are stored in chunks but ignored for ordering and
comparison during merges. When two entries have the same data, their diffs are summed
regardless of stored timestamp. On output, all entries are yielded as
`(data, since_ts, diff)`, stamped with the current `since_ts`.

**When `since` advances:** Existing compacted entries need no update. Stale timestamps are
never consulted for ordering or output. This avoids the cost of rebuilding chunks.

**Why proportionality 3:** With proportionality 2, a chain containing all insertions and a
second chain containing all retractions can satisfy the invariant—infinite overhead over the
consolidated representation. Proportionality ≥ 3 forces merges often enough that insertions
and retractions meet and cancel.

**Merge logic:** Pure k-way merge by data. When same data, sum diffs. If sum is zero, skip.
No time advancement, no cursor splitting.

### The Future Group: `BucketChain`

Contains all corrections at times > `since_ts`. Uses `BucketChain<Storage<Row>>` from
`src/timely-util/src/temporal.rs`.

**Why `BucketChain` instead of chain-of-chains:** V2's chain-of-chains is ordered by
`(time, data)` globally. When many timestamps are in flight, `advance_by` must split at
every distinct time during every merge. `BucketChain` avoids this entirely by bucketing
updates into power-of-two time ranges. Each bucket covers `[offset, offset + 2^bits)`.

Key properties of `BucketChain`:
- **Insertion:** `find_mut(timestamp)` locates the bucket in O(log B). Push the update into the bucket's internal storage.
- **Peel:** `peel(frontier)` extracts all buckets strictly before the frontier, splitting boundary buckets as needed.
- **Restore:** `restore(&mut fuel)` rebalances bucket sizes with fuel-based amortization. The chain tolerates being temporarily un-well-formed.
- **Scalability:** 1000 distinct timestamps in a range of width 1024 need ~10 buckets (log₂), not 1000 chains.

**Bucket storage type:**

```rust
/// The storage for a single bucket in the BucketChain.
struct Storage<D: Data> {
    /// A chain-of-chains sorted by (time, data), consolidated within.
    /// Maintains proportionality invariant (factor 3).
    inner: ChainList<D>,
}

impl<D: Data> Bucket for Storage<D> {
    type Timestamp = Timestamp;

    fn split(self, timestamp: &Timestamp, fuel: &mut i64) -> (Self, Self) {
        // Split each chain at the timestamp boundary.
        // Left: updates with time < timestamp
        // Right: updates with time >= timestamp
        // Each half is already sorted by (time, data) within its range.
        // Deduct work from fuel.
        ...
    }
}
```

Each bucket's `ChainList` maintains its own proportionality invariant. Merges within a
bucket are pure `(time, data)` merges—no `advance_by`, no splitting. All times are > since,
so time advancement is a no-op.

### Chunks: Columnar `Column<C>` at 2 MiB

Replace `TimelyStack`-based `Chunk<D>` with `Column<C>`-backed chunks using the Columnar
crate.

**Why Columnar over columnation:**
- Columnar is the newer API, actively developed.
- `Indexed::write` serializes data into contiguous `Region<u64>` allocations—exactly 2 MiB.
- `Column::Align(Region<u64>)` integrates with lgalloc for transparent spill-to-disk.
- `Row` already implements `Columnar` (`src/repr/src/row.rs:454`).
- `Timestamp` and `Diff` are primitives with trivial `Columnar` impls.

**Chunk lifecycle:**

A chunk starts as a `Typed` container being actively built. Once it reaches ~2 MiB
(matching the `ColumnBuilder` logic in `src/timely-util/src/columnar/builder.rs`), it is
frozen into a contiguous aligned allocation.

```rust
/// A chunk of updates being built.
struct BuildingChunk<D: Columnar> {
    container: <(D, Timestamp, Diff) as Columnar>::Container,
}

impl<D: Columnar> BuildingChunk<D> {
    fn push(&mut self, data: &D, time: Timestamp, diff: Diff) {
        self.container.push((data, time, diff));
    }

    fn at_capacity(&self) -> bool {
        // Same 2 MiB logic as ColumnBuilder (1 << 18 words = 2 MiB)
        let words = Indexed::length_in_words(&self.container.borrow());
        let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
        round - words < round / 10
    }

    /// Freeze into a contiguous 2 MiB allocation.
    fn freeze(self) -> FrozenChunk<D> {
        let borrow = self.container.borrow();
        let words = Indexed::length_in_words(&borrow);
        let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
        let mut alloc = alloc_aligned_zeroed(round);
        let writer = std::io::Cursor::new(bytemuck::cast_slice_mut(&mut alloc[..]));
        Indexed::write(writer, &borrow).unwrap();
        FrozenChunk {
            data: Column::Align(alloc),
            len: self.container.len(),
        }
    }
}

/// An immutable chunk backed by a contiguous ~2 MiB allocation.
struct FrozenChunk<D: Columnar> {
    data: Column<(D, Timestamp, Diff)>,
    len: usize,
}

impl<D: Columnar> FrozenChunk<D> {
    fn borrow(&self) -> /* borrowed view */ {
        self.data.borrow()
    }
}
```

**Key differences from V2 chunks:**
- V2: ~8 KB index vector + separate columnation regions = many small allocations.
- V3: single contiguous ~2 MiB allocation containing all data (index + variable-length row bytes) via `Indexed::write`.
- One allocation per chunk. 2 MiB aligns with huge page sizes, ideal for lgalloc/mmap.
- Iteration via `Column::borrow()` is zero-copy for the `Align` variant (pointer arithmetic). The cost will be further reduced to a few instructions in future Columnar versions.

### The `ChainList` Type

Both the compacted group and each bucket's storage use a `ChainList`—a list of chains
maintaining the proportionality invariant. The type is parameterized by its merge
comparator:

```rust
struct ChainList<D: Data> {
    chains: Vec<Chain<D>>,
}
```

Two merge functions share a common k-way merge core parameterized by a closure:

- `merge_by_data(chains) -> Chain` — for the compacted group. Compares by data only, consolidates same-data entries.
- `merge_by_time_data(chains) -> Chain` — for future buckets. Compares by (time, data), consolidates same-(time, data) entries.

The `Chain` and `FrozenChunk` types are identical for both groups. Only the merge
comparator differs.

**Restoring the chain invariant (fueled):** After inserting a chain, proportionality may be
violated. Restoration merges adjacent chains from the tail:

```
while chain_invariant_violated(chains):
    merge last two chains
    deduct fuel
    if fuel exhausted: break
```

This allows amortizing merge work across multiple inserts. The invariant does not need to
hold at all times—it is restored incrementally.

### `Data` Trait

V3 requires `Columnar` instead of `Columnation`:

```rust
pub trait DataV3: differential_dataflow::Data + Columnar {}
```

Since `DataflowError` does not implement `Columnar`, V3 is only instantiated for `Row`.

### DataflowError Handling

`DataflowError` corrections continue to use `CorrectionV2`. The write operator maintains
two separate correction buffers:

```rust
struct Corrections {
    ok: CorrectionV3<Row>,            // new V3 design
    err: CorrectionV2<DataflowError>, // existing V2
}
```

Some customers have multiple GiB of errors, so `CorrectionV2`'s columnation-backed storage
(with its `TimelyStack` chunks and lgalloc integration) remains important. Plain `Vec`
storage would be a regression.

### Operations

#### `insert(updates)` / `insert_negated(updates)`

```
1. Advance times: max(time, since_ts) for each update.
2. Partition updates by time:
   - time == since_ts → compacted stage
   - time > since_ts  → bucket_chain.find_mut(time).inner.insert(update)
3. Compacted stage: when full, sort by data, consolidate, freeze chunk,
   create chain → insert into compacted.chains.
4. Restore chain invariant in compacted (fueled).
5. Each bucket's ChainList handles its own staging and proportionality.
6. Call bucket_chain.restore(&mut fuel) periodically to maintain bucket invariant.
```

**Fuel budget for `BucketChain::restore`:** Proportional to the number of inserted updates.
The bucket chain tolerates being temporarily un-well-formed—`peel` and `find` still work.

#### `advance_since(new_since)`

This is the only operation that moves data between groups:

```
1. Store new_since.
2. Advance times in the compacted stage.
3. Peel the BucketChain: bucket_chain.peel(new_since_frontier)
   → Returns Vec<Storage<Row>>, all with times ≤ new_since.
4. For each peeled Storage:
   a. Updates are sorted by (time, data) within the bucket.
   b. Within each distinct time, updates are sorted by data.
   c. Do a k-way merge of per-time runs, comparing by data only, to produce
      a data-sorted chain.
   d. Insert result into compacted.chains.
5. Restore compacted chain invariant (fueled).
6. Call bucket_chain.restore(&mut fuel) to rebalance remaining buckets.
```

**Steady-state cost:** Since typically advances by one timestamp per batch append. The
`peel` extracts one or a few small buckets. The merge in step 4c has T' = 1 distinct time,
so it degenerates to a direct insertion with no interleaving needed.

**This is the only place time-reordering occurs.** In V2, this reordering happens on every
chain merge (via `advance_by`). In V3, it happens only during `advance_since`, which is
called once per batch append.

#### `updates_before(upper)`

```
1. Consolidate the compacted group:
   - Merge all compacted chains into one (merge by data, consolidate).
   - Iterate the result, yielding (data, since_ts, diff).
2. Read future updates before upper:
   - For each bucket with times < upper (in time order):
     a. Consolidate the bucket's ChainList into a single chain.
     b. Iterate the chain, yielding (data, time, diff).
   - Since buckets have non-overlapping time ranges and are iterated in order,
     this yields globally (time, data)-ordered output without cross-bucket merging.
3. Return concatenated iterator: compacted updates, then future updates.
```

**No cross-group consolidation needed.** Compacted entries are at effective time `since_ts`.
Future entries are at times > `since_ts`. Different times cannot consolidate.

**Data stays in the buffer** after iteration (same as V2). Removed only when negated persist
feedback arrives and consolidates with existing entries.

#### `consolidate_at_since()`

Merge all compacted chains into one chain (merge by data, consolidate). Same as step 1 of
`updates_before`.

### Feature Flag

New dyncfg `ENABLE_CORRECTION_V3`:

```rust
pub const ENABLE_CORRECTION_V3: Config<bool> = Config::new(
    "enable_compute_correction_v3",
    false,  // off by default initially
    "Whether compute should use the V3 MV sink correction buffer \
     (Columnar chunks, BucketChain for future updates). \
     Only applies to Row-typed corrections; DataflowError uses V2.",
);
```

The `Correction` enum gains a third variant:

```rust
pub(super) enum Correction<D: Data> {
    V1(CorrectionV1<D>),
    V2(CorrectionV2<D>),
    V3(CorrectionV3<D>),  // Only instantiated for Row
}
```

Selection in the write operator:

```rust
let ok_correction = if ENABLE_CORRECTION_V3.get(config) {
    Correction::V3(CorrectionV3::new(...))
} else if ENABLE_CORRECTION_V2.get(config) {
    Correction::V2(CorrectionV2::new(...))
} else {
    Correction::V1(CorrectionV1::new(...))
};
// Errors always use V2:
let err_correction = if ENABLE_CORRECTION_V2.get(config) {
    Correction::V2(CorrectionV2::new(...))
} else {
    Correction::V1(CorrectionV1::new(...))
};
```

### New Files and Modules

- `src/compute/src/sink/correction_v3.rs` — `CorrectionV3<D>` implementation.
- `src/compute/src/sink/correction_v3/chain.rs` — `Chain`, `FrozenChunk`, `ChainList`, `Cursor` types.
- `src/compute/src/sink/correction_v3/merge.rs` — `merge_by_data`, `merge_by_time_data`, k-way merge core.
- Modified: `src/compute/src/sink/correction.rs` — add V3 variant to `Correction` enum.
- Modified: `src/compute-types/src/dyncfgs.rs` — add `ENABLE_CORRECTION_V3`.
- Modified: `src/compute/src/sink/materialized_view.rs` — wire up V3 for ok corrections.

`BucketChain` and `Bucket` trait from `src/timely-util/src/temporal.rs` are used as-is.
`Column`, `ColumnBuilder`, and `alloc_aligned_zeroed` from `src/timely-util/src/columnar`
are used as-is. `BucketTimestamp` for `Timestamp` is already implemented in
`src/repr/src/timestamp.rs`.

### Summary of Improvements

| Aspect | V2 | V3 |
|--------|----|----|
| Merge during insert | `advance_by` splits K×T cursors | No splitting; pure merge within compacted or bucket |
| Many timestamps | Chain per timestamp in worst case | `BucketChain`: ~log₂(range) buckets |
| Chunk size | ~8 KB (170 elements) | ~2 MiB contiguous `Region<u64>` via Columnar |
| Spill-to-disk | columnation + lgalloc (many small regions) | `Column::Align` + lgalloc (single 2 MiB region) |
| Since advancement | Lazy (deferred to every merge) | Eager via `BucketChain::peel`, fueled `restore` |
| Consolidation | Requires physical time advancement + re-sort | Compacted: by data only. Future: no advancement. |
| DataflowError | Same as Row | Delegates to V2 (columnation + lgalloc) |
| Chain proportionality | 3 | 3 (unchanged; prevents infinite overhead) |
