# Factorized arrangement — status & next-session plan

**Goal:** Differential-dataflow arrangement backed by factorized (trie-structured) columnar storage (`KVUpdates<K, V, T, R>`), with the key win being key/value deduplication through the batching→merging→spine pipeline, not just at final-batch time.

**Branch:** `worktree-factorized-columns`
**Worktree:** `.claude/worktrees/factorized-columns`

---

## What's built (done)

### Core arrangement stack — `src/timely-util/src/columnar/factorized/`

* **`batch.rs`** (`FactBatch`, `FactCursor`, `FactMerger`, `FactBuilder`)
  * `FactBatch<K, V, T, R>` wraps `KVUpdates` + `Description<T>` + update count. Implements `BatchReader`, `Batch`, `WithLayout`.
  * `FactCursor` navigates the trie with `(key_cursor, val_cursor)` indices; bounds via `child_range()` at each level. Full `Cursor` trait.
  * `seek_key` / `seek_val` use `galloping_advance` (free function, matches DD's `BatchContainer::advance` algorithm; SMALL=16 cutoff for tiny ranges). 2–12x speedup on large batches.
  * `FactMerger` — key-by-key trie merge: `merge_key` → `copy_key` / `merge_vals` → `stash_updates` (time compaction via `advance_by`) → `seal_updates` (consolidation, drop zeros). Fuel-based interruption at key granularity. `stash_updates` uses `reserve + extend` (15–29% speedup over per-element push).
  * `FactBuilder` — accumulates sorted `Vec<((K,V),T,R)>` chunks, calls `form()` in `done()` to build the trie.

* **`container.rs`** (`Coltainer<C>`) — `BatchContainer` impl wrapping a columnar `C::Container`. Implements galloping `advance()` for seek. Used as the `KeyContainer/ValContainer/TimeContainer/DiffContainer` in `FactLayout` purely for type-machinery (the trie stores data; `Coltainer` only satisfies the `BatchContainer` bound required by `Layout`).

* **`layout.rs`** (`FactLayout<K, V, T, R>`) — DD `Layout` impl wiring `Coltainer`s + `OffsetList`.

* **`column.rs`** (`FactColumn<K, V, T, R>`) — timely wire container for `KVUpdates`. Three-variant enum `Typed / Bytes / Align` mirroring `Column<C>`. Implements `Accountable` (record count = leaf length), `PushInto<(KV, T, R)>` (via `push_flat`), `DrainContainer` (custom `FactColumnDrain` walks the trie yielding `(K, V, T, R)` refs), `SizableContainer`, `Clone`, `Default`, `ContainerBytes` (using `indexed::encode/decode` on `Level`'s `AsBytes/FromBytes`).
  * **Important:** `FactColumn` is the wire format; it is NOT currently used by the batcher path. See "Gaps" below.

* **`tests_prop.rs`** — 6 property tests comparing against a `BTreeMap` oracle: cursor, seek_key, seek_val, merge, merge+compaction, builder vs `form()`.

* **Type aliases** in `factorized.rs`:
  ```rust
  pub type FactValSpine<K, V, T, R>   = Spine<Rc<FactBatch<K, V, T, R>>>;
  pub type FactValBatcher<K, V, T, R> = MergeBatcher<
      Vec<((K, V), T, R)>,
      ContainerChunker<Vec<((K, V), T, R)>>,
      VecInternalMerger<(K, V), T, R>,
  >;
  pub type FactValBuilder<K, V, T, R> = RcBuilder<FactBuilder<K, V, T, R>>;
  ```

### Integration — `src/compute/`

* `typedefs.rs` — `FactRowRowSpine/Batcher/Builder<T, R>` aliases specialized for `Row × Row`.
* `extensions/arrange.rs` — `ArrangementSize` impl for `Arranged<Spine<Rc<FactBatch<K, V, T, R>>>>`. Uses `indexed::length_in_words(&batch.storage.borrowed()) * 8` as both size and capacity, with 1 allocation per batch.
* `typedefs` made `pub`; `RowRowBuilder/RowBatcher/RowBuilder` re-exported through it so benches outside the crate can see them.

### Benchmarks — `src/timely-util/benches/factorized.rs`

* Existing foundation benches: `push_flat`, `form`, `iter`, `dedup_ratio`, `kv_form`, `kv_iter`, `kv_serialized`, `sort_cost`, `builder_pipeline`, `radix_sort`.
* Arrangement stack benches (u64 keyed): `arrangement/builder`, `arrangement/cursor/{traverse,seek_key}`, `arrangement/merge/{no_compaction,with_compaction}`, `arrangement/container_bytes/{serialize,deserialize_drain}`.
* Row-like benches (`Vec<u8>` keyed, stand-in for `Row`): `arrangement_row/{builder,cursor,merge}` with configurable `key_len`.
* Configurations span 10K → 1M updates.

### End-to-end benchmark — `src/compute/benches/arrange_row.rs`

Timely worker + `arrange_core` comparing `RowRowSpine` (ColumnationStack-backed) vs `FactRowRowSpine` (factorized trie). Current results on dev machine:

| Config | RowRowSpine | FactRowRowSpine |
|--------|-------------|------------------|
| 10K /  k=100 /v=1000   | 3.07 Melem/s | 3.09 Melem/s (parity) |
| 100K / k=1000 /v=10000 | 2.90 Melem/s | 2.53 Melem/s (-13%) |

### Cross-cutting fixes

* Updated `AsBytes` / `Index` impls in `mz-ore::overflowing` and `mz-repr::{row,timestamp}` to match columnar's new trait shape (`SLICE_COUNT + get_byte_slice`; `Cursor<'a> + cursor(range)`).
* Added `columnar = { git = ... }` to `[patch.crates-io]` so differential-dataflow uses the same git version of columnar as our workspace.
* Replaced `as` conversions with `mz_ore::cast::CastFrom`; switched `factorized/mod.rs` to sibling-file `factorized.rs` to satisfy `clippy::mod_module_files`.

---

## Key performance findings

* **Cursor traversal:** 12–32 Gelem/s, same for u64 and Vec<u8> keys — trie structure + stride-1 leaf bounds iterate at memory-bandwidth speed.
* **Merge:** 300+ Melem/s with or without compaction, same for u64 and Vec<u8>. Comparisons happen once per key, not per update.
* **Seek (galloping):** 2–12x faster than linear scan for large batches. Variable-length `Vec<u8>` keys: ~10x slower than `u64` (per-compare byte cost).
* **Builder:** u64 22 Melem/s; Vec<u8> 9 Melem/s (2–4x slower due to data volume). Biggest cost is `Vec<tuple>` buffer step before `form()`.
* **End-to-end parity at small scale; -13% at 100K** — see next-session plan for the fix.

---

## Gaps / deferred work

### 1. Trie-aware batcher (the structural win)

**Current state:** `FactValBatcher` is literally DD's `MergeBatcher<Vec<..>, ContainerChunker<Vec>, VecInternalMerger<..>>`. No trie-awareness in the batcher stage. The trie is only built inside `FactBuilder::done()`, after tuples pass through the entire batching pipeline as flat `Vec<tuple>` chunks.

**Why the -13% in the arrange bench:** baseline `RowRowSpine` uses `ColumnationChunker + ColInternalMerger`, which keep data in arena-backed `ColumnationStack` chunks throughout batching. Our path allocates plain `Vec` chunks and reallocates on merge. We lose on allocator pressure even though the final batch is smaller.

**The structural win we're leaving on the table:** trie chunks would deduplicate keys/values *during* consolidation, before the builder stage. For a workload with high key repetition (typical of join reductions), batcher-stage memory would drop proportionally with dedup ratio — potentially 10× or more on realistic data.

See next-session plan below.

### 2. `ArrangementFlavor::Local` / `JoinedFlavor::Local` hardcoded to `RowRowAgent`

Blocks in-place swap at `src/compute/src/render/context.rs::arrange_collection` and `src/compute/src/render/join/linear_join.rs`. Fix requires either:
* Making `ArrangementFlavor::Local` generic over the agent type (wide blast radius: every pattern match downstream)
* Adding `ArrangementFlavor::FactLocal` variant with parallel code paths

Out of scope for the performance-win next session. Tackle only when we have a proven speedup worth productionizing.

### 3. `ArrangementSize` approximation

Current impl returns `length_in_words * 8` as both size and capacity, 1 allocation. Not wrong, but doesn't distinguish live from capacity, so metrics will look flat. Proper impl would thread `heap_size` through each columnar container; low-priority until we care about arrangement-size dashboards.

### 4. `Row` end-to-end in `FactBatch` unit tests

We validated `Vec<u8>` keyed batches (proxy for `Row` — same variable-length-slice-ref shape). A direct `FactBatch<Row, Row, Timestamp, Diff>` unit test can't live in `mz-timely-util` (cycle: `mz-repr → mz-timely-util`). The `mz-compute` arrange bench does exercise the real `Row` path end-to-end, which is the stronger validation.

---

## Next session: trie-aware batcher

**Working branch:** continue on `worktree-factorized-columns` or branch off.
**Working directory:** `.claude/worktrees/factorized-columns`

### Target architecture

```
Input  (per-worker)      :  Vec<((K,V),T,R)>                (unchanged)
  │
  ▼
FactTrieChunker          :  sort + consolidate → emit KVUpdates chunks
  │                          (struct-of-arrays from the start)
  ▼
FactTrieInternalMerger   :  merges two sorted KVUpdates chunks pairwise,
  │                          deduplicating keys/vals as it goes
  ▼
FactBuilder              :  accepts KVUpdates chunks (NEW input type),
  │                          concatenates into final FactBatch
  ▼
FactValSpine             :  unchanged
```

The batcher output and builder input both become `KVUpdates<K,V,T,R>` (owned trie). The win: internal merge stage is O(distinct keys) in allocations instead of O(updates).

### Task 1 — `FactTrieChunker`

**File:** `src/timely-util/src/columnar/factorized/chunker.rs` (new)

Mirrors the shape of `differential_dataflow::trace::implementations::chunker::ContainerChunker` but outputs `KVUpdates` instead of the same input container.

```rust
pub struct FactTrieChunker<K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    /// Staging buffer of unsorted (or partially-sorted) tuples.
    pending: Vec<((K, V), T, R)>,
    /// Finished trie chunks ready for extract().
    ready: VecDeque<KVUpdates<K, V, T, R>>,
}

impl<K, V, T, R> ContainerBuilder for FactTrieChunker<K, V, T, R>
where /* full FactBounds */
{
    type Container = KVUpdates<K, V, T, R>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        /* if self.ready non-empty, pop and return */
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        /* sort self.pending, consolidate adjacent equal (k,v,t),
           drop zero-diff entries, call KVUpdates::form to build trie,
           push to self.ready, then extract */
    }
}

impl<K, V, T, R> PushInto<Vec<((K,V),T,R)>> for FactTrieChunker<K, V, T, R> {
    fn push_into(&mut self, mut vec: Vec<((K,V),T,R)>) {
        self.pending.append(&mut vec);
        // Optionally: if pending is large enough, sort+consolidate+form now
        // instead of deferring to finish().
    }
}
```

**Design notes:**
* Flush policy: consolidate + form when `pending.len() >= threshold` (say 1 MB worth of tuples), or at `finish()`. Avoids pathological `pending` growth.
* Consolidation in the staging phase can use DD's existing `consolidation::consolidate` on `Vec<(Key, Val, Time, Diff)>` flattened; or a custom pass that sorts by `(K, V, T)` and folds diffs. Either is fine for first cut.
* `form()` constructs the trie and gives natural dedup of keys/vals. That's where the structural win lands.

**Tests:**
* Push tuples with duplicate keys/vals, verify extract returns a single trie with correct structure.
* Push across many calls, verify final output consolidates across all inputs.

### Task 2 — `FactTrieInternalMerger`

**File:** `src/timely-util/src/columnar/factorized/batcher.rs` (new, or merge with chunker)

Implements `differential_dataflow::trace::implementations::merge_batcher::Merger` trait (note: `merge_batcher::Merger`, NOT `trace::Merger`). Merges *chains* of `KVUpdates` chunks, not two batches.

```rust
pub struct FactTrieInternalMerger<K, V, T, R> { /* ... */ }

impl<K, V, T, R> merge_batcher::Merger for FactTrieInternalMerger<K, V, T, R>
where /* bounds */
{
    type Chunk = KVUpdates<K, V, T, R>;
    type Time = T;

    fn merge(
        &mut self,
        list1: Vec<Self::Chunk>,
        list2: Vec<Self::Chunk>,
        output: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    ) {
        /* Interleave-merge flattened chunks. For two sorted chunk sequences,
           emit a single merged sequence of chunks. Each output chunk is a
           KVUpdates trie built by interleaving and deduplicating.

           Implementation sketch:
           1. Flatten list1 and list2 into two cursors that yield (K, V, T, R) refs
              in sorted order.
           2. Walk both cursors in lockstep, emitting to a KVUpdates builder.
              The builder detects key/val changes and seals bounds (essentially
              the same logic as KVUpdates::form but fed from two cursors).
           3. Flush the builder into output chunks, rotating buffers through stash.

           The gain vs ColInternalMerger: the per-key / per-val cost is paid once,
           not once per update. Equal keys across chunks produce a single output
           key with a merged val range. */
    }

    fn extract(
        &mut self,
        merged: Vec<Self::Chunk>,
        upper: AntichainRef<Self::Time>,
        frontier: &mut Antichain<Self::Time>,
        readied: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    ) {
        /* Partition each chunk by time:
           - Updates with times less than upper → readied (goes to builder/seal).
           - Updates with times not less than upper → kept (stays in batcher).
           Updates the frontier with the minimum held time.

           Implementation:
           1. For each input chunk, walk (K, V, T, R) refs.
           2. Split each into two output tries based on upper comparison.
           3. If either side is non-empty, push to respective output Vec.
           4. Track minimum "kept" time in frontier. */
    }
}
```

**Design notes:**
* The merge algorithm is basically `FactMerger::work` reorganized for chunk streams. Consider factoring a shared helper that takes two trie cursors and an output builder.
* `extract` is novel; the existing `FactMerger` doesn't do frontier partitioning. Read `ColInternalMerger::extract` for reference pattern.
* Chunk-size target: same as current (~1 MB). Large enough that the amortized cost of the trie build is cheap; small enough that per-chunk allocations fit in cache.

**Tests:**
* Merge two single-chunk inputs, verify output equals what `FactMerger` produces on equivalent batches.
* Merge multi-chunk inputs with overlapping keys, verify dedup.
* Extract with a frontier that splits a chunk, verify `readied` and `kept` are disjoint and complete.

### Task 3 — `FactBuilder::Input` switches to `KVUpdates`

**File:** `src/timely-util/src/columnar/factorized/batch.rs` (modify)

```rust
impl<K, V, T, R> Builder for FactBuilder<K, V, T, R> {
    type Input = KVUpdates<K, V, T, R>;  // was Vec<((K,V),T,R)>
    type Output = FactBatch<K, V, T, R>;

    fn push(&mut self, chunk: &mut Self::Input) {
        /* Concatenate chunk into self.result. Since each chunk is already a
           sorted trie and chunks arrive in sorted order, we need a trie-concat
           that merges at boundaries where the last key of self.result equals
           the first key of chunk.

           For disjoint key ranges (common case after the batcher has
           deduplicated globally), this is effectively a concat: copy lists/bounds
           with offset adjustments.

           At key boundaries where trailing key of prior chunk matches leading
           key of incoming chunk, merge the val ranges. */
    }

    fn done(self, description: Description<T>) -> Self::Output {
        FactBatch { storage: self.result, description, updates: self.update_count }
    }
}
```

**Design notes:**
* The batcher now hands us pre-dedup'd trie chunks. No more `form()` call here — `form` happened in the chunker.
* The boundary-merge case may be rare after a good chunker; document assumption and handle correctly even if rare.

**Tests:** existing `test_builder_*` tests rewrite with trie inputs.

### Task 4 — Wire through `FactValBatcher` type alias

**File:** `src/timely-util/src/columnar/factorized.rs` (modify type alias)

```rust
pub type FactValBatcher<K, V, T, R> = MergeBatcher<
    Vec<((K, V), T, R)>,                    // outer input unchanged
    FactTrieChunker<K, V, T, R>,            // new
    FactTrieInternalMerger<K, V, T, R>,     // new
>;
```

### Task 5 — Rerun benchmarks

* `cargo bench -p mz-timely-util --bench factorized -- arrangement` — verify no regression on micro-benches; merge/builder should be faster or equal.
* `cargo bench -p mz-compute --bench arrange_row` — primary goal. Expect FactRowRowSpine to move from -13% to parity-or-better vs RowRowSpine. The bigger the dedup ratio in the test data (more duplicate keys), the larger the win should be.
* Add a new bench config with high dedup (few distinct keys, many updates per key) to surface the structural win clearly.

### Estimated effort

2–3 hours for an experienced columnar-infra engineer. The chunker is ~100 lines (mostly plumbing). The internal merger is ~200 lines and is the core algorithmic work (merge two trie streams with frontier-aware split). Builder change is small. Wiring + re-running benchmarks adds another hour.

### Risk flags

* The internal merger's correctness is subtle. Property tests (new, for chunk-level merge) highly recommended before trusting benchmarks.
* Allocation behavior of the chunker matters as much as the algorithm. Watch for `Vec<tuple>` reallocations in `push_into` — prealloc and reuse `stash` buffers.
* `form()` is O(n log n) if input unsorted; ensure chunker always sorts before calling, or pay the price per chunk.

---

## File map (current)

```
src/timely-util/src/columnar/
├── factorized.rs              # sibling-file entry: Level, Lists, KVUpdates,
│                              # form/iter/push_flat, type aliases
└── factorized/
    ├── batch.rs               # FactBatch, FactCursor, FactMerger, FactBuilder
    ├── container.rs           # Coltainer<C>: BatchContainer impl
    ├── layout.rs              # FactLayout: DD Layout impl
    ├── column.rs              # FactColumn: timely wire container
    └── tests_prop.rs          # proptest oracle comparisons

src/timely-util/benches/
└── factorized.rs              # all microbenchmarks

src/compute/
├── src/typedefs.rs            # FactRowRowSpine/Batcher/Builder aliases
├── src/extensions/arrange.rs  # ArrangementSize impl for FactBatch
└── benches/arrange_row.rs     # end-to-end RowRow vs FactRowRow bench
```

## Commit history (on branch)

```
bench(compute): add arrange_row comparing RowRowSpine vs FactRowRowSpine
feat(compute): add FactRowRowSpine aliases + ArrangementSize impl
bench(timely-util): add row-like (Vec<u8> keyed) arrangement benchmarks
test(timely-util): validate FactBatch bounds for Row-like variable-length keys
perf(timely-util): galloping seek_key/seek_val, 2-12x speedup on large batches
fix(timely-util): replace `as` conversions with `CastFrom`, switch to sibling-file module
fix: address CI lints in overflowing, container, and mod
fix: update AsBytes/Index impls for columnar crate API changes
perf(timely-util): reserve+extend in stash_updates for 15-29% merge speedup
bench(timely-util): add 1M-element configs to arrangement benchmarks
bench(timely-util): add arrangement stack benchmarks
feat(timely-util): add FactColumn container with ContainerBytes, batcher alias
feat(timely-util): add FactValBatcher type alias for standard Vec-based merge batcher
feat(timely-util): add factorized arrangement stack (batch, cursor, merger, builder)
```
