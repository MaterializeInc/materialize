# Freshness investigation in Materialize's compute layer

## Goal

Identify bottlenecks, inefficiencies, and improvement opportunities that affect **freshness** — the lag between when data arrives at compute inputs and when downstream consumers (materialized views, subscribes, peeks) see the updated results.

## Areas to investigate

### 1. Dataflow rendering pipeline (`src/compute/src/render/`)
* How dataflows are constructed and scheduled.
* Operator fusion, batching, and scheduling inefficiencies.
* Join rendering (`render/join/`) — delta joins, linear joins, arrangement sharing.
* Reduce, threshold, top-k: do these introduce unnecessary buffering?

### 2. Arrangement management (`src/compute/src/arrangement.rs`, `row_spine.rs`, `typedefs.rs`)
* Compaction policies and their effect on freshness.
* Arrangement sharing and reuse across dataflows.
* Spine merging overhead and how it interacts with frontier advancement.

### 3. Sink / output path (`src/compute/src/sink/`)
* `materialized_view.rs` — the persist-write path for MVs.
* Correction buffers (`correction.rs`, `correction_v2.rs`) — do they add latency?
* How `refresh` schedules interact with freshness.
* Subscribe output path.

### 4. Frontier propagation and scheduling
* How input frontiers flow through the dataflow graph.
* Timely progress tracking overhead.
* `compute_state/` — frontier management, peek scheduling.
* `extensions/` — any custom scheduling or progress logic.

### 5. Persist integration (`mz-persist-client`, `mz-txn-wal`)
* Read path: how compute sources data from persist shards.
* Write path: how MVs write back to persist.
* Batching, flushing, and listen latency.

### 6. Upstream dependencies
* `mz-timely-util` — scheduling utilities, reentrant operators.
* `mz-storage-operators` — source rendering operators used in compute.
* `mz-compute-types` / `mz-compute-client` — frontier protocol between controller and replicas.

## Output

Write findings to `log.md` with:
* Section per area investigated.
* Key code paths and their freshness implications.
* Concrete improvement ideas with estimated impact (high/medium/low).

---

# Columnar container migration plan

## Goal

Migrate the compute dataflow pipeline from row-at-a-time `Vec<(Row, T, Diff)>` containers to columnar `Column<((K, V), T, R)>` containers.
This improves throughput (better cache locality, SIMD potential, reduced allocation overhead) and can improve freshness by processing more data per timely activation.

## Current state

### Container landscape

| Path | Container | Layout | Notes |
|------|-----------|--------|-------|
| **Logging, linear joins** | `Column<((Row,Row),T,Diff)>` | `Col2ValBatcher` + `ColumnBuilder` | Already columnar |
| **Arrangement keys** | `Column<((Row,Row),T,Diff)>` | `FormArrangementKey` in context.rs | Already columnar |
| **Network transport** | `Column::Bytes` / `Column::Align` | Timely channel serialization | Already columnar |
| **Reduce, threshold, top-k** | `Vec<((Row,V),T,D)>` | `RowRowBatcher`/`RowBatcher` + `RowRowSpine` | Row-based |
| **Delta joins** | Row arrangements | `RowRowAgent` | Row-based |
| **MFP evaluation** | `Vec<(Row,T,Diff)>` | `CapacityContainerBuilder` | Row-at-a-time |
| **Flat-map** | `Vec<(Row,T,Diff)>` | `ConsolidatingContainerBuilder` | Row-at-a-time |
| **Sinks** | Row streams | Correction buffers | Row-based |

### Type hierarchy

```
Stream container → Batcher → Builder → Batch (in Spine)
                   ↓           ↓          ↓
                Col2ValBatcher ColumnBuilder OrdValBatch<Layout>
                MergeBatcher   OrdValBuilder  └─ Layout determines containers
                                                 ├─ MzStack (TimelyStack for all)
                                                 ├─ RowRowLayout (DatumContainer keys/vals)
                                                 └─ RowValLayout (DatumContainer keys, TimelyStack vals)
```

### Expression evaluation

MFP is strictly row-at-a-time today:
* `MirScalarExpr::eval(&self, datums: &[Datum], arena) -> Result<Datum>` — one row, one result
* `SafeMfpPlan::evaluate_inner()` — iterates expressions, evaluates predicates per row
* `DatumVec::borrow_with(&row)` unpacks entire Row into `Vec<Datum>` per evaluation
* Temporal filters also row-at-a-time in `MfpPlan::evaluate()`

## High-impact migration areas

### Area 1: Stream containers between operators (HIGH)

**What:** Replace `Vec<(Row, T, Diff)>` with `Column<((Row, ()), T, Diff)>` for inter-operator streams.

**Why high impact:** Every operator touches these containers.
Better cache locality for consolidation and sorting.
Reduces per-row allocation overhead (columnar amortizes across batch).

**Files to change:**
* `src/compute/src/render/context.rs` — `as_collection_core()`, output container types
* `src/compute/src/render/flat_map.rs` — output container builder
* `src/compute/src/render/reduce.rs` — consolidation containers
* `src/compute/src/render/top_k.rs` — consolidation containers
* `src/compute/src/render/threshold.rs` — arrangement output
* `src/compute/src/render/errors.rs` — error stream containers

**Dependencies:** Requires all operators to accept `Column` input and produce `Column` output.
Consolidation path (`consolidate_named_if`) must work with columnar containers.

### Area 2: Vectorized MFP evaluation (HIGH, large effort)

**What:** Evaluate scalar expressions on column batches instead of individual rows.

**Why high impact:** MFP is applied at every source, every `Get` node, every `Mfp` node.
Row unpacking (`DatumVec::borrow_with`) and per-row arena allocation dominate CPU in filter-heavy queries.

**Files to change:**
* `src/expr/src/scalar.rs` — add `eval_batch()` method to `MirScalarExpr`
* `src/expr/src/linear.rs` — add `evaluate_batch()` to `SafeMfpPlan`
* `src/repr/src/` — columnar Datum representation (column of i64, column of strings, etc.)
* `src/compute/src/render/context.rs` — `as_collection_core()` batch evaluation path
* `src/compute/src/render/flat_map.rs` — batch MFP drain

**Challenges:**
* Variable-length data (strings, arrays, nested rows) needs columnar representation.
* Short-circuit semantics change: predicate failure produces a mask, not an early return.
* Error handling per-row must map to per-batch with error positions.
* Temporal filter interaction (per-row timestamp modification).

### Area 3: Delta join columnar path (MEDIUM-HIGH)

**What:** Use `Col2ValBatcher` + `ColumnBuilder` in delta joins (already done for linear joins).

**Why:** Delta joins are the primary join strategy.
Currently use Row arrangements; half_join output goes through `Vec` containers.

**Files to change:**
* `src/compute/src/render/join/delta_join.rs` — output container types
* `src/compute/src/render/join/mz_join_core.rs` — work buffer containers

**Dependencies:** Requires half_join output to support `Column` containers.

### Area 4: Sink correction buffers (MEDIUM)

**What:** Make correction buffers (V1/V2) work with columnar inputs instead of row-at-a-time inserts.

**Why:** The MV sink is the final step before persist write.
Columnar correction buffers could batch consolidation more efficiently.

**Files to change:**
* `src/compute/src/sink/correction.rs` — `CorrectionV1`
* `src/compute/src/sink/correction_v2.rs` — `CorrectionV2`, `Stage`, `Chain`
* `src/compute/src/sink/materialized_view.rs` — batch formation from correction buffer

### Area 5: Arrangement spine containers (MEDIUM, long-term)

**What:** Replace `DatumContainer` with true columnar key/value storage in arrangement spines.

**Why:** Cursor-based lookups during joins and peeks traverse DatumContainer, which stores bytes.
Columnar key storage enables vectorized comparison during seek/lookup.

**Files to change:**
* `src/compute/src/row_spine.rs` — `DatumContainer` → columnar alternative
* `src/compute/src/typedefs.rs` — new layout types

**Challenges:**
* `DatumContainer` already provides dictionary compression.
* Columnar alternative must handle variable-length Row data.
* Must maintain `BatchContainer` trait compatibility.

## Recommended migration order

1. **Stream containers** (Area 1) — foundation, everything else builds on this.
2. **Delta join columnar path** (Area 3) — follows naturally once streams are columnar.
3. **Sink correction buffers** (Area 4) — independent of MFP, can parallelize.
4. **Vectorized MFP** (Area 2) — largest effort, highest payoff, can be incremental.
5. **Arrangement spines** (Area 5) — long-term, requires careful benchmarking vs DatumContainer.

## Key infrastructure already in place

* `Column<C>` enum with Typed/Bytes/Align variants (timely-util/columnar.rs)
* `ColumnBuilder<C>` with 2MB chunk targeting (timely-util/columnar/builder.rs)
* `Chunker<TimelyStack<...>>` for consolidation (timely-util/columnar/batcher.rs)
* `Col2ValBatcher` / `Col2KeyBatcher` type aliases (timely-util/columnar.rs)
* `columnar_exchange()` for network partitioning (timely-util/columnar.rs)
* Working examples in logging and linear joins
