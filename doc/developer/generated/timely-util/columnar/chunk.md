---
source: src/timely-util/src/columnar/chunk.rs
revision: 24a45d84c0
---

# timely-util::columnar::chunk

`ColumnChunk<D, T, R>`: differential's `Chunk` trait implemented over [`ColumnBody`](body.md)-backed sorted updates, with an optional buffer-pool spill path.

## Overview

A `ColumnChunk` is a sorted, consolidated run of `(D, T, R)` updates. It has two storage variants:

- **`Resident`** — an `Rc`-shared `ColumnBody<(D, T, R)>` on the heap. Fresh input, merge output, and small tails live here.
- **`Spilled`** — the serialized body in the process `Pool`, with a resident `SpilledBody` holding the record count, the first and last data items (the fence entries), the time bounds (`time_lower` / `time_upper`) that `extract` consults for whole-chunk passthrough, a `compressed` flag recording which codec the body was stored under, and `len_bytes` recording the body's serialized size before the pool's codec saw it (retained because the pool reports no per-chunk figure, so a chunk that reported nothing would drop its operator's share of the batcher's memory out of introspection tables). The generational depth is stored in the `Spilled` variant itself, not in the body, because a body is `Rc`-shared across chunk copies and aging must not depend on how many callers hold it. No reference into pool memory ever exists outside a single call: `extract_into` and `fetch_into` copy out into caller-owned scratch.

## Spill gate

Three process-wide atomic flags gate spilling. `COMPUTE_SPILL_ENABLED` and `STORAGE_SPILL_ENABLED` compose as an OR for `ColumnChunk` commits: either flag being set routes commits to the pool installed by `crate::pool_config`. `SINK_SPILL_ENABLED` gates `try_spill_ref` independently — enabling the compute or storage legs does not spill sink bodies, and enabling the sink leg does not spill chunks. Each subsystem writes only its own flag via `set_compute_spill_enabled`, `set_storage_spill_enabled`, or `set_sink_spill_enabled`. A thread-local `SPILL_OVERRIDE` takes precedence over all flags for tests and benchmarks.

Spilling happens at `settle` (the `Chunk` trait's designated commit point) via `ColumnChunk::commit`. Bodies smaller than `SPILL_MIN_BYTES` (64 KiB) stay resident regardless of the gate, since that is below the pool's smallest size class.

## Compression depth floor

A process-wide `COMPRESS_MIN_DEPTH` atomic (`u8`, default `DEFAULT_COMPRESS_MIN_DEPTH = 1`) controls which spilled bodies are stored under the lz4 codec and which use the identity codec. Bodies at depth strictly below the floor use `IDENTITY_CODEC` (encode and decode are copies; still pool-budgeted); bodies at or above the floor use `LZ4_CODEC`. The floor is read per commit via `codec_for_depth`, so changes apply to running dataflows. A thread-local override (`COMPRESS_MIN_DEPTH_OVERRIDE`, `#[cfg(test)]`) takes precedence for tests.

A chunk a merge passes forward without rewriting calls `survive_merge`, which increments its depth (saturating at `u8::MAX`) and, if the body was stored under the identity codec but the new depth meets the floor, re-spills it under the compressing codec. This prevents long-lived bodies from remaining uncompressed indefinitely.

## Generational depth

Every chunk carries a `u8` depth counting merge cadences survived. Fresh chunks are depth `0`. A merge output is one generation past its deepest input (saturating at `u8::MAX`). A chunk a merge carries forward untouched also gains a generation via `survive_merge`. Rewrites within a generation (`extract`, `advance`, coalescing in `settle`) preserve depth. Depth belongs to the chunk, not the `SpilledBody`, so aging does not depend on how many callers share the body via `Rc`. At spill time, depth becomes the pool's `ChunkHints`; hints are fixed at insert, so a chunk aged without a re-spill keeps its original band.

## `Chunk` implementation

- **`merge`** — disjoint-range fast path: if one front's key span lies entirely below the other's first key, the lower front is forwarded via `survive_merge` (incrementing its depth and migrating its codec if it crossed the compression floor). Overlapping fronts are loaded, merged via `Column::merge_from` (gallop bulk-copies + semigroup consolidation), and re-spilled if above the floor; `spill_column` borrows the merged column (allowing the caller to reuse the allocation). Untouched survivors from the exhausted-input drain phase also pass through `survive_merge`.
- **`extract`** — partitions one chunk by time frontier into `keep` and `ship` sides. Consults resident time bounds (`chunk_time_bounds`) first: if all maximal times are strictly before the frontier the chunk ships whole; if all minimal times are at or after the frontier the chunk is kept whole. Both whole-chunk paths leave spilled bodies unloaded. Otherwise the body is loaded and records are partitioned element-by-element.
- **`advance`** — concatenates all input chunks, advances times by the frontier lattice-monotonically, consolidates per-group advanced times, withholds the trailing `D` group as carry (unless `done`).
- **`settle`** — coalesces sub-threshold residents into a carry until the carry reaches `at_commit_size`, then commits it via `ColumnChunk::commit`. Already-spilled chunks pass through untouched. An internal `settle_graded` variant accepts a `commit` flag; callers that read output back immediately pass `false` so the body is not serialized into a pool slot only to be copied straight back out.

## Compression codec

`Lz4Codec` / `LZ4_CODEC` — the `ExtentCodec` chunk consumers pass to `Pool::insert_with`. Encodes as a little-endian `u32` body-length prefix followed by one lz4 block (`lz4_flex::block::compress_prepend_size` format). Byte-identical to the previous hard-coded extent framing.

## `try_spill_ref`

`try_spill_ref<C: Columnar>(column: &Column<C>, depth: u8) -> Option<ChunkHandle>` spills a serialized copy of `column` into the process pool without consuming it, returning `None` when the sink spill gate is off, no pool is installed, or the body is smaller than `SPILL_MIN_BYTES`. The encode writes directly into the pool slot (no intermediate allocation). Callers that receive `Some` can clear and reuse their column allocation; callers that receive `None` remain responsible for keeping the body resident. Consumers that keep their own chunk representation (e.g. the compute MV sink correction buffer) use this instead of `ColumnChunk` because they need both the spilled handle and access to the original column data independently.

## `UnloadChunk` implementation

`ColumnChunk<(K, V), T, R>` implements `UnloadChunk`, the bulk-read capability for probe-key lookups. `locate` answers the three-way comparison against the resident fence entries (never loading a spilled body). `extract_into` gallops the chunk's keys for each probe; spilled bodies are decoded into thread-local scratch and discarded after the call (using the non-admitting read path to avoid re-promoting evicted bodies back to residency).

## Adapter types

- **`UnchunkBuilder<Bu, D, T, R>`** — a `differential_dataflow::trace::Builder` that adapts `ColumnChunk` input to a column-input builder by loading each chunk's body at seal time, one chunk at a time, to bound transient peak memory.
- **`ChunkChunker<D, T, R>`** — a `ContainerBuilder` for `arrange_core` over `ColumnChunk`s, delegating to `ColumnChunker` and wrapping its sorted/consolidated output chunks.
