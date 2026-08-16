---
source: src/timely-util/src/columnar/chunk.rs
revision: f0cdedca04
---

# timely-util::columnar::chunk

`ColumnChunk<D, T, R>`: differential's `Chunk` trait implemented over `Column`-shaped updates, with an optional buffer-pool spill path.

## Overview

A `ColumnChunk` is a sorted, consolidated run of `(D, T, R)` updates. It has two storage variants:

- **`Resident`** — an `Rc`-shared `Column<(D, T, R)>` on the heap. Fresh input, merge output, and small tails live here.
- **`Spilled`** — the serialized column body in the process `Pool`, with a resident `SpilledBody` holding the record count and the first and last data items (the fence entries). No reference into pool memory ever exists outside a single call: `extract_into` and `fetch_into` copy out into caller-owned scratch.

## Spill gate

Two process-wide atomic flags (`COMPUTE_SPILL_ENABLED`, `STORAGE_SPILL_ENABLED`) compose as an OR. Either flag being set routes commits to the pool installed by `crate::pool_config`. Each subsystem writes only its own flag. A thread-local `SPILL_OVERRIDE` takes precedence over both flags for tests and benchmarks.

Spilling happens at `settle` (the `Chunk` trait's designated commit point) via `ColumnChunk::commit`. Bodies smaller than `SPILL_MIN_BYTES` (64 KiB) stay resident regardless of the gate, since that is below the pool's smallest size class.

## Generational depth

Every chunk carries a `u8` depth fixed at creation. Fresh chunks are depth `0`. A merge output is one generation past its deepest input (saturating at `u8::MAX`). Rewrites within a generation (`extract`, `advance`, coalescing in `settle`) preserve depth. At spill time, depth becomes the pool's `ChunkHints`, so repeatedly merged (older, colder) data lands in deeper eviction bands.

## `Chunk` implementation

- **`merge`** — disjoint-range fast path: if one front's key span lies entirely below the other's first key, the lower front passes to the output untouched (spilled bodies are not loaded). Overlapping fronts are loaded, merged via `Column::merge_from` (gallop bulk-copies + semigroup consolidation), and re-spilled if above the floor. Untouched survivors are restored in their original form.
- **`extract`** — partitions one chunk by time frontier into `keep` and `ship` sides, cutting at the ship threshold mid-loop.
- **`advance`** — concatenates all input chunks, advances times by the frontier lattice-monotonically, consolidates per-group advanced times, withholds the trailing `D` group as carry (unless `done`).
- **`settle`** — coalesces sub-threshold residents into a carry until the carry reaches `at_commit_size`, then commits it via `ColumnChunk::commit`. Already-spilled chunks pass through untouched.

## Compression codec

`Lz4Codec` / `LZ4_CODEC` — the `ExtentCodec` chunk consumers pass to `Pool::insert_with`. Encodes as a little-endian `u32` body-length prefix followed by one lz4 block (`lz4_flex::block::compress_prepend_size` format). Byte-identical to the previous hard-coded extent framing.

## `UnloadChunk` implementation

`ColumnChunk<(K, V), T, R>` implements `UnloadChunk`, the bulk-read capability for probe-key lookups. `locate` answers the three-way comparison against the resident fence entries (never loading a spilled body). `extract_into` gallops the chunk's keys for each probe; spilled bodies are decoded into thread-local scratch and discarded after the call (using the non-admitting read path to avoid re-promoting evicted bodies back to residency).

## Adapter types

- **`UnchunkBuilder<Bu, D, T, R>`** — a `differential_dataflow::trace::Builder` that adapts `ColumnChunk` input to a column-input builder by loading each chunk's body at seal time, one chunk at a time, to bound transient peak memory.
- **`ChunkChunker<D, T, R>`** — a `ContainerBuilder` for `arrange_core` over `ColumnChunk`s, delegating to `ColumnChunker` and wrapping its sorted/consolidated output chunks.
