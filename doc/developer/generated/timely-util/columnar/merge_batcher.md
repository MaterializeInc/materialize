---
source: src/timely-util/src/columnar/merge_batcher.rs
revision: 3506b9aee8
---

# timely-util::columnar::merge_batcher

Merge-batcher for `Column<(D, T, R)>` chunks with per-chunk paging via `ColumnPager`.

This module forks the differential dataflow merge-batcher framework so chains can hold `PagedColumn` entries. As chunks are produced (on insert, merge, or extract), they are handed to the pager and stored in whatever form the policy returns — resident, paged to swap, or compressed. During merge and extract, chunks are fetched back lazily through `FetchIter`.

Reuses the consolidation building blocks from `super::batcher`: `ColumnChunker` for input consolidation, and the inherent `Column::merge_from` / `Column::extract` methods for per-chunk merge and frontier-split.

## `ColumnMergeBatcher<D, T, R>`

The main batcher struct implementing `differential_dataflow::trace::Batcher`.

Fields:
- `chunker: ColumnChunker<(D, T, R)>` — accumulates and consolidates pushed inputs.
- `chains: Vec<VecDeque<PagedColumn<(D, T, R)>>>` — the merge-sorted chain stack; each entry is a deque of paged chunks.
- `lower`, `frontier: Antichain<T>` — track the batch lower bound and current progress frontier.
- `pager_override: Option<ColumnPager>` — when set, used instead of `column_pager::global_pager()`. Intended for tests; production leaves this unset so dyncfg-driven pager reinstalls take effect on the next chunk.
- `logger`, `operator_id` — for `BatcherEvent` introspection logging.

The pager is resolved per call via `self.pager()`, which returns the override if set, otherwise calls `column_pager::global_pager()`. A dyncfg flip of `enable_column_paged_batcher` therefore takes effect on the very next chunk without rebuilding the operator.

`push_container` feeds a `Column<(D, T, R)>` into the chunker, then routes each completed chunk through the pager and into the chain stack via `insert_chain`.

`seal` finishes the chunker, merges all chains into one, then calls `extract_chain` to split the merged result into shipped chunks (times strictly before `upper`) and kept chunks (times at or after `upper`). Shipped chunks are materialized and passed to the `Builder`; kept chunks are re-pushed into `self.chains` as paged entries.

`chain_push` and `chain_pop` wrap the chain stack with `BatcherEvent` accounting. Only `PagedColumn::Resident` entries contribute to the `mz_arrangement_batcher_*` introspection counters; `Paged` and `Compressed` entries contribute zero, because bytes on swap or in a pager file are not part of RSS.

`insert_chain` maintains the chain stack invariant: while the youngest chain is at least half the size of its predecessor, merge them. This keeps the total number of chains logarithmic in the number of inserted chunks.

## `FetchIter<'a, D, T, R>`

A streaming materializer over a deque of `PagedColumn` entries. `next()` pops the front entry and calls `ColumnPager::take` to produce a resident `Column`. `into_paged()` drains remaining entries as `PagedColumn`s without materializing, used in the drain-tail phase of `merge_chains` once one input side is exhausted.

## `merge_chains`

A two-way merge driver. Pulls heads from two `FetchIter`s and delegates to `Column::merge_from` for per-record gallop / consolidation. Finished output chunks are routed through the pager via a `sink` callback. When one side is exhausted, the remaining partially-consumed head is copied via `drain_side` and then the rest of the deque is passed straight to the sink without materializing — avoiding unnecessary page-in/page-out round trips.

Whole-chunk passthrough is not implemented: peeking at the endpoints of a paged head would force materialization. A future optimization could add it for `PagedColumn::Resident` heads (where peeking is free) or by storing first/last keys in the pager's chunk metadata.

## `extract_chain`

Walks a merged `FetchIter` chunk-by-chunk, calling `Column::extract` on each to partition entries into keep (times at or after `upper`) and ship (times strictly before `upper`) buffers. Full keep and ship buffers are routed through the pager before being handed to their respective sinks. Partial buffers are flushed after the loop completes.
