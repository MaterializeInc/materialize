---
source: src/timely-util/src/columnar/merge_batcher.rs
revision: 5d6bfccaf8
---

# timely-util::columnar::merge_batcher

A merge-batcher for `Column` chunks that routes chunks through a `ColumnPager` so they can be paged out to swap or compressed storage between merge and extract steps.

This module forks the `differential_dataflow` merge-batcher framework. Chains hold `PagedColumn` entries rather than resident `Column`s. The pager is consulted on every chunk produced during insert, merge, and extract; chunks are fetched back lazily during merge and seal via `FetchIter`.

Key types and functions:

* `ColumnMergeBatcher<D, T, R>` — implements `differential_dataflow::trace::Batcher` and `timely::container::PushInto<Column<(D, T, R)>>`. Maintains a stack of `VecDeque<PagedColumn>` chains. On `push_into` it receives an already-consolidated `Column` chunk and pages it. On `seal` it merges all chains, partitions by the frontier (`extract_chain`), returns the ship side to the caller's `Builder`, and retains the keep side paged for the next round. The pager is resolved lazily per call from `column_pager::global_pager`; tests can pin a specific pager via `set_pager`.
* `FetchIter<'a, D, T, R>` — a streaming materializer over a `VecDeque<PagedColumn>`. Calling `next()` pops the front entry and calls `ColumnPager::take` to produce a resident `Column`. `into_paged` drains remaining entries without materializing, for use in the drain-tail phase of `merge_chains`.
* `merge_chains` — a two-way merge driver. Pulls heads from two `FetchIter`s, applies a whole-chunk passthrough fast path for disjoint-range chains, and routes each finished output chunk through the pager. Recycled empty chunks are kept in a caller-supplied `stash` (capped at `STASH_CAP = 2`) to avoid per-chunk allocation in the merge inner loop.
* `extract_chain` — streams through a merged `FetchIter` chunk by chunk, calling `Column::extract` to partition records by an `upper` frontier, routing keep and ship outputs through the pager and a recycling stash.
* `account_chunk` — returns `(records, size, capacity, allocations)` for a `PagedColumn` entry; paged-out and compressed entries contribute zero so only resident bytes appear in `BatcherEvent` memory accounting.
