---
source: src/mysql-util/src/partition.rs
revision: 9d524e817c
---

# mysql-util::partition

Primary key space partitioner for MySQL string columns. Divides a `utf8mb4_bin` `CHAR`/`VARCHAR` primary key column into up to `num_workers` roughly even key ranges using prefix-based BFS and `KeyProber` range estimates.

## `partition_table`

The public entry point. Accepts a live MySQL connection (inside a `REPEATABLE READ` transaction), a qualified table reference, the primary key column name, the desired worker count, an estimated total row count, a `min_split_threshold`, and a `max_probed_prefixes` budget. Returns up to `num_workers - 1` exclusive boundary strings that divide the key space into `num_workers` partitions.

The function delegates partitioning to the internal `partition` function and traces the resulting boundaries at the `TRACE` level, redacting them outside CI.

## Algorithm

### Target granularity

`partition` computes a `target_max_rows_per_prefix` as `estimated_row_count / (workers * 4)`, bounded below by `min_split_threshold` and 1. The 4x multiplier causes the algorithm to produce at least 4x more intermediate prefixes than workers before selecting boundaries. This guards against inaccurate MySQL row estimates: `EXPLAIN`-based estimates can overcount by ~2x on large ranges and cap at ~half the table size, so subdividing the space more finely than needed keeps individual estimates smaller and more reliable.

### BFS prefix expansion (`compute_boundaries`)

Starting from a single root prefix covering the entire key space, the algorithm iterates BFS rounds. In each round every prefix whose `estimated_rows` exceeds `target_max_rows_per_prefix` is expanded into child prefixes one character longer via `children_prefixes`, subject to a `max_probed_prefixes` budget that is decremented once per prefix probed. When the budget is exhausted mid-walk, the partial split is discarded and the parent prefix is kept as a leaf. Prefixes within the target are carried forward unchanged. The loop stops when no prefix is split in a round.

After BFS, the algorithm recomputes the total estimated rows across all leaf prefixes (which can diverge from the original top-level estimate) and assigns worker boundaries greedily: it walks the prefixes in key order, accumulating rows, and emits a boundary at each prefix whose cumulative total crosses a multiple of `total / workers`.

### Child prefix enumeration (`children_prefixes`)

Given a parent prefix, `children_prefixes` asks `KeyProber` to find the first key in the parent's range at depth `parent.depth + 1`, then repeatedly finds the next key prefix that does not share the current prefix, collecting adjacent non-overlapping child prefixes. Each child's row estimate comes from `KeyProber::estimate_range_rows`. Keys whose value equals their own prefix exactly (or sorts below the prefix space at that depth) are dropped, which is acceptable given the approximate nature of the algorithm.
