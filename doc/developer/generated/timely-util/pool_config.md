---
source: src/timely-util/src/pool_config.rs
revision: 93dcb0ef5a
---

# timely-util::pool_config

Process-wide installation and configuration point for the buffer pool that backs chunk spilling.

## Overview

The module manages a singleton `mz_ore::pool::Pool` shared by every spill consumer in the process. Replacing the pool instance on reconfiguration would split live chunk handle accounting across two budgets, so live reconfiguration instead retunes the one instance in place via `apply_pool_config`.

## Key functions

**`global_pool() -> Option<Pool>`** — returns the process-wide pool, initializing it on first call via `OnceLock`. On platforms where the virtual address space reservation fails, logs a warning and returns `None` permanently. Each call reserves up to tens of TiB of virtual address space (physical memory is only consumed per resident chunk), so processes that never spill should never call this.

**`global_pool_peek() -> Option<Pool>`** — returns the pool only if something already initialized it. Metrics scrapes use this so that observing a process that never spills does not trigger the reservation as a side effect.

**`active_pool() -> Option<Pool>`** — returns the pool only when `apply_pool_config` has installed and budgeted it. Chunk spill consumers call this at each spill decision; a process that has not been configured keeps every chunk resident regardless of the global enable flags in `columnar::chunk`.

**`apply_pool_config(cfg: PoolPagerConfig) -> bool`** — applies a buffer-pool configuration and marks the pool active. Calls `global_pool()` to initialize it on first use, then retunes the budget, RSS target, spill threads, and eager-backing flag in place. Returns `false` (changing nothing) if the pool reservation failed. After a successful call, `active_pool()` resolves to the pool.

## `PoolPagerConfig`

Configuration inputs to `apply_pool_config`. All sizes are absolute bytes resolved by the caller against physical RAM (`mz_ore::memory::physical_memory_bytes`), never against a potentially swap-inclusive limit.

| Field | Meaning |
|---|---|
| `budget_bytes` | Resident-bytes budget for uncompressed pool slots |
| `spill_threads` | Off-worker eviction I/O threads (spawn-once) |
| `eager_backing` | Whether idle spill threads compress chunks ahead of pressure |
| `rss_target_bytes` | Ceiling on total pool RSS; zero collapses the compressed-resident tier |

## Submodule

`metrics` — Prometheus gauges over the pool's stats, registered once and peekable at scrape time without initializing the pool.
