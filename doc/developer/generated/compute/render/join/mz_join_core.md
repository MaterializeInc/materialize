---
source: src/compute/src/render/join/mz_join_core.rs
revision: de27f5b2b1
---

# mz-compute::render::join::mz_join_core

Implements a fuel-limited join core operator that avoids emitting large output bursts by processing a bounded number of join results per scheduling quantum.
Unlike the default differential join, it tracks remaining fuel and yields control back to Timely when fuel is exhausted, resuming on the next activation.
This prevents the join from monopolizing the worker thread when a large cross-product must be produced.
At start-up, the operator preloads existing trace batches into deferred work and records fixed `preload_upper1`/`preload_upper2` frontiers marking the extent of that preload. Arriving input batches whose upper is at or below the corresponding preload frontier are skipped; those beyond it are joined even if `advance_upper` has since moved the mutable `acknowledged` frontier past them. This guards against a scenario where trace merging consolidates an in-flight batch's updates away, making the shared trace appear empty at that range while finer-time consumers still require the updates. The `acknowledged` frontier is advanced only when the arriving batch's lower is at or below the current acknowledged value, and a `soft_assert_or_log` guards against regression.
