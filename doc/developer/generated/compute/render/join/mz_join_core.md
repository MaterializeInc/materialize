---
source: src/compute/src/render/join/mz_join_core.rs
revision: e79a6d96d9
---

# mz-compute::render::join::mz_join_core

Implements a fuel-limited join core operator that avoids emitting large output bursts by processing a bounded number of join results per scheduling quantum.
Unlike the default differential join, it tracks remaining fuel and yields control back to Timely when fuel is exhausted, resuming on the next activation.
This prevents the join from monopolizing the worker thread when a large cross-product must be produced.
