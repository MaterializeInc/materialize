---
source: src/adapter/src/coord/sequencer/inner/peek.rs
revision: 4267863081
---

# adapter::coord::sequencer::inner::peek

Implements the coordinator-side peek sequencing path: `sequence_peek_stage` drives the multi-stage pipeline (optimize, timestamp selection, real-time recency, plan execution) via the `PeekStage` enum, spawning off-thread optimizer tasks where appropriate.
Also handles `EXPLAIN PLAN` by running the optimizer with an `Explain` mode `OptimizerConfig` and formatting the result via the `explain` module.
