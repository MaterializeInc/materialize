---
source: src/persist-client/src/internal/machine.rs
revision: 5680493e7d
---

# persist-client::internal::machine

Implements `Machine`, the core state-machine driver that sequences all shard operations (compare-and-append, reader/writer registration and expiry, since downgrade, compaction, rollup) by retrying CaS against `Applier` until successful.
Each operation is a pure state transition function applied to a `State` snapshot; `Machine` handles the retry loop, maintenance scheduling, and error propagation.
Also provides the `retry_external` and `retry_determinate` helpers that standardize backoff behavior for transient storage errors.
Includes configurable compaction claiming via `CLAIM_UNCLAIMED_COMPACTIONS`, `CLAIM_COMPACTION_PERCENT`, and `CLAIM_COMPACTION_MIN_VERSION` dynamic configs.
