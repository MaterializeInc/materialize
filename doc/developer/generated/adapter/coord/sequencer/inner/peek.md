---
source: src/adapter/src/coord/sequencer/inner/peek.rs
revision: f936d67792
---

# adapter::coord::sequencer::inner::peek

Implements the coordinator-side peek and COPY TO sequencing paths.
`sequence_peek`, `sequence_copy_to`, and `explain_peek` each call `peek_validate` to construct an initial `PeekStage`, then drive it through the multi-stage `PeekStage` pipeline (`LinearizeTimestamp` → `RealTimeRecency` → `TimestampReadHold` → `Optimize` → `Finish`, with side branches for `ExplainPlan`, `ExplainPushdown`, `CopyToPreflight`, and `CopyToDataflow`) via the `Staged` trait and `sequence_staged`, spawning off-thread optimizer tasks where appropriate.
`EXPLAIN PLAN` runs the optimizer in explain mode and formats the result via the `explain` module; `EXPLAIN PUSHDOWN` follows a similar path through `PeekStageExplainPushdown`.
In the `RealTimeRecency` stage, the RTR future is awaited via `Coordinator::await_real_time_recent_timestamp` so that `StorageError::RtrTimeout` and `StorageError::RtrDropFailure` are converted to humanized `AdapterError` variants before propagating.
