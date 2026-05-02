# adapter::coord::sequencer::inner

Per-statement sequencing implementations split from `inner.rs` for the
most complex SQL statement types. Each child file owns one focused area.

## Files (LOC ≈ 7,133 across this directory)

| File | What it owns |
|---|---|
| `peek.rs` | `Staged for PeekStage` — multi-stage SELECT/EXPLAIN: `LinearizeTimestamp` → `RealTimeRecency` → `TimestampReadHold` → `Optimize` → `Finish` (or `ExplainPlan`/`ExplainPushdown`/`CopyTo{Preflight,Dataflow}`) |
| `subscribe.rs` | `Staged for SubscribeStage` — `OptimizeMir` → `TimestampOptimizeLir` → `Finish` (or `Explain`); installs the dataflow on the compute cluster and returns a `RowBatchStream` to pgwire |
| `cluster.rs` | `CREATE/ALTER/DROP CLUSTER` and `CREATE/ALTER CLUSTER REPLICA` sequencing; replica-scheduling and managed-cluster reconciliation glue |
| `copy_from.rs` | `COPY FROM` ingestion path (file/HTTP/S3) |
| `create_index.rs` | Index creation: optimizer invocation + dataflow install |
| `create_materialized_view.rs` | MV creation: optimizer + storage shard + dataflow install |
| `create_view.rs` | Logical view creation (no dataflow) |
| `create_continual_task.rs` | Continual-task creation (compute sink fed by transitions of an input collection) |
| `secret.rs` | Secret create/alter/drop, including secrets-controller side effects |
| `explain_timestamp.rs` | `EXPLAIN TIMESTAMP` |

## Key concepts

- **`Staged` trait** *(defined in `inner.rs`, one level up)* — the contract every multi-stage sequencer enum implements. Two methods: `validity()` returns a mutable `PlanValidity` (re-checked between stages), and `stage()` advances by one step, returning a `StageResult` that is either `Immediate` (loop again) or `Handle` (off-thread task whose result will be re-queued via `Coordinator::message_handler`).
- **Stage enums** *(`PeekStage`, `SubscribeStage`, etc.)* — one variant per checkpoint in the pipeline. Variants carry the in-flight state needed to resume after off-thread work (optimizer compilation, timestamp linearization, real-time recency wait).
- **Off-thread escape hatch.** Heavy work (optimizer passes, RTR waits) is moved off the coordinator's main thread by returning `StageResult::Handle`; the resumed `Message` carries the next `PeekStage` back to the coordinator's single-threaded loop.
- **`PlanValidity`.** Snapshot of catalog identity (cluster id, role memberships, dependency ids) captured at sequence start; re-validated between stages so a concurrent DDL invalidates the in-flight peek/subscribe instead of producing a stale result.
- **`return_if_err!` macro.** Short-circuit: convert `Err` to `ExecuteResponse` and send to the client.
- **Adjacency to `inner.rs`.** `inner.rs` itself implements the simpler `sequence_*` methods (most DDL, transaction control, SHOW/SET/RESET/FETCH/RAISE) inline; this directory holds the variants whose pipelines are too long to keep there.

## Cross-references

- Parent `sequencer.rs` contains `sequence_plan`, the top-level dispatch over `Plan`, plus shared utilities (`statistics_oracle`, `eval_copy_to_uri`, `check_log_reads`).
- `Coordinator::optimize` ↔ `crate::optimize` for the optimizer interface.
- `Coordinator::peek` (separate from this) for the lower-level peek protocol against the compute layer.
- Generated developer docs: `doc/developer/generated/adapter/coord/sequencer/inner/`.
