---
source: src/adapter/src/coord/peek.rs
revision: 4002e6039c
---

# adapter::coord::peek

Defines the data structures and execution logic for coordinator-side peeks (SELECT queries).
`PeekDataflowPlan` describes a slow-path peek that requires a dataflow; `FastPathPlan` covers constant results, direct arrangement reads (`PeekExisting`), and persist fast-path reads (`PeekPersist`).
`PlannedPeek` bundles a `PeekPlan` (either `FastPath` or `SlowPath`) with the timestamp determination, connection ID, result type, and source IDs needed for execution.
`implement_peek_plan` ships the peek to the compute layer or evaluates it immediately; `create_peek_response_stream` converts the compute-layer `PeekResponse` into a stream of `PeekResponseUnary` values carrying rows, errors, or a cancellation signal.
`implement_slow_path_peek` is the coordinator command handler for `Command::ExecuteSlowPathPeek`; `implement_copy_to` ships a COPY TO dataflow and spawns a background task to await completion.
`PeekResponseUnary` carries rows, errors, a cancellation signal, or a `DependencyDropped(DroppedDependency)` variant (used when a relation or cluster dependency was dropped mid-flight) back to the client.
`DroppedDependency` is an enum with `Relation { name }` and `Cluster { name }` variants; it formats as a quoted SQL identifier with kind prefix (e.g. `relation "db.schema.t"`) and provides `query_terminated_error` and `to_concurrent_dependency_drop` helpers for producing user-facing error messages.
`create_fast_path_plan` inspects the optimized dataflow plan to detect whether a fast-path execution is possible, returning a `FastPathPlan` if so.
