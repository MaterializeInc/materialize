---
source: src/adapter/src/coord/peek.rs
revision: 1bc92070ac
---

# adapter::coord::peek

Defines the data structures and execution logic for coordinator-side peeks (SELECT queries).
`PeekDataflowPlan` describes a slow-path peek that requires a dataflow; `FastPathPlan` covers constant results, direct arrangement reads, and persist fast-path reads.
`implement_peek_plan` ships the peek to the compute layer or evaluates it immediately, returning a `PeekResponseUnary` stream; `PeekResponseUnary` carries rows, errors, a cancellation signal, or a `DependencyDropped(DroppedDependency)` variant (used when a relation or cluster dependency was dropped mid-flight) back to the client.
`DroppedDependency` is an enum with `Relation { name }` and `Cluster { name }` variants; it formats as a quoted SQL identifier with kind prefix (e.g. `relation "db.schema.t"`) and provides `query_terminated_error` and `to_concurrent_dependency_drop` helpers for producing user-facing error messages.
