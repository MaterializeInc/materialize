---
source: src/adapter/src/coord/peek.rs
revision: bce428d203
---

# adapter::coord::peek

Defines the data structures and execution logic for coordinator-side peeks (SELECT queries).
`PeekDataflowPlan` describes a slow-path peek that requires a dataflow; `FastPathPlan` covers constant results, direct arrangement reads, and persist fast-path reads.
`implement_peek_plan` ships the peek to the compute layer or evaluates it immediately, returning a `PeekResponseUnary` stream; `PeekResponseUnary` carries rows, errors, or a cancellation signal back to the client.
