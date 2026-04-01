---
source: src/adapter/src/coord/peek.rs
revision: 5680493e7d
---

# adapter::coord::peek

Defines the data structures and execution logic for coordinator-side peeks (SELECT queries).
`PeekPlan` discriminates between fast-path (`FastPathPlan`) and slow-path (`PeekDataflowPlan`) execution; `FastPathPlan` covers constant results, direct arrangement reads, and persist fast-path reads.
`implement_peek_plan` ships the peek to the compute layer or evaluates it immediately, returning a `PeekResponseUnary` stream; `PeekResponseUnary` carries rows, errors, or a cancellation signal back to the client.
