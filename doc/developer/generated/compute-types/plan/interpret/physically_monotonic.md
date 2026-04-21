---
source: src/compute-types/src/plan/interpret/physically_monotonic.rs
revision: b55d3dee25
---

# compute-types::plan::interpret::physically_monotonic

Implements `Interpreter` for the `PhysicallyMonotonic` domain, inferring whether each `Plan` node produces a physically monotonic collection (i.e., one that never retracts data) in a single-time dataflow.
`PhysicallyMonotonic(true)` is the bottom (monotonic), `PhysicallyMonotonic(false)` is the top.
`SingleTimeMonotonic` extends this interpreter with a set of globally monotonic `GlobalId`s, allowing external knowledge (e.g. append-only sources) to seed the analysis.
