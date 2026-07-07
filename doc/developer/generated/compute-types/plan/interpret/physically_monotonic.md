---
source: src/compute-types/src/plan/interpret/physically_monotonic.rs
revision: e926ec3a86
---

# compute-types::plan::interpret::physically_monotonic

Implements `Interpreter` for the `PhysicallyMonotonic` domain, inferring whether each `LirRelationExpr` node produces a physically monotonic collection (i.e., one that never retracts data) in a single-time dataflow.
`PhysicallyMonotonic(true)` is the bottom (monotonic), `PhysicallyMonotonic(false)` is the top.
`SingleTimeMonotonic` extends this interpreter with a set of globally monotonic `GlobalId`s, allowing external knowledge (e.g. append-only sources) to seed the analysis. Method signatures accept `LirScalarExpr`-based types; temporal bound checks use `mfp.has_temporal_bounds()` (previously `has_temporal_predicates()`).
