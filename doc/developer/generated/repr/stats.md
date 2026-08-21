---
source: src/repr/src/stats.rs
revision: 95baa04a85
---

# mz-repr::stats

Provides persist statistics implementations for non-primitive `mz-repr` types: decodes Arrow column statistics (min/max) for `Numeric`, timestamps, intervals, JSONB, and other complex types that need custom codec-aware stat extraction.
These stats are used by persist's pushdown filters to skip reading shards or batches that provably cannot match a query's filter predicates.
`col_values` extracts a `(lower, upper)` datum pair from a `ColumnStatKinds`. For `Float32` and `Float64` columns, bounds are first passed through `float_bounds`, which returns `None` (treating the column as unconstrained) when the lower bound is a negative NaN but the upper bound is not: persist records float stats in IEEE-754 total order where `-NaN` sorts below `-Infinity`, while `Datum` ordering ranks every NaN above every finite value, so such bounds cannot be expressed as a valid datum interval.
Legacy V0 stats use `AtomicBytesStats` (a `BytesStats::Atomic` variant), which carries bounds encoded as `ProtoDatum` bytes without a type tag. `col_values` decodes these bytes and validates that the resulting `Datum` matches the expected column type before using them. A mismatch (which can occur under version skew or schema change) is logged via `soft_panic_or_log!` and returns `None` rather than using a wrong-typed bound that could exclude valid column values.
