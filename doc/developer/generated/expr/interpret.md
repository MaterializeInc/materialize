---
source: src/expr/src/interpret.rs
revision: c5a44faf03
---

# mz-expr::interpret

Implements an abstract interpreter for `MirScalarExpr` and `MapFilterProject` that propagates range/nullability specifications over datums.
The core types are `ResultSpec<'a>` (an overapproximation of the set of values an expression can produce, tracking nulls, errors, and a `Values` range) and `ColumnSpecs` (a per-column `ResultSpec` for a relation).
`Interpreter` and `MfpEval` provide the evaluation interface; `Trace` is a no-op interpreter used to determine which predicates are pushdownable (i.e., evaluable at the source).
Several timestamp/date/interval binary functions (`add_timestamp_interval`, `add_timestamp_tz_interval`, `sub_timestamp_interval`, `sub_timestamp_tz_interval`, `add_date_interval`, `sub_date_interval`, `date_bin_timestamp`, `date_bin_timestamp_tz`) are treated non-monotone in their interval/stride arguments because `Interval`'s lex order (months, days, micros) does not respect calendar-aware arithmetic with day-clamping. However, the interpreter uses a `DynamicMonotone` handler for `add_timestamp_interval` and related functions that recovers monotonicity at runtime when the interval argument is a single literal value with `months == 0`: in that case the operation reduces to adding a fixed number of microseconds, which is monotone in the timestamp argument. This allows filter pushdown to propagate tight timestamp bounds for day-only or microsecond-only interval predicates.
`Values::union` for two `Nested` specs keeps only keys present in both sides (with per-key specs unioned); keys missing from one side are dropped because the absent side contributes `anything` and `x ∪ anything = anything`.
`ColumnSpecs` overrides `mfp_filter` and `mfp_plan_filter` to propagate the `fallible` flag from MFP expressions that are not referenced by any predicate or temporal bound. The runtime MFP evaluator runs every expression once predicates pass, so an expression that errors on the actual data turns the whole row into an `Err` regardless of whether a predicate references it; without this override, persist filter pushdown could wrongly discard parts that produce error rows.
This module drives filter-pushdown decisions in the explain and optimization pipelines.
