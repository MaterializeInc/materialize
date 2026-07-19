---
source: src/expr/src/interpret.rs
revision: d08f8f74a0
---

# mz-expr::interpret

Implements an abstract interpreter for `MirScalarExpr` and `MapFilterProject` that propagates range/nullability specifications over datums.
The core types are `ResultSpec<'a>` (an overapproximation of the set of values an expression can produce, tracking nulls, errors, and a `Values` range) and `ColumnSpecs` (a per-column `ResultSpec` for a relation).
`Interpreter` and `MfpEval` provide the evaluation interface; `Trace` is a no-op interpreter used to determine which predicates are pushdownable (i.e., evaluable at the source).
Several timestamp/date/interval binary functions (`add_timestamp_interval`, `add_timestamp_tz_interval`, `sub_timestamp_interval`, `sub_timestamp_tz_interval`, `add_date_interval`, `sub_date_interval`, `date_bin_timestamp`, `date_bin_timestamp_tz`) are treated non-monotone in their interval/stride arguments because `Interval`'s lex order (months, days, micros) does not respect calendar-aware arithmetic with day-clamping. However, the interpreter uses a `DynamicMonotone` handler for `add_timestamp_interval` and related functions that recovers monotonicity at runtime when the interval argument is a single literal value with `months == 0`: in that case the operation reduces to adding a fixed number of microseconds, which is monotone in the timestamp argument. This allows filter pushdown to propagate tight timestamp bounds for day-only or microsecond-only interval predicates.
`Values::union` for two `Nested` specs keeps only keys present in both sides (with per-key specs unioned); keys missing from one side are dropped because the absent side contributes `anything` and `x ∪ anything = anything`.
`Values::intersect` of a `Nested` spec and a `Within` range returns the `Nested` side as a sound over-approximation rather than `Empty`.
`ColumnSpecs` overrides `mfp_filter` and `mfp_plan_filter` to propagate the `fallible` flag from MFP expressions that are not referenced by any predicate or temporal bound. The runtime MFP evaluator runs every expression once predicates pass, so an expression that errors on the actual data turns the whole row into an `Err` regardless of whether a predicate references it; without this override, persist filter pushdown could wrongly discard parts that produce error rows.
`ResultSpec` provides `is_single_value()` and `may_be_infinite()` accessors. `datum_is_nan` and `datum_is_infinite` are helper functions used during monotone range narrowing.
`flat_map` monotone narrowing skips ranges whose bounds include `NaN` (NaN is a fixed point of monotone functions but sorts as max, breaking endpoint soundness). The monotone arm treats value, null, and error as orthogonal channels: the output range is bounded by the value endpoints, but null and error channels from the endpoints are carried through independently.
`ColumnSpecs::unary`, `ColumnSpecs::binary`, and `ColumnSpecs::variadic` force `fallible=true` for a `could_error` function over a multi-valued input range, because endpoint sampling cannot prove a function infallible over an interior value. `ColumnSpecs::binary` falls back to `Values::All` for functions that are not `is_infinity_monotone` when an operand may be infinite (e.g. `Mul`/`Div`). `ColumnSpecs::if_then_else` maps a null condition to the `els` branch, matching `MirScalarExpr::eval` behavior.
This module drives filter-pushdown decisions in the explain and optimization pipelines.
