---
source: src/expr/src/interpret.rs
revision: f1513fb122
---

# mz-expr::interpret

Implements an abstract interpreter for `MirScalarExpr` and `MapFilterProject` that propagates range/nullability specifications over datums.
The core types are `ResultSpec<'a>` (an overapproximation of the set of values an expression can produce, tracking nulls, errors, and a `Values` range) and `ColumnSpecs` (a per-column `ResultSpec` for a relation).
`Interpreter` and `MfpEval` provide the evaluation interface; `Trace` is a no-op interpreter used to determine which predicates are pushdownable (i.e., evaluable at the source).
Several timestamp/date/interval binary functions (`add_timestamp_interval`, `add_timestamp_tz_interval`, `sub_timestamp_interval`, `sub_timestamp_tz_interval`, `add_date_interval`, `sub_date_interval`, `date_bin_timestamp`, `date_bin_timestamp_tz`) are marked non-monotone in their interval/stride arguments because `Interval`'s lex order (months, days, micros) does not respect calendar-aware arithmetic with day-clamping.
`Values::union` for two `Nested` specs keeps only keys present in both sides (with per-key specs unioned); keys missing from one side are dropped because the absent side contributes `anything` and `x ∪ anything = anything`.
This module drives filter-pushdown decisions in the explain and optimization pipelines.
