---
source: src/expr/src/relation/func.rs
revision: e64fd130d9
---

# mz-expr::relation::func

Defines `AggregateFunc`, the enum of all aggregate functions (sum, count, min, max, string_agg, array_agg, jsonb_agg, etc.) and their evaluation logic over `(Datum, Diff)` iterators.
`AggregateFunc::eval` takes `Item = (Datum, Diff)` pairs: `count` sums diffs directly; signed integer sums use `sum_signed_int_counted`, which accumulates `value * diff` in i128; aggregates for which `ignores_multiplicity` returns true (min, max, any, all) evaluate over the distinct datums ignoring the diff; and all remaining aggregates use `expand_counts` to materialize one datum per unit of diff before evaluation.
Also defines `TableFunc`, the enum of set-returning functions (generate_series, jsonb_array_elements, regexp_extract, generate_subscripts, etc.) that produce relation-valued output.
`TableFunc::GenerateSeriesUnoptimized` is an int64 `generate_series` variant the optimizer promises to leave as an enumeration (no transform may apply cardinality shortcuts to it); it lives in `mz_unsafe` and is intended for tests that rely on enumeration work happening.
`TableFunc::GenerateSubscriptsArray` generates subscripts for the requested dimension of an array, using the dimension's actual lower bound (not always 1) and upper bound (`lower_bound + length - 1`), correctly handling arrays with custom lower bounds.
Contains the window function frame types (`WindowFrame`, `WindowFrameBound`, `WindowFrameUnits`) and evaluation for `LagLead` and `FirstLastValue` window functions.
`TableFunc::GuardSubquerySize` enforces the scalar-subquery cardinality contract: counts of 0 or 1 emit no rows and let the subquery's own output flow through, a count greater than 1 returns `EvalError::MultipleRowsFromSubquery`, and a negative count returns `EvalError::NegativeRowsFromSubquery`. Zero is a legitimate input: over a provably empty subquery body, the optimizer can vacuously rewrite the counted expression to null (yielding a count of 0), which subsequent simplification passes may surface as a literal argument evaluated during optimization; emitting no rows is correct because the empty subquery decorrelates to NULL via the outer lookup.
`TableFunc` has two repeat variants: `RepeatRow` repeats a row by the given `i64` count (which may be negative, producing negative diffs and making the function non-monotonic and incompatible with `WITH ORDINALITY`), and `RepeatRowNonNegative` errors on a negative count and is safe for use with `WITH ORDINALITY`.
The corresponding free functions are `repeat_row` and `repeat_row_non_negative`.
The public constant `REPEAT_ROW_NAME` holds the display name `"repeat_row"` and is re-exported from the crate root.
`AnalyzedRegex` wraps a compiled regex and a `Vec<CaptureGroupDesc>` and an `AnalyzedRegexOpts`. Each `CaptureGroupDesc` carries the capture index, optional name, and a `nullable` flag; all capture groups are unconditionally marked `nullable: true` at construction time.
The timestamp `generate_series` iterator (`TimestampRangeStepInclusive`) detects lack of progress at each step and terminates the series rather than looping. A mixed month/day step can reach a fixed point (month addition saturates onto a short month boundary and the day component then returns to the start), which would otherwise loop forever. This deliberately diverges from PostgreSQL, which loops indefinitely on such inputs; in Materialize the loop is reachable in materialized views and indexes where no `statement_timeout` can interrupt it.
