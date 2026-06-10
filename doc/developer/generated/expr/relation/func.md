---
source: src/expr/src/relation/func.rs
revision: 931f79a1d8
---

# mz-expr::relation::func

Defines `AggregateFunc`, the enum of all aggregate functions (sum, count, min, max, string_agg, array_agg, jsonb_agg, etc.) and their evaluation logic over `Datum` iterators.
Also defines `TableFunc`, the enum of set-returning functions (generate_series, jsonb_array_elements, regexp_extract, generate_subscripts, etc.) that produce relation-valued output.
`TableFunc::GenerateSubscriptsArray` generates subscripts for the requested dimension of an array, using the dimension's actual lower bound (not always 1) and upper bound (`lower_bound + length - 1`), correctly handling arrays with custom lower bounds.
Contains the window function frame types (`WindowFrame`, `WindowFrameBound`, `WindowFrameUnits`) and evaluation for `LagLead` and `FirstLastValue` window functions.
The `TableFunc::SubqueryScalar` evaluation checks the row count: a count of 1 returns no rows (success), a count greater than 1 returns `EvalError::MultipleRowsFromSubquery`, and a negative count returns `EvalError::NegativeRowsFromSubquery`.
`TableFunc` has two repeat variants: `RepeatRow` repeats a row by the given `i64` count (which may be negative, producing negative diffs and making the function non-monotonic and incompatible with `WITH ORDINALITY`), and `RepeatRowNonNegative` errors on a negative count and is safe for use with `WITH ORDINALITY`.
The corresponding free functions are `repeat_row` and `repeat_row_non_negative`.
The public constant `REPEAT_ROW_NAME` holds the display name `"repeat_row"` and is re-exported from the crate root.
`AnalyzedRegex` wraps a compiled regex and a `Vec<CaptureGroupDesc>` and an `AnalyzedRegexOpts`. Each `CaptureGroupDesc` carries the capture index, optional name, and a `nullable` flag; all capture groups are unconditionally marked `nullable: true` at construction time.
