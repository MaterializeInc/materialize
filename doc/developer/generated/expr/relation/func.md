---
source: src/expr/src/relation/func.rs
revision: 5f785f23fd
---

# mz-expr::relation::func

Defines `AggregateFunc`, the enum of all aggregate functions (sum, count, min, max, string_agg, array_agg, jsonb_agg, etc.) and their evaluation logic over `Datum` iterators.
Also defines `TableFunc`, the enum of set-returning functions (generate_series, jsonb_array_elements, regexp_extract, etc.) that produce relation-valued output.
Contains the window function frame types (`WindowFrame`, `WindowFrameBound`, `WindowFrameUnits`) and evaluation for `LagLead` and `FirstLastValue` window functions.
