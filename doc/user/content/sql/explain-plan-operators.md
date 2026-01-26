---
title: "Explain plan operators"
description: ""
menu:
  main:
    parent: reference
    name: Explain plan operators
    identifier: 'explain-plan-operators'
    weight: 140
disable_list: true
---

Materialize offers several output formats for [`EXPLAIN
PLAN`](/sql/explain-plan/) and debugging. LIR plans as rendered in
[`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping)
are deliberately succinct, while the plans in other formats give more detail.

The decorrelated and optimized plans from `EXPLAIN DECORRELATED PLAN
FOR ...`, `EXPLAIN LOCALLY OPTIMIZED PLAN FOR ...`, and `EXPLAIN
OPTIMIZED PLAN FOR ...` are in a mid-level representation that is
closer to LIR than SQL. The raw plans from `EXPLAIN RAW PLAN FOR ...`
are closer to SQL (and therefore less indicative of how the query will
actually run).

{{< tabs >}}

{{< tab "In fully optimized physical (LIR) plans (Default)" >}}
{{< explain-plans/operator-table data="explain_plan_operators" planType="LIR" >}}
{{< /tab >}}

{{< tab "In decorrelated and optimized plans" >}}
{{< explain-plans/operator-table data="explain_plan_operators" planType="optimized" >}}
{{< /tab >}}

{{< tab "In raw plans" >}}
{{< explain-plans/operator-table data="explain_plan_operators" planType="raw" >}}
{{< /tab >}}

{{< /tabs >}}

Operators are sometimes marked as `Fused ...`. This indicates that the operator is fused with its input, i.e., the operator below it. That is, if you see a `Fused X` operator above a `Y` operator:

```
→Fused X
  →Y
```

Then the `X` and `Y` operators will be combined into a single, more efficient operator.

See also:

- [`EXPLAIN PLAn`](/sql/explain-plan/)
