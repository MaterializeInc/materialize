---
title: "Lead over"
description: "Use idiomatic Materialize SQL to access the next row's value (lead) when ordered by a field that advances in a regular pattern, such as in regular intervals."
menu:
  main:
    parent: idiomatic-materialize-sql
    identifier: idiomatic-materialize-lead
    weight: 40
---

## Overview

The "lead over" query pattern accesses the field value of the next row as
determined by some ordering.

For "lead over (order by)" queries whose ordering can be represented by some
equality condition (such as when ordering by a field that increases at a regular
interval), Materialize provides an idiomatic SQL as an alternative to the window
function.

{{< callout >}}

### Materialize and window functions

{{% include-headless "/headless/materialize-window-functions" %}}

{{</ callout >}}

## Idiomatic Materialize SQL

{{< important >}}

Do not use if the "lead over (order by)" ordering cannot be represented by an
equality match.

{{</ important >}}

### Exclude the last row in results

**Idiomatic Materialize SQL:** To access the lead (next row's field value)
ordered by some field that increases in **regular** intervals, use a self join
that specifies an **equality condition** on the order by field (e.g., `WHERE
t1.order_field = t2.order_field - 1`, `WHERE t1.order_field = t2.order_field *
2`, etc.). The query *excludes* the last row in the results since it does not
have a next row.

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>
<tr>
<td><blue>Idiomatic Materialize SQL</blue></td>
<td class="copyableCode">

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="lead" field="extra_syntax_idiomatic_exclude" %}}

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="lead" field="syntax_anti_pattern" %}}

</td>
</tr>

</tbody>
</table>

### Include the last row in results

**Idiomatic Materialize SQL:** To access the lead (next row's field value)
ordered by some field that increases in **regular** intervals, use a self [`LEFT
JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join) that specifies an
**equality condition** on the order by field (e.g., `ON t1.order_field =
t2.order_field - 1`, `ON t1.order_field = t2.order_field * 2`, etc.). The `LEFT
JOIN/LEFT OUTER JOIN` query *includes* the last row, returning `null` as its
lead value.

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>
<tr>
<td><blue>Idiomatic Materialize SQL</blue></td>
<td class="copyableCode">

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="lead" field="extra_syntax_idiomatic_include" %}}

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="lead" field="syntax_anti_pattern" %}}

</td>
</tr>

</tbody>
</table>

## Examples

{{< note >}}

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).

{{</ note >}}

### Find next row's value (exclude the last row in results)

Using idiomatic Materialize SQL, the following example finds the next day's
order total. That is, the example uses a self join on `orders_daily_totals`. The
row ordering on the `order_date` field is represented by an **equality
condition** using an [interval of `1
DAY`](/sql/types/interval/#valid-operations). The
query excludes the last row in the results since the last row does not have a
next row.

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>

<tr>
<td><blue>Materialize SQL</blue> ✅</td>
<td class="copyableCode">

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="lead" field="extra_example_idiomatic_exclude" %}}

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="lead" field="example_anti_pattern" %}}

</td>
</tr>
</tbody>
</table>

### Find next row's value (include the last row in results)

Using idiomatic Materialize SQL, the following example finds the next day's
order total. The example uses a self [`LEFT JOIN/LEFT OUTER
JOIN`](/sql/select/join/#left-outer-join) on `orders_daily_totals`. The row
ordering on the `order_date` field is represented by an **equality condition**
using an [interval of `1
DAY`](/sql/types/interval/#valid-operations)). The
query includes the last row in the results, using `null` as the next row's
value.

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>

<tr>
<td><blue>Materialize SQL</blue> ✅</td>
<td class="copyableCode">

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="lead" field="extra_example_idiomatic_include" %}}

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="lead" field="example_anti_pattern" %}}

</td>
</tr>

</tbody>
</table>

## See also

- [Lag over](/transform-data/idiomatic-materialize-sql/lag)
- [`INTERVAL`](/sql/types/interval/)
- [`LEFT JOIN/LEFT OUTER JOIN`](/sql/select/join/#left-outer-join)
- [`LEAD()`](/sql/functions/#lead)
- [Window functions](/sql/functions/#window-functions)
