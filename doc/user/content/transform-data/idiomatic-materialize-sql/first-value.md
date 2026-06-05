---
title: "First value in group"
description: "Use idiomatic Materialize SQL to find the first value in each group."
menu:
  main:
    parent: idiomatic-materialize-sql
    identifier: idiomatic-materialize-first-value
    weight: 10
---

## Overview

The "first value in each group" query pattern returns the first value, according
to some ordering, in each group.

{{< callout >}}

### Materialize and window functions

{{% include-headless "/headless/materialize-window-functions" %}}

{{</ callout >}}

## Idiomatic Materialize SQL

**Idiomatic Materialize SQL:** To find the first value in each group, use
[MIN()](/sql/functions/#min) or [MAX()](/sql/functions/#max) aggregate function
in a subquery.

<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>
<tr>
<td><blue>Materialize SQL</blue></td>
<td class="copyableCode">

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="first-value" field="syntax_idiomatic" %}}

</td>
</tr>

<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="first-value" field="syntax_anti_pattern" %}}

</td>
</tr>

</tbody>
</table>

### Query hints

To further improve the memory usage of the idiomatic Materialize SQL, you can
specify a [`AGGREGATE INPUT GROUP SIZE` query hint](/sql/select/#query-hints) in
the idiomatic Materialize SQL.

```mzsql
SELECT tableA.fieldA, tableA.fieldB, minmax.Z
 FROM tableA,
 (SELECT fieldA,
    MIN(fieldZ),
    MAX(fieldZ)
 FROM tableA
 GROUP BY fieldA
 OPTIONS (AGGREGATE INPUT GROUP SIZE = ...)
 ) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```

For more information on setting `AGGREGATE INPUT GROUP SIZE`, see
[Optimization](/transform-data/optimization/#query-hints).

## Examples

{{< note >}}

The example data can be found in the
[Appendix](/transform-data/idiomatic-materialize-sql/appendix/example-orders).

{{</ note >}}

### Use MIN() to find the first value

Using idiomatic Materialize SQL, the following example finds the lowest item
price in each order and calculates the difference between the price of each item
in the order and the lowest price. The example uses a subquery that groups by
the `order_id` and selects `MIN(price)` to find the lowest price (i.e., first
value if ordered by ascending price values).

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

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="first-value" field="extra_example_idiomatic_min" %}}

</td>
</tr>
<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="first-value" field="extra_example_anti_pattern_min" %}}

</td>
</tr>
</tbody>
</table>

### Use MAX() to find the first value

Using idiomatic Materialize SQL, the following example finds the highest item
price in each order and calculates the difference between the price of each item
in the order and the highest price. The example uses a subquery that groups by
the `order_id` and selects `MAX(price)` to find the highest price (i.e., first
value if ordered by descending price values).

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

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="first-value" field="extra_example_idiomatic_max" %}}

</td>
</tr>
<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="first-value" field="extra_example_anti_pattern_max" %}}

</td>
</tr>
</tbody>
</table>

### Use MIN() and MAX() to find the first values

Using idiomatic Materialize SQL, the following example finds the lowest and the
highest item price in each order and calculates the difference between each item
in the order and these prices. The example uses a subquery that groups by the
`order_id` and selects `MIN(price)` as the lowest price (i.e., first
value if ordered by price values) and `MAX(price)` as the
highest price (i.e., first
value if ordered by descending price values).

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

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="first-value" field="example_idiomatic" %}}

</td>
</tr>
<tr>
<td><red>Anti-pattern</red> ❌</td>
<td>

{{% include-from-yaml data="idiomatic_mzsql/patterns_window_functions" name="first-value" field="example_anti_pattern" %}}

</td>
</tr>
</tbody>
</table>

## See also

- [Last value in a group](/transform-data/idiomatic-materialize-sql/last-value)
- [`MIN()`](/sql/functions/#min)
- [`MAX()`](/sql/functions/#max)
- [Query hints for MIN/MAX](/transform-data/optimization/#query-hints)
- [Window functions](/sql/functions/#window-functions)
