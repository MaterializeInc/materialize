<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Overview](/docs/self-managed/v25.2/transform-data/)  /  [Idiomatic
Materialize
SQL](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/)

</div>

# Lag over

## Overview

The “lag over (order by )” query pattern accesses the field value of the
previous row as determined by some ordering.

For “lag over (order by)” queries whose ordering can be represented by
some equality condition (such as when ordering by a field that increases
at a regular interval), Materialize provides an idiomatic SQL as an
alternative to the window function.

<div class="callout">

<div>

### Materialize and window functions

For [window
functions](/docs/self-managed/v25.2/sql/functions/#window-functions),
when an input record in a partition (as determined by the `PARTITION BY`
clause of your window function) is added/removed/changed, Materialize
recomputes the results for the entire window partition. This means that
when a new batch of input data arrives (that is, every second), **the
amount of computation performed is proportional to the total size of the
touched partitions**.

For example, assume that in a given second, 20 input records change, and
these records belong to **10** different partitions, where the average
size of each partition is **100**. Then, amount of work to perform is
proportional to computing the window function results for
**10\*100=1000** rows.

To avoid performance issues that may arise as the number of records
grows, consider rewriting your query to use idiomatic Materialize SQL
instead of window functions. If your query cannot be rewritten without
the window functions and the performance of window functions is
insufficient for your use case, please [contact our
team](/docs/self-managed/v25.2/support/).

</div>

</div>

## Idiomatic Materialize SQL

<div class="important">

**! Important:** Do not use if the “lag over (order by)” ordering cannot
be represented by an equality match.

</div>

### Exclude the first row in results

**Idiomatic Materialize SQL:** To access the lag (previous row’s field
value) ordered by some field that increases in a **regular** pattern,
use a self join that specifies an **equality condition** on the order by
field (e.g., `WHERE t1.order_field = t2.order_field + 1`,
`WHERE t1.order_field = t2.order_field * 2`, etc.). The query *excludes*
the first row since it does not have a previous row.

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr>
<td>Idiomatic Materialize SQL</td>
<td class="copyableCode"><p>Use a self join that specifies an
<strong>equality match</strong> on the lag’s order by field (e.g.,
<code>fieldA</code>). The order by field must increment in a regular
pattern in order to be represented by an equality condition (e.g.,
<code>WHERE t1.fieldA = t2.fieldA + ...</code>). The query
<em>excludes</em> the first row in the results since it does not have a
previous row.</p>
<div class="important">
<strong>! Important:</strong> The idiomatic Materialize SQL applies only
to those “lag over” queries whose ordering can be represented by some
<strong>equality condition</strong>.
</div>
<br />
&#10;<div class="highlight">
<pre class="chroma" tabindex="0"><code>-- Excludes the first row in the results --
SELECT t1.fieldA, t2.fieldB as previous_row_value
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA + ... -- or some other operand
ORDER BY fieldA;</code></pre>
</div></td>
</tr>
<tr>
<td>Anti-pattern ❌</td>
<td><p>Avoid the use of <a
href="/docs/self-managed/v25.2/sql/functions/#lag"><code>LAG(fieldZ) OVER (ORDER BY ...)</code></a>
window function when the order by field increases in a regular
pattern.</p>
<br />
&#10;<div style="background-color: var(--code-block)">
<pre tabindex="0"><code>-- Anti-pattern. Avoid. --
SELECT fieldA, ...
    LAG(fieldZ) OVER (ORDER BY fieldA) as previous_row_value
FROM tableA;</code></pre>
</div></td>
</tr>
</tbody>
</table>

### Include the first row in results

**Idiomatic Materialize SQL:** To access the lag (previous row’s field
value) ordered by some field that increases in a **regular** pattern,
use a self
[`LEFT JOIN/LEFT OUTER JOIN`](/docs/self-managed/v25.2/sql/select/join/#left-outer-join)
that specifies an **equality condition** on the order by field (e.g.,
`ON t1.order_field = t2.order_field + 1`,
`ON t1.order_field = t2.order_field * 2`, etc.). The
`LEFT JOIN/LEFT OUTER JOIN` query *includes* the first row, returning
`null` as its lag value.

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr>
<td>Idiomatic Materialize SQL</td>
<td class="copyableCode"><p>Use a self <a
href="/docs/self-managed/v25.2/sql/select/join/#left-outer-join"><code>LEFT JOIN/LEFT OUTER JOIN</code></a>
(e.g., <code>FROM tableA t1 LEFT JOIN tableA t2</code>) that specifies
an <strong>equality match</strong> on the lag’s order by field (e.g.,
<code>fieldA</code>). The order by field must increment in a regular
pattern in order to be represented by an equality condition (e.g.,
<code>ON t1.fieldA = t2.fieldA + ...</code>). The query
<em>includes</em> the first row, returning <code>null</code> as its lag
value.</p>
<div class="important">
<strong>! Important:</strong> The idiomatic Materialize SQL applies only
to those “lag over” queries whose ordering can be represented by some
<strong>equality condition</strong>.
</div>
<br />
&#10;<div class="highlight">
<pre class="chroma" tabindex="0"><code>-- Includes the first row in the results --
SELECT t1.fieldA, t2.fieldB as previous_row_value
FROM tableA t1
LEFT JOIN tableA t2
ON t1.fieldA = t2.fieldA + ... -- or some other operand
ORDER BY fieldA;</code></pre>
</div></td>
</tr>
<tr>
<td>Anti-pattern ❌</td>
<td><p>Avoid the use of <a
href="/docs/self-managed/v25.2/sql/functions/#lag"><code>LAG(fieldZ) OVER (ORDER BY ...) window function</code></a>
when the order by field increases in a regular pattern.</p>
<br />
&#10;<div style="background-color: var(--code-block)">
<pre tabindex="0"><code>SELECT fieldA, ...
    LAG(fieldZ) OVER (ORDER BY fieldA) as previous_row_value
FROM tableA;</code></pre>
</div></td>
</tr>
</tbody>
</table>

## Examples

<div class="note">

**NOTE:** The example data can be found in the
[Appendix](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/appendix/example-orders).

</div>

### Find previous row’s value (exclude the first row in results)

Using idiomatic Materialize SQL, the following example finds the
previous day’s order total. That is, the example uses a self join on
`orders_daily_totals`. The row ordering on the `order_date` field is
represented by an **equality condition** using an [interval of
`1 DAY`](/docs/self-managed/v25.2/sql/types/interval/#valid-operations).
The query excludes the first row in the results since the first row does
not have a previous row.

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr>
<td>Materialize SQL ✅</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>-- Excludes the first row in results --
SELECT o1.order_date, o1.daily_total,
    o2.daily_total as previous_daily_total
FROM orders_daily_totals o1, orders_daily_totals o2
WHERE o1.order_date = o2.order_date + INTERVAL &#39;1&#39; DAY
ORDER BY order_date;</code></pre>
</div>
<div class="important">
<strong>! Important:</strong> The idiomatic Materialize SQL applies only
to those “lag over” queries whose ordering can be represented by some
<strong>equality condition</strong>.
</div></td>
</tr>
<tr>
<td>Anti-pattern ❌</td>
<td><p>Avoid the use of <a
href="/docs/self-managed/v25.2/sql/functions/#lag"><code>LAG() OVER (ORDER BY ...)</code>
window function</a> to access previous row’s value if the order by field
increases in a regular pattern.</p>
<br />
&#10;<div style="background-color: var(--code-block)">
<pre tabindex="0"><code>-- Anti-pattern. Includes the first row&#39;s value. --
SELECT order_date, daily_total,
    LAG(daily_total) OVER (ORDER BY order_date) as previous_daily_total
FROM orders_daily_totals;</code></pre>
</div></td>
</tr>
</tbody>
</table>

### Find previous row’s value (include the first row in results)

Using idiomatic Materialize SQL, the following example finds the
previous day’s order total. The example uses a self
[`LEFT JOIN/LEFT OUTER JOIN`](/docs/self-managed/v25.2/sql/select/join/#left-outer-join)
on `orders_daily_totals`. The row ordering on the `order_date` field is
represented by an **equality condition** using an [interval of
`1 DAY`](/docs/self-managed/v25.2/sql/types/interval/#valid-operations).
The query includes the first row in the results, using `null` as the
previous value.

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr>
<td>Materialize SQL ✅</td>
<td class="copyableCode"><div class="highlight">
<pre class="chroma" tabindex="0"><code>-- Include the first row in results --
SELECT o1.order_date, o1.daily_total,
    o2.daily_total as previous_daily_total
FROM orders_daily_totals o1
LEFT JOIN orders_daily_totals o2
ON o1.order_date = o2.order_date + INTERVAL &#39;1&#39; DAY
ORDER BY order_date;</code></pre>
</div>
<div class="important">
<strong>! Important:</strong> The idiomatic Materialize SQL applies only
to those “lag over” queries whose ordering can be represented by some
<strong>equality condition</strong>.
</div></td>
</tr>
<tr>
<td>Anti-pattern ❌</td>
<td><p>Avoid the use of <a
href="/docs/self-managed/v25.2/sql/functions/#lag"><code>LAG() OVER (ORDER BY ...)</code>
window function</a> to access previous row’s value if the order by field
increases in a regular pattern.</p>
<br />
&#10;<div style="background-color: var(--code-block)">
<pre tabindex="0"><code>-- Anti-pattern. Includes the first row&#39;s value. --
SELECT order_date, daily_total,
    LAG(daily_total) OVER (ORDER BY order_date) as previous_daily_total
FROM orders_daily_totals;</code></pre>
</div></td>
</tr>
</tbody>
</table>

## See also

- [Lead
  over](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/lead)
- [`INTERVAL`](/docs/self-managed/v25.2/sql/types/interval/)
- [`LEFT JOIN/LEFT OUTER JOIN`](/docs/self-managed/v25.2/sql/select/join/#left-outer-join)
- [`LAG()`](/docs/self-managed/v25.2/sql/functions/#lag)
- [Window
  functions](/docs/self-managed/v25.2/sql/functions/#window-functions)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/transform-data/idiomatic-materialize-sql/lag.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
