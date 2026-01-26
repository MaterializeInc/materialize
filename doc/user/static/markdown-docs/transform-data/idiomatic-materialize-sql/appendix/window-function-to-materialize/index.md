# Window function to idiomatic Materialize

Cheatsheet for window functions to idiomatic Materialize SQL.



Materialize offers a wide range of [window
functions](/sql/functions/#window-functions). However, for some
[`LAG()`](/sql/functions/#lag), [`LEAD()`](/sql/functions/#lead),
[`ROW_NUMBER()`](/sql/functions/#row_number),
[`FIRST_VALUE()`](/sql/functions/#first_value), and
[`LAST_VALUE()`](/sql/functions/#last_value) use cases, Materialize provides its
own idiomatic query patterns that do <red>not</red> use the window functions and
can provide better performance.

> ### Materialize and window functions
>
> For [window functions](/sql/functions/#window-functions), when an input record
> in a partition (as determined by the `PARTITION BY` clause of your window
> function) is added/removed/changed, Materialize recomputes the results for the
> entire window partition. This means that when a new batch of input data arrives
> (that is, every second), **the amount of computation performed is proportional
> to the total size of the touched partitions**.
>
> For example, assume that in a given second, 20 input records change, and these
> records belong to **10** different partitions, where the average size of each
> partition is **100**. Then, amount of work to perform is proportional to
> computing the window function results for **10\*100=1000** rows.
>
> To avoid performance issues that may arise as the number of records grows,
> consider rewriting your query to use idiomatic Materialize SQL instead of window
> functions. If your query cannot be rewritten without the window functions and
> the performance of window functions is insufficient for your use case, please
> [contact our team](/support/).
>
>
>




<table>
<thead>
<tr>
<th>
Windows function anti-pattern
</th>
<th>
Materialize idiomatic SQL
</th>
</tr>
</thead>
<tbody>

<tr>
<td colspan=2>

**First value within groups.** For more information and examples, see [Idiomatic Materialize SQL: First
value](/transform-data/idiomatic-materialize-sql/first-value/).

</td>
</tr>
<tr>
<td>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, fieldB,
 FIRST_VALUE(fieldZ)
   OVER (PARTITION BY fieldA ORDER BY ...)
FROM tableA
ORDER BY fieldA, ...;
```

</div>
</td>
<td class="copyableCode">

```mzsql
SELECT tableA.fieldA, tableA.fieldB, minmax.Z
FROM tableA,
     (SELECT fieldA,
        MIN(fieldZ)
      FROM tableA
      GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```

</td>
</tr>

<tr>
<td colspan=2>

**Lag over whose order by field advances in a regular pattern.**
For more information and examples, see [Idiomatic Materialize SQL: Lag
over](/transform-data/idiomatic-materialize-sql/lag/).

</td>
</tr>
<tr>
<td>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid --
SELECT fieldA, ...
  LAG(fieldZ)
    OVER (ORDER BY fieldA) as previous_row_value
FROM tableA;
```

</div>
</td>
<td class="copyableCode">

```mzsql
-- Excludes the first row in the results --
SELECT t1.fieldA, t2.fieldB as previous_row_value
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA + ...
ORDER BY fieldA;
```

</td>
</tr>

<tr>
<td colspan=2>

**Last value within groups.** For more information and examples, see [Idiomatic Materialize SQL: Last value in
group](/transform-data/idiomatic-materialize-sql/last-value/).


</td>
</tr>
<tr>
<td>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Unsupported range. --
SELECT fieldA, fieldB,
  LAST_VALUE(fieldZ)
    OVER (PARTITION BY fieldA ORDER BY fieldZ
          RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING)
FROM tableA
ORDER BY fieldA, ...;
```

</div>
</td>
<td class="copyableCode">

```mzsql
SELECT tableA.fieldA, tableA.fieldB, minmax.Z
 FROM tableA,
      (SELECT fieldA,
         MAX(fieldZ)
       FROM tableA
       GROUP BY fieldA) minmax
WHERE tableA.fieldA = minmax.fieldA
ORDER BY fieldA ... ;
```

</td>
</tr>

<tr>
<td colspan=2>

**Lead over whose order by field advances in a regular pattern.** For more
information and examples, see [Idiomatic Materialize SQL: Lead
over](/transform-data/idiomatic-materialize-sql/lead/).

</td>
</tr>
<tr>
<td>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, ...
    LEAD(fieldZ)
      OVER (ORDER BY fieldA) as next_row_value
FROM tableA;
```

</div>
</td>
<td class="copyableCode">

```mzsql
-- Excludes the last row in the results --
SELECT t1.fieldA, t2.fieldB as next_row_value
FROM tableA t1, tableA t2
WHERE t1.fieldA = t2.fieldA - ...
ORDER BY fieldA;
```

</td>
</tr>

<tr>
<td colspan=2>

**Top-K queries.** For more information and examples, see [Idiomatic Materialize SQL: Top-K in
group](/transform-data/idiomatic-materialize-sql/top-k/).

</td>
</tr>
<tr>
<td>
<div style="background-color: var(--code-block)">

```nofmt
-- Anti-pattern. Avoid. --
SELECT fieldA, fieldB, ...
FROM (
  SELECT fieldA, fieldB, ... , fieldZ,
     ROW_NUMBER() OVER (PARTITION BY fieldA
     ORDER BY fieldZ ... ) as rn
  FROM tableA)
WHERE rn <= K
ORDER BY fieldA, fieldZ ...;
```

</div>
</td>
<td class="copyableCode">

```mzsql
SELECT fieldA, fieldB, ...
FROM (SELECT DISTINCT fieldA FROM tableA) grp,
  LATERAL (SELECT fieldB, ... , fieldZ FROM tableA
           WHERE fieldA = grp.fieldA
           ORDER BY fieldZ ... LIMIT K)
ORDER BY fieldA, fieldZ ... ;
```

</td>
</tr>
</tbody>
</table>
