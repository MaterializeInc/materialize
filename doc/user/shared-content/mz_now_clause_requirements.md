```mzsql
mz_now() <comparison_operator> <numeric_expr | timestamp_expr>
```

- {{< include-md file="shared-content/mz_now_operators.md" >}}

- `mz_now()` can only be compared to either a
  [`numeric`](/sql/types/numeric) expression or a
  [`timestamp`](/sql/types/timestamp) expression not containing `mz_now()`.

- `mz_now()` clauses can only be combined using an `AND`, and all top-level
  `WHERE` or `HAVING` conditions must be combined using an `AND`, even if the
  `mz_now()` clause is nested.

  For example,

  {{< yaml-table data="mz_now/mz_now_combination" >}}

- If part of a  `WHERE` clause, the `WHERE` clause cannot be an [aggregate
 `FILTER` expression](/sql/functions/filters).
