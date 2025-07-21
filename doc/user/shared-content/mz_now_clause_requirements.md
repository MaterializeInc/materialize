```mzsql
mz_now() <comparison_operator> <numeric_expr | timestamp_expr>
```

- {{< include-md file="shared-content/mz_now_operators.md" >}}

- `mz_now()` can only be compared to either a
  [`numeric`](/sql/types/numeric) expression or a
  [`timestamp`](/sql/types/timestamp) expression not containing `mz_now()`.
