---
headless: true
---

In append mode, the Iceberg table is a changelog rather than a snapshot of
current state. Every change is written as a data row: `_mz_diff` is `+1` for an
insertion and `-1` for a deletion, and an update appears as both rows, sharing
one `_mz_timestamp`.

A query engine reading the table can reconstruct current state in one of two
ways:

| Approach | How it works | When to use it |
| --- | --- | --- |
| Consolidate by diff | Group by every column, and keep the groups whose `_mz_diff` values sum to a positive number. | The sinked relation has no unique key, or you want a query that does not depend on one. |
| Latest version per key | Rank the rows within each key by `_mz_timestamp` descending, breaking ties on `_mz_diff` descending, then keep the top-ranked row where `_mz_diff` is `+1`. | The sinked relation has a unique key. Avoids grouping by every column, so it does not grow harder to write as the relation gets wider. |

Both approaches return the same result. Two details matter for correctness:

- An update writes `-1` and `+1` at the *same* `_mz_timestamp`, so ranking by
  timestamp alone is ambiguous. Breaking ties on `_mz_diff` descending is what
  selects the new version of the row rather than the old one.

- The latest row for a deleted key carries `_mz_diff = -1`, so filtering for
  `+1` after ranking is what removes deleted keys from the result.

Identifier quoting rules differ between query engines. Materialize creates
Iceberg identifiers in lowercase, so an engine that resolves unquoted
identifiers as uppercase requires them to be quoted.
