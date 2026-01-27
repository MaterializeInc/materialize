# FAQ: Indexes
Frequently asked questions about indexes.
## Do indexes in Materialize support `ORDER BY`?

No. Indexes in Materialize do not support `ORDER BY` clauses.

Indexes in Materialize do not order their keys using the data type's natural
ordering and instead orders by its internal representation of the key (the tuple
of key length and value).

As such, indexes in Materialize currently do not provide optimizations for:

- Range queries; that is queries using `>`, `>=`,
  `<`, `<=`, `BETWEEN` clauses (e.g., `WHERE
  quantity > 10`,  `price >= 10 AND price <= 50`, and `WHERE quantity
  BETWEEN 10 AND 20`).

- `GROUP BY`, `ORDER BY` and `LIMIT` clauses.

## Do indexes in Materialize support range queries?

No. Indexes in Materialize do not support range queries.

Indexes in Materialize do not order their keys using the data type's natural
ordering and instead orders by its internal representation of the key (the tuple
of key length and value).

As such, indexes in Materialize currently do not provide optimizations for:

- Range queries; that is queries using `>`, `>=`,
  `<`, `<=`, `BETWEEN` clauses (e.g., `WHERE
  quantity > 10`,  `price >= 10 AND price <= 50`, and `WHERE quantity
  BETWEEN 10 AND 20`).

- `GROUP BY`, `ORDER BY` and `LIMIT` clauses.
