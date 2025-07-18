When used in a materialized view definition, a view definition that is being
indexed (i.e., although you can create the view and perform ad-hoc query on
the view, you cannot create an index on that view), or a `SUBSCRIBE`
statement:

- `mz_now()` clauses can only be combined using an `AND`, and

- All top-level `WHERE` or `HAVING` conditions must be combined using an `AND`,
  even if the `mz_now()` clause is nested.
