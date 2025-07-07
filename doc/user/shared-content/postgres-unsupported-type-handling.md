Replicating tables that contain **unsupported data types** is possible via the
[`TEXT COLUMNS`](/sql/create-table/#syntax). When decoded as `text`, the
specified columns will not have the expected PostgreSQL type features. For
example:

* [`enum`]: When decoded as `text`, the resulting `text` values will
  not observe the implicit ordering of the original PostgreSQL `enum`; instead,
  Materialize will sort the values as `text`.

* [`money`]: When decoded as `text`, the resulting `text` value
  cannot be cast back to `numeric` since PostgreSQL adds typical currency
  formatting to the output.

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html
