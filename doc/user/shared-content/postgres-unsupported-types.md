Replicating tables that contain **unsupported [data types](/sql/types/)** is
possible via the `TEXT COLUMNS` option. The specified columns will be treated
as `text`; i.e., will not have the expected PostgreSQL type features. For
example:

* [`enum`]: When decoded as `text`, the implicit ordering of the original
  PostgreSQL `enum` type is not preserved; instead, Materialize will sort values
  as `text`.

* [`money`]: When decoded as `text`, resulting `text` value cannot be cast back
  to  `numeric`, since PostgreSQL adds typical currency formatting to the
  output.

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html
