To replicate upstream tables with **unsupported [data types](/sql/types/)**, you
can use the:

- [`TEXT COLUMNS`](/sql/create-table/postgres/#syntax) option. When using the `TEXT COLUMNS` option, the specified
  columns will be treated as `text`; i.e., will not have the expected PostgreSQL
  type features. For example:

  * [`enum`]: When decoded as `text`, the implicit ordering of the original
    PostgreSQL `enum` type is not preserved; instead, Materialize will sort values
    as `text`.

  * [`money`]: When decoded as `text`, resulting `text` value cannot be cast back
    to  `numeric`, since PostgreSQL adds typical currency formatting to the
    output.

- [`EXCLUDE COLUMNS`](/sql/create-table/postgres/#syntax) option.

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html
