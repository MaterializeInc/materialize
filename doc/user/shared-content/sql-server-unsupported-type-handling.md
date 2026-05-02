Replicating tables that contain **unsupported [data types](/sql/types/)** is possible via the [`EXCLUDE COLUMNS` option](/sql/create-source/sql-server/#handling-unsupported-types) for the
following types:

<ul style="column-count: 3">
<li><code>text</code></li>
<li><code>ntext</code></li>
<li><code>image</code></li>
<li><code>varbinary(max)</code></li>
</ul>

Columns with the specified types need to be excluded because [SQL Server does not provide
the "before"](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-2017#large-object-data-types)
value when said column is updated.

To replicate tables that contain the following unsupported data types:
- `text`
- `ntext`
- `image`
- `varbinary(max)`

You can use either the `TEXT COLUMNS` or the `EXCLUDE COLUMNS` option.

- For `text` and `ntext` columns:
  - You can use `TEXT COLUMNS` to expose them as varchar and nvarchar, respectively.
  - You can use `EXCLUDE COLUMNS` to omit them from replication.
- For `image` and `varbinary(max)` columns:
  - You can use `EXCLUDE COLUMNS`.
