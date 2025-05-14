### Schema changes

{{< include-md file="shared-content/schema-changes-in-progress.md" >}}

{{% schema-changes %}}

### Supported types

Materialize natively supports the following SQL Server types:

<ul style="column-count: 3">
<li><code>tinyint</code></li>
<li><code>smallint</code></li>
<li><code>int</code></li>
<li><code>bigint</code></li>
<li><code>real</code></li>
<li><code>double</code></li>
<li><code>bit</code></li>
<li><code>decimal</code></li>
<li><code>numeric</code></li>
<li><code>money</code></li>
<li><code>smallmoney</code></li>
<li><code>char</code></li>
<li><code>nchar</code></li>
<li><code>varchar</code></li>
<li><code>nvarchar</code></li>
<li><code>sysname</code></li>
<li><code>binary</code></li>
<li><code>varbinary</code></li>
<li><code>json</code></li>
<li><code>date</code></li>
<li><code>time</code></li>
<li><code>smalldatetime</code></li>
<li><code>datetime</code></li>
<li><code>datetime2</code></li>
<li><code>datetimeoffset</code></li>
<li><code>uniqueidentifier</code></li>
</ul>

Replicating tables that contain **unsupported [data types](/sql/types/)** is possible via the [`EXCLUDE COLUMNS` option](/sql/create-source/sql-server/#handling-unsupported-types) for the
following types:

<ul style="column-count: 3">
<li><code>text</code></li>
<li><code>ntext</code></li>
<li><code>image</code></li>
<li><code>varchar(max)</code></li>
<li><code>nvarchar(max)</code></li>
<li><code>varbinary(max)</code></li>
</ul>

Columns with the specified types need to be excluded because [SQL Server does not provide
the "before"](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-2017#large-object-data-types)
value when said column is updated.

### Timestamp Rounding

The `time`, `datetime2`, and `datetimeoffset` types in SQL Server have a default
scale of 7 decimal places, or in other words a accuracy of 100 nanoseconds. But
the corresponding types in Materialize only support a scale of 6 decimal places.
If a column in SQL Server has a higher scale than what Materialize can support, it
will be rounded up to the largest scale possible.

```
-- In SQL Server
CREATE TABLE my_timestamps (a datetime2(7));
INSERT INTO my_timestamps VALUES
  ('2000-12-31 23:59:59.99999'),
  ('2000-12-31 23:59:59.999999'),
  ('2000-12-31 23:59:59.9999999');

-- Replicated into Materialize
SELECT * FROM my_timestamps;
'2000-12-31 23:59:59.999990'
'2000-12-31 23:59:59.999999'
'2001-01-01 00:00:00'
```
