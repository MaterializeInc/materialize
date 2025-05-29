{{< tabs >}}
{{< tab "Supported MySQL types">}}

{{< include-md file="shared-content/mysql-supported-types.md" >}}

Replicating tables that contain **unsupported data types** is
possible via the [`TEXT COLUMNS` option](#text-columns) for the
following types:

<ul style="column-count: 1">
<li><code>enum</code></li>
<li><code>year</code></li>
</ul>

The specified columns will be treated as `text`, and will thus not offer the
expected MySQL type features. For any unsupported data types not listed above,
use the [`EXCLUDE COLUMNS`](#exclude-columns) option.

{{</ tab >}}

{{< tab "Supported PostgreSQL types">}}

{{< include-md file="shared-content/postgres-supported-types.md" >}}

Replicating tables that contain **unsupported data types** is possible via the
[`TEXT COLUMNS` option](#text-columns). When decoded as `text`, the specified
columns will not have the expected PostgreSQL type features. For example:

* [`enum`]: When decoded as `text`, the resulting `text` values will
  not observe the implicit ordering of the original PostgreSQL `enum`; instead,
  Materialize will sort the values as `text`.

* [`money`]: When decoded as `text`, the resulting `text` value
  cannot be cast back to `numeric` since PostgreSQL adds typical currency
  formatting to the output.

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html

{{</ tab >}}

{{< tab "Supported SQL Server types">}}

{{< include-md file="shared-content/sql-server-supported-types.md" >}}

Replicating tables that contain **unsupported data types** is possible via the
[`EXCLUDE COLUMNS`
option](#exclude-columns) for the
following types:

<ul style="column-count: 3">
<li><code>text</code></li>
<li><code>ntext</code></li>
<li><code>image</code></li>
<li><code>varchar(max)</code></li>
<li><code>nvarchar(max)</code></li>
<li><code>varbinary(max)</code></li>
</ul>

**Timestamp rounding**

{{< include-md file="shared-content/sql-server-timestamp-rounding.md" >}}

{{</ tab >}}
{{</ tabs >}}

See also [Materialize SQL data types](/sql/types/).
