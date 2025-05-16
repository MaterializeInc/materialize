### Schema changes

{{< include-md file="shared-content/schema-changes-in-progress.md" >}}

{{% schema-changes %}}

### Supported types

{{< include-md file="shared-content/mysql-supported-types.md" >}}

Replicating tables that contain **unsupported data types** is possible via the
[`TEXT COLUMNS`
option](/sql/create-table/#syntax) for the
following types:

<ul style="column-count: 1">
<li><code>enum</code></li>
<li><code>year</code></li>
</ul>

The specified columns will be treated as `text`, and will thus not offer the
expected MySQL type features. For any unsupported data types not listed above,
use the [`EXCLUDE
COLUMNS`](/sql/create-table/#syntax) option.

### Truncation

Upstream tables replicated into Materialize should not be truncated. If an
upstream table is truncated while replicated, the whole source becomes
inaccessible and will not produce any data until it is recreated. Instead of
truncating, you can use an unqualified `DELETE` to remove all rows from the table:

```mzsql
DELETE FROM t;
```
