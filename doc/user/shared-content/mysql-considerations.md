### Schema changes

{{< include-md file="shared-content/schema-changes-in-progress.md" >}}

{{% schema-changes %}}

### Supported types

Materialize natively supports the following MySQL types:

<ul style="column-count: 3">
<li><code>bigint</code></li>
<li><code>binary</code></li>
<li><code>bit</code></li>
<li><code>blob</code></li>
<li><code>boolean</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>datetime</code></li>
<li><code>decimal</code></li>
<li><code>double</code></li>
<li><code>float</code></li>
<li><code>int</code></li>
<li><code>json</code></li>
<li><code>longblob</code></li>
<li><code>longtext</code></li>
<li><code>mediumblob</code></li>
<li><code>mediumint</code></li>
<li><code>mediumtext</code></li>
<li><code>numeric</code></li>
<li><code>real</code></li>
<li><code>smallint</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>tinyblob</code></li>
<li><code>tinyint</code></li>
<li><code>tinytext</code></li>
<li><code>varbinary</code></li>
<li><code>varchar</code></li>
</ul>

Replicating tables that contain **unsupported [data types](/sql/types/)** is
possible via the [`TEXT COLUMNS`
option](/sql/create-source/mysql/#handling-unsupported-types) for the following
types:

<ul style="column-count: 1">
<li><code>enum</code></li>
<li><code>year</code></li>
</ul>

The specified columns will be treated as `text`, and will thus not offer the
expected MySQL type features. For any unsupported data types not listed above,
use the [`EXCLUDE COLUMNS`](/sql/create-source/mysql/#excluding-columns) option.

### Truncation

Upstream tables replicated into Materialize should not be truncated. If an
upstream table is truncated while replicated, the whole source becomes
inaccessible and will not produce any data until it is recreated. Instead of
truncating, you can use an unqualified `DELETE` to remove all rows from the table:

```mzsql
DELETE FROM t;
```
