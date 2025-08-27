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
