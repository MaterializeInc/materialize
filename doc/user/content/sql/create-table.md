---
title: "CREATE TABLE"
description: "`CREATE TABLE` creates a table that is persisted in durable storage."
pagerank: 40
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'commands'
---

`CREATE TABLE` defines a table that is persisted in durable storage. In
Materialize, you can create:

- User-populated tables. User-populated tables can be written to (i.e.,
  [`INSERT`]/[`UPDATE`]/[`DELETE`]) by the user.

- [Source-populated](/concepts/sources/) tables. Source-populated tables cannot
  be written to by the user; they are populated through data ingestion from a
  source.

Tables can be joined with other tables, materialized views, and views. Tables in
Materialize are similar to tables in standard relational databases: they consist
of rows and columns where the columns are fixed when the table is created.

## Syntax

{{< tabs >}}

{{< tab "User-populated tables" >}}

To create a table that users can write to (i.e., perform
[`INSERT`](/sql/insert/)/[`UPDATE`](/sql/update/)/[`DELETE`](/sql/delete/)
operations):

```mzsql
CREATE [TEMP|TEMPORARY] TABLE <table_name> (
  <column_name> <column_type> [NOT NULL][DEFAULT <default_expr>]
  [, ...]
)
[WITH (
  RETAIN HISTORY [=] FOR <duration>
)]
;
```

{{% yaml-table data="syntax_options/create_table_options_user_populated" %}}

{{</ tab >}}

{{< tab "Source-populated tables (DB source)" >}}

To create a table from a [source](/sql/create-source/), where the source maps to
an external database system:

{{< note >}}

Users cannot write to source-populated tables; i.e., users cannot perform
[`INSERT`](/sql/insert/)/[`UPDATE`](/sql/update/)/[`DELETE`](/sql/delete/)
operations on source-populated tables.

{{</ note >}}

```mzsql
CREATE TABLE <table_name> FROM SOURCE <source_name> (REFERENCE <ref_object>)
[WITH (
    TEXT COLUMNS (<fq_column_name> [, ...])
  | EXCLUDE COLUMNS (<fq_column_name> [, ...])
  [, ...]
)]
;
```

{{% yaml-table data="syntax_options/create_table_options_source_populated_db" %}}

<a name="supported-db-source-types" ></a>

{{< tabs >}}
{{< tab "Supported MySQL types">}}

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
Materialize natively supports the following PostgreSQL types (including the
array type for each of the types):

<ul style="column-count: 3">
<li><code>bool</code></li>
<li><code>bpchar</code></li>
<li><code>bytea</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>daterange</code></li>
<li><code>float4</code></li>
<li><code>float8</code></li>
<li><code>int2</code></li>
<li><code>int2vector</code></li>
<li><code>int4</code></li>
<li><code>int4range</code></li>
<li><code>int8</code></li>
<li><code>int8range</code></li>
<li><code>interval</code></li>
<li><code>json</code></li>
<li><code>jsonb</code></li>
<li><code>numeric</code></li>
<li><code>numrange</code></li>
<li><code>oid</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>timestamptz</code></li>
<li><code>tsrange</code></li>
<li><code>tstzrange</code></li>
<li><code>uuid</code></li>
<li><code>varchar</code></li>
</ul>

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
{{</ tabs >}}

See also [Materialize SQL data types](/sql/types/).

{{</ tab >}}


{{</ tabs >}}

## Details

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

### Known limitations

Tables do not currently support:

- Primary keys
- Unique constraints
- Check constraints

See also the known limitations for [`INSERT`](../insert#known-limitations),
[`UPDATE`](../update#known-limitations), and [`DELETE`](../delete#known-limitations).

### Temporary tables

The `TEMP`/`TEMPORARY` keyword creates a temporary table. Temporary tables are
automatically dropped at the end of the SQL session and are not visible to other
connections. They are always created in the special `mz_temp` schema.

Temporary tables may depend upon other temporary database objects, but non-temporary
tables may not depend on temporary objects.

## Examples

### Creating a table

You can create a table `t` with the following statement:

```mzsql
CREATE TABLE t (a int, b text NOT NULL);
```

Once a table is created, you can inspect the table with various `SHOW` commands.

```mzsql
SHOW TABLES;
TABLES
------
t

SHOW COLUMNS IN t;
name       nullable  type
-------------------------
a          true      int4
b          false     text
```

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the table definition.
- `USAGE` privileges on the schemas that all types in the statement are contained in.

## Related pages

- [`INSERT`](../insert)
- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP TABLE`](../drop-table)

[`INSERT`]: /sql/insert/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/
