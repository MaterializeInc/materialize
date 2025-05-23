---
title: "CREATE TABLE"
description: "Reference page for `CREATE TABLE`. `CREATE TABLE` creates a table that is persisted in durable storage."
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

- Webhook-populated tables. Webhook-populated tables cannot be written to by the
  user; they are populated through data posted to associated **public** webhook
  endpoint, automatically created with the table creation.

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
  PARTITION BY (<column_name> [, ...]) |
  RETAIN HISTORY [=] FOR <duration>
)]
;
```

{{% yaml-table data="syntax_options/create_table/create_table_options_user_populated" %}}

{{</ tab >}}

{{< tab "Source-populated tables (DB connector)" >}}

To create a table from a [source](/sql/create-source/) connected (via native
connector) to an external database system:

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
  | PARTITION BY (<column_name> [, ...])
  [, ...]
)]
;
```

{{% yaml-table data="syntax_options/create_table/create_table_options_source_populated_db" %}}

<a name="supported-db-source-types" ></a>

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

{{</ tab >}}

{{< tab "Source-populated tables (via Kafka/Redpanda)" >}}

To create a table from a source, where the source is connected to
Kafka/Redpanda:

{{< note >}}

- Users cannot write to source-populated tables; i.e., users cannot perform
[`INSERT`](/sql/insert/)/[`UPDATE`](/sql/update/)/[`DELETE`](/sql/delete/)
operations on source-populated tables.

- {{< include-md file="shared-content/kafka-redpanda-shorthand.md" >}}

{{</ note >}}


{{< tabs >}}

{{< tab "FORMAT AVRO" >}}

Use the following syntax to create a table from a [Kafka
source](/sql/create-source/), ingesting messages encoded in Avro using the
schema from the Confluent Schema Registry.

By default, the table contains the columns specified in the value schema. You
can include additional columns using the `INCLUDE` options.

```mzsql
CREATE TABLE <table_name> FROM SOURCE <source_name> [(REFERENCE <ref_object>)]
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <conn_name>
    [KEY STRATEGY <strategy>]
    [VALUE STRATEGY <strategy>]
[INCLUDE
    KEY [AS <name>] | PARTITION [AS <name>] | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>] | HEADERS [AS <name>] | HEADER <key_name> AS <name> [BYTES]
  [, ...]
]
[ENVELOPE
    NONE  --  Default.  Uses the append-only envelope.
  | [DEBEZIUM] UPSERT [(VALUE DECODING ERRORS = INLINE [AS name])]
  | DEBEZIUM
]
[WITH (PARTITION BY (<column_name> [, ...]))]
;
```


{{% yaml-table data="syntax_options/create_table/create_table_options_source_populated_kafka_avro"
%}}
{{</ tab >}}

{{< tab "FORMAT PROTOBUF MESSAGE">}}

Creates a table from a [Kafka source](/sql/create-source/), ingesting messages
encoded in PROTOBUF format, specifying the hex encoded schema.

By default, the table contains the columns specified in the schema. You can
include additional columns using the `INCLUDE` options.

{{< tip >}}

- Ensure your messages are raw Protobuf-encoded messages; i.e., ensure they
  don't include prefixes, such as Confluent Schema Registry framing prefix.

- If you want to specify an `UPSERT ENVELOPE`, use `KEY FORMAT ... VALUE
  FORMAT ...` instead.

{{</ tip >}}


```mzsql
CREATE TABLE <table_name> FROM SOURCE <source_name> [(REFERENCE <ref_object>)]
FORMAT PROTOBUF MESSAGE <msg_name> USING SCHEMA <hex_encoded_schema>
[INCLUDE
    PARTITION [AS <name>] | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>] | HEADERS [AS <name>] | HEADER <key_name> AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]  --  Default.  Uses the append-only envelope.
[WITH (PARTITION BY (<column_name> [, ...]))]
;
```

{{% yaml-table data="syntax_options/create_table/create_table_options_source_populated_kafka_protobuf_msg"
%}}

{{</ tab >}}

{{< tab "FORMAT PROTOBUF using CSR" >}}

Creates a table from a [Kafka source](/sql/create-source/), ingesting messages
encoded in PROTOBUF format using the schema information from Confluent Schema
Registry. By default, the table contains the columns specified in the schema.
You can include additional columns using the `INCLUDE` options.

{{< tip >}}

- If your `.proto` file includes a package declaration, you must specify the
fully qualified message name using `FORMAT PROTOBUF MESSAGE` instead of `FORMAT
PROTOBUF using CONFLUENT SCHEMA REGISTRY CONNECTION`.

- If you want to specify an `UPSERT ENVELOPE`, use `KEY FORMAT ... VALUE
  FORMAT ...` instead.

{{</ tip >}}

```mzsql
CREATE TABLE <table_name> FROM SOURCE <source_name> [(REFERENCE <ref_object>)]
FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION <conn_name>
[INCLUDE
    PARTITION [AS <name>] | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>] | HEADERS [AS <name>] | HEADER <key_name> AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]  --  Default.  Uses the append-only envelope.
[WITH (PARTITION BY (<column_name> [, ...]))]
;
```

{{% yaml-table data="syntax_options/create_table/create_table_options_source_populated_kafka_protobuf_csr"
%}}

{{</ tab >}}

{{< tab "FORMAT JSON" >}}

Creates a table from a [Kafka source](/sql/create-source/), where the
messages are JSON records.

By default, creates a table with 1 column named `data` of type `jsonb`. You can
include additional columns using the `INCLUDE` options.

```mzsql
CREATE TABLE <table_name> FROM SOURCE <source_name> [(REFERENCE <ref_object>)]
FORMAT JSON
[INCLUDE
   PARTITION [AS <name>] | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>] | HEADERS [AS <name>] | HEADER <key_name> AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]  --  Default.  Uses the append-only envelope.
;
```

{{% yaml-table data="syntax_options/create_table/create_table_options_source_populated_kafka_json"
%}}

{{</ tab >}}

{{< tab "FORMAT CSV" >}}

Creates a table from a [Kafka source](/sql/create-source/), where the
messages are CSV records with the specified number of columns. By default,

- The columns are named `column1`, `column2`...`columnN`. You can specify
  alternative column names by listing the new column names after the table name
  `(<column_name1>, <column_name2>, ...)`.

- The data is decoded as [`text`](/sql/types/text).

You can include additional columns using the `INCLUDE` options.

```mzsql
CREATE TABLE <table_name> [(<col_name> [, ...])]
FROM SOURCE <source_name> [(REFERENCE <ref_object>)]
FORMAT CSV WITH <num> COLUMNS [DELIMITED BY <char>]
[INCLUDE PARTITION [AS <name>] | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>] | HEADERS [AS <name>] | HEADER <key_name> AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]              --  Default.  Uses the append-only envelope.
[WITH (PARTITION BY (<column_name> [, ...]))]
;
```

{{% yaml-table data="syntax_options/create_table/create_table_options_source_populated_kafka_csv"
%}}
{{</ tab >}}

{{< tab "FORMAT TEXT/BYTES" >}}

Creates a table from a [Kafka source](/sql/create-source/), where the messages
are decoded either as text (`FORMAT TEXT`) or bytes (`FORMAT BYTES`).

By default, creates a table with 1 column:

- If `FORMAT TEXT`, the column name is `text` of type `text`.

- If `FORMAT BYTES`, the column name is `data` of type `bytea`.

You can include additional columns using the `INCLUDE` options.

```mzsql
CREATE TABLE <table_name> FROM SOURCE <source_name> [(REFERENCE <ref_object>)]
FORMAT  <TEXT | BYTES>
[INCLUDE
   PARTITION [AS <name>] | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>] | HEADERS [AS <name>] | HEADER <key_name> AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]              --  Default.  Uses the append-only envelope.
[WITH (PARTITION BY (<column_name> [, ...]))]
;
```

{{% yaml-table data="syntax_options/create_table/create_table_options_source_populated_kafka_text"
%}}

{{</ tab >}}

{{< tab "KEY FORMAT VALUE  FORMAT" >}}

```mzsql
CREATE TABLE <table_name> FROM SOURCE <source_name> [(REFERENCE <ref_object>)]
KEY FORMAT <format1> VALUE FORMAT <format2>
-- <format1> and <format2> can be:
   -- AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <conn_name>
   --     [KEY STRATEGY
   --       INLINE <schema> | ID <schema_registry_id> | LATEST ]
   --     [VALUE STRATEGY
   --       INLINE <schema> | ID <schema_registry_id> | LATEST ]
  -- | PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION <conn_name>
  -- | PROTOBUF MESSAGE <msg_name> USING SCHEMA <encoded_schema>
  -- | CSV WITH <num> COLUMNS DELIMITED BY <char>
  -- | JSON | TEXT | BYTES
[INCLUDE
    KEY [AS <name>] | PARTITION [AS <name>] | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>] | HEADERS [AS <name>] | HEADER <key_name> AS <name> [BYTES]
  [, ...]
]
[ENVELOPE
    NONE  --  Default.  Uses the append-only envelope.
  | DEBEZIUM
  | UPSERT [(VALUE DECODING ERRORS = INLINE [AS name])]
]
;
```

{{% yaml-table data="syntax_options/create_table/create_table_options_source_populated_kafka"
%}}

{{</ tab >}}
{{</ tabs >}}

{{</ tab >}}

{{< tab "Webhook-populated table" >}}

To create a table (and the associated **public** webhook URL) that is
populated with data POSTed to the associated webhook URL.

{{< warning >}}
This is a public URL that is open to the internet and has no security.
{{</ warning >}}

The created table has, by default, 1 column `body`.  You can specify `INCLUDE
<header_option>` to include header columns.


```mzsql
CREATE TABLE <table_name>
FROM WEBHOOK
  BODY FORMAT <TEXT | JSON [ARRAY] | BYTES>
  [ INCLUDE <header_option> ]
  -- <header_option> can be:
  -- INCLUDE HEADER <header_name> AS <col_name> [BYTES] [, ... ]
  -- | INCLUDE HEADERS [ ([NOT] <header_name> [, [NOT] <header_name> [, ...] ]) ]
  [ CHECK (
      [ WITH (<BODY|HEADERS|SECRET <secret_name>> [AS <alias>] [BYTES] [, ... ]) ]
      <check_expression>
  ) ]
```

{{% yaml-table data="syntax_options/create_table/create_table_options_webhook_populated"
%}}

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

### Required privileges

The privileges required to execute the command are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the table definition.
- `USAGE` privileges on the schemas that all types in the statement are
  contained in.

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


## Related pages

- [`INSERT`](../insert)
- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP TABLE`](../drop-table)

[`INSERT`]: /sql/insert/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/
