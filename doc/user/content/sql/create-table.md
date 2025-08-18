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

`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create:

- User-populated tables. User-populated tables can be written to (i.e.,
  [`INSERT`]/[`UPDATE`]/[`DELETE`]) by the user.

- [Source-populated](/concepts/sources/) tables. Source-populated tables are
  read-only tables; they cannot be written to by the user. These tables are
  populated by data ingestion from a source.

- Webhook-populated tables. Webhook-populated tables cannot be written to by the
  user; they are read-only. These tables are populated through data posted to
  the associated **public** webhook URL, which is automatically created with the
  table creation.

Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, and views; and you
can create views/materialized views/indexes on tables.

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

{{< tab "Source-populated tables (via DB connector)" >}}

To create a table from a [source](/sql/create-source/) connected (via
native connector) to an external PostgreSQL/MySQL/SQL Server database system:

{{< note >}}

- {{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

- {{< include-md file="shared-content/create-table-from-source-snapshotting.md"
  >}}

{{</ note >}}

```mzsql
CREATE TABLE <table_name> FROM SOURCE <source_name> (REFERENCE <upstream_table>)
[WITH (
    TEXT COLUMNS (<column_name> [, ...])    -- Available for PostgreSQL and MySQL
  | EXCLUDE COLUMNS (<column_name> [, ...]) -- Available for MySQL and SQL Server
  | PARTITION BY (<column_name> [, ...])
  [, ...]
)]
;
```

{{% yaml-table data="syntax_options/create_table/create_table_options_source_populated_db" %}}

{{</ tab >}}

{{< tab "Source-populated tables (connected via Kafka/Redpanda)" >}}

To create a table from a source, where the source is connected to
Kafka/Redpanda:

{{< note >}}

- {{< include-md file="shared-content/kafka-redpanda-shorthand.md" >}}

- {{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

- {{< include-md file="shared-content/create-table-from-source-snapshotting.md"
  >}}

{{</ note >}}


{{< tabs >}}

{{< tab "FORMAT AVRO" >}}

Use the following syntax to create a read-only table from a [Kafka
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

{{< tab "FORMAT JSON" >}}

Creates a read-only table from a [Kafka source](/sql/create-source/), where the
messages are JSON records.

By default, creates a table with 1 column named `data` of type
[`jsonb`](/sql/types/jsonb/). You can include additional columns using the
`INCLUDE` options.


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

{{< tip >}}
Once created, we recommend creating a view on your table that maps the
individual fields to columns with the required data types. You can use the [JSON
parsing widget](/sql/types/jsonb/#parsing) to generate the view definition.

See [Example: Create table from Kafka source](#create-a-table-kafka-source).

{{</ tip >}}

{{</ tab >}}

{{< tab "FORMAT TEXT/BYTES" >}}

Creates a read-only table from a [Kafka source](/sql/create-source/), where the
messages are decoded either as text (`FORMAT TEXT`) or bytes (`FORMAT BYTES`).

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

{{< tab "FORMAT CSV" >}}

Creates a read-only table from a [Kafka source](/sql/create-source/), where the
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

{{< tab "KEY FORMAT VALUE  FORMAT" >}}

```mzsql
CREATE TABLE <table_name> FROM SOURCE <source_name> [(REFERENCE <ref_object>)]
KEY FORMAT <format1> VALUE FORMAT <format2>
-- <format1> and <format2> can be:
   -- AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <conn_name>
   --     [KEY STRATEGY <strategy>]
   --     [VALUE STRATEGY <strategy>]
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
populated with data **POST**ed to the associated webhook URL.

{{< warning >}}
This is a public URL that is open to the internet and has no security.
{{</ warning >}}

{{< note >}}
{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}
{{</ note >}}

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

<a name="supported-db-source-types"></a>

### Upstream sources and supported data types

{{< include-md file="shared-content/create-table-supported-types.md" >}}

### Source-populated tables and snapshotting

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

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

The privileges required to execute the command are:gs


- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the table definition.
- `USAGE` privileges on the schemas that all types in the statement are
  contained in.

## Examples

### Create a table (user-populated)

{{% include-example file="examples/create-table/example_user_defined_table"
 example="create-table" %}}

Once a table is created, you can inspect the table with various `SHOW` commands.
For example:

- [`SHOW TABLES`](/sql/show-tables/)

  {{% include-example file="examples/create-table/example_user_defined_table"
 example="show-tables" %}}

- [`SHOW COLUMNS`](/sql/show-columns/)

  {{% include-example file="examples/create-table/example_user_defined_table"
 example="show-columns" %}}

#### Read/write to the new table

Once a user-populated table is created, you can perform CRUD
(Create/Read/Update/Write) operations on it.

{{% include-example file="examples/create-table/example_user_defined_table"
 example="write-to-table" %}}

{{% include-example file="examples/create-table/example_user_defined_table"
 example="read-from-table" %}}

### Create a table (PostgreSQL Source)

{{< note >}}

The example assumes you have configured your upstream PostgreSQL 11+ (i.e.,
enabled logical replication, created the publication for the various tables and
replication user, and updated the network configuration).

For details about configuring your upstream system, see the [PostgreSQL
integration guides](/ingest-data/postgres/#supported-versions-and-services).

{{</ note >}}

{{% include-example file="examples/create-table/example_postgres_table"
 example="create-table" %}}

Once a table is created, you can inspect the table with various `SHOW`
commands. For example:

- [`SHOW TABLES`](/sql/show-tables/)

  {{% include-example file="examples/create-table/example_postgres_table"
 example="show-tables" %}}

- [`SHOW COLUMNS`](/sql/show-tables/)

  {{% include-example file="examples/create-table/example_postgres_table"
 example="show-columns" %}}

#### Query the read-only table

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

Once the snapshotting process completes, you can query the table:

{{% include-example file="examples/create-table/example_postgres_table"
 example="read-from-table" %}}

### Create a table (MySQL Source)

{{< note >}}

The example assumes you have configured your upstream MySQL 5.7+ (i.e.,
enabled GTID-based binlog replication, created the
replication user, and updated the network configuration as needed).

For details about configuring your upstream system, see the [MySQL
integration guides](/ingest-data/mysql/#supported-versions-and-services).

{{</ note >}}

{{% include-example file="examples/create-table/example_mysql_table"
 example="create-table" %}}

Once a table is created, you can inspect the table with various `SHOW`
commands. For example:

- [`SHOW TABLES`](/sql/show-tables/)

  {{% include-example file="examples/create-table/example_mysql_table"
 example="show-tables" %}}

- [`SHOW COLUMNS`](/sql/show-tables/)

  {{% include-example file="examples/create-table/example_mysql_table"
 example="show-columns" %}}

#### Query the read-only table

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

Once the snapshotting process completes, you can query the table:

{{% include-example file="examples/create-table/example_mysql_table"
 example="read-from-table" %}}

### Create a table (Kafka Source)

{{< tip >}}
The same syntax may be used for Redpanda.
{{</ tip >}}

{{< tabs  >}}
{{< tab "FORMAT AVRO">}}

{{% include-example file="examples/create-table/example_kafka_table_avro"
 example="create-table" %}}

Once a table is created, you can inspect the table with various `SHOW`
commands. For example:

- [`SHOW TABLES`](/sql/show-tables/)

  {{% include-example file="examples/create-table/example_kafka_table_avro"
 example="show-tables" %}}

- [`SHOW COLUMNS`](/sql/show-tables/)

  {{% include-example file="examples/create-table/example_kafka_table_avro"
 example="show-columns" %}}

#### Query the read-only table

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

{{% include-example file="examples/create-table/example_kafka_table_avro"
 example="read-from-table" %}}

{{< /tab >}}
{{< tab "FORMAT JSON">}}

{{% include-example file="examples/create-table/example_kafka_table_json"
 example="create-table" %}}

Once a table is created, you can inspect the table with various `SHOW`
commands. For example:

- [`SHOW TABLES`](/sql/show-tables/)

  {{% include-example file="examples/create-table/example_kafka_table_json"
 example="show-tables" %}}

- [`SHOW COLUMNS`](/sql/show-tables/)

  {{% include-example file="examples/create-table/example_kafka_table_json"
 example="show-columns" %}}

#### Query the read-only table

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

Once the snapshotting process completes, you can query the table:

{{% include-example file="examples/create-table/example_kafka_table_json"
 example="read-from-table" %}}

#### Create a view from table

{{% include-example file="examples/create-table/example_kafka_table_json"
 example="create-a-view-from-table" %}}

{{< /tab >}}

{{< tab "FORMAT TEXT">}}

```mzsql
CREATE SOURCE text_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT TEXT
  ENVELOPE UPSERT;
```

{{< /tab >}}
{{< tab "FORMAT CSV">}}

```mzsql
CREATE SOURCE csv_source (col_foo, col_bar, col_baz)
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT CSV WITH 3 COLUMNS;
```

{{< /tab >}}
{{< /tabs >}}


## Related pages

- [`INSERT`](../insert)
- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP TABLE`](../drop-table)
- [Ingest data](/ingest-data/)

[`INSERT`]: /sql/insert/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/
