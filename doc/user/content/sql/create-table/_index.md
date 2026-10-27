---
title: "CREATE TABLE"
description: "`CREATE TABLE` creates a table that is persisted in durable storage."
pagerank: 40
disable_list: true
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'commands'
    identifier: 'create-table'
---

`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create:

- [Read-write tables](/sql/create-table/user-populated/). With read-write
  tables, users can read ([`SELECT`]) and write to the tables ([`INSERT`],
  [`UPDATE`], [`DELETE`]).

- Read-only tables from sources that use the new syntax:
  [PostgreSQL](/sql/create-table/postgres/),
  [MySQL](/sql/create-table/mysql/),
  [SQL Server](/sql/create-table/sql-server/), and
  [Kafka/Redpanda](/sql/create-table/kafka/). Users cannot write ([`INSERT`],
  [`UPDATE`], [`DELETE`]) to these tables. These tables are populated by [data
  ingestion from a source](/ingest-data/).
  {{% include-example file="examples/create_table_postgres"
  example="syntax-version-requirement" %}}

[//]: # "TODO(morsapaes) Bring back When to use a table? once there's more
clarity around best practices."

## Syntax summary

{{< tabs >}}

{{< tab "Read-write table" >}}

{{% include-example file="examples/create_table_user_populated"
example="syntax" %}}

For details, see [CREATE TABLE: Read-write
table](/sql/create-table/user-populated/).
{{< /tab >}}

{{< tab "PostgreSQL source table" >}}

{{% include-example file="examples/create_table_postgres"
example="syntax" %}}

For details, see [CREATE TABLE: PostgreSQL source
table](/sql/create-table/postgres/).
{{< /tab >}}

{{< tab "MySQL source table" >}}

{{% include-example file="examples/create_table_mysql"
example="syntax" %}}

For details, see [CREATE TABLE: MySQL source table](/sql/create-table/mysql/).
{{< /tab >}}

{{< tab "SQL Server source table" >}}

{{% include-example file="examples/create_table_sql_server"
example="syntax" %}}

For details, see [CREATE TABLE: SQL Server source
table](/sql/create-table/sql-server/).
{{< /tab >}}

{{< tab "Kafka source table" >}}
{{< tabs >}}

{{< tab "Format Avro" >}}

{{% include-example file="examples/create_table_kafka"
example="syntax-avro" %}}

{{< /tab >}}

{{< tab "Format JSON" >}}

{{% include-example file="examples/create_table_kafka"
example="syntax-json" %}}

{{< /tab >}}

{{< tab "Format TEXT/BYTES" >}}

{{% include-example file="examples/create_table_kafka"
example="syntax-text-bytes" %}}

{{< /tab >}}

{{< tab "Format CSV" >}}

{{% include-example file="examples/create_table_kafka"
example="syntax-csv" %}}

{{< /tab >}}

{{< tab "Format Protobuf" >}}

{{% include-example file="examples/create_table_kafka"
example="syntax-protobuf" %}}

{{< /tab >}}

{{< tab "KEY FORMAT VALUE FORMAT" >}}

{{% include-example file="examples/create_table_kafka"
example="syntax-key-value-format" %}}

{{< /tab >}}

{{< /tabs >}}

For details, see [CREATE TABLE: Kafka source table](/sql/create-table/kafka/).
{{< /tab >}}

{{< /tabs >}}

## Related pages

- [`INSERT`]
- [`DROP TABLE`](/sql/drop-table)

[`INSERT`]: /sql/insert/
[`SELECT`]: /sql/select/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/
