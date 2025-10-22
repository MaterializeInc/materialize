---
title: "CREATE TABLE"
description: "CREATE TABLE reference page"
disable_list: true
menu:
  main:
    parent: 'commands'
    identifier: 'create-table'
---

{{< source-versioning-disambiguation is_new=true other_ref="[old reference page](/sql/create-table-v1/)" >}}

`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create:
- Read-write tables. With read-write tables, users can read ([`SELECT`]) and
  write to the tables ([`INSERT`], [`UPDATE`], [`DELETE`]).

- [Source-populated](/concepts/sources/) tables. Source-populated tables do not
  support ([`INSERT`], [`UPDATE`], [`DELETE`]). These tables are populated by
  [data ingestion from a source](/ingest-data/).

- Webhook-populated tables. Webhook-populated tables do not support ([`INSERT`],
  [`UPDATE`], [`DELETE`]). These tables are populated through data posted to the
  associated **public** webhook URL, which is automatically created with the
  table creation.

Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, and views; and you
can create views/materialized views/indexes on tables.

## Syntax summary

{{< tabs >}}
{{< tab "Read-write table" >}}
{{% include-example file="examples/create_table/example_user_populated_table"
 example="syntax" %}}

For details, see [CREATE TABLE: Read-write
table](/sql/create-table/read-write/).
{{< /tab >}}
{{< tab "MySQL">}}
{{% include-example file="examples/create_table/example_mysql_table"
 example="syntax" %}}
For details, see [CREATE TABLE: MySQL](/sql/create-table/mysql/).
{{< /tab >}}
{{< tab "PostgreSQL">}}
{{% include-example file="examples/create_table/example_postgres_table"
 example="syntax" %}}
For details, see [CREATE TABLE: PostgreSQL](/sql/create-table/postgres/).
{{< /tab >}}
{{< tab "Sql Server">}}
{{% include-example file="examples/create_table/example_sqlserver_table"
 example="syntax" %}}
For details, see [CREATE TABLE: SQL Server](/sql/create-table/sql-server/).
{{< /tab >}}
{{< tab "Kafka" >}}
{{< tabs >}}

{{< tab "Format Avro" >}}
{{% include-example file="examples/create_table/example_kafka_table_avro"
 example="syntax" %}}
{{< /tab >}}

{{< tab "Format JSON" >}}
{{% include-example file="examples/create_table/example_kafka_table_json"
 example="syntax" %}}
{{< /tab >}}

{{< tab "Format TEXT/BYTES" >}}
{{% include-example file="examples/create_table/example_kafka_table_text"
 example="syntax" %}}
{{< /tab >}}

{{< tab "Format CSV" >}}
{{% include-example file="examples/create_table/example_kafka_table_csv"
 example="syntax" %}}
{{< /tab >}}

{{< tab "KEY FORMAT VALUE FORMAT" >}}
{{% include-example file="examples/create_table/example_kafka_table_key_value"
 example="syntax" %}}
{{< /tab >}}

{{< /tabs >}}

For details, see [CREATE TABLE: Kafka](/sql/create-table/kafka/).

{{< /tab >}}
{{< tab "Webhook">}}
{{% include-example file="examples/create_table/example_webhook_table"
 example="syntax" %}}
For details, see [CREATE TABLE: Webhook](/sql/create-table/webhook/).
{{< /tab >}}
{{< /tabs >}}

[`INSERT`]: /sql/insert/
[`SELECT`]: /sql/select/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/
