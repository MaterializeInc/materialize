---
title: "CREATE TABLE: Kafka source table"
description: "Create a read-only table from a Kafka/Redpanda source (new syntax)."
menu:
  main:
    parent: 'create-table'
    name: "Kafka source table"
    identifier: 'create-table-kafka'
    weight: 50
---

In Materialize, you can create read-only tables from [Kafka/Redpanda sources
created using the new syntax](/sql/create-source/kafka-v2/).

## Syntax

{{< note >}}
{{% include-headless "/headless/create-table-from-source-readonly" %}}
{{< /note >}}

{{< tabs level=3 >}}

{{< tab "Format Avro" >}}

{{% include-syntax file="examples/create_table_kafka"
example="syntax-avro" %}}

See also [Avro details](#avro).
{{< /tab >}}

{{< tab "Format JSON" >}}

{{% include-syntax file="examples/create_table_kafka"
example="syntax-json" %}}

See also [JSON details](#json).
{{< /tab >}}

{{< tab "Format TEXT/BYTES" >}}

{{% include-syntax file="examples/create_table_kafka"
example="syntax-text-bytes" %}}

{{< /tab >}}

{{< tab "Format CSV" >}}

{{% include-syntax file="examples/create_table_kafka"
example="syntax-csv" %}}

{{< /tab >}}

{{< tab "Format Protobuf" >}}

{{% include-syntax file="examples/create_table_kafka"
example="syntax-protobuf" %}}

See also [Protobuf details](#protobuf).

{{< /tab >}}

{{< tab "KEY FORMAT VALUE FORMAT" >}}

{{% include-syntax file="examples/create_table_kafka"
example="syntax-key-value-format" %}}

{{< /tab >}}

{{< /tabs >}}

## Envelopes

{{% include-headless "/headless/kafka-envelopes" %}}

## Details

### Avro

{{% include-headless "/headless/kafka-format-avro-details" %}}

### JSON

{{% include-headless "/headless/kafka-format-json-details" %}}

### Protobuf

{{% include-headless "/headless/kafka-format-protobuf-details" %}}

### Exposing source metadata

{{% include-headless "/headless/kafka-include-metadata" %}}

### Excluding or recasting a field

A Kafka table's columns are determined entirely by its format (for Avro, the
reader schema). To exclude or recast a field, project or cast it in a view on
top of the table.

### DDL transaction block

For performance, when issuing multiple `CREATE TABLE FROM SOURCE...` statements,
use within a [transaction block](/sql/begin/#ddl-only-transactions).

### Source-populated tables and snapshotting

{{% include-headless "/headless/create-table-from-source-snapshotting" %}}

### Handling table schema changes

The use of `CREATE SOURCE` (new syntax) with `CREATE TABLE FROM SOURCE` allows
for the handling of the upstream schema changes, specifically adding or dropping
columns, without downtime. For details, see [Kafka: Handling upstream schema
changes with zero downtime](/ingest-data/kafka/source-versioning/).

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-table" %}}

## Examples

### Create a table

{{% include-example file="examples/create_table_kafka"
 example="create-table" %}}

{{% include-example file="examples/create_table_kafka"
 example="show-tables" %}}

{{% include-example file="examples/create_table_kafka"
 example="show-columns" %}}

{{% include-example file="examples/create_table_kafka"
 example="read-from-table" %}}

## Related pages

- [`CREATE SOURCE: Kafka/Redpanda (New Syntax)`](/sql/create-source/kafka-v2/)
- [`DROP TABLE`](/sql/drop-table)
