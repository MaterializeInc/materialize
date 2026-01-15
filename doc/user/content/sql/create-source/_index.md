---
title: "CREATE SOURCE"
description: "`CREATE SOURCE` connects Materialize to an external data source."
disable_list: true
menu:
  main:
    parent: commands
    identifier: 'create-source'
---

A [source](/concepts/sources/) describes an external system you want Materialize to read data from, and provides details about how to decode and interpret that data.

## Syntax summary

<!--"Docs Note: Using include-example shortcode instead of include-syntax since only want the code snippet on this page."
-->

{{< tabs >}}

{{< tab "PostgreSQL (New)" >}}

{{% include-example file="examples/create_source_postgres" example="syntax" %}}

For details, see [CREATE SOURCE: PostgreSQL (New Syntax)](/sql/create-source/postgres-v2/).
{{< /tab >}}

{{< tab "PostgreSQL (Legacy)" >}}

{{% include-example file="examples/create_source_postgres_legacy" example="syntax" %}}

For details, see [CREATE SOURCE: PostgreSQL (Legacy)](/sql/create-source/postgres/).
{{< /tab >}}
{{< tab "MySQL" >}}

{{% include-example file="examples/create_source_mysql" example="syntax" %}}

For details, see [CREATE SOURCE: MySQL](/sql/create-source/mysql/).
{{< /tab >}}

{{< tab "SQL Server" >}}

{{% include-example file="examples/create_source_sql_server" example="syntax"
%}}

For details, see [CREATE SOURCE: SQL Server](/sql/create-source/sql-server/).

{{< /tab >}}

{{< tab "Kafka/Redpanda" >}}

{{< tabs >}}

{{< tab "Format Avro" >}}

{{% include-example file="examples/create_source_kafka" example="syntax-avro" %}}

{{< /tab >}}

{{< tab "Format JSON" >}}

{{% include-example file="examples/create_source_kafka" example="syntax-json" %}}

{{< /tab >}}

{{< tab "Format TEXT/BYTES" >}}

{{% include-example file="examples/create_source_kafka" example="syntax-text-bytes" %}}

{{< /tab >}}

{{< tab "Format CSV" >}}

{{% include-example file="examples/create_source_kafka" example="syntax-csv" %}}

{{< /tab >}}
{{< tab "Format Protobuf" >}}

{{% include-example file="examples/create_source_kafka"
example="syntax-protobuf" %}}
{{< /tab >}}

{{< tab "KEY FORMAT VALUE FORMAT" >}}

{{% include-example file="examples/create_source_kafka" example="syntax-key-value-format" %}}

{{< /tab >}}

{{< /tabs >}}

For details, see [CREATE SOURCE: Kafka/Redpanda](/sql/create-source/kafka/).
{{< /tab >}}

{{< tab "Webhook" >}}

{{% include-example file="examples/create_source_webhook" example="syntax" %}}

For details, see [CREATE SOURCE: Webhook](/sql/create-source/webhook/).
{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute `CREATE SOURCE` are:

{{< include-md file="shared-content/sql-command-privileges/create-source.md" >}}

## Available guides

The following guides step you through setting up sources:

{{< include-md file="shared-content/multilink-box-native-connectors.md" >}}

## Best practices

### Separate cluster(s) for sources

In production, if possible, use a dedicated cluster for
[sources](/concepts/sources/); i.e., avoid putting sources on the same cluster
that hosts compute objects, sinks, and/or serves queries.

{{% include-from-yaml data="best_practices_details" name="architecture-upsert-source" %}}


### Sizing a source

Some sources are low traffic and require relatively few resources to handle data ingestion, while others are high traffic and require hefty resource allocations. The cluster in which you place a source determines the amount of CPU, memory, and disk available to the source.

It's a good idea to size up the cluster hosting a source when:

  * You want to **increase throughput**. Larger sources will typically ingest data
    faster, as there is more CPU available to read and decode data from the
    upstream external system.

  * You are using the [upsert
    envelope](/sql/create-source/kafka/#upsert-envelope) or [Debezium
    envelope](/sql/create-source/kafka/#debezium-envelope), and your source
    contains **many unique keys**. These envelopes maintain state proportional
    to the number of unique keys in the upstream external system. Larger sizes
    can store more unique keys.

Sources share the resource allocation of their cluster with all other objects in
the cluster. Colocating multiple sources onto the same cluster can be more
resource efficient when you have many low-traffic sources that occasionally need
some burst capacity.


## Related pages

- [Sources](/concepts/sources/)
- [`SHOW SOURCES`](/sql/show-sources/)
- [`SHOW COLUMNS`](/sql/show-columns/)
- [`SHOW CREATE SOURCE`](/sql/show-create-source/)
