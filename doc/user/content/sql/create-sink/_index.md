---
title: "CREATE SINK"
description: "`CREATE SINK` connects Materialize to an external data sink."
disable_list: true
pagerank: 30
menu:
  main:
    parent: commands
    identifier: 'create-sink'
---

A [sink](/concepts/sinks/) describes an external system you
want Materialize to write data to, and provides details about how to encode
that data. You can define a sink over a materialized view, source, or table.

## Syntax summary

<!--"Docs Note: Using include-example shortcode instead of include-syntax since only want the code snippet on this page."
-->

{{< tabs >}}

{{< tab "Kafka/Redpanda" >}}

{{< tabs >}}

{{< tab "Format Avro" >}}

{{% include-example file="examples/create_sink_kafka" example="syntax-avro" %}}

{{< /tab >}}

{{< tab "Format JSON" >}}

{{% include-example file="examples/create_sink_kafka" example="syntax-json" %}}

{{< /tab >}}

{{< tab "Format TEXT/BYTES" >}}

{{% include-example file="examples/create_sink_kafka" example="syntax-text-bytes" %}}

{{< /tab >}}

{{< tab "KEY FORMAT VALUE FORMAT" >}}

By default, the message key is encoded using the same format as the message value. However, you can set the key and value encodings explicitly using the `KEY FORMAT ... VALUE FORMAT`.

{{% include-example file="examples/create_sink_kafka" example="syntax-key-value-format" %}}

{{< /tab >}}

{{< /tabs >}}


For details, see [CREATE Sink: Kafka/Redpanda](/sql/create-sink/kafka/).
{{< /tab >}}
{{< /tabs >}}

## Best practices

### Sizing a sink

Some sinks require relatively few resources to handle data ingestion, while
others are high traffic and require hefty resource allocations. The cluster in
which you place a sink determines the amount of CPU and memory available to the
sink.

Sinks share the resource allocation of their cluster with all other objects in
the cluster. Colocating multiple sinks onto the same cluster can be more
resource efficient when you have many low-traffic sinks that occasionally need
some burst capacity.

## Details

A sink cannot be created directly on a catalog object. As a workaround you can
create a materialized view on a catalog object and create a sink on the
materialized view.

### Kafka transaction markers

{{< include-md file="shared-content/kafka-transaction-markers.md" >}}

[//]: # "TODO(morsapaes) Add best practices for sizing sinks."

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-sink.md" >}}

## Related pages

- [Sinks](/concepts/sinks/)
- [`SHOW SINKS`](/sql/show-sinks/)
- [`SHOW COLUMNS`](/sql/show-columns/)
- [`SHOW CREATE SINK`](/sql/show-create-sink/)
