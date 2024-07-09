---
title: "CREATE SINK"
description: "`CREATE SINK` connects Materialize to an external data sink."
disable_list: true
pagerank: 30
menu:
  main:
    parent: reference
    name: Sinks
    identifier: 'create-sink'
    weight: 30
---

A [sink](/concepts/sinks/) describes an external system you
want Materialize to write data to, and provides details about how to encode
that data.

## Connectors

Materialize bundles **native connectors** that allow writing data to the
following external systems:

{{< multilinkbox >}}
{{< linkbox title="Message Brokers" >}}
- [Kafka](/sql/create-sink/kafka)
- [Redpanda](/sql/create-sink/kafka)
{{</ linkbox >}}
{{</ multilinkbox >}}

For details on the syntax, supported formats and features of each connector,
check out the dedicated `CREATE SINK` documentation pages.

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

[//]: # "TODO(morsapaes) Add best practices for sizing sinks."

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `SELECT` privileges on the item being written out to an external system.
  - NOTE: if the item is a view, then the view owner must also have the necessary privileges to
    execute the view definition.
- `CREATE` privileges on the containing cluster if the sink is created in an existing cluster.
- `CREATECLUSTER` privileges on the system if the sink is not created in an existing cluster.
- `USAGE` privileges on all connections and secrets used in the sink definition.
- `USAGE` privileges on the schemas that all connections and secrets in the statement are contained in.

## Related pages

- [Sinks](/concepts/sinks/)
- [`SHOW SINKS`](/sql/show-sinks/)
- [`SHOW COLUMNS`](/sql/show-columns/)
- [`SHOW CREATE SINK`](/sql/show-create-sink/)
