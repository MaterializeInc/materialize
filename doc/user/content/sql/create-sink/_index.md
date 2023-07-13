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

A [sink](../../get-started/key-concepts/#sinks) describes an external system you
want Materialize to write data to, and provides details about how to encode
that data.

## Connectors

Materialize bundles **native sink connectors** for the following external
systems:

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
others are high traffic and require hefty resource allocations. You choose the
amount of CPU and memory available to a sink using the `SIZE` option, and
adjust the provisioned size after sink creation using the [`ALTER SINK`](/sql/alter-sink) command.

Sinks that specify the `SIZE` option are linked to a single-purpose cluster
dedicated to maintaining that sink.

You can also choose to place a sink in an existing
[cluster](/get-started/key-concepts/#clusters) by using the `IN CLUSTER` option.
Sinks in a cluster share the resource allocation of the cluster with all other
objects in the cluster.

Colocating multiple sinks onto the same cluster can be more resource efficient
when you have many low-traffic sinks that occasionally need some burst
capacity.

[//]: # "TODO(morsapaes) Add best practices for sizing sinks."

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `SELECT` privileges on the item being written out to an external system.
  - NOTE: if the item is a view, then the view owner must also have the necessary privileges to
    execute the view definition.
- `CREATE` privileges on the containing cluster if the sink is created in an existing cluster.
- `CREATECLUSTER` privileges on the system if the sink is not created in an existing cluster.
- `USAGE` privileges on all connections and secrets used in the sink definition.

## Related pages

- [Key Concepts](../../get-started/key-concepts/)
- [`SHOW SINKS`](/sql/show-sinks/)
- [`SHOW COLUMNS`](/sql/show-columns/)
- [`SHOW CREATE SINK`](/sql/show-create-sink/)
