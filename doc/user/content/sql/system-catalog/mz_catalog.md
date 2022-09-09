---
title: "mz_catalog"
description: "mz_catalog is a system catalog that exposes metadata in Materialize's native format."
menu:
  main:
    parent: 'system-catalog'
    name: mz_catalog
    weight: 1
---

## Overview
The following sections describe the available relations in the `mz_catalog` schema.
The `mz_catalog` schemas can be classified into two categories:

  * [Object properties](#object-properties), which contain metadata about objects
    within the Materialize instance. This incudes descriptions of each database,
    schema, source, table, view, sink, and index in the system.

  * [Dataflow properties](#dataflow-properties), which contains metadata about the
    Materialize instance's dataflows. This includes both descriptions and metrics.
    These relations can be useful for understanding and debugging Materialize’s
    performance.

## Object Properties
### `mz_array_types`

The `mz_array_types` table contains a row for each array type in the system.

Field          | Type                       | Meaning
---------------|----------------------------|--------
`type_id`      | [`text`](../../types/text) | The ID of the array type.
`element_id`   | [`text`](../../types/text) | The ID of the array's element type.

### `mz_audit_events`

The `mz_audit_events` table records create, alter, and drop events for the
other objects in the system catalog.

 Field           | Type                                          | Meaning
-----------------|-----------------------------------------------|---------
 `id  `          | [`bigint`](../../types/integer/#bigint-info)  | The ordered id of the event.                                                          | The type of the event: `create`, `drop`, `alter`, or `rename`.
 `object_type`   | [`text`](../../types/text)                    | The type of the affected object: `cluster`, `cluster-replica`, `index`, `materialized-view`, `sink`, `source`, or `view`.
 `event_details` | [`jsonb`](../../types/jsonb)                  | Additional details about the event. The shape of the details varies based on `event_type` and `object_type`.
 `user`          | [`text`](../../types/text)                    | The user who triggered the event.
 `occurred_at`   | [`timestamp with time zone`](../../types/timestamp/#timestamp-with-time-zone-info) | The time at which the event occurred.

### `mz_base_types`

The `mz_base_types` table contains a row for each base type in the system.

Field          | Type                       | Meaning
---------------|----------------------------|----------
`type_id`      | [`text`](../../types/text) | The ID of the type.

### `mz_clusters`

The `mz_clusters` view contains a row for each cluster in the system.

Field       | Type                                          | Meaning
------------|-----------------------------------------------|--------
`id`        | [`bigint`](../../types/integer/#bigint-info)  | The ID of the cluster.
`name`      | [`text`](../../types/text)                    | The name of the cluster.

### `mz_cluster_replicas`

The `mz_cluster_replicas` view contains a row for each cluster replica in the system.

Field               | Type                                          | Meaning
--------------------|-----------------------------------------------|--------
`cluster_id`        | [`bigint`](../../types/integer/#bigint-info)  | The ID of the cluster that the replica belongs to. Corresponds to [`mz_clusters.id`](#mz_clusters).
`id`                | [`bigint`](../../types/integer/#bigint-info)  | The ID of the cluster replica.
`name`              | [`text`](../../types/text)                    | The name of the cluster replica.
`size`              | [`text`](../../types/text)                    | The size of the cluster replica.
`availability_zone` | [`text`](../../types/text)                    | The availability zone to which the cluster replica belongs. May be `NULL`.
`status`            | [`text`](../../types/text)                    | The status of the cluster replica. Either `unhealthy` or `healthy`.

### `mz_columns`

The `mz_columns` contains a row for each column in each table, source, and view
in the system.

Field            | Type                                         | Meaning
-----------------|----------------------------------------------|--------
`id`             | [`bigint`](../../types/integer/#bigint-info) | The unique ID of the table, source, or view containing the column.
`name`           | [`text`](../../types/text)                   | The name of the column.
`position`       | [`bigint`](../../types/integer/#bigint-info) | The 1-indexed position of the column in its containing table, source, or view.
`nullable`       | [`boolean`](../../types/boolean)             | Can the column contain a `NULL` value?
`type`           | [`text`](../../types/text)                   | The data type of the column.
`default`        | [`text`](../../types/text)                   | The default expression of the column.
`type_oid`       | [`oid`](../../types/oid)                     | The OID of the type of the column (references `mz_types`).

### `mz_databases`

The `mz_databases` table contains a row for each database in the system.

Field  | Type                                         | Meaning
-------|----------------------------------------------|--------
`id`   | [`bigint`](../../types/integer/#bigint-info) | Materialize's unique ID for the database.
`oid`  | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the database.
`name` | [`text`](../../types/text)                   | The name of the database.

### `mz_functions`

The `mz_functions` table contains a row for each function in the system.

 Field         | Type                                         | Meaning
---------------|----------------------------------------------|---------
 `id`          | [`text`](../../types/text)                   | Materialize's unique ID for the function.
 `oid`         | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the function.
 `schema_id`   | [`bigint`](../../types/integer/#bigint-info) | The ID of the schema to which the function belongs.
 `name`        | [`text`](../../types/text)                   | The name of the function.
 `arg_ids`     | [`text array`](../../types/array)            | The function's arguments' types. Elements refers to `mz_types.id`.
 `variadic_id` | [`text`](../../types/text)                   | The variadic array parameter's elements, or `NULL` if the function does not have a variadic parameter. Refers to `mz_types.id`.
 `ret_id`      | [`text`](../../types/text)                   | The returned value's type, or `NULL` if the function does not return a value. Refers to `mz_types.id`. Note that for table functions with > 1 column, this type corresponds to [`record`](../../types/record).
 `ret_set`     | [`boolean`](../../types/boolean)             | Whether the returned value is a set, i.e. the function is a table function.

### `mz_indexes`

The `mz_indexes` table contains a row for each index in the system.

 Field   | Type                       | Meaning
---------|----------------------------|---------
 `id`    | [`text`](../../types/text) | Materialize's unique ID for the index.
 `oid`   | [`oid`](../../types/oid)   | A [PostgreSQL-compatible OID](../../types/oid) for the index.
 `name`  | [`text`](../../types/text) | The name of the index.
 `on_id` | [`text`](../../types/text) | The ID of the relation on which the index is built.

### `mz_index_columns`

The `mz_index_columns` table contains a row for each column in each index in the
system. For example, an index on `(a, b + 1)` would have two rows in this table,
one for each of the two columns in the index.

For a given row, if `field_number` is null then `expression` will be nonnull, or
vice-versa.

 Field            | Type                                         | Meaning
------------------|----------------------------------------------|---------
 `index_id`       | [`text`](../../types/text)                   | The ID of the index which contains this column.
 `index_position` | [`bigint`](../../types/integer/#bigint-info) | The 1-indexed position of this column within the index. (The order of columns in an index does not necessarily match the order of columns in the relation on which the index is built.)
 `on_position`    | [`bigint`](../../types/integer/#bigint-info) | If not `NULL`, specifies the 1-indexed position of a column in the relation on which this index is built that determines the value of this index column.
 `on_expression`  | [`text`](../../types/text)                   | If not `NULL`, specifies a SQL expression that is evaluated to compute the value of this index column. The expression may contain references to any of the columns of the relation.
 `nullable`       | [`boolean`](../../types/boolean)             | Can this column of the index evaluate to `NULL`?

### `mz_kafka_sinks`

The `mz_kafka_sinks` table contains a row for each Kafka sink in the system.

Field                | Type                        | Meaning
---------------------|-----------------------------|--------
`sink_id`            | [`text`](../../types/text)  | The ID of the sink.
`topic`              | [`text`](../../types/text)  | The name of the Kafka topic into which the sink is writing.
`consistency_topic`  | [`text`](../../types/text)  | The name of the Kafka topic into which the sink is writing consistency information. This is `NULL` when the sink does not write consistency information.

### `mz_list_types`

The `mz_list_types` table contains a row for each list type in the system.

Field        | Type                        | Meaning
-------------|-----------------------------|--------
`type_id`    | [`text`](../../types/text)  | The ID of the list type.
`element_id` | [`text`](../../types/text)  | The IID of the list's element type.

#### `mz_map_types`

The `mz_map_types` table contains a row for each map type in the system.

Field          | Type                          | Meaning
---------------|-------------------------------|----------
`type_id`      | [`text`](../../types/text)    | The ID of the map type.
`key_id `      | [`text`](../../types/text)    | The ID of the map's key type.
`value_id`     | [`text`](../../types/text)    | The ID of the map's value type.

### `mz_objects`

The `mz_objects` view contains a row for each table, source, view, materialized view, sink,
index, connection, secret, type, and function in the system.

 Field       | Type                                         | Meaning
-------------|----------------------------------------------|---------
 `id`        | [`text`](../../types/text)                   | Materialize's unique ID for the object.
 `oid`       | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the object.
 `schema_id` | [`bigint`](../../types/integer/#bigint-info) | The ID of the schema to which the object belongs.
 `name`      | [`text`](../../types/text)                   | The name of the object.
 `type`      | [`text`](../../types/text)                   | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.

### `mz_pseudo_types`

The `mz_pseudo_types` table contains a row for each pseudo type in the system.

Field          | Type                       | Meaning
---------------|----------------------------|----------
`type_id`      | [`text`](../../types/text) | The ID of the type.

### `mz_relations`

The `mz_relations` view contains a row for each table, source, view, and
materialized view in the system.

 Field       | Type                                         | Meaning
-------------|----------------------------------------------|---------
 `id`        | [`text`](../../types/text)                   | Materialize's unique ID for the relation.
 `oid`       | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the relation.
 `schema_id` | [`bigint`](../../types/integer/#bigint-info) | The ID of the schema to which the relation belongs.
 `name`      | [`text`](../../types/text)                   | The name of the relation.
 `type`      | [`text`](../../types/text)                   | The type of the relation: either `table`, `source`, `view`, or `materialized view`.

### `mz_roles`

The `mz_roles` table contains a row for each role in the system.

 Field  | Type                                         | Meaning
--------|----------------------------------------------|---------
 `id`   | [`bigint`](../../types/integer/#bigint-info) | Materialize's unique ID for the role.
 `oid`  | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the role.
 `name` | [`text`](../../types/text)                   | The name of the role.

### `mz_schemas`

The `mz_schemas` table contains a row for each schema in the system.

 Field         | Type                                         | Meaning
---------------|----------------------------------------------|---------
 `id`          | [`bigint`](../../types/integer/#bigint-info) | Materialize's unique ID for the schema.
 `oid`         | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the schema.
 `database_id` | [`bigint`](../../types/integer/#bigint-info) | The ID of the database containing the schema.
 `name`        | [`text`](../../types/text)                   | The name of the schema.

### `mz_sinks`

The `mz_sinks` table contains a row for each sink in the system.

 Field       | Type                                         | Meaning
-------------|----------------------------------------------|---------
 `id`        | [`text`](../../types/text)                   | Materialize's unique ID for the sink.
 `oid`       | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the sink.
 `schema_id` | [`bigint`](../../types/integer/#bigint-info) | The ID of the schema to which the sink belongs.
 `name`      | [`text`](../../types/text)                   | The name of the sink.
 `type`      | [`text`](../../types/text)                   | The type of the sink: `kafka`.

### `mz_sources`

The `mz_sources` table contains a row for each source in the system.

 Field       | Type                                         | Meaning
-------------|----------------------------------------------|---------
 `id`        | [`text`](../../types/text)                   | Materialize's unique ID for the source.
 `oid`       | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the source.
 `schema_id` | [`bigint`](../../types/integer/#bigint-info) | The ID of the schema to which the source belongs.
 `name`      | [`text`](../../types/text)                   | The name of the source.
 `type`      | [`text`](../../types/text)                   | The type of the source: `kafka` or `postgres`.

### `mz_storage_usage`

The `mz_storage_usage` table contains a row for each storage utilization snapshot taken within the retention period (60 days as of July 2022)

Field                  | Type                                         | Meaning
---------------------- | ---------------------------------------------| --------
`object_id`            | [`text`](../../types/text)                   | Materialize's unique ID for the storage object.
`size_bytes`           | [`bigint`](../../types/integer/#bigint-info) | The size in bytes of the storage object.
`collection_timestamp` | [`timestamp with time zone`](../../types/timestamp/#timestamp-with-time-zone-info)| The time at which the storage snapshot was collected.

### `mz_tables`

The `mz_tables` table contains a row for each table in the system.

 Field            | Type                                         | Meaning
------------------|----------------------------------------------|---------
 `id`             | [`text`](../../types/text)                   | Materialize's unique ID for the table.
 `oid`            | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the table.
 `schema_id`      | [`bigint`](../../types/integer/#bigint-info) | The ID of the schema to which the table belongs.
 `name`           | [`text`](../../types/text)                   | The name of the table.
 `persisted_name` | [`text`](../../types/text)                   | The name of the table's persisted materialization, or `NULL` if the table is not being persisted.

### `mz_types`

The `mz_types` table contains a row for each type in the system.

Field          | Type                                         | Meaning
---------------|----------------------------------------------|----------
`id`           | [`text`](../../types/text)                   | Materialize's unique ID for the type.
`oid`          | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the type.
`schema_id`    | [`bigint`](../../types/integer/#bigint-info) | The ID of the schema to which the type belongs.
`name`         | [`text`](../../types/text)                   | The name of the type.

### `mz_views`

The `mz_views` table contains a row for each view in the system.

Field          | Type                                         | Meaning
---------------|----------------------------------------------|----------
`id`           | [`text`](../../types/text)                   | Materialize's unique ID for the view.
`oid`          | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the view.
`schema_id`    | [`bigint`](../../types/integer/#bigint-info) | The ID of the schema to which the view belongs.
`name`         | [`text`](../../types/text)                   | The name of the view.
`definition`   | [`text`](../../types/text)                   | The view definition (a `SELECT` query).


## Dataflow Properties
{{< warning >}}
The dataflow system catalog is not part of Materialize's stable interface.
Backwards-incompatible changes to these tables may be made at any time.
{{< /warning >}}

Each dataflow property table or view is unique to each [cluster replica](#mz_cluster_replicas).
See the [Cluster Replica](/overview/key-concepts/#cluster-replicas)
conceptual overview for more conext.
These table and view are suffixed by the cluster replica ID, e.g.`mz_dataflows_1`.
Querying the base table (e.g. `mz_dataflows`) will return data for
current cluster replica.

### `mz_arrangement_sizes`

The `mz_arrangement_sizes` source describes the size of each [arrangement](/overview/arrangements/#arrangements) in
the system.

Field      | Type                                         | Meaning
-----------|----------------------------------------------|--------
`operator` | [`bigint`](../../types/integer/#bigint-info) | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker`   | [`bigint`](../../types/integer/#bigint-info) | The worker hosting the arrangement.
`records`  | [`bigint`](../../types/integer/#bigint-info) | The number of records in the arrangement.
`batches`  | [`bigint`](../../types/integer/#bigint-info) | The number of batches in the arrangement.

### `mz_arrangement_sharing`

The `mz_arrangement_sharing` source describes how many times each [arrangement](/overview/arrangements/#arrangements)
in the system is used.

Field      | Type                                         | Meaning
-----------|----------------------------------------------|--------
`operator` | [`bigint`](../../types/integer/#bigint-info) | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker`   | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread hosting the arrangement.
`count`    | [`bigint`](../../types/integer/#bigint-info) | The number of operators that share the arrangement.

### `mz_dataflow_channels`

The `mz_dataflow_channels` source describes the communication channels between
[dataflow](/overview/arrangements/#dataflows) operators. A communication channel connects one of the outputs of a
source operator to one of the inputs of a target operator.

Field         | Type                                         | Meaning
--------------|----------------------------------------------|--------
`id`          | [`bigint`](../../types/integer/#bigint-info) | The ID of the channel.
`worker`      | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread hosting the channel.
`source_node` | [`bigint`](../../types/integer/#bigint-info) | The ID of the source operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`source_port` | [`bigint`](../../types/integer/#bigint-info) | The source operator's output port.
`target_node` | [`bigint`](../../types/integer/#bigint-info) | The ID of the target operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`target_port` | [`bigint`](../../types/integer/#bigint-info) | The target operator's input port.

### `mz_dataflows`

The `mz_dataflows` view describes the [dataflows](/overview/arrangements/#dataflows) in the system.

 Field      | Type                                         | Meaning
------------|----------------------------------------------|---------
 `id`       | [`bigint`](../../types/integer/#bigint-info) | The ID of the dataflow.
 `worker`   | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread hosting the dataflow.
 `local_id` | [`bigint`](../../types/integer/#bigint-info) | The scope-local index of the dataflow.
 `name`     | [`text`](../../types/text)                   | The internal name of the dataflow.

### `mz_dataflow_addresses`

The `mz_dataflow_addresses` source describes how the dataflow channels
and operators in the system are nested into scopes.

 Field     | Type                                         | Meaning
-----------|----------------------------------------------|---------
 `id`      | [`bigint`](../../types/integer/#bigint-info) | The ID of the channel or operator. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels) or [`mz_dataflow_operators.id`](#mz_dataflow_operators).
 `worker`  | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread hosting the channel or operator.
 `address` | [`bigint list`](../../types/list)            | A list of scope-local indexes indicating the path from the root to this channel or operator.

### `mz_dataflow_operator_dataflows`

The `mz_dataflow_operator_dataflows` view describes the [dataflow](/overview/arrangements/#dataflows) to which each
dataflow operator belongs.

 Field           | Type                                         | Meaning
-----------------|----------------------------------------------|---------
 `id`            | [`bigint`](../../types/integer/#bigint-info) | The ID of the operator.
 `name`          | [`text`](../../types/text)                   | The internal name of the operator.
 `worker`        | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread hosting the operator.
 `dataflow_id`   | [`bigint`](../../types/integer/#bigint-info) | The ID of the dataflow hosting the operator.
 `dataflow_name` | [`text`](../../types/text)                   | The name of the dataflow hosting the operator.

### `mz_dataflow_operators`

The `mz_dataflow_operators` source describes the dataflow operators in the
system.

 Field    | Type                                         | Meaning
----------|----------------------------------------------|---------
 `id`     | [`bigint`](../../types/integer/#bigint-info) | The ID of the operator.
 `worker` | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread hosting the operator.
 `name`   | [`text`](../../types/text)                   | The name of the operator.

### `mz_message_counts`

The `mz_message_counts` source describes the messages sent and received over the
dataflow channels in the system.

Field           | Type       | Meaning
----------------|------------|--------
`channel`       | [`bigint`](../../types/integer/#bigint-info) | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels).
`source_worker` | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread sending the message.
`target_worker` | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread receiving the message.
`sent`          | [`bigint`](../../types/integer/#bigint-info) | The number of messages sent.
`received`      | [`bigint`](../../types/integer/#bigint-info) | The number of messages received.

### `mz_materialization_dependencies`

The `mz_materialization_dependencies` source describes the dependency structure between each [dataflow](/overview/arrangements/#dataflows) and
the sources of their data. To create a complete dependency structure, the source column includes also
other dataflows.

 Field      | Type                                         | Meaning
------------|----------------------------------------------|--------
 `dataflow` | [`text`](../../types/text)                   | The ID of the index that created the dataflow. Corresponds to [`mz_materializations.global_id`](#mz_materializations).
 `source`   | [`text`](../../types/text)                   | The ID of the source. Corresponds to [`mz_sources.id`](#mz_sources) or [`mz_tables.id`](#mz_tables) or [`mz_materializations.global_id`](#mz_materializations).
 `worker`   | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread hosting the dataflow.

### `mz_materialization_frontiers`

The `mz_materialization_frontiers` view describes the frontier for each
[dataflow](/overview/arrangements/#dataflows) in the system across all workers. The frontier describes the earliest
timestamp at which the output of the dataflow may change; data prior to that
timestamp is sealed.

For per-worker frontier information, see
[`mz_worker_materialization_frontiers`](#mz_worker_materialization_frontiers).

 Field        | Type                                         | Meaning
--------------|----------------------------------------------|---------
 `global_id ` | [`text`](../../types/text)                   | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_materializations.global_id`](#mz_materializations).
 `time`       | [`bigint`](../../types/integer/#bigint-info) | The next timestamp at which the materialization may change.

### `mz_materialization_source_frontiers`

The `mz_materialization_source_frontiers` view describes the frontiers for every
storage source used in a [dataflow](/overview/arrangements/#dataflows) in the system across all workers. The frontier
describes the earliest timestamp at which the output of the source instantiation
at the dataflow layer may change; data prior to that timestamp is sealed.

For per-worker frontier information, see
[`mz_worker_materialization_source_frontiers`](#mz_worker_materialization_source_frontiers).

 Field       | Type                                         | Meaning
-------------|----------------------------------------------|---------
 `global_id` | [`text`](../../types/text)                   | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_materializations.global_id`](#mz_materializations).
 `source`    | [`text`](../../types/text)                   | The ID of the input storage source for the dataflow. Corresponds to either [`mz_sources.id`](#mz_sources) or [`mz_tables.id`](#mz_tables) or [`mz_materialized_views.id`](#mz_materialized_views).
 `time`      | [`bigint`](../../types/integer/#bigint-info) | The next timestamp at which the source instantiation may change.

### `mz_materializations`

The `mz_materializations` source describes the dataflows created by indexes and materialized views in the system.

Field       | Type                                          | Meaning
------------|-----------------------------------------------|--------
`global_id` | [`text`](../../types/text)                    | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_indexes.id`](#mz_indexes) or [`mz_materialized_views.id`](#mz_materialized_views).
`worker`    | [`bigint`](../../types/integer/#bigint-info)  | The ID of the worker thread hosting the corresponding [dataflow](/overview/arrangements/#dataflows).

### `mz_materialized_views`

The `mz_materialized_views` table contains a row for each materialized view in the system.

Field          | Type                                         | Meaning
---------------|----------------------------------------------|----------
`id`           | [`text`](../../types/text)                   | Materialize's unique ID for the materialized view.
`oid`          | [`oid`](../../types/oid)                     | A [PostgreSQL-compatible OID](../../types/oid) for the materialized view.
`schema_id`    | [`bigint`](../../types/integer/#bigint-info) | The ID of the schema to which the materialized view belongs.
`name`         | [`text`](../../types/text)                   | The name of the materialized view.
`cluster_id`   | [`bigint`](../../types/integer/#bigint-info) | The ID of the cluster maintaining the materialized view.
`definition`   | [`text`](../../types/text)                   | The materialized view definition (a `SELECT` query).

### `mz_peek_active`

The `mz_peek_active` source describes all read queries ("peeks") that are
pending in the dataflow layer.

Field      | Type                                         | Meaning
-----------|----------------------------------------------|--------
`id`       | [`uuid`](../../types/uuid)                   | The ID of the peek request.
`worker`   | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread servicing the peek.
`index_id` | [`text`](../../types/text)                   | The ID of the index the peek is targeting.
`time`     | [`bigint`](../../types/integer/#bigint-info) | The timestamp the peek has requested.

### `mz_peek_durations`

The `mz_peek_durations` source describes a histogram of the duration of read
queries ("peeks") in the dataflow layer.

Field         | Type                                         | Meaning
--------------|----------------------------------------------|--------
`worker`      | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread servicing the peek.
`duration_ns` | [`bigint`](../../types/integer/#bigint-info) | The upper bound of the bucket in nanoseconds.
`count`       | [`bigint`](../../types/integer/#bigint-info) | The (noncumulative) count of peeks in this bucket.

### `mz_records_per_dataflow`

The `mz_records_per_dataflow` view describes the number of records in each
[dataflow](/overview/arrangements/#dataflows) on each worker in the system.

For the same information aggregated across all workers, see
[`mz_records_per_dataflow_global`](#mz_records_per_dataflow_global).

Field     | Type                                          | Meaning
----------|-----------------------------------------------|--------
`id`      | [`bigint`](../../types/integer/#bigint-info)  | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).
`name`    | [`text`](../../types/text)                    | The internal name of the dataflow.
`worker`  | [`bigint`](../../types/integer/#bigint-info)  | The ID of the worker thread hosting the dataflow.
`records` | [`numeric`](../../types/numeric)              | The number of records in the dataflow.

### `mz_records_per_dataflow_global`

The `mz_records_per_dataflow_global` view describes the number of records in each
[dataflow](/overview/arrangements/#dataflows) in the system.

For the same information broken down across workers, see
[`mz_records_per_dataflow`](#mz_records_per_dataflow).

Field     | Type                                          | Meaning
----------|-----------------------------------------------|--------
`id`      | [`bigint`](../../types/integer/#bigint-info)  | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).
`name`    | [`text`](../../types/text)                    | The internal name of the dataflow.
`records` | [`numeric`](../../types/numeric)              | The number of records in the dataflow.

### `mz_records_per_dataflow_operator`

The `mz_records_per_dataflow_operator` view describes the number of records in
each [dataflow](/overview/arrangements/#dataflows) operator in the system.

Field         | Type                                          | Meaning
--------------|-----------------------------------------------|--------
`id`          | [`bigint`](../../types/integer/#bigint-info)  | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`name`        | [`text`](../../types/text)     | The internal name of the dataflow.
`worker`      | [`bigint`](../../types/integer/#bigint-info)  | The ID of the worker thread hosting the dataflow.
`dataflow_id` | [`bigint`](../../types/integer/#bigint-info)  | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).
`records`     | [`numeric`](../../types/numeric)              | The number of records in the dataflow.

### `mz_scheduling_elapsed`

The `mz_scheduling_elapsed` source describes the total amount of time spent in
each [dataflow](/overview/arrangements/#dataflows) operator.

Field        | Type                                         | Meaning
-------------|----------------------------------------------|--------
`id`         | [`bigint`](../../types/integer/#bigint-info) | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker`     | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread hosting the operator.
`elapsed_ns` | [`bigint`](../../types/integer/#bigint-info) | The total elapsed time spent in the operator in nanoseconds.

### `mz_scheduling_histogram`

The `mz_scheduling_histogram` source stores a histogram describing the
duration of each invocation for each [dataflow](/overview/arrangements/#dataflows) operator.

Field         | Type                                         | Meaning
--------------|----------------------------------------------|--------
`id`          | [`bigint`](../../types/integer/#bigint-info) | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker`      | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread hosting the operator.
`duration_ns` | [`bigint`](../../types/integer/#bigint-info) | The upper bound of the bucket.
`count`       | [`bigint`](../../types/integer/#bigint-info) | The number of recordings in the bucket.

### `mz_scheduling_parks`

The `mz_scheduling_parks` source stores a histogram describing [dataflow](/overview/arrangements/#dataflows) worker
park events. A park event occurs when a worker has no outstanding work.

Field       | Type                                         | Meaning
------------|----------------------------------------------| -------
`worker`    | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread.
`slept_for` | [`bigint`](../../types/integer/#bigint-info) | The actual length of the park event.
`requested` | [`bigint`](../../types/integer/#bigint-info) | The requested length of the park event.
`count`     | [`bigint`](../../types/integer/#bigint-info) | The number of park events in this bucket.

### `mz_worker_materialization_frontiers`

The `mz_worker_materialization_frontiers` source describes each worker's
frontier for each [dataflow](/overview/arrangements/#dataflows) in the system. The frontier describes the earliest
timestamp at which the output of the dataflow may change; data prior to that
timestamp is sealed.

For frontier information aggregated across all workers, see
[`mz_materialization_frontiers`](#mz_materialization_frontiers).

Field       | Type                                         | Meaning
------------|----------------------------------------------|--------
`global_id` | [`text`](../../types/text)                   | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_materializations.global_id`](#mz_materializations).
`worker`    | [`bigint`](../../types/integer/#bigint-info) | The ID of the worker thread hosting the dataflow.
`time`      | [`bigint`](../../types/integer/#bigint-info) | The next timestamp at which the dataflow may change.

### `mz_worker_materialization_source_frontiers`

The `mz_worker_materialization_source_frontiers` source describes the frontiers that
each worker is aware of for every storage source used in a [dataflow](/overview/arrangements/#dataflows) in the system. The
frontier describes the earliest timestamp at which the output of the source instantiation
at the dataflow layer may change; data prior to that timestamp is sealed.

For frontier information aggregated across all workers, see
[`mz_materialization_source_frontiers`](#mz_materialization_source_frontiers).

Field       | Type                                          | Meaning
------------|-----------------------------------------------|--------
`global_id` | [`text`](../../types/text)                    | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_materializations.global_id`](#mz_materializations).
`source`    | [`text`](../../types/text)                    | The ID of the input storage source for the dataflow. Corresponds to either [`mz_sources.id`](#mz_sources) or [`mz_tables.id`](#mz_tables) or [`mz_materialized_views.id`](#mz_materialized_views).
`worker`    | [`bigint`](../../types/integer/#bigint-info)  | The ID of the worker thread hosting the dataflow.
`time`      | [`bigint`](../../types/integer/#bigint-info)  | The next timestamp at which the source instantiation may change.

### `mz_worker_materialization_delays`

The `mz_worker_materialization_delays` source provides, for each worker,
a histogram of wall-clock delays between observations of storage source frontier
advancements at the dataflow layer and the advancements of the corresponding
[dataflow](/overview/arrangements/#dataflows) frontiers.

Field       | Type                                          | Meaning
------------|-----------------------------------------------|--------
`global_id` | [`text`](../../types/text)                    | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_materializations.global_id`](#mz_materializations).
`source`    | [`text`](../../types/text)                    | The ID of the input storage source for the dataflow. Corresponds to either [`mz_sources.id`](#mz_sources) or [`mz_tables.id`](#mz_tables) or [`mz_materialized_views.id`](#mz_materialized_views).
`worker`    | [`bigint`](../../types/integer/#bigint-info)  | The ID of the worker thread hosting the dataflow.
`delay_ns`  | [`bigint`](../../types/integer/#bigint-info)  | The upper bound of the bucket in nanoseconds.
`count`     | [`bigint`](../../types/integer/#bigint-info)  | The (noncumulative) count of delay measurements in this bucket.