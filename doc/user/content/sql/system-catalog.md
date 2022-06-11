---
title: "System catalog"
description: "The system catalog stores metadata about your Materialize instance."
aliases:
  - /sql/system-tables
menu:
  main:
    parent: 'reference'
    weight: 160
---

Materialize exposes a system catalog that contains metadata about the running
Materialize instance.

{{< warning >}}
The system tables are not part of Materialize's stable interface.
Backwards-incompatible changes to these tables may be made at any time.
{{< /warning >}}

## Details

The system catalog consists of two schemas that are implicitly available in
all databases:

  * [`mz_catalog`](#mz_catalog), which exposes metadata in Materialize's
    native format.

  * [`pg_catalog`](#pg_catalog), which presents the data in `mz_catalog` in
    the format used by PostgreSQL.

These schemas contain sources, tables, and views that expose metadata like:

  * Descriptions of each database, schema, source, table, view, sink, and
    index in the system.

  * Descriptions of all running dataflows.

  * Metrics about dataflow execution.

Whenever possible, applications should prefer to query `mz_catalog` over
`pg_catalog`. The mapping between Materialize concepts and PostgreSQL concepts
is not one-to-one, and so the data in `pg_catalog` cannot accurately represent
the particulars of Materialize. For example, PostgreSQL has no notion of
[sinks](/sql/create-sink), and therefore `pg_catalog` does not display
information about the sinks available in a Materialize.

## `mz_catalog`

The following sections describe the available objects in the `mz_catalog`
schema.

### `mz_array_types`

The `mz_array_types` table contains a row for each array type in the system.

Field          | Type       | Meaning
---------------|------------|--------
`type_id`      | [`text`]   | The ID of the array type.
`element_id`   | [`text`]   | The ID of the array's element type.

### `mz_arrangement_sharing`

The `mz_arrangement_sharing` source describes how many times each [arrangement]
in the system is used.

Field      | Type       | Meaning
-----------|------------|--------
`operator` | [`bigint`] | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker`   | [`bigint`] | The ID of the worker thread hosting the arrangement.
`count`    | [`bigint`] | The number of operators that share the arrangement.

### `mz_arrangement_sizes`

The `mz_arrangement_sizes` source describes the size of each [arrangement] in
the system.

Field      | Type       | Meaning
-----------|------------|--------
`operator` | [`bigint`] | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker`   | [`bigint`] | The worker hosting the arrangement.
`records`  | [`bigint`] | The number of records in the arrangement.
`batches`  | [`bigint`] | The number of batches in the arrangement.

### `mz_base_types`

The `mz_base_types` table contains a row for each base type in the system.

Field          | Type       | Meaning
---------------|------------|----------
`type_id`      | [`text`]   | The ID of the type.

### `mz_columns`

The `mz_columns` contains a row for each column in each table, source, and view
in the system.

Field            | Type        | Meaning
-----------------|-------------|--------
`id`             | [`bigint`]  | The unique ID of the table, source, or view containing the column.
`name`           | [`text`]    | The name of the column.
`position`       | [`bigint`]  | The 1-indexed position of the column in its containing table, source, or view.
`nullable`       | [`boolean`] | Can the column contain a `NULL` value?
`type`           | [`text`]    | The data type of the column.
`default`        | [`text`]    | The default expression of the column.
`type_oid`       | [`oid`]     | The OID of the type of the column (references `mz_types`).

### `mz_databases`

The `mz_databases` table contains a row for each database in the system.

Field  | Type       | Meaning
-------|------------|--------
`id`   | [`bigint`] | Materialize's unique ID for the database.
`oid`  | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the database.
`name` | [`text`]   | The name of the database.

### `mz_dataflow_channels`

The `mz_dataflow_channels` source describes the communication channels between
[dataflow] operators. A communication channel connects one of the outputs of a
source operator to one of the inputs of a target operator.

Field         | Type       | Meaning
--------------|------------|--------
`id`          | [`bigint`] | The ID of the channel.
`worker`      | [`bigint`] | The ID of the worker thread hosting the channel.
`source_node` | [`bigint`] | The ID of the source operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`source_port` | [`bigint`] | The source operator's output port.
`target_node` | [`bigint`] | The ID of the target operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`target_port` | [`bigint`] | The target operator's input port.

### `mz_dataflow_names`

The `mz_dataflow_names` view describes the [dataflows][dataflow] in the system.

Field      | Type       | Meaning
-----------|------------|--------
`id`       | [`bigint`] | The ID of the dataflow.
`worker`   | [`bigint`] | The ID of the worker thread hosting the dataflow.
`local_id` | [`bigint`] | The scope-local index of the dataflow.
`name`     | [`text`]   | The internal name of the dataflow.

### `mz_dataflow_operator_addresses`

The `mz_dataflow_operator_addresses` source describes how the dataflow channels
and operators in the system are nested into scopes.

Field     | Type            | Meaning
----------|-----------------|--------
`id`      | [`bigint`]      | The ID of the channel or operator. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels) or [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker`  | [`bigint`]      | The ID of the worker thread hosting the channel or operator.
`address` | [`bigint list`] | A list of scope-local indexes indicating the path from the root to this channel or operator.

### `mz_dataflow_operator_dataflows`

The `mz_dataflow_operator_dataflows` view describes the [dataflow] to which each
dataflow operator belongs.

Field           | Type       | Meaning
----------------|------------|--------
`id`            | [`bigint`] | The ID of the operator.
`name`          | [`text`]   | The internal name of the operator.
`worker`        | [`bigint`] | The ID of the worker thread hosting the operator.
`dataflow_id`   | [`bigint`] | The ID of the dataflow hosting the operator.
`dataflow_name` | [`text`]   | The name of the dataflow hosting the operator.

### `mz_dataflow_operators`

The `mz_dataflow_operators` source describes the dataflow operators in the
system.

Field    | Type       | Meaning
---------|------------|--------
`id`     | [`bigint`] | The ID of the operator.
`worker` | [`bigint`] | The ID of the worker thread hosting the operator.
`name`   | [`text`]   | The name of the operator.

### `mz_functions`

The `mz_functions` table contains a row for each function in the system.

Field         | Type           | Meaning
--------------|----------------|--------
`id`          | [`text`]       | Materialize's unique ID for the function.
`oid`         | [`oid`]        | A [PostgreSQL-compatible OID][oid] for the function.
`schema_id`   | [`bigint`]     | The ID of the schema to which the function belongs.
`name`        | [`text`]       | The name of the function.
`arg_ids`     | [`text array`] | The function's arguments' types. Elements refers to `mz_types.id`.
`variadic_id` | [`text`]       | The variadic array parameter's elements, or `NULL` if the function does not have a variadic parameter. Refers to `mz_types.id`.
`ret_id`      | [`text`]       | The returned value's type, or `NULL` if the function does not return a value. Refers to `mz_types.id`. Note that for table functions with > 1 column, this type corresponds to [`record`].
`ret_set`     | [`bool`]       | Whether the returned value is a set, i.e. the function is a table function.

### `mz_indexes`

The `mz_indexes` table contains a row for each index in the system.

Field        | Type        | Meaning
-------------|-------------|--------
`id`         | [`text`]    | Materialize's unique ID for the index.
`oid`        | [`oid`]     | A [PostgreSQL-compatible OID][oid] for the index.
`name`       | [`text`]    | The name of the index.
`on_id`      | [`text`]    | The ID of the relation on which the index is built.
`volatility` | [`text`]    | Whether the index is [volatile](/overview/volatility). Either `volatile`, `nonvolatile`, or `unknown`.

### `mz_index_columns`

The `mz_index_columns` table contains a row for each column in each index in the
system. For example, an index on `(a, b + 1)` would have two rows in this table,
one for each of the two columns in the index.

For a given row, if `field_number` is null then `expression` will be nonnull, or
vice-versa.

Field            | Type        | Meaning
-----------------|-------------|--------
`index_id`       | [`text`]    | The ID of the index which contains this column.
`index_position` | [`bigint`]  | The 1-indexed position of this column within the index. (The order of columns in an index does not necessarily match the order of columns in the relation on which the index is built.)
`on_position`    | [`bigint`]  | If not `NULL`, specifies the 1-indexed position of a column in the relation on which this index is built that determines the value of this index column.
`on_expression`  | [`text`]    | If not `NULL`, specifies a SQL expression that is evaluated to compute the value of this index column. The expression may contain references to any of the columns of the relation.
`nullable`       | [`boolean`] | Can this column of the index evaluate to `NULL`?

### `mz_kafka_sinks`

The `mz_kafka_sinks` table contains a row for each Kafka sink in the system.

Field                | Type     | Meaning
---------------------|----------|--------
`sink_id`            | [`text`] | The ID of the sink.
`topic`              | [`text`] | The name of the Kafka topic into which the sink is writing.
`consistency_topic`  | [`text`] | The name of the Kafka topic into which the sink is writing consistency information. This is `NULL` when the sink does not write consistency information.

### `mz_list_types`

The `mz_list_types` table contains a row for each list type in the system.

Field        | Type     | Meaning
-------------|----------|--------
`type_id`    | [`text`] | The ID of the list type.
`element_id` | [`text`] | The IID of the list's element type.

### `mz_message_counts`

The `mz_message_counts` source describes the messages sent and received over the
dataflow channels in the system.

Field           | Type       | Meaning
----------------|------------|--------
`channel`       | [`bigint`] | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels).
`source_worker` | [`bigint`] | The ID of the worker thread sending the message.
`target_worker` | [`bigint`] | The ID of the worker thread receiving the message.
`sent`          | [`bigint`] | The number of messages sent.
`received`      | [`bigint`] | The number of messages received.

### `mz_materialization_dependencies`

The `mz_materialization_dependencies` source describes the sources that each
[dataflow] depends on.

Field      | Type       | Meaning
-----------|------------|--------
`dataflow` | [`text`]   | The ID of the index that created the dataflow. Corresponds to [`mz_indexes.id`](#mz_indexes).
`source`   | [`text`]   | The ID of the source. Corresponds to [`mz_sources.id`](#mz_sources).
`worker`   | [`bigint`] | The ID of the worker thread hosting the dataflow.

### `mz_materialization_frontiers`

The `mz_materialization_frontiers` view describes the frontier for each
[dataflow] in the system across all workers. The frontier describes the earliest
timestamp at which the output of the dataflow may change; data prior to that
timestamp is sealed.

For per-worker frontier information, see
[`mz_worker_materialization_frontiers`](#mz_worker_materialization_frontiers).

Field       | Type       | Meaning
------------|------------|--------
`global_id` | [`text`]   | The ID of the index that created the dataflow. Corresponds to [`mz_indexes.id`](#mz_indexes).
`time`      | [`bigint`] | The next timestamp at which the materialization may change.

### `mz_materializations`

The `mz_materializations` source describes the indexes in the system.

Field    | Type       | Meaning
---------|------------|--------
`name`   | [`text`]   | The ID of the index. (`name` is a misnomer.)
`worker` | [`bigint`] | The ID of the worker thread hosting the index.

### `mz_map_types`

The `mz_map_types` table contains a row for each map type in the system.

Field          | Type       | Meaning
---------------|------------|----------
`type_id`      | [`text`]   | The ID of the map type.
`key_id `      | [`text`]   | The ID of the map's key type.
`value_id`     | [`text`]   | The ID of the map's value type.

### `mz_objects`

The `mz_objects` view contains a row for each table, source, view, sink, and
index in the system.

Field       | Type       | Meaning
------------|------------|--------
`id`        | [`text`]   | Materialize's unique ID for the object.
`oid`       | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the object.
`schema_id` | [`bigint`] | The ID of the schema to which the object belongs.
`name`      | [`text`]   | The name of the object.
`type`      | [`text`]   | The type of the object: either `table`, `source`, `view`, `sink`, or `index`.

### `mz_peek_active`

The `mz_peek_active` source describes all read queries ("peeks") that are
pending in the dataflow layer.

Field      | Type       | Meaning
-----------|------------|--------
`id`       | [`uuid`]   | The ID of the peek request.
`worker`   | [`bigint`] | The ID of the worker thread servicing the peek.
`index_id` | [`text`]   | The ID of the index the peek is targeting.
`time`     | [`bigint`] | The timestamp the peek has requested.

### `mz_peek_durations`

The `mz_peek_durations` source describes a histogram of the duration of read
queries ("peeks") in the dataflow layer.

Field         | Type       | Meaning
--------------|------------|--------
`worker`      | [`bigint`] | The ID of the worker thread servicing the peek.
`duration_ns` | [`bigint`] | The upper bound of the bucket in nanoseconds.
`count`       | [`bigint`] | The (noncumulative) count of peeks in this bucket.

### `mz_pseudo_types`

The `mz_pseudo_types` table contains a row for each psuedo type in the system.

Field          | Type       | Meaning
---------------|------------|----------
`type_id`      | [`text`]   | The ID of the type.

### `mz_records_per_dataflow`

The `mz_records_per_dataflow` view describes the number of records in each
[dataflow] on each worker in the system.

For the same information aggregated across all workers, see
[`mz_records_per_dataflow_global`](#mz_records_per_dataflow_global).

Field     | Type        | Meaning
----------|-------------|--------
`id`      | [`bigint`]  | The ID of the dataflow. Corresponds to [`mz_dataflow_names.id`](#mz_dataflow_names).
`name`    | [`text`]    | The internal name of the dataflow.
`worker`  | [`bigint`]  | The ID of the worker thread hosting the dataflow.
`records` | [`numeric`] | The number of records in the dataflow.

### `mz_records_per_dataflow_global`

The `mz_records_per_dataflow_global` view describes the number of records in each
[dataflow] in the system.

For the same information broken down across workers, see
[`mz_records_per_dataflow`](#mz_records_per_dataflow).

Field     | Type        | Meaning
----------|-------------|--------
`id`      | [`bigint`]  | The ID of the dataflow. Corresponds to [`mz_dataflow_names.id`](#mz_dataflow_names).
`name`    | [`text`]    | The internal name of the dataflow.
`records` | [`numeric`] | The number of records in the dataflow.

### `mz_records_per_dataflow_operator`

The `mz_records_per_dataflow_operator` view describes the number of records in
each [dataflow] operator in the system.

Field         | Type        | Meaning
--------------|-------------|--------
`id`          | [`bigint`]  | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`name`        | [`text`]    | The internal name of the dataflow.
`worker`      | [`bigint`]  | The ID of the worker thread hosting the dataflow.
`dataflow_id` | [`bigint`]  | The ID of the dataflow. Corresponds to [`mz_dataflow_names.id`](#mz_dataflow_names).
`records`     | [`numeric`] | The number of records in the dataflow.

### `mz_relations`

The `mz_relations` view contains a row for each table, source, and view in the
system.

Field       | Type       | Meaning
------------|------------|--------
`id`        | [`text`]   | Materialize's unique ID for the relation.
`oid`       | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the relation.
`schema_id` | [`bigint`] | The ID of the schema to which the relation belongs.
`name`      | [`text`]   | The name of the relation.
`type`      | [`text`]   | The type of the relation: either `table`, `source`, or `view`.

### `mz_roles`

The `mz_roles` table contains a row for each role in the system.

Field  | Type       | Meaning
-------|------------|--------
`id`   | [`bigint`] | Materialize's unique ID for the role.
`oid`  | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the role.
`name` | [`text`]   | The name of the role.

### `mz_scheduling_elapsed`

The `mz_scheduling_elapsed` source describes the total amount of time spent in
each [dataflow] operator.

Field        | Type       | Meaning
-------------|------------|--------
`id`         | [`bigint`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker`     | [`bigint`] | The ID of the worker thread hosting the operator.
`elapsed_ns` | [`bigint`] | The total elapsed time spent in the operator in nanoseconds.

### `mz_scheduling_histogram`

The `mz_scheduling_histogram` source stores a histogram describing the
duration of each invocation for each [dataflow] operator.

Field         | Type       | Meaning
--------------|------------|--------
`id`          | [`bigint`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker`      | [`bigint`] | The ID of the worker thread hosting the operator.
`duration_ns` | [`bigint`] | The upper bound of the bucket.
`count`       | [`bigint`] | The number of recordings in the bucket.

### `mz_scheduling_parks`

The `mz_scheduling_parks` source stores a histogram describing [dataflow] worker
park events. A park event occurs when a worker has no outstanding work.

Field       | Type       | Meaning
------------|------------| -------
`worker`    | [`bigint`] | The ID of the worker thread.
`slept_for` | [`bigint`] | The actual length of the park event.
`requested` | [`bigint`] | The requested length of the park event.
`count`     | [`bigint`] | The number of park events in this bucket.

### `mz_schemas`

The `mz_schemas` table contains a row for each schema in the system.

Field         | Type       | Meaning
--------------|------------|--------
`id`          | [`bigint`] | Materialize's unique ID for the schema.
`oid`         | [`oid`]    | A [PostgreSQL-compatible oid][oid] for the schema.
`database_id` | [`bigint`] | The ID of the database containing the schema.
`name`        | [`text`]   | The name of the schema.

### `mz_sinks`

The `mz_sinks` table contains a row for each sink in the system.

Field            | Type        | Meaning
-----------------|-------------|--------
`id`             | [`text`]    | Materialize's unique ID for the sink.
`oid`            | [`oid`]     | A [PostgreSQL-compatible OID][oid] for the sink.
`schema_id`      | [`bigint`]  | The ID of the schema to which the sink belongs.
`name`           | [`text`]    | The name of the sink.
`type`           | [`text`]    | The type of the sink: `kafka`.
`volatility`     | [`text`]    | Whether the sink is [volatile](/overview/volatility). Either `volatile`, `nonvolatile`, or `unknown`.

### `mz_sources`

The `mz_sources` table contains a row for each source in the system.

Field            | Type       | Meaning
-----------------|------------|----------
`id`             | [`text`]   | Materialize's unique ID for the source.
`oid`            | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the source.
`schema_id`      | [`bigint`] | The ID of the schema to which the source belongs.
`name`           | [`text`]   | The name of the source.
`type`           | [`text`]   | The type of the source: `kafka`, `postgres`, or `pubnub`.
`volatility`     | [`text`]   | Whether the source is [volatile](/overview/volatility). Either `volatile`, `nonvolatile`, or `unknown`.

### `mz_tables`

The `mz_tables` table contains a row for each table in the system.

Field            | Type       | Meaning
-----------------|------------|----------
`id`             | [`text`]   | Materialize's unique ID for the table.
`oid`            | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the table.
`schema_id`      | [`bigint`] | The ID of the schema to which the table belongs.
`name`           | [`text`]   | The name of the table.
`persisted_name` | [`text`]   | The name of the table's persisted materialization, or `NULL` if the table is not being persisted.

### `mz_types`

The `mz_types` table contains a row for each type in the system.

Field          | Type       | Meaning
---------------|------------|----------
`id`           | [`text`]   | Materialize's unique ID for the type.
`oid`          | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the type.
`schema_id`    | [`bigint`] | The ID of the schema to which the type belongs.
`name`         | [`text`]   | The name of the type.

### `mz_views`

The `mz_views` table contains a row for each view in the system.

Field          | Type        | Meaning
---------------|-------------|----------
`id`           | [`text`]    | Materialize's unique ID for the view.
`oid`          | [`oid`]     | A [PostgreSQL-compatible OID][oid] for the view.
`schema_id`    | [`bigint`]  | The ID of the schema to which the view belongs.
`name`         | [`text`]    | The name of the view.
`volatility`   | [`text`]    | Whether the view is [volatile](/overview/volatility). Either `volatile`, `nonvolatile`, or `unknown`.
`definition`   | [`text`]    | The view definition (a `SELECT` query).

### `mz_worker_materialization_frontiers`

The `mz_worker_materialization_frontiers` source describes each worker's
frontier for each [dataflow] in the system. The frontier describes the earliest
timestamp at which the output of the dataflow may change; data prior to that
timestamp is sealed.

For frontier information aggregated across all workers, see
[`mz_materialization_frontiers`](#mz_materialization_frontiers).

Field       | Type       | Meaning
------------|------------|--------
`global_id` | [`text`]   | The ID of the index that created the dataflow. Corresponds to [`mz_indexes.id`](#mz_indexes).
`worker`    | [`bigint`] | The ID of the worker thread hosting the dataflow.
`time`      | [`bigint`] | The next timestamp at which the dataflow may change.

## `pg_catalog`

Materialize has compatibility shims for the following relations from [PostgreSQL's
system catalog](https://www.postgresql.org/docs/current/catalogs.html):

  * [`pg_am`](https://www.postgresql.org/docs/current/catalog-pg-am.html)
  * [`pg_attribute`](https://www.postgresql.org/docs/current/catalog-pg-attribute.html)
  * [`pg_class`](https://www.postgresql.org/docs/current/catalog-pg-class.html)
  * [`pg_collation`](https://www.postgresql.org/docs/current/catalog-pg-collation.html)
  * [`pg_constraint`](https://www.postgresql.org/docs/current/catalog-pg-constraint.html)
  * [`pg_database`](https://www.postgresql.org/docs/current/catalog-pg-database.html)
  * [`pg_description`](https://www.postgresql.org/docs/current/catalog-pg-description.html)
  * [`pg_enum`](https://www.postgresql.org/docs/current/catalog-pg-enum.html)
  * [`pg_index`](https://www.postgresql.org/docs/current/catalog-pg-index.html)
  * [`pg_inherits`](https://www.postgresql.org/docs/current/catalog-pg-inherits.html)
  * [`pg_namespace`](https://www.postgresql.org/docs/current/catalog-pg-namespace.html)
  * [`pg_policy`](https://www.postgresql.org/docs/current/catalog-pg-policy.html)
  * [`pg_proc`](https://www.postgresql.org/docs/current/catalog-pg-proc.html)
  * [`pg_range`](https://www.postgresql.org/docs/current/catalog-pg-range.html)
  * [`pg_roles`](https://www.postgresql.org/docs/current/view-pg-roles.html)
  * [`pg_settings`](https://www.postgresql.org/docs/current/view-pg-settings.html)
  * [`pg_tables`](https://www.postgresql.org/docs/current/view-pg-tables.html)
  * [`pg_type`](https://www.postgresql.org/docs/current/catalog-pg-type.html)
  * [`pg_views`](https://www.postgresql.org/docs/current/view-pg-views.html)

These compatibility shims are largely incomplete. Most are lacking some columns
that are present in PostgreSQL, or if they do include the column the result set
its value may always be `NULL`. The precise nature of the incompleteness is
intentionally undocumented. New tools developed against Materialize should use
the documented [`mz_catalog`](#mz_catalog) API instead.

If you are having trouble making a PostgreSQL tool work with Materialize, please
[file a GitHub issue][gh-issue]. Many PostgreSQL tools can be made to work with
Materialize with minor changes to the `pg_catalog` compatibility shim.

## `information_schema`

Materialize has compatibility shims for the following relations from the
SQL standard [`information_schema`](https://www.postgresql.org/docs/current/infoschema-schema.html)
schema, which is automatically available in all databases:

  * [`columns`](https://www.postgresql.org/docs/current/infoschema-columns.html)
  * [`tables`](https://www.postgresql.org/docs/current/infoschema-tables.html)

These compatibility shims are largely incomplete. Most are lacking some columns
that are present in the SQL standard, or if they do include the column the
result set its value may always be `NULL`. The precise nature of the
incompleteness is intentionally undocumented. New tools developed against
Materialize should use the documented [`mz_catalog`](#mz_catalog) API instead.

[`bigint`]: /sql/types/bigint
[`bigint list`]: /sql/types/list
[`boolean`]: /sql/types/boolean
[`bytea`]: /sql/types/bytea
[`double precision`]: /sql/types/double-precision
[`jsonb`]: /sql/types/jsonb
[`numeric`]: /sql/types/numeric
[`oid`]: /sql/types/oid
[`text`]: /sql/types/text
[`timestamp`]: /sql/types/timestamp
[`timestamp with time zone`]: /sql/types/timestamp
[`uuid`]: /sql/types/uuid
[gh-issue]: https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md
[oid]: /sql/types/oid
[`text array`]: /sql/types/array
[arrangement]: /overview/arrangements/#arrangements
[dataflow]: /overview/arrangements/#dataflows
[librdkafka]: https://github.com/edenhill/librdkafka/tree/v{{< librdkafka-version >}}
[`STATISTICS.md`]: https://github.com/edenhill/librdkafka/tree/v{{< librdkafka-version >}}/STATISTICS.md
