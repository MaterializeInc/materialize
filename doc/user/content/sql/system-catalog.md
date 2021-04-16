---
title: "System Catalog"
description: "The system catalog stores metadata about your Materialize instance."
aliases:
  - /sql/system-tables
weight: 14
menu:
  main:
    parent: 'sql'
---

{{< version-added v0.5.0 />}}

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
---------------|------------|----------
`type_id`      | [`text`]   | The ID of the array type.
`element_id`   | [`text`]   | The ID of the array's element type.

### `mz_avro_ocf_sinks`

The `mz_avro_ocf_sinks` table contains a row for each Avro OCF sink in the
system.

Field     | Type     | Meaning
----------|----------|--------
`sink_id` | [`text`] | The ID of the sink.
`path`    | `bytea`  | The path to the Avro OCF file into which the sink is writing.

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

### `mz_databases`

The `mz_databases` table contains a row for each database in the system.

Field  | Type       | Meaning
-------|------------|--------
`id`   | [`bigint`] | Materialize's unique ID for the database.
`oid`  | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the database.
`name` | [`text`]   | The name of the database.

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

### `mz_indexes`

The `mz_indexes` table contains a row for each index in the system.

Field        | Type        | Meaning
-------------|-------------|--------
`id`         | [`text`]    | Materialize's unique ID for the index.
`oid`        | [`oid`]     | A [PostgreSQL-compatible OID][oid] for the index.
`name`       | [`text`]    | The name of the index.
`on_id`      | [`text`]    | The ID of the relation on which the index is built.
`volatility` | [`text`]    | Whether the the index is [volatile](/overview/volatility). Either `volatile`, `nonvolatile`, or `unknown`.

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

### `mz_kafka_consumer_partitions`

The `mz_kafka_consumer_partitions` table contains a row for partition being
read by a Kafka consumer in the system.

Field           | Type       | Meaning
----------------|------------|--------
`consumer_name` | [`text`]   | The handle name for the consumer.
`source_id`     | [`text`]   | The ID of the source that created this consumer object. Corresponds to `mz_source_info.source_id`.
`dataflow_id`   | [`bigint`] | The ID of the dataflow reading from the consumer. Corresponds to `mz_source_info.dataflow_id`.
`partition_id`  | [`text`]   | The ID of the topic partition the consumer is reading from.
`rx_msgs`       | [`bigint`] | The number of messages read by this consumer since materialized startup.
`rx_bytes`      | [`bigint`] | The number of bytes read by this consumer since materialized startup.
`tx_msgs`       | [`bigint`] | The number of messages sent by this consumer since materialized startup.
`tx_bytes`      | [`bigint`] | The number of bytes sent by this consumer since materialized startup.
`lo_offset`     | [`bigint`] | The partition's low watermark offset on the broker.
`hi_offset`     | [`bigint`] | The partition's high watermark offset on the broker.
`ls_offset`     | [`bigint`] | The partition's last stable offset on the broker.
`app_offset`    | [`bigint`] | The offset of the last message passed to materialized + 1.
`consumer_lag`  | [`bigint`] | Difference between the `hi_offset` and `app_offset`.

### `mz_kafka_sinks`

The `mz_kafka_sinks` table contains a row for each Kafka sink in the system.

Field     | Type     | Meaning
----------|----------|--------
`sink_id` | [`text`] | The ID of the sink.
`topic`   | [`text`] | The name of the Kafka topic into which the sink is writing.

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

### `mz_pseudo_types`

The `mz_pseudo_types` table contains a row for each psuedo type in the system.

Field          | Type       | Meaning
---------------|------------|----------
`type_id`      | [`text`]   | The ID of the type.

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

Field          | Type        | Meaning
---------------|-------------|--------
`id`           | [`text`]    | Materialize's unique ID for the sink.
`oid`          | [`oid`]     | A [PostgreSQL-compatible OID][oid] for the sink.
`schema_id`    | [`bigint`]  | The ID of the schema to which the sink belongs.
`name`         | [`text`]    | The name of the sink.
`volatility`   | [`text`]    | Whether the sink is [volatile](/overview/volatility). Either `volatile`, `nonvolatile`, or `unknown`.

### `mz_source_info`

The `mz_source_info` table contains a row for each partition of each source
in the system.

Field          | Type       | Meaning
---------------|------------|----------
`source_name`  | [`text`]   | Materialize's internal name for the source.
`source_id`    | [`text`]   | Materialize's unique ID for the source. Corresponds to `mz_sources.id`.
`dataflow_id`  | [`bigint`] | The ID of the dataflow responsible for processing this source.
`partition_id` | [`text`]   | The ID of the partition within the source. The concept of partition varies by source type. Not all sources types have multiple partitions, in which case there will be only one entry for partition ID `0`.
`offset`       | [`bigint`] | The highest offset processed by this source.
`timestamp`    | [`bigint`] | The largest `mz_timestamp` processed by this source.

### `mz_sources`

The `mz_sources` table contains a row for each source in the system.

Field          | Type       | Meaning
---------------|------------|----------
`id`           | [`text`]   | Materialize's unique ID for the source.
`oid`          | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the source.
`schema_id`    | [`bigint`] | The ID of the schema to which the source belongs.
`name`         | [`text`]   | The name of the source.
`volatility`   | [`text`]   | Whether the source is [volatile](/overview/volatility). Either `volatile`, `nonvolatile`, or `unknown`.

### `mz_tables`

The `mz_tables` table contains a row for each table in the system.

Field          | Type       | Meaning
---------------|------------|----------
`id`           | [`text`]   | Materialize's unique ID for the table.
`oid`          | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the table.
`schema_id`    | [`bigint`] | The ID of the schema to which the table belongs.
`name`         | [`text`]   | The name of the table.

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

## `pg_catalog`

Materialize has compatibility shims for the following tables from [PostgreSQL's
system catalog](https://www.postgresql.org/docs/current/catalogs.html):

  * [`pg_attribute`](https://www.postgresql.org/docs/current/catalog-pg-attribute.html)
  * [`pg_class`](https://www.postgresql.org/docs/current/catalog-pg-class.html)
  * [`pg_database`](https://www.postgresql.org/docs/current/catalog-pg-database.html)
  * [`pg_description`](https://www.postgresql.org/docs/current/catalog-pg-description.html)
  * [`pg_enum`](https://www.postgresql.org/docs/current/catalog-pg-enum.html)
  * [`pg_index`](https://www.postgresql.org/docs/current/catalog-pg-index.html)
  * [`pg_namespace`](https://www.postgresql.org/docs/current/catalog-pg-namespace.html)
  * [`pg_proc`](https://www.postgresql.org/docs/current/catalog-pg-proc.html)
  * [`pg_range`](https://www.postgresql.org/docs/current/catalog-pg-range.html)
  * [`pg_type`](https://www.postgresql.org/docs/current/catalog-pg-type.html)

These compatibility shims are largely incomplete. Most are lacking some columns
that are present in PostgreSQL, or if they do include the column the result set
its value may always be `NULL`. The precise nature of the incompleteness is
intentionally undocumented. New tools developed against Materialize should use
the documented [`mz_catalog`](#mz_catalog) API instead.

If you are having trouble making a PostgreSQL tool work with Materialize, please
[file a GitHub issue][gh-issue]. Many PostgreSQL tools can be made to work with
Materialize with minor changes to the `pg_catalog` compatibility shim.

[`bigint`]: /sql/types/bigint
[`boolean`]: /sql/types/boolean
[`oid`]: /sql/types/oid
[`text`]: /sql/types/text
[gh-issue]: https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md
[oid]: /sql/types/oid
[`text array`]: /sql/types/array
