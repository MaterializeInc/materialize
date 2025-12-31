---
audience: developer
canonical_url: https://materialize.com/docs/sql/system-catalog/
complexity: advanced
description: The system catalog stores metadata about your Materialize instance.
doc_type: reference
keywords:
- System catalog
- '`processes`'
- EXPLAIN PHYSICAL
- 'Warning:'
- ALTER ROLE
- '`size`'
- CREATE AN
- '`workers`'
- ALTER STATEMENT
- DROP EVENTS
product_area: Indexes
status: beta
title: System catalog
---

# System catalog

## Purpose
The system catalog stores metadata about your Materialize instance.

If you need to understand the syntax and options for this command, you're in the right place.


The system catalog stores metadata about your Materialize instance.


Materialize exposes a system catalog that contains metadata about the running
Materialize instance.

The system catalog consists of several schemas that are implicitly available in
all databases. These schemas contain sources, tables, and views that expose
different types of metadata.

  * [`mz_catalog`](mz_catalog), which exposes metadata in Materialize's
    native format.

  * [`pg_catalog`](pg_catalog), which presents the data in `mz_catalog` in
    the format used by PostgreSQL.

  * [`information_schema`](information_schema), which presents the data in
    `mz_catalog` in the format used by the SQL standard's information_schema.

  * [`mz_internal`](mz_internal), which exposes internal metadata about
    Materialize in an unstable format that is likely to change.

  * [`mz_introspection`](mz_introspection), which contains replica
    introspection relations.

These schemas contain sources, tables, and views that expose metadata like:

  * Descriptions of each database, schema, source, table, view, sink, and
    index in the system.

  * Descriptions of all running dataflows.

  * Metrics about dataflow execution.

Whenever possible, applications should prefer to query `mz_catalog` over
`pg_catalog`. The mapping between Materialize concepts and PostgreSQL concepts
is not one-to-one, and so the data in `pg_catalog` cannot accurately represent
the particulars of Materialize.


---

## information_schema


Materialize has compatibility shims for the following relations from the
SQL standard [`information_schema`](https://www.postgresql.org/docs/current/infoschema-schema.html)
schema, which is automatically available in all databases:

  * [`applicable_roles`](https://www.postgresql.org/docs/current/infoschema-applicable-roles.html)
  * [`character_sets`](https://www.postgresql.org/docs/current/infoschema-character-sets.html)
  * [`columns`](https://www.postgresql.org/docs/current/infoschema-columns.html)
  * [`enabled_roles`](https://www.postgresql.org/docs/current/infoschema-enabled-roles.html)
  * [`key_column_usage`](https://www.postgresql.org/docs/current/infoschema-key-column-usage.html)
  * [`referential_constraints`](https://www.postgresql.org/docs/current/infoschema-referential-constraints.html)
  * [`role_table_grants`](https://www.postgresql.org/docs/current/infoschema-role-table-grants.html)
  * [`routines`](https://www.postgresql.org/docs/current/infoschema-routines.html)
  * [`schemata`](https://www.postgresql.org/docs/current/infoschema-schemata.html)
  * [`tables`](https://www.postgresql.org/docs/current/infoschema-tables.html)
  * [`table_constraints`](https://www.postgresql.org/docs/current/infoschema-table-constraints.html)
  * [`table_privileges`](https://www.postgresql.org/docs/current/infoschema-table-privileges.html)
  * [`triggers`](https://www.postgresql.org/docs/current/infoschema-triggers.html)
  * [`views`](https://www.postgresql.org/docs/current/infoschema-views.html)

These compatibility shims are largely incomplete. Most are lacking some columns
that are present in the SQL standard, or if they do include the column the
result set its value may always be `NULL`. The precise nature of the
incompleteness is intentionally undocumented. New tools developed against
Materialize should use the documented [`mz_catalog`](../mz_catalog) API instead.


---

## mz_catalog


The following sections describe the available relations in the `mz_catalog`
schema. These relations contain metadata about objects in Materialize,
including descriptions of each database, schema, source, table, view, sink, and
index in the system.

> **Warning:** 
Views that directly reference these objects cannot include `NATURAL JOIN` or
`*` expressions. Instead, project the required columns and convert all `NATURAL JOIN`s
to `USING` joins.


### `mz_array_types`

The `mz_array_types` table contains a row for each array type in the system.

<!-- RELATION_SPEC mz_catalog.mz_array_types -->
Field          | Type       | Meaning
---------------|------------|--------
`id`           | [`text`]   | The ID of the array type.
`element_id`   | [`text`]   | The ID of the array's element type.

### `mz_audit_events`

The `mz_audit_events` table records create, alter, and drop events for the
other objects in the system catalog.

<!-- RELATION_SPEC mz_catalog.mz_audit_events -->
Field           | Type                         | Meaning
----------------|------------------------------|--------
`id  `          | [`uint8`]                    | Materialize's unique, monotonically increasing ID for the event.
`event_type`    | [`text`]                     | The type of the event: `create`, `drop`, or `alter`.
`object_type`   | [`text`]                     | The type of the affected object: `cluster`, `cluster-replica`, `connection`, `database`, `function`, `index`, `materialized-view`, `role`, `schema`, `secret`, `sink`, `source`, `table`, `type`, or `view`.
`details`       | [`jsonb`]                    | Additional details about the event. The shape of the details varies based on `event_type` and `object_type`.
`user`          | [`text`]                     | The user who triggered the event, or `NULL` if triggered by the system.
`occurred_at`   | [`timestamp with time zone`] | The time at which the event occurred. Guaranteed to be in order of event creation. Events created in the same transaction will have identical values.

### `mz_aws_privatelink_connections`

The `mz_aws_privatelink_connections` table contains a row for each AWS
PrivateLink connection in the system.

<!-- RELATION_SPEC mz_catalog.mz_aws_privatelink_connections -->
Field       | Type      | Meaning
------------|-----------|--------
`id`        | [`text`]  | The ID of the connection.
`principal` | [`text`]  | The AWS Principal that Materialize will use to connect to the VPC endpoint.

### `mz_base_types`

The `mz_base_types` table contains a row for each base type in the system.

<!-- RELATION_SPEC mz_catalog.mz_base_types -->
Field          | Type       | Meaning
---------------|------------|----------
`id`           | [`text`]   | The ID of the type.

### `mz_cluster_replica_frontiers`


The `mz_cluster_replica_frontiers` table describes the per-replica frontiers of
sources, sinks, materialized views, indexes, and subscriptions in the system,
as observed from the coordinator.

[`mz_compute_frontiers`](../mz_introspection/#mz_compute_frontiers) is similar to
`mz_cluster_replica_frontiers`, but `mz_compute_frontiers` reports the
frontiers known to the active compute replica, while
`mz_cluster_replica_frontiers` reports the frontiers of all replicas. Note also
that `mz_compute_frontiers` is restricted to compute objects (indexes,
materialized views, and subscriptions) while `mz_cluster_replica_frontiers`
contains storage objects that are installed on replicas (sources, sinks) as
well.

At this time, we do not make any guarantees about the freshness of these numbers.

<!-- RELATION_SPEC mz_catalog.mz_cluster_replica_frontiers -->
| Field            | Type             | Meaning                                                                |
| -----------------| ---------------- | --------                                                               |
| `object_id`      | [`text`]         | The ID of the source, sink, index, materialized view, or subscription. |
| `replica_id`     | [`text`]         | The ID of a cluster replica.                                           |
| `write_frontier` | [`mz_timestamp`] | The next timestamp at which the output may change.                     |

### `mz_cluster_replica_sizes`

The `mz_cluster_replica_sizes` table contains a mapping of logical sizes
(e.g. `100cc`) to physical sizes (number of processes, and CPU and memory allocations per process).

This table was previously in the `mz_internal` schema. All queries previously referencing
`mz_internal.mz_cluster_replica_sizes` should now reference `mz_catalog.mz_cluster_replica_sizes`.

> **Warning:** 
The values in this table may change at any time. You should not rely on them for
any kind of capacity planning.


<!-- RELATION_SPEC mz_catalog.mz_cluster_replica_sizes -->
- **`size`**: [`text`] | The human-readable replica size.
- **`processes`**: [`uint8`] | The number of processes in the replica.
- **`workers`**: [`uint8`] | The number of Timely Dataflow workers per process.
- **`cpu_nano_cores`**: [`uint8`] | The CPU allocation per process, in billionths of a vCPU core.
- **`memory_bytes`**: [`uint8`] | The RAM allocation per process, in billionths of a vCPU core.
- **`disk_bytes`**: [`uint8`] | The disk allocation per process.
- **`credits_per_hour`**: [`numeric`] | The number of compute credits consumed per hour.

### `mz_cluster_replicas`

The `mz_cluster_replicas` table contains a row for each cluster replica in the system.

<!-- RELATION_SPEC mz_catalog.mz_cluster_replicas -->
Field               | Type      | Meaning
--------------------|-----------|--------
`id`                | [`text`]  | Materialize's unique ID for the cluster replica.
`name`              | [`text`]  | The name of the cluster replica.
`cluster_id`        | [`text`]  | The ID of the cluster to which the replica belongs. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters).
`size`              | [`text`]  | The cluster replica's size, selected during creation.
`availability_zone` | [`text`]  | The availability zone in which the cluster is running.
`owner_id`          | [`text`]  | The role ID of the owner of the cluster replica. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`disk`              | [`boolean`] | If the replica has a local disk.

### `mz_clusters`

The `mz_clusters` table contains a row for each cluster in the system.

<!-- RELATION_SPEC mz_catalog.mz_clusters -->
- **`id`**: [`text`] | Materialize's unique ID for the cluster.
- **`name`**: [`text`] | The name of the cluster.
- **`owner_id`**: [`text`] | The role ID of the owner of the cluster. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
- **`privileges`**: [`mz_aclitem array`] | The privileges belonging to the cluster.
- **`managed`**: [`boolean`] | Whether the cluster is a [managed cluster](/sql/create-cluster/) with automatically managed replicas.
- **`size`**: [`text`] | If the cluster is managed, the desired size of the cluster's replicas. `NULL` for unmanaged clusters.
- **`replication_factor`**: [`uint4`] | If the cluster is managed, the desired number of replicas of the cluster. `NULL` for unmanaged clusters.
- **`disk`**: [`boolean`] | **Unstable** If the cluster is managed, `true` if the replicas have the `DISK` option . `NULL` for unmanaged clusters.
- **`availability_zones`**: [`text list`] | **Unstable** If the cluster is managed, the list of availability zones specified in `AVAILABILITY ZONES`. `NULL` for unmanaged clusters.
- **`introspection_debugging`**: [`boolean`] | Whether introspection of the gathering of the introspection data is enabled.
- **`introspection_interval`**: [`interval`] | The interval at which to collect introspection data.

### `mz_columns`

The `mz_columns` contains a row for each column in each table, source, and view
in the system.

<!-- RELATION_SPEC mz_catalog.mz_columns -->
Field            | Type        | Meaning
-----------------|-------------|--------
`id`             | [`text`]    | The unique ID of the table, source, or view containing the column.
`name`           | [`text`]    | The name of the column.
`position`       | [`uint8`]   | The 1-indexed position of the column in its containing table, source, or view.
`nullable`       | [`boolean`] | Can the column contain a `NULL` value?
`type`           | [`text`]    | The data type of the column.
`default`        | [`text`]    | The default expression of the column.
`type_oid`       | [`oid`]     | The OID of the type of the column (references `mz_types`).
`type_mod`       | [`integer`] | The packed type identifier of the column.

### `mz_connections`

The `mz_connections` table contains a row for each connection in the system.

<!-- RELATION_SPEC mz_catalog.mz_connections -->
Field        | Type                 | Meaning
-------------|----------------------|--------
`id`         | [`text`]             | The unique ID of the connection.
`oid`        | [`oid`]              | A [PostgreSQL-compatible OID][`oid`] for the connection.
`schema_id`  | [`text`]             | The ID of the schema to which the connection belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`       | [`text`]             | The name of the connection.
`type`       | [`text`]             | The type of the connection: `confluent-schema-registry`, `kafka`, `postgres`, or `ssh-tunnel`.
`owner_id`   | [`text`]             | The role ID of the owner of the connection. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges` | [`mz_aclitem array`] | The privileges belonging to the connection.
`create_sql` | [`text`]             | The `CREATE` SQL statement for the connection.
`redacted_create_sql` | [`text`]    | The redacted `CREATE` SQL statement for the connection.

### `mz_databases`

The `mz_databases` table contains a row for each database in the system.

<!-- RELATION_SPEC mz_catalog.mz_databases -->
Field       | Type                 | Meaning
------------|----------------------|--------
`id`        | [`text`]             | Materialize's unique ID for the database.
`oid`       | [`oid`]              | A [PostgreSQL-compatible OID][`oid`] for the database.
`name`      | [`text`]             | The name of the database.
`owner_id`  | [`text`]             | The role ID of the owner of the database. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`| [`mz_aclitem array`] | The privileges belonging to the database.

### `mz_default_privileges`

The `mz_default_privileges` table contains information on default privileges
that will be applied to new objects when they are created.

<!-- RELATION_SPEC mz_catalog.mz_default_privileges -->
Field         | Type     | Meaning
--------------|----------|--------
`role_id`     | [`text`] | Privileges described in this row will be granted on objects created by `role_id`. The role ID `p` stands for the `PUBLIC` pseudo-role and applies to all roles.
`database_id` | [`text`] | Privileges described in this row will be granted only on objects in the database identified by `database_id` if non-null.
`schema_id`   | [`text`] | Privileges described in this row will be granted only on objects in the schema identified by `schema_id` if non-null.
`object_type` | [`text`] | Privileges described in this row will be granted only on objects of type `object_type`.
`grantee`     | [`text`] | Privileges described in this row will be granted to `grantee`. The role ID `p` stands for the `PUBLIC` pseudo-role and applies to all roles.
`privileges`  | [`text`] | The set of privileges that will be granted.

### `mz_egress_ips`

The `mz_egress_ips` table contains a row for each potential IP address that the
system may connect to external systems from.

<!-- RELATION_SPEC mz_catalog.mz_egress_ips -->
Field           | Type        | Meaning
----------------|-------------|--------
`egress_ip`     | [`text`]    | The start of the range of IP addresses.
`prefix_length` | [`integer`] | The number of leading bits in the CIDR netmask.
`cidr`          | [`text`]    | The CIDR representation.

### `mz_functions`

The `mz_functions` table contains a row for each function in the system.

<!-- RELATION_SPEC mz_catalog.mz_functions -->
Field                       | Type           | Meaning
----------------------------|----------------|--------
`id`                        | [`text`]       | Materialize's unique ID for the function.
`oid`                       | [`oid`]        | A [PostgreSQL-compatible OID][`oid`] for the function.
`schema_id`                 | [`text`]       | The ID of the schema to which the function belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`                      | [`text`]       | The name of the function.
`argument_type_ids`         | [`text array`] | The ID of each argument's type. Each entry refers to `mz_types.id`.
`variadic_argument_type_id` | [`text`]       | The ID of the variadic argument's type, or `NULL` if the function does not have a variadic argument. Refers to `mz_types.id`.
`return_type_id`            | [`text`]       | The returned value's type, or `NULL` if the function does not return a value. Refers to `mz_types.id`. Note that for table functions with > 1 column, this type corresponds to [`record`].
`returns_set`               | [`boolean`]    | Whether the function returns a set, i.e. the function is a table function.
`owner_id`                  | [`text`]       | The role ID of the owner of the function. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).

### `mz_indexes`

The `mz_indexes` table contains a row for each index in the system.

<!-- RELATION_SPEC mz_catalog.mz_indexes -->
Field        | Type        | Meaning
-------------|-------------|--------
`id`         | [`text`]    | Materialize's unique ID for the index.
`oid`        | [`oid`]     | A [PostgreSQL-compatible OID][`oid`] for the index.
`name`       | [`text`]    | The name of the index.
`on_id`      | [`text`]    | The ID of the relation on which the index is built.
`cluster_id` | [`text`]    | The ID of the cluster in which the index is built.
`owner_id`   | [`text`]    | The role ID of the owner of the index. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`create_sql` | [`text`]    | The `CREATE` SQL statement for the index.
`redacted_create_sql` | [`text`] | The redacted `CREATE` SQL statement for the index.

### `mz_index_columns`

The `mz_index_columns` table contains a row for each column in each index in the
system. For example, an index on `(a, b + 1)` would have two rows in this
table, one for each of the two columns in the index.

For a given row, if `field_number` is null then `expression` will be nonnull, or
vice-versa.

<!-- RELATION_SPEC mz_catalog.mz_index_columns -->
Field            | Type        | Meaning
-----------------|-------------|--------
`index_id`       | [`text`]    | The ID of the index which contains this column. Corresponds to [`mz_indexes.id`](/sql/system-catalog/mz_catalog/#mz_indexes).
`index_position` | [`uint8`]   | The 1-indexed position of this column within the index. (The order of columns in an index does not necessarily match the order of columns in the relation on which the index is built.)
`on_position`    | [`uint8`]   | If not `NULL`, specifies the 1-indexed position of a column in the relation on which this index is built that determines the value of this index column.
`on_expression`  | [`text`]    | If not `NULL`, specifies a SQL expression that is evaluated to compute the value of this index column. The expression may contain references to any of the columns of the relation.
`nullable`       | [`boolean`] | Can this column of the index evaluate to `NULL`?

### `mz_kafka_connections`

The `mz_kafka_connections` table contains a row for each Kafka connection in the
system.

<!-- RELATION_SPEC mz_catalog.mz_kafka_connections -->
Field                 | Type           | Meaning
----------------------|----------------|--------
`id`                  | [`text`]       | The ID of the connection.
`brokers`             | [`text array`] | The addresses of the Kafka brokers to connect to.
`sink_progress_topic` | [`text`]       | The name of the Kafka topic where any sinks associated with this connection will track their progress information and other metadata. The contents of this topic are unspecified.

### `mz_kafka_sinks`

The `mz_kafka_sinks` table contains a row for each Kafka sink in the system.

<!-- RELATION_SPEC mz_catalog.mz_kafka_sinks -->
Field                | Type     | Meaning
---------------------|----------|--------
`id`                 | [`text`] | The ID of the sink.
`topic`              | [`text`] | The name of the Kafka topic into which the sink is writing.

### `mz_kafka_sources`


The `mz_kafka_sources` table contains a row for each Kafka source in the system.

This table was previously in the `mz_internal` schema. All queries previously referencing
`mz_internal.mz_kafka_sources` should now reference `mz_catalog.mz_kafka_sources`.

<!-- RELATION_SPEC mz_catalog.mz_kafka_sources -->
| Field                  | Type           | Meaning                                                                                                   |
|------------------------|----------------|-----------------------------------------------------------------------------------------------------------|
| `id`                   | [`text`]       | The ID of the Kafka source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).        |
| `group_id_prefix`      | [`text`]       | The value of the `GROUP ID PREFIX` connection option.                                                     |
| `topic          `      | [`text`]       | The name of the Kafka topic the source is reading from.                                                              |


### `mz_iceberg_sinks`

The `mz_iceberg_sinks` table contains a row for each Iceberg sink in the system.

<!-- RELATION_SPEC mz_catalog.mz_iceberg_sinks -->
| Field          | Type     | Meaning                                                                                                   |
|----------------|----------|-----------------------------------------------------------------------------------------------------------|
| `id`           | [`text`] | The ID of the sink.                                                                                       |
| `namespace`     | [`text`] | The namespace of the Iceberg table into which the sink is writing.                                       |
| `table`     | [`text`] | The Iceberg table into which the sink is writing.                                             |


### `mz_list_types`

The `mz_list_types` table contains a row for each list type in the system.

<!-- RELATION_SPEC mz_catalog.mz_list_types -->
Field        | Type     | Meaning
-------------|----------|--------
`id`         | [`text`] | The ID of the list type.
`element_id` | [`text`] | The IID of the list's element type.
`element_modifiers` | [`uint8 list`] | The element type modifiers, or `NULL` if none.

### `mz_map_types`

The `mz_map_types` table contains a row for each map type in the system.

<!-- RELATION_SPEC mz_catalog.mz_map_types -->
Field          | Type       | Meaning
---------------|------------|----------
`id`           | [`text`]   | The ID of the map type.
`key_id `      | [`text`]   | The ID of the map's key type.
`value_id`     | [`text`]   | The ID of the map's value type.
`key_modifiers` | [`uint8 list`] | The key type modifiers, or `NULL` if none.
`value_modifiers` | [`uint8 list`] | The value type modifiers, or `NULL` if none.

### `mz_materialized_views`

The `mz_materialized_views` table contains a row for each materialized view in
the system.

<!-- RELATION_SPEC mz_catalog.mz_materialized_views -->
Field          | Type                 | Meaning
---------------|----------------------|----------
`id`           | [`text`]             | Materialize's unique ID for the materialized view.
`oid`          | [`oid`]              | A [PostgreSQL-compatible OID][`oid`] for the materialized view.
`schema_id`    | [`text`]             | The ID of the schema to which the materialized view belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`         | [`text`]             | The name of the materialized view.
`cluster_id`   | [`text`]             | The ID of the cluster maintaining the materialized view. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters).
`definition`   | [`text`]             | The materialized view definition (a `SELECT` query).
`owner_id`     | [`text`]             | The role ID of the owner of the materialized view. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`   | [`mz_aclitem array`] | The privileges belonging to the materialized view.
`create_sql`   | [`text`]             | The `CREATE` SQL statement for the materialized view.
`redacted_create_sql` | [`text`]      | The redacted `CREATE` SQL statement for the materialized view.

### `mz_objects`

The `mz_objects` view contains a row for each table, source, view, materialized
view, sink, index, connection, secret, type, and function in the system.

IDs for all objects represented in `mz_objects` share a namespace. If there is a
view with ID `u1`, there will never be a table, source, view, materialized view,
sink, index, connection, secret, type, or function with ID `u1`.

<!-- RELATION_SPEC mz_catalog.mz_objects -->
Field       | Type                 | Meaning
------------|----------------------|--------
`id`        | [`text`]             | Materialize's unique ID for the object.
`oid`       | [`oid`]              | A [PostgreSQL-compatible OID][`oid`] for the object.
`schema_id` | [`text`]             | The ID of the schema to which the object belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`      | [`text`]             | The name of the object.
`type`      | [`text`]             | The type of the object: one of `table`, `source`, `view`, `materialized-view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.
`owner_id`  | [`text`]             | The role ID of the owner of the object. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`cluster_id`| [`text`]             | The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters). `NULL` for other object types.
`privileges`| [`mz_aclitem array`] | The privileges belonging to the object.

### `mz_pseudo_types`

The `mz_pseudo_types` table contains a row for each pseudo type in the system.

<!-- RELATION_SPEC mz_catalog.mz_pseudo_types -->
Field          | Type       | Meaning
---------------|------------|----------
`id`           | [`text`]   | The ID of the type.

### `mz_relations`

The `mz_relations` view contains a row for each table, source, view, and
materialized view in the system.

<!-- RELATION_SPEC mz_catalog.mz_relations -->
Field       | Type                 | Meaning
------------|----------------------|--------
`id`        | [`text`]             | Materialize's unique ID for the relation.
`oid`       | [`oid`]              | A [PostgreSQL-compatible OID][`oid`] for the relation.
`schema_id` | [`text`]             | The ID of the schema to which the relation belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`      | [`text`]             | The name of the relation.
`type`      | [`text`]             | The type of the relation: either `table`, `source`, `view`, or `materialized view`.
`owner_id`  | [`text`]             | The role ID of the owner of the relation. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`cluster_id`| [`text`]             | The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters). `NULL` for other object types.
`privileges`| [`mz_aclitem array`] | The privileges belonging to the relation.

### `mz_recent_storage_usage`


The `mz_recent_storage_usage` table describes the storage utilization of each
table, source, and materialized view in the system in the most recent storage
utilization assessment. Storage utilization assessments occur approximately
every hour.

See [`mz_storage_usage`](../mz_catalog#mz_storage_usage) for historical storage
usage information.

<!-- RELATION_SPEC mz_catalog.mz_recent_storage_usage -->
Field                  | Type                         | Meaning
---------------------- | ---------------------------- | -----------------------------------------------------------
`object_id`            | [`text`]                     | The ID of the table, source, or materialized view.
`size_bytes`           | [`uint8`]                    | The number of storage bytes used by the object in the most recent assessment.


### `mz_roles`

The `mz_roles` table contains a row for each role in the system.

<!-- RELATION_SPEC mz_catalog.mz_roles -->
Field            | Type       | Meaning
-----------------|------------|--------
`id`             | [`text`]   | Materialize's unique ID for the role.
`oid`            | [`oid`]    | A [PostgreSQL-compatible OID][`oid`] for the role.
`name`           | [`text`]   | The name of the role.
`inherit`        | [`boolean`]   | Indicates whether the role has inheritance of privileges.
`rolcanlogin`    | [`boolean`]   | Indicates whether the role can log in.
`rolsuper`       | [`boolean`]   | Indicates whether the role is a superuser.

### `mz_role_members`

The `mz_role_members` table contains a row for each role membership in the
system.

<!-- RELATION_SPEC mz_catalog.mz_role_members -->
Field     | Type       | Meaning
----------|------------|--------
`role_id` | [`text`]   | The ID of the role the `member` is a member of. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`member`  | [`text`]   | The ID of the role that is a member of `role_id`. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`grantor` | [`text`]   | The ID of the role that granted membership of `member` to `role_id`. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).

### `mz_role_auth`
<!-- RELATION_SPEC mz_catalog.mz_role_auth -->

Field       | Type       | Meaning
------------|------------|--------
`role_id`   | [`text`]   | The ID of the role. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`role_oid`  | [`oid`]    | A [PostgreSQL-compatible OID][`oid`] for the role.
`password_hash` | [`text`]   | The hashed password for the role, if any. Uses the `SCRAM-SHA-256` algorithm.
`updated_at` | [`timestamp with time zone`] | The time at which the password was last updated.

### `mz_role_parameters`

The `mz_role_parameters` table contains a row for each configuration parameter
whose default value has been altered for a given role. See [`ALTER ROLE ... SET`](/sql/alter-role/#syntax)
on setting default configuration parameter values per role.

<!-- RELATION_SPEC mz_catalog.mz_role_parameters -->
Field     | Type       | Meaning
----------|------------|--------
`role_id` | [`text`]   | The ID of the role whose configuration parameter default is set. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`parameter_name`  | [`text`]   | The configuration parameter name. One of the supported [configuration parameters](/sql/set/#key-configuration-parameters).
`parameter_value` | [`text`]   | The default value of the parameter for the given role. Can be either a single value, or a comma-separated list of values for configuration parameters that accept a list.

### `mz_schemas`

The `mz_schemas` table contains a row for each schema in the system.

<!-- RELATION_SPEC mz_catalog.mz_schemas -->
Field         | Type                 | Meaning
--------------|----------------------|--------
`id`          | [`text`]             | Materialize's unique ID for the schema.
`oid`         | [`oid`]              | A [PostgreSQL-compatible oid][`oid`] for the schema.
`database_id` | [`text`]             | The ID of the database containing the schema. Corresponds to [`mz_databases.id`](/sql/system-catalog/mz_catalog/#mz_databases).
`name`        | [`text`]             | The name of the schema.
`owner_id`    | [`text`]             | The role ID of the owner of the schema. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`  | [`mz_aclitem array`] | The privileges belonging to the schema.

### `mz_secrets`

The `mz_secrets` table contains a row for each connection in the system.

<!-- RELATION_SPEC mz_catalog.mz_secrets -->
Field            | Type                 | Meaning
-----------------|----------------------|--------
`id`             | [`text`]             | The unique ID of the secret.
`oid`            | [`oid`]              | A [PostgreSQL-compatible oid][`oid`] for the secret.
`schema_id`      | [`text`]             | The ID of the schema to which the secret belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`           | [`text`]             | The name of the secret.
`owner_id`       | [`text`]             | The role ID of the owner of the secret. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`     | [`mz_aclitem array`] | The privileges belonging to the secret.

### `mz_ssh_tunnel_connections`

The `mz_ssh_tunnel_connections` table contains a row for each SSH tunnel
connection in the system.

<!-- RELATION_SPEC mz_catalog.mz_ssh_tunnel_connections -->
Field                 | Type           | Meaning
----------------------|----------------|--------
`id`                  | [`text`]       | The ID of the connection.
`public_key_1`        | [`text`]       | The first public key associated with the SSH tunnel.
`public_key_2`        | [`text`]       | The second public key associated with the SSH tunnel.

### `mz_sinks`

The `mz_sinks` table contains a row for each sink in the system.

<!-- RELATION_SPEC mz_catalog.mz_sinks -->
Field            | Type     | Meaning
-----------------|----------|--------
`id`             | [`text`] | Materialize's unique ID for the sink.
`oid`            | [`oid`]  | A [PostgreSQL-compatible OID][`oid`] for the sink.
`schema_id`      | [`text`] | The ID of the schema to which the sink belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`           | [`text`] | The name of the sink.
`type`           | [`text`] | The type of the sink: `kafka`.
`connection_id`  | [`text`] | The ID of the connection associated with the sink, if any. Corresponds to [`mz_connections.id`](/sql/system-catalog/mz_catalog/#mz_connections).
`size`           | [`text`] | The size of the sink.
`envelope_type`  | [`text`] | The [envelope](/sql/create-sink/kafka/#envelopes) of the sink: `upsert`, or `debezium`.
`format`         | [`text`] | *Deprecated* The [format](/sql/create-sink/kafka/#formats) of the Kafka messages produced by the sink: `avro`, `json`, `text`, or `bytes`.
`key_format`     | [`text`] | The [format](/sql/create-sink/kafka/#formats) of the Kafka message key for messages produced by the sink: `avro`, `json`, `bytes`, `text`, or `NULL`.
`value_format`   | [`text`] | The [format](/sql/create-sink/kafka/#formats) of the Kafka message value for messages produced by the sink: `avro`, `json`, `text`, or `bytes`.
`cluster_id`     | [`text`] | The ID of the cluster maintaining the sink. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters).
`owner_id`       | [`text`] | The role ID of the owner of the sink. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`create_sql`     | [`text`] | The `CREATE` SQL statement for the sink.
`redacted_create_sql` | [`text`] | The redacted `CREATE` SQL statement for the sink.

### `mz_sources`

The `mz_sources` table contains a row for each source in the system.

<!-- RELATION_SPEC mz_catalog.mz_sources -->
Field            | Type                 | Meaning
-----------------|----------------------|----------
`id`             | [`text`]             | Materialize's unique ID for the source.
`oid`            | [`oid`]              | A [PostgreSQL-compatible OID][`oid`] for the source.
`schema_id`      | [`text`]             | The ID of the schema to which the source belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`           | [`text`]             | The name of the source.
`type`           | [`text`]             | The type of the source: `kafka`, `mysql`, `postgres`, `load-generator`, `progress`, or `subsource`.
`connection_id`  | [`text`]             | The ID of the connection associated with the source, if any. Corresponds to [`mz_connections.id`](/sql/system-catalog/mz_catalog/#mz_connections).
`size`           | [`text`]             | *Deprecated* The [size](/sql/create-source/#sizing-a-source) of the source.
`envelope_type`  | [`text`]             | For Kafka sources, the [envelope](/sql/create-source/#envelopes) type: `none`, `upsert`, or `debezium`. `NULL` for other source types.
`key_format`     | [`text`]             | For Kafka sources, the [format](/sql/create-source/#formats) of the Kafka message key: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`, or `NULL`.
`value_format`     | [`text`]           | For Kafka sources, the [format](/sql/create-source/#formats) of the Kafka message value: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`. `NULL` for other source types.
`cluster_id`     | [`text`]             | The ID of the cluster maintaining the source. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters).
`owner_id`       | [`text`]             | The role ID of the owner of the source. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`     | [`mz_aclitem array`] | The privileges granted on the source.
`create_sql`     | [`text`]             | The `CREATE` SQL statement for the source.
`redacted_create_sql` | [`text`]        | The redacted `CREATE` SQL statement for the source.

### `mz_storage_usage`

> **Warning:** 
This view is not indexed in the `mz_catalog_server` cluster. Querying this view
can be slow due to the amount of unindexed data that must be scanned.


The `mz_storage_usage` table describes the historical storage utilization of
each table, source, and materialized view in the system. Storage utilization is
assessed approximately every hour.


Consider querying
[`mz_catalog.mz_recent_storage_usage`](#mz_recent_storage_usage)
instead if you are interested in only the most recent storage usage information.


<!-- RELATION_SPEC mz_catalog.mz_storage_usage -->
Field                  | Type                         | Meaning
---------------------- | ---------------------------- | -----------------------------------------------------------
`object_id`            | [`text`]                     | The ID of the table, source, or materialized view.
`size_bytes`           | [`uint8`]                    | The number of storage bytes used by the object.
`collection_timestamp` | [`timestamp with time zone`] | The time at which storage usage of the object was assessed.

### `mz_system_privileges`

The `mz_system_privileges` table contains information on system privileges.

<!-- RELATION_SPEC mz_catalog.mz_system_privileges -->
Field         | Type     | Meaning
--------------|----------|--------
`privileges` | [`mz_aclitem`] | The privileges belonging to the system.

### `mz_tables`

The `mz_tables` table contains a row for each table in the system.

<!-- RELATION_SPEC mz_catalog.mz_tables -->
Field        | Type                 | Meaning
-------------|----------------------|----------
`id`         | [`text`]             | Materialize's unique ID for the table.
`oid`        | [`oid`]              | A [PostgreSQL-compatible OID][`oid`] for the table.
`schema_id`  | [`text`]             | The ID of the schema to which the table belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`       | [`text`]             | The name of the table.
`owner_id`   | [`text`]             | The role ID of the owner of the table. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges` | [`mz_aclitem array`] | The privileges belonging to the table.
`create_sql` | [`text`]             | The `CREATE` SQL statement for the table.
`redacted_create_sql` | [`text`]    | The redacted `CREATE` SQL statement for the table.
`source_id`  | [`text`]             | The ID of the source associated with the table, if any. Corresponds to [`mz_sources.id`](/sql/system-catalog/mz_catalog/#mz_sources).

### `mz_timezone_abbreviations`

The `mz_timezone_abbreviations` view contains a row for each supported timezone
abbreviation. A "fixed" abbreviation does not change its offset or daylight
status based on the current time. A non-"fixed" abbreviation is dependent on
the current time for its offset, and must use the [`timezone_offset`](/sql/functions/#timezone_offset)
function to find its properties. These correspond to the
`pg_catalog.pg_timezone_abbrevs` table, but can be materialized as they do not
depend on the current time.

<!-- RELATION_SPEC mz_catalog.mz_timezone_abbreviations -->
Field           | Type         | Meaning
----------------|--------------|----------
`abbreviation`  | [`text`]     | The timezone abbreviation.
`utc_offset`    | [`interval`] | The UTC offset of the timezone or `NULL` if fixed.
`dst`           | [`boolean`]  | Whether the timezone is in daylight savings or `NULL` if fixed.
`timezone_name` | [`text`]     | The full name of the non-fixed timezone or `NULL` if not fixed.

### `mz_timezone_names`

The `mz_timezone_names` view contains a row for each supported timezone. Use
the [`timezone_offset`](/sql/functions/#timezone_offset) function for
properties of a timezone at a certain timestamp. These correspond to the
`pg_catalog.pg_timezone_names` table, but can be materialized as they do not
depend on the current time.

<!-- RELATION_SPEC mz_catalog.mz_timezone_names -->
Field        | Type                 | Meaning
-------------|----------------------|----------
`name`       | [`text`]             | The timezone name.

### `mz_types`

The `mz_types` table contains a row for each type in the system.

<!-- RELATION_SPEC mz_catalog.mz_types -->
Field          | Type                 | Meaning
---------------|----------------------|----------
`id`           | [`text`]             | Materialize's unique ID for the type.
`oid`          | [`oid`]              | A [PostgreSQL-compatible OID][`oid`] for the type.
`schema_id`    | [`text`]             | The ID of the schema to which the type belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`         | [`text`]             | The name of the type.
`category`     | [`text`]             | The category of the type.
`owner_id`     | [`text`]             | The role ID of the owner of the type. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`   | [`mz_aclitem array`] | The privileges belonging to the type.
`create_sql`   | [`text`]             | The `CREATE` SQL statement for the type.
`redacted_create_sql` | [`text`]      | The redacted `CREATE` SQL statement for the type.

### `mz_views`

The `mz_views` table contains a row for each view in the system.

<!-- RELATION_SPEC mz_catalog.mz_views -->
Field          | Type                 | Meaning
---------------|----------------------|----------
`id`           | [`text`]             | Materialize's unique ID for the view.
`oid`          | [`oid`]              | A [PostgreSQL-compatible OID][`oid`] for the view.
`schema_id`    | [`text`]             | The ID of the schema to which the view belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`         | [`text`]             | The name of the view.
`definition`   | [`text`]             | The view definition (a `SELECT` query).
`owner_id`     | [`text`]             | The role ID of the owner of the view. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`   | [`mz_aclitem array`] | The privileges belonging to the view.
`create_sql`    | [`text`]            | The `CREATE` SQL statement for the view.
`redacted_create_sql` | [`text`]      | The redacted `CREATE` SQL statement for the view.

[`bigint`]: /sql/types/bigint
[`boolean`]: /sql/types/boolean
[`integer`]: /sql/types/integer/
[`interval`]: /sql/types/interval
[`jsonb`]: /sql/types/jsonb
[`mz_aclitem`]: /sql/types/mz_aclitem
[`mz_aclitem array`]: /sql/types/mz_aclitem
[`mz_timestamp`]: /sql/types/mz_timestamp
[`numeric`]: /sql/types/numeric/
[`oid`]: /sql/types/oid
[`record`]: /sql/types/record
[`text`]: /sql/types/text
[`timestamp with time zone`]: /sql/types/timestamp
[`text array`]: /sql/types/array
[`text list`]: /sql/types/list/
[`uint8`]: /sql/types/uint8
[`uint8 list`]: /sql/types/list
[`uint4`]: /sql/types/uint4

<!-- RELATION_SPEC_UNDOCUMENTED mz_catalog.mz_operators -->


---

## mz_internal


The following sections describe the available objects in the `mz_internal`
schema.

> **Warning:** 
The objects in the `mz_internal` schema are not part of Materialize's stable interface.
Backwards-incompatible changes to these objects may be made at any time.


> **Warning:** 
`SELECT` statements may reference these objects, but creating views that
reference these objects is not allowed.


## `mz_object_global_ids`

The `mz_object_global_ids` table maps Materialize catalog item IDs to global IDs.

<!-- RELATION_SPEC mz_internal.mz_object_global_ids -->
| Field        | Type     | Meaning                                                                                             |
|--------------|----------|-----------------------------------------------------------------------------------------------------|
| `id`         | [`text`] | The ID of the object. Corresponds to [`mz_objects.id`](/sql/system-catalog/mz_catalog/#mz_objects). |
| `global_id`  | [`text`] | The global ID of the object.                                                                        |

## `mz_recent_activity_log`

> **Public Preview:** This feature is in public preview.

> **Warning:** 
Do not rely on all statements being logged in this view. Materialize
controls the maximum rate at which statements are sampled, and may change
this rate at any time.


> **Warning:** 
Entries in this view may be cleared on restart (e.g., during Materialize maintenance windows).


The `mz_recent_activity_log` view contains a log of the SQL statements
that have been issued to Materialize in the last 24 hours, along
with various metadata about them.

Entries in this log may be sampled. The sampling rate is controlled by
the configuration parameter `statement_logging_sample_rate`, which may be set
to any value between 0 and 1. For example, to disable statement
logging entirely for a session, execute `SET
statement_logging_sample_rate TO 0`. Materialize may apply a lower
sampling rate than the one set in this parameter.

The view can be accessed by Materialize _superusers_ or users that have been
granted the [`mz_monitor` role](/security/appendix/appendix-built-in-roles/#system-catalog-roles).

<!-- RELATION_SPEC mz_internal.mz_recent_activity_log -->
- **`execution_id`**: [`uuid`] | An ID that is unique for each executed statement.
- **`sample_rate`**: [`double precision`] | The actual rate at which the statement was sampled.
- **`cluster_id`**: [`text`] | The ID of the cluster the statement execution was directed to. Corresponds to [mz_clusters.id](/sql/system-catalog/mz_catalog/#mz_clusters).
- **`application_name`**: [`text`] | The value of the `application_name` configuration parameter at execution time.
- **`cluster_name`**: [`text`] | The name of the cluster with ID `cluster_id` at execution time.
- **`database_name`**: [`text`] | The value of the `database` configuration parameter at execution time.
- **`search_path`**: [`text list`] | The value of the `search_path` configuration parameter at execution time.
- **`transaction_isolation`**: [`text`] | The value of the `transaction_isolation` configuration parameter at execution time.
- **`execution_timestamp`**: [`uint8`] | The logical timestamp at which execution was scheduled.
- **`transient_index_id`**: [`text`] | The internal index of the compute dataflow created for the query, if any.
- **`params`**: [`text array`] | The parameters with which the statement was executed.
- **`mz_version`**: [`text`] | The version of Materialize that was running when the statement was executed.
- **`began_at`**: [`timestamp with time zone`] | The wall-clock time at which the statement began executing.
- **`finished_at`**: [`timestamp with time zone`] | The wall-clock time at which the statement finished executing.
- **`finished_status`**: [`text`] | The final status of the statement (e.g., `success`, `canceled`, `error`, or `aborted`). `aborted` means that Materialize exited before the statement finished executing.
- **`error_message`**: [`text`] | The error message, if the statement failed.
- **`result_size`**: [`bigint`] | The size in bytes of the result, for statements that return rows.
- **`rows_returned`**: [`bigint`] | The number of rows returned, for statements that return rows.
- **`execution_strategy`**: [`text`] | For `SELECT` queries, the strategy for executing the query. `constant` means computed in the control plane without the involvement of a cluster, `fast-path` means read by a cluster directly from an in-memory index, and `standard` means computed by a temporary dataflow.
- **`transaction_id`**: [`uint8`] | The ID of the transaction that the statement was part of. Note that transaction IDs are only unique per session.
- **`prepared_statement_id`**: [`uuid`] | An ID that is unique for each prepared statement. For example, if a statement is prepared once and then executed multiple times, all executions will have the same value for this column (but different values for `execution_id`).
- **`sql_hash`**: [`bytea`] | An opaque value uniquely identifying the text of the query.
- **`prepared_statement_name`**: [`text`] | The name given by the client library to the prepared statement.
- **`session_id`**: [`uuid`] | An ID that is unique for each session. Corresponds to [mz_sessions.id](#mz_sessions).
- **`prepared_at`**: [`timestamp with time zone`] | The time at which the statement was prepared.
- **`statement_type`**: [`text`] | The _type_ of the statement, e.g. `select` for a `SELECT` query, or `NULL` if the statement was empty.
- **`throttled_count`**: [`uint8`] | The number of statement executions that were dropped due to throttling before the current one was seen. If you have a very high volume of queries and need to log them without throttling, [contact our team](/support/).
- **`connected_at`**: [`timestamp with time zone`] | The time at which the session was established.
- **`initial_application_name`**: [`text`] | The initial value of `application_name` at the beginning of the session.
- **`authenticated_user`**: [`text`] | The name of the user for which the session was established.
- **`sql`**: [`text`] | The SQL text of the statement.


## `mz_aws_connections`

The `mz_aws_connections` table contains a row for each AWS connection in the
system.

<!-- RELATION_SPEC mz_internal.mz_aws_connections -->
| Field                         | Type      | Meaning
|-------------------------------|-----------|--------
| `id`                          | [`text`]  | The ID of the connection.
| `endpoint`                    | [`text`]  | The value of the `ENDPOINT` option, if set.
| `region`                      | [`text`]  | The value of the `REGION` option, if set.
| `access_key_id`               | [`text`]  | The value of the `ACCESS KEY ID` option, if provided in line.
| `access_key_id_secret_id`     | [`text`]  | The ID of the secret referenced by the `ACCESS KEY ID` option, if provided via a secret.
| `secret_access_key_secret_id` | [`text`]  | The ID of the secret referenced by the `SECRET ACCESS KEY` option, if set.
| `session_token`               | [`text`]  | The value of the `SESSION TOKEN` option, if provided in line.
| `session_token_secret_id`     | [`text`]  | The ID of the secret referenced by the `SESSION TOKEN` option, if provided via a secret.
| `assume_role_arn`             | [`text`]  | The value of the `ASSUME ROLE ARN` option, if set.
| `assume_role_session_name`    | [`text`]  | The value of the `ASSUME ROLE SESSION NAME` option, if set.
| `principal`                   | [`text`]  | The ARN of the AWS principal Materialize will use when assuming the provided role, if the connection is configured to use role assumption.
| `external_id`                 | [`text`]  | The external ID Materialize will use when assuming the provided role, if the connection is configured to use role assumption.
| `example_trust_policy`        | [`jsonb`] | An example of an IAM role trust policy that allows this connection's principal and external ID to assume the role.

## `mz_aws_privatelink_connection_status_history`

The `mz_aws_privatelink_connection_status_history` table contains a row describing
the historical status for each AWS PrivateLink connection in the system.

<!-- RELATION_SPEC mz_internal.mz_aws_privatelink_connection_status_history -->
| Field             | Type                       | Meaning                                                    |
|-------------------|----------------------------|------------------------------------------------------------|
| `occurred_at`     | `timestamp with time zone` | Wall-clock timestamp of the status change.       |
| `connection_id`   | `text`                     | The unique identifier of the AWS PrivateLink connection. Corresponds to [`mz_catalog.mz_connections.id`](../mz_catalog#mz_connections).   |
| `status`          | `text`                     | The status of the connection: one of `pending-service-discovery`, `creating-endpoint`, `recreating-endpoint`, `updating-endpoint`, `available`, `deleted`, `deleting`, `expired`, `failed`, `pending`, `pending-acceptance`, `rejected`, or `unknown`.                        |

## `mz_aws_privatelink_connection_statuses`

The `mz_aws_privatelink_connection_statuses` table contains a row describing
the most recent status for each AWS PrivateLink connection in the system.

<!-- RELATION_SPEC mz_internal.mz_aws_privatelink_connection_statuses -->
| Field | Type | Meaning |
|-------|------|---------|
| `id` | [`text`] | The ID of the connection. Corresponds to [`mz_catalog.mz_connections.id`](../mz_catalog#mz_sinks). |
| `name` | [`text`] | The name of the connection.  |
| `last_status_change_at` | [`timestamp with time zone`] | Wall-clock timestamp of the connection status change.|
| `status` | [`text`] | | The status of the connection: one of `pending-service-discovery`, `creating-endpoint`, `recreating-endpoint`, `updating-endpoint`, `available`, `deleted`, `deleting`, `expired`, `failed`, `pending`, `pending-acceptance`, `rejected`, or `unknown`. |

## `mz_cluster_deployment_lineage`

The `mz_cluster_deployment_lineage` table shows the blue/green deployment lineage of all clusters in [`mz_clusters`](../mz_catalog/#mz_clusters). It determines all cluster IDs that are logically the same cluster.

<!-- RELATION_SPEC mz_internal.mz_cluster_deployment_lineage -->
| Field                               | Type         | Meaning                                                        |
|-------------------------------------|--------------|----------------------------------------------------------------|
| `cluster_id`                        | [`text`]     | The ID of the cluster. Corresponds to [`mz_clusters.id`](../mz_catalog/#mz_clusters) (though the cluster may no longer exist). |
| `current_deployment_cluster_id`                              | [`text`]     | The cluster ID of the last cluster in `cluster_id`'s blue/green lineage (the cluster is guaranteed to exist). |
| `cluster_name`   | [`text`] | The name of the cluster |

## `mz_cluster_schedules`

The `mz_cluster_schedules` table shows the `SCHEDULE` option specified for each cluster.

<!-- RELATION_SPEC mz_internal.mz_cluster_schedules -->
| Field                               | Type         | Meaning                                                        |
|-------------------------------------|--------------|----------------------------------------------------------------|
| `cluster_id`                        | [`text`]     | The ID of the cluster. Corresponds to [`mz_clusters.id`](../mz_catalog/#mz_clusters). |
| `type`                              | [`text`]     | `on-refresh`, or `manual`. Default: `manual`                   |
| `refresh_hydration_time_estimate`   | [`interval`] | The interval given in the `HYDRATION TIME ESTIMATE` option.    |

## `mz_cluster_replica_metrics`

The `mz_cluster_replica_metrics` view gives the last known CPU and RAM utilization statistics
for all processes of all extant cluster replicas.

At this time, we do not make any guarantees about the exactness or freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_metrics -->
| Field               | Type         | Meaning
| ------------------- | ------------ | --------
| `replica_id`        | [`text`]     | The ID of a cluster replica.
| `process_id`        | [`uint8`]    | The ID of a process within the replica.
| `cpu_nano_cores`    | [`uint8`]    | Approximate CPU usage, in billionths of a vCPU core.
| `memory_bytes`      | [`uint8`]    | Approximate RAM usage, in bytes.
| `disk_bytes`        | [`uint8`]    | Approximate disk usage, in bytes.
| `heap_bytes`        | [`uint8`]    | Approximate heap (RAM + swap) usage, in bytes.
| `heap_limit`        | [`uint8`]    | Available heap (RAM + swap) space, in bytes.

## `mz_cluster_replica_metrics_history`


The `mz_cluster_replica_metrics_history` table records resource utilization metrics
for all processes of all extant cluster replicas.

At this time, we do not make any guarantees about the exactness or freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_metrics_history -->
| Field            | Type      | Meaning
| ---------------- | --------- | --------
| `replica_id`     | [`text`]  | The ID of a cluster replica.
| `process_id`     | [`uint8`] | The ID of a process within the replica.
| `cpu_nano_cores` | [`uint8`] | Approximate CPU usage, in billionths of a vCPU core.
| `memory_bytes`   | [`uint8`] | Approximate memory usage, in bytes.
| `disk_bytes`     | [`uint8`] | Approximate disk usage, in bytes.
| `occurred_at`    | [`timestamp with time zone`] | Wall-clock timestamp at which the event occurred.
| `heap_bytes`     | [`uint8`] | Approximate heap (RAM + swap) usage, in bytes.
| `heap_limit`     | [`uint8`] | Available heap (RAM + swap) space, in bytes.

## `mz_cluster_replica_statuses`

The `mz_cluster_replica_statuses` view contains a row describing the status
of each process in each cluster replica in the system.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_statuses -->
| Field        | Type                         | Meaning                                                                                                 |
|--------------|------------------------------|---------------------------------------------------------------------------------------------------------|
| `replica_id` | [`text`]                     | Materialize's unique ID for the cluster replica.                                                        |
| `process_id` | [`uint8`]                    | The ID of the process within the cluster replica.                                                       |
| `status`     | [`text`]                     | The status of the cluster replica: `online` or `offline`.                                               |
| `reason`     | [`text`]                     | If the cluster replica is in a `offline` state, the reason (if available). For example, `oom-killed`.   |
| `updated_at` | [`timestamp with time zone`] | The time at which the status was last updated.                                                          |

## `mz_cluster_replica_status_history`


The `mz_cluster_replica_status_history` table records status changes
for all processes of all extant cluster replicas.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_status_history -->
| Field         | Type      | Meaning
|---------------|-----------|--------
| `replica_id`  | [`text`]  | The ID of a cluster replica.
| `process_id`  | [`uint8`] | The ID of a process within the replica.
| `status`      | [`text`]  | The status of the cluster replica: `online` or `offline`.
| `reason`      | [`text`]  | If the cluster replica is in an `offline` state, the reason (if available). For example, `oom-killed`.
| `occurred_at` | [`timestamp with time zone`] | Wall-clock timestamp at which the event occurred.

## `mz_cluster_replica_utilization`

The `mz_cluster_replica_utilization` view gives the last known CPU and RAM utilization statistics
for all processes of all extant cluster replicas, as a percentage of the total resource allocation.

At this time, we do not make any guarantees about the exactness or freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_utilization -->
| Field            | Type                 | Meaning
|------------------|----------------------|---------
| `replica_id`     | [`text`]             | The ID of a cluster replica.
| `process_id`     | [`uint8`]            | The ID of a process within the replica.
| `cpu_percent`    | [`double precision`] | Approximate CPU usage, in percent of the total allocation.
| `memory_percent` | [`double precision`] | Approximate RAM usage, in percent of the total allocation.
| `disk_percent`   | [`double precision`] | Approximate disk usage, in percent of the total allocation.
| `heap_percent`   | [`double precision`] | Approximate heap (RAM + swap) usage, in percent of the total allocation.

## `mz_cluster_replica_utilization_history`


The `mz_cluster_replica_utilization_history` view records resource utilization metrics
for all processes of all extant cluster replicas, as a percentage of the total resource allocation.

At this time, we do not make any guarantees about the exactness or freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_utilization_history -->
| Field            | Type                 | Meaning
|------------------|----------------------|--------
| `replica_id`     | [`text`]             | The ID of a cluster replica.
| `process_id`     | [`uint8`]            | The ID of a process within the replica.
| `cpu_percent`    | [`double precision`] | Approximate CPU usage, in percent of the total allocation.
| `memory_percent` | [`double precision`] | Approximate RAM usage, in percent of the total allocation.
| `disk_percent`   | [`double precision`] | Approximate disk usage, in percent of the total allocation.
| `heap_percent`   | [`double precision`] | Approximate heap (RAM + swap) usage, in percent of the total allocation.
| `occurred_at`    | [`timestamp with time zone`] | Wall-clock timestamp at which the event occurred.

## `mz_cluster_replica_history`

The `mz_cluster_replica_history` view contains information about the timespan of
each replica, including the times at which it was created and dropped
(if applicable).

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_history -->
- **`replica_id`**: [`text`] | The ID of a cluster replica.
- **`size`**: [`text`] | The size of the cluster replica. Corresponds to [`mz_cluster_replica_sizes.size`](../mz_catalog#mz_cluster_replica_sizes).
- **`cluster_id`**: [`text`] | The ID of the cluster associated with the replica.
- **`cluster_name`**: [`text`] | The name of the cluster associated with the replica.
- **`replica_name`**: [`text`] | The name of the replica.
- **`created_at`**: [`timestamp with time zone`] | The time at which the replica was created.
- **`dropped_at`**: [`timestamp with time zone`] | The time at which the replica was dropped, or `NULL` if it still exists.
- **`credits_per_hour`**: [`numeric`] | The number of compute credits consumed per hour. Corresponds to [`mz_cluster_replica_sizes.credits_per_hour`](../mz_catalog#mz_cluster_replica_sizes).

## `mz_cluster_replica_name_history`

The `mz_cluster_replica_name_history` view contains historical information about names of each cluster replica.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_name_history -->
| Field                 | Type                         | Meaning                                                                                                                                   |
|-----------------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `occurred_at`          | [`timestamp with time zone`]                     |  The time at which the cluster replica was created or renamed. `NULL` if it's a built in system cluster replica.                                                                                                              |
| `id`          | [`text`]                     | The ID of the cluster replica.                                                                                                              |
| `previous_name`                | [`text`]                     | The previous name of the cluster replica. `NULL` if there was no previous name.   |
| `new_name`        | [`text`]                     | The new name of the cluster replica.                                                                                     |

## `mz_internal_cluster_replicas`

The `mz_internal_cluster_replicas` table lists the replicas that are created and maintained by Materialize support.

<!-- RELATION_SPEC mz_internal.mz_internal_cluster_replicas -->
| Field      | Type     | Meaning                                                                                                     |
|------------|----------|-------------------------------------------------------------------------------------------------------------|
| id         | [`text`] | The ID of a cluster replica. Corresponds to [`mz_cluster_replicas.id`](../mz_catalog/#mz_cluster_replicas). |

## `mz_pending_cluster_replicas`

The `mz_pending_cluster_replicas` table lists the replicas that were created during managed cluster alter statement that has not yet finished. The configurations of these replica may differ from the cluster's configuration.

<!-- RELATION_SPEC mz_internal.mz_pending_cluster_replicas -->
| Field      | Type     | Meaning                                                                                                     |
|------------|----------|-------------------------------------------------------------------------------------------------------------|
| id         | [`text`] | The ID of a cluster replica. Corresponds to [`mz_cluster_replicas.id`](../mz_catalog/#mz_cluster_replicas). |

## `mz_comments`

The `mz_comments` table stores optional comments (i.e., descriptions) for objects in the database.

<!-- RELATION_SPEC mz_internal.mz_comments -->
| Field          | Type        | Meaning                                                                                      |
| -------------- |-------------| --------                                                                                     |
| `id`           | [`text`]    | The ID of the object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects).           |
| `object_type`  | [`text`]    | The type of object the comment is associated with.                                           |
| `object_sub_id`| [`integer`] | For a comment on a column of a relation, the column number. `NULL` for other object types.   |
| `comment`      | [`text`]    | The comment itself.                                                                          |

## `mz_compute_dependencies`

The `mz_compute_dependencies` table describes the dependency structure between each compute object (index, materialized view, or subscription) and the sources of its data.

In contrast to [`mz_object_dependencies`](#mz_object_dependencies), this table only lists dependencies in the compute layer.
SQL objects that don't exist in the compute layer (such as views) are omitted.

<!-- RELATION_SPEC mz_internal.mz_compute_dependencies -->
| Field       | Type     | Meaning                                                                                                                                                                                                                                                                                            |
| ----------- | -------- | --------                                                                                                                                                                                                                                                                                           |
| `object_id`     | [`text`] | The ID of a compute object. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), or [`mz_internal.mz_subscriptions`](#mz_subscriptions).                                                           |
| `dependency_id` | [`text`] | The ID of a compute dependency. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources), or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |

## `mz_compute_hydration_statuses`

The `mz_compute_hydration_statuses` view describes the per-replica hydration status of each compute object (index, materialized view).

A compute object is hydrated on a given replica when it has fully processed the initial snapshot of data available in its inputs.

<!-- RELATION_SPEC mz_internal.mz_compute_hydration_statuses -->
| Field            | Type         | Meaning  |
| ---------------- | ------------ | -------- |
| `object_id`      | [`text`]     | The ID of a compute object. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes) or [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views) |
| `replica_id`     | [`text`]     | The ID of a cluster replica. |
| `hydrated`       | [`boolean`]  | Whether the compute object is hydrated on the replica. |
| `hydration_time` | [`interval`] | The amount of time it took for the replica to hydrate the compute object. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_hydration_times -->

## `mz_compute_operator_hydration_statuses`

The `mz_compute_operator_hydration_statuses` table describes the dataflow operator hydration status of compute objects (indexes or materialized views).

A dataflow operator is hydrated on a given replica when it has fully processed the initial snapshot of data available in its inputs.

<!-- RELATION_SPEC mz_internal.mz_compute_operator_hydration_statuses -->
| Field                   | Type        | Meaning  |
| ----------------------- | ----------- | -------- |
| `replica_id`            | [`text`]    | The ID of a cluster replica. |
| `object_id`             | [`text`]    | The ID of a compute object. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes) or [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views). |
| `physical_plan_node_id` | [`uint8`]   | The ID of a node in the physical plan of the compute object. Corresponds to a `node_id` displayed in the output of `EXPLAIN PHYSICAL PLAN WITH (node identifiers)`. |
| `hydrated`              | [`boolean`] | Whether the node is hydrated on the replica. |

## `mz_frontiers`

The `mz_frontiers` table describes the frontiers of each source, sink, table,
materialized view, index, and subscription in the system, as observed from the
coordinator.

At this time, we do not make any guarantees about the freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_frontiers -->
| Field            | Type             | Meaning                                                                             |
| ---------------- | ------------     | --------                                                                            |
| `object_id`      | [`text`]         | The ID of the source, sink, table, index, materialized view, or subscription.       |
| `read_frontier`  | [`mz_timestamp`] | The earliest timestamp at which the output is still readable.                       |
| `write_frontier` | [`mz_timestamp`] | The next timestamp at which the output may change.                                  |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_global_frontiers -->

## `mz_history_retention_strategies`

The `mz_history_retention_strategies` describes the history retention strategies
for tables, sources, indexes, materialized views that are configured with a
[history retention
period](/transform-data/patterns/durable-subscriptions/#history-retention-period).

<!-- RELATION_SPEC mz_internal.mz_history_retention_strategies -->
| Field | Type | Meaning |
| - | - | - |
| `id` | [`text`] | The ID of the object. |
| `strategy` | [`text`] | The strategy. `FOR` is the only strategy, and means the object's compaction window is the duration of the `value` field. |
| `value` | [`jsonb`] | The value of the strategy. For `FOR`, is a number of milliseconds. |

## `mz_hydration_statuses`

The `mz_hydration_statuses` view describes the per-replica hydration status of
each object powered by a dataflow.

A dataflow-powered object is hydrated on a given replica when the respective
dataflow has fully processed the initial snapshot of data available in its
inputs.

<!-- RELATION_SPEC mz_internal.mz_hydration_statuses -->
| Field        | Type        | Meaning  |
| -----------  | ----------- | -------- |
| `object_id`  | [`text`]    | The ID of a dataflow-powered object. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), [`mz_internal.mz_subscriptions`](#mz_subscriptions), [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources), or [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks). |
| `replica_id` | [`text`]    | The ID of a cluster replica. |
| `hydrated`   | [`boolean`] | Whether the object is hydrated on the replica. |

## `mz_license_keys`

The `mz_license_keys` table describes the license keys which are currently in
use.

<!-- RELATION_SPEC mz_internal.mz_license_keys -->
| Field            | Type                         | Meaning  |
| -----------      | -----------                  | -------- |
| `id`             | [`text`]                     | The identifier of the license key. |
| `organization`   | [`text`]                     | The name of the organization that this license key was issued to. |
| `environment_id` | [`text`]                     | The environment ID that this license key was issued for. |
| `expiration`     | [`timestamp with time zone`] | The date and time when this license key expires. |
| `not_before`     | [`timestamp with time zone`] | The start of the validity period for this license key. |

## `mz_index_advice`

> **Warning:** 
Following the advice in this view might not always yield resource usage
optimizations. You should test any changes in a development environment
before deploying the changes to production.


The `mz_index_advice` view provides advice on opportunities to optimize resource
usage (memory and CPU) in Materialize. The advice provided suggests either
creating indexes or materialized views to precompute intermediate results that
can be reused across several objects, or removing unnecessary indexes or
materialized views.


### Known limitations

The suggestions are based on the graph of dependencies between objects and do
not take into account other important factors, like the actual usage patterns
and execution plans. This means that following the advice in this view **might
not always lead to resource usage optimizations**. In some cases, the provided
advice might lead to suboptimal execution plans or even increased resource
usage. For example:

- If a materialized view or an index has been created for direct querying, the
  dependency graph will not reflect this nuance and `mz_index_advice` might
  recommend using an unindexed view instead. In this case, you should refer to
  the reference documentation for [query optimization](/transform-data/optimization/#indexes)
  instead.
- If a view is depended on by multiple objects that use very selective filters,
  or multiple projections that can be pushed into or even beyond the view,
  adding an index may increase resource usage.
- If an index has been created to [enable delta joins](/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins),
  removing it may lead to lower memory utilization, but the delta join
  optimization will no longer be used in the join implementation.

To guarantee that there are no regressions given your specific usage patterns,
it's important to test any changes in a development environment before
deploying the changes to production.


<!-- RELATION_SPEC mz_internal.mz_index_advice -->
| Field                    | Type        | Meaning  |
| ------------------------ | ----------- | -------- |
| `object_id`              | [`text`]    | The ID of the object. Corresponds to [mz_objects.id](/sql/system-catalog/mz_catalog/#mz_objects). |
| `hint`                   | [`text`]    | A suggestion to either change the object (e.g. create an index, turn a materialized view into an indexed view) or keep the object unchanged. |
| `details`                | [`text`]    | Additional details on why the `hint` was proposed based on the dependencies of the object. |
| `referenced_object_ids`  | [`list`]    | The IDs of objects referenced by `details`. Corresponds to [mz_objects.id](/sql/system-catalog/mz_catalog/#mz_objects). |

## `mz_materialization_dependencies`

The `mz_materialization_dependencies` view describes the dependency structure between each materialization (materialized view, index, or sink) and the sources of its data.

In contrast to [`mz_object_dependencies`](#mz_object_dependencies), this view only lists dependencies in the dataflow layer.
SQL objects that don't exist in the dataflow layer (such as views) are omitted.

<!-- RELATION_SPEC mz_internal.mz_materialization_dependencies -->
| Field       | Type     | Meaning                                                                                                                                                                                                                                                                                            |
| ----------- | -------- | --------                                                                                                                                                                                                                                                                                           |
| `object_id`     | [`text`] | The ID of a materialization. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), or [`mz_catalog.mz_sinks.id`](#mz_subscriptions).                                                           |
| `dependency_id` | [`text`] | The ID of a dataflow dependency. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources), or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |

## `mz_materialization_lag`

The `mz_materialization_lag` view describes the difference between the input
frontiers and the output frontier for each materialized view, index, and sink
in the system. For hydrated dataflows, this lag roughly corresponds to the time
it takes for updates at the inputs to be reflected in the output.

At this time, we do not make any guarantees about the freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_materialization_lag -->
| Field                     | Type             | Meaning                                                                                  |
| ------------------------- | ---------------- | --------                                                                                 |
| `object_id`               | [`text`]         | The ID of the materialized view, index, or sink.                                         |
| `local_lag`               | [`interval`]     | The amount of time the materialization lags behind its direct inputs.                    |
| `global_lag`              | [`interval`]     | The amount of time the materialization lags behind its root inputs (sources and tables). |
| `slowest_local_input_id`  | [`text`]         | The ID of the slowest direct input.                                                      |
| `slowest_global_input_id` | [`text`]         | The ID of the slowest root input.                                                        |

## `mz_materialized_view_refresh_strategies`

The `mz_materialized_view_refresh_strategies` table shows the refresh strategies
specified for materialized views. If a materialized view has multiple refresh
strategies, a row will exist for each.

<!-- RELATION_SPEC mz_internal.mz_materialized_view_refresh_strategies -->
| Field                  | Type       | Meaning                                                                                       |
|------------------------|------------|-----------------------------------------------------------------------------------------------|
| `materialized_view_id` | [`text`]   | The ID of the materialized view. Corresponds to [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views)  |
| `type`                 | [`text`]   | `at`, `every`, or `on-commit`. Default: `on-commit`                                           |
| `interval`             | [`interval`] | The refresh interval of a `REFRESH EVERY` option, or `NULL` if the `type` is not `every`.   |
| `aligned_to`           | [`timestamp with time zone`] | The `ALIGNED TO` option of a `REFRESH EVERY` option, or `NULL` if the `type` is not `every`. |
| `at`                   | [`timestamp with time zone`] | The time of a `REFRESH AT`, or `NULL` if the `type` is not `at`.            |

## `mz_materialized_view_refreshes`

The `mz_materialized_view_refreshes` table shows the time of the last
successfully completed refresh and the time of the next scheduled refresh for
each materialized view with a refresh strategy other than `on-commit`.

<!-- RELATION_SPEC mz_internal.mz_materialized_view_refreshes -->
| Field                    | Type                         | Meaning                                                                                                                      |
|--------------------------|------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `materialized_view_id`   | [`text`]                     | The ID of the materialized view. Corresponds to [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views) |
| `last_completed_refresh` | [`mz_timestamp`]             | The time of the last successfully completed refresh. `NULL` if the materialized view hasn't completed any refreshes yet.  |
| `next_refresh`           | [`mz_timestamp`]             | The time of the next scheduled refresh. `NULL` if the materialized view has no future scheduled refreshes.                 |

## `mz_object_dependencies`

The `mz_object_dependencies` table describes the dependency structure between
all database objects in the system.

<!-- RELATION_SPEC mz_internal.mz_object_dependencies -->
| Field                   | Type         | Meaning                                                                                       |
| ----------------------- | ------------ | --------                                                                                      |
| `object_id`             | [`text`]     | The ID of the dependent object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects).  |
| `referenced_object_id`  | [`text`]     | The ID of the referenced object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |

## `mz_object_fully_qualified_names`

The `mz_object_fully_qualified_names` view enriches the [`mz_catalog.mz_objects`](/sql/system-catalog/mz_catalog/#mz_objects) view with namespace information.

<!-- RELATION_SPEC mz_internal.mz_object_fully_qualified_names -->
- **`id`**: [`text`] | Materialize's unique ID for the object.
- **`name`**: [`text`] | The name of the object.
- **`object_type`**: [`text`] | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.
- **`schema_id`**: [`text`] | The ID of the schema to which the object belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
- **`schema_name`**: [`text`] | The name of the schema to which the object belongs. Corresponds to [`mz_schemas.name`](/sql/system-catalog/mz_catalog/#mz_schemas).
- **`database_id`**: [`text`] | The ID of the database to which the object belongs. Corresponds to [`mz_databases.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
- **`database_name`**: [`text`] | The name of the database to which the object belongs. Corresponds to [`mz_databases.name`](/sql/system-catalog/mz_catalog/#mz_databases).
- **`cluster_id`**: [`text`] | The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters). `NULL` for other object types.

## `mz_object_lifetimes`

The `mz_object_lifetimes` view enriches the [`mz_catalog.mz_objects`](/sql/system-catalog/mz_catalog/#mz_objects) view with information about the last lifetime event that occurred for each object in the system.

<!-- RELATION_SPEC mz_internal.mz_object_lifetimes -->
| Field           | Type                           | Meaning                                                                                                                                        |
| --------------- | ------------------------------ | -------------------------------------------------                                                                                              |
| `id`            | [`text`]                       | Materialize's unique ID for the object.                                                                                                        |
| `previous_id`   | [`text`]                       | The object's previous ID, if one exists.                                                                                                       |
| `object_type`   | [`text`]                       | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`. |
| `event_type`    | [`text`]                       | The lifetime event, either `create` or `drop`.                                                                                                 |
| `occurred_at`   | [`timestamp with time zone`]   | Wall-clock timestamp of when the event occurred.                                                                                               |

## `mz_object_history`

The `mz_object_history` view enriches the [`mz_catalog.mz_objects`](/sql/system-catalog/mz_catalog/#mz_objects) view with historical information about each object in the system.

<!-- RELATION_SPEC mz_internal.mz_object_history -->
| Field           | Type                           | Meaning                                                                                                                                        |
| --------------- | ------------------------------ | -------------------------------------------------                                                                                              |
| `id`            | [`text`]                       | Materialize's unique ID for the object.                                                                                                        |
| `cluster_id`   | [`text`]                       | The object's cluster ID. `NULL` if the object has no associated cluster.                                                                                                       |
| `object_type`   | [`text`]                       | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`. |
| `created_at`    | [`timestamp with time zone`]                       | Wall-clock timestamp of when the object was created. `NULL` for built in system objects.                                                                                                |
| `dropped_at`   | [`timestamp with time zone`]   | Wall-clock timestamp of when the object was dropped. `NULL` for built in system objects or if the object hasn't been dropped.                                              |

## `mz_object_transitive_dependencies`

The `mz_object_transitive_dependencies` view describes the transitive dependency structure between
all database objects in the system.
The view is defined as the transitive closure of [`mz_object_dependencies`](#mz_object_dependencies).

<!-- RELATION_SPEC mz_internal.mz_object_transitive_dependencies -->
| Field                   | Type         | Meaning                                                                                                               |
| ----------------------- | ------------ | --------                                                                                                              |
| `object_id`             | [`text`]     | The ID of the dependent object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects).                          |
| `referenced_object_id`  | [`text`]     | The ID of the (possibly transitively) referenced object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |

## `mz_notices`

> **Public Preview:** This feature is in public preview.

The `mz_notices` view contains a list of currently active notices emitted by the
system. The view can be accessed by Materialize _superusers_.

<!-- RELATION_SPEC mz_internal.mz_notices -->
- **`id`**: [`text`] | Materialize's unique ID for this notice.
- **`notice_type`**: [`text`] | The notice type.
- **`message`**: [`text`] | A brief description of the issue highlighted by this notice.
- **`hint`**: [`text`] | A high-level hint that tells the user what can be improved.
- **`action`**: [`text`] | A concrete action that will resolve the notice.
- **`redacted_message`**: [`text`] | A redacted version of the `message` column. `NULL` if no redaction is needed.
- **`redacted_hint`**: [`text`] | A redacted version of the `hint` column. `NULL` if no redaction is needed.
- **`redacted_action`**: [`text`] | A redacted version of the `action` column. `NULL` if no redaction is needed.
- **`action_type`**: [`text`] | The type of the `action` string (`sql_statements` for a valid SQL string or `plain_text` for plain text).
- **`object_id`**: [`text`] | The ID of the materialized view or index. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). For global notices, this column is `NULL`.
- **`created_at`**: [`timestamp with time zone`] | The time at which the notice was created. Note that some notices are re-created on `environmentd` restart.

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_optimizer_notices -->

## `mz_notices_redacted`

> **Public Preview:** This feature is in public preview.

The `mz_notices_redacted` view contains a redacted list of currently active
optimizer notices emitted by the system. The view can be accessed by Materialize
_superusers_ and Materialize support.

<!-- RELATION_SPEC mz_internal.mz_notices_redacted -->
- **`id`**: [`text`] | Materialize's unique ID for this notice.
- **`notice_type`**: [`text`] | The notice type.
- **`message`**: [`text`] | A redacted brief description of the issue highlighted by this notice.
- **`hint`**: [`text`] | A redacted high-level hint that tells the user what can be improved.
- **`action`**: [`text`] | A redacted concrete action that will resolve the notice.
- **`action_type`**: [`text`] | The type of the `action` string (`sql_statements` for a valid SQL string or `plain_text` for plain text).
- **`object_id`**: [`text`] | The ID of the materialized view or index. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). For global notices, this column is `NULL`.
- **`created_at`**: [`timestamp with time zone`] | The time at which the notice was created. Note that some notices are re-created on `environmentd` restart.

## `mz_postgres_sources`

The `mz_postgres_sources` table contains a row for each PostgreSQL source in the
system.

<!-- RELATION_SPEC mz_internal.mz_postgres_sources -->
| Field               | Type             | Meaning                                                                                                        |
| ------------------- | ---------------- | --------                                                                                                       |
| `id`                | [`text`]         | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                   |
| `replication_slot`  | [`text`]         | The name of the replication slot in the PostgreSQL database that Materialize will create and stream data from. |
| `timeline_id`       | [`uint8`]        | The PostgreSQL [timeline ID](https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-TIMELINES) determined on source creation.

## `mz_postgres_source_tables`

The `mz_postgres_source_tables` table contains the mapping between each Materialize
subsource or table and the corresponding upstream PostgreSQL table being ingested.

<!-- RELATION_SPEC mz_internal.mz_postgres_source_tables -->
| Field               | Type             | Meaning                                                                                                        |
| ------------------- | ---------------- | --------                                                                                                       |
| `id`                | [`text`]         | The ID of the subsource or table. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables).                   |
| `schema_name`       | [`text`]         | The schema of the upstream table being ingested. |
| `table_name`        | [`text`]         | The name of the upstream table being ingested.   |

## `mz_mysql_source_tables`

The `mz_mysql_source_tables` table contains the mapping between each Materialize
subsource or table and the corresponding upstream MySQL table being ingested.

<!-- RELATION_SPEC mz_internal.mz_mysql_source_tables -->
| Field               | Type             | Meaning                                                                                                        |
| ------------------- | ---------------- | --------                                                                                                       |
| `id`                | [`text`]         | The ID of the subsource or table. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables).                   |
| `schema_name`       | [`text`]         | The schema ([or, database](https://dev.mysql.com/doc/refman/8.0/en/glossary.html#glos_schema)) of the upstream table being ingested. |
| `table_name`        | [`text`]         | The name of the upstream table being ingested. |

## `mz_sql_server_source_tables`

The `mz_sql_server_source_tables` table contains the mapping between each Materialize
subsource or table and the corresponding upstream SQL Server table being ingested.

<!-- RELATION_SPEC mz_internal.mz_sql_server_source_tables -->
| Field               | Type             | Meaning                                                                                                        |
| ------------------- | ---------------- | --------                                                                                                       |
| `id`                | [`text`]         | The ID of the subsource or table. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables).                   |
| `schema_name`       | [`text`]         | The schema of the upstream table being ingested. |
| `table_name`        | [`text`]         | The name of the upstream table being ingested. |

## `mz_kafka_source_tables`

The `mz_kafka_source_tables` table contains the mapping between each Materialize
table and the corresponding upstream Kafka topic being ingested.

<!-- RELATION_SPEC mz_internal.mz_kafka_source_tables -->
| Field               | Type             | Meaning                                                                                                        |
| ------------------- | ---------------- | --------                                                                                                       |
| `id`                | [`text`]         | The ID of the table. Corresponds to [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables).                   |
| `topic`             | [`text`]         | The topic being ingested. |
| `envelope_type`     | [`text`]         | The [envelope](/sql/create-source/#envelopes) type: `none`, `upsert`, or `debezium`. `NULL` for other source types. |
| `key_format`        | [`text`]         | The [format](/sql/create-source/#formats) of the Kafka message key: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`, or `NULL`. |
| `value_format`      | [`text`]         | The [format](/sql/create-source/#formats) of the Kafka message value: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`. `NULL` for other source types. |

<!--
## `mz_prepared_statement_history`

The `mz_prepared_statement_history` table contains a subset of all
statements that have been prepared. It only contains statements that
have one or more corresponding executions in
[`mz_statement_execution_history`](#mz_statement_execution_history).

| Field         | Type                         | Meaning                                                                                                                           |
|---------------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `id`          | [`uuid`]                     | The globally unique ID of the prepared statement.                                                                                        |
| `session_id`  | [`uuid`]                     | The globally unique ID of the session that prepared the statement. Corresponds to [`mz_session_history.id`](#mz_session_history). |
| `name`        | [`text`]                     | The name of the prepared statement (the default prepared statement's name is the empty string).                                   |
| `sql`         | [`text`]                     | The SQL text of the prepared statement.                                                                                           |
| `prepared_at` | [`timestamp with time zone`] | The time at which the statement was prepared.                                                                                     |
-->


## `mz_session_history`

The `mz_session_history` table contains all the sessions that have
been established in the last 30 days, or (even if older) that are
referenced from
[`mz_recent_activity_log`](#mz_recent_activity_log).

> **Warning:** 
Do not rely on all sessions being logged in this view. Materialize
controls the maximum rate at which statements are sampled, and may change
this rate at any time.


<!-- RELATION_SPEC mz_internal.mz_session_history -->
| Field                | Type                         | Meaning                                                                                                                           |
|----------------------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `session_id`         | [`uuid`]                     | The globally unique ID of the session. Corresponds to [`mz_sessions.id`](#mz_sessions).                                           |
| `connected_at`       | [`timestamp with time zone`] | The time at which the session was established.                                                                                    |
| `initial_application_name`   | [`text`]                     | The `application_name` session metadata field.                                                                                    |
| `authenticated_user` | [`text`]                     | The name of the user for which the session was established.                                                                       |


### `mz_recent_storage_usage`

The `mz_recent_storage_usage` table describes the storage utilization of each
table, source, and materialized view in the system in the most recent storage
utilization assessment. Storage utilization assessments occur approximately
every hour.

See [`mz_storage_usage`](../mz_catalog#mz_storage_usage) for historical storage
usage information.

Field                  | Type                         | Meaning
---------------------- | ---------------------------- | -----------------------------------------------------------
`object_id`            | [`text`]                     | The ID of the table, source, or materialized view.
`size_bytes`           | [`uint8`]                    | The number of storage bytes used by the object in the most recent assessment.


## `mz_sessions`

The `mz_sessions` table contains a row for each active session in the system.

<!-- RELATION_SPEC mz_internal.mz_sessions -->
| Field            | Type                           | Meaning                                                                                                                   |
| -----------------| ------------------------------ | --------                                                                                                                  |
| `id`             | [`uuid`]                       | The globally unique ID of the session. |
| `connection_id`  | [`uint4`]                      | The connection ID of the session. Unique only for active sessions and can be recycled. Corresponds to [`pg_backend_pid()`](/sql/functions/#pg_backend_pid). |
| `role_id`        | [`text`]                       | The role ID of the role that the session is logged in as. Corresponds to [`mz_catalog.mz_roles`](../mz_catalog#mz_roles). |
| `client_ip`      | [`text`]                       | The IP address of the client that initiated the session.                                                                  |
| `connected_at`   | [`timestamp with time zone`]   | The time at which the session connected to the system.                                                                    |


## `mz_network_policies`

The `mz_network_policies` table contains a row for each network policy in the
system.

<!-- RELATION_SPEC mz_internal.mz_network_policies -->
| Field            | Type                  | Meaning                                                                                                            |
| -----------------| ----------------------| --------                                                                                                           |
| `id`             | [`text`]              | The ID of the network policy.                                                                                      |
| `name`           | [`text`]              | The name of the network policy.                                                                                    |
| `owner_id`       | [`text`]              | The role ID of the owner of the network policy. Corresponds to [`mz_catalog.mz_roles.id`](../mz_catalog#mz_roles). |
| `privileges`     | [`mz_aclitem array`]  | The privileges belonging to the network policy.                                                                    |
| `oid`            | [`oid`]               | A [PostgreSQL-compatible OID][`oid`] for the network policy.                                                       |

## `mz_network_policy_rules`

The `mz_network_policy_rules` table contains a row for each network policy rule
in the system.

<!-- RELATION_SPEC mz_internal.mz_network_policy_rules -->
| Field            | Type       | Meaning                                                                                                |
| -----------------| ----------------------| --------                                                                                    |
| `name`           | [`text`]   | The name of the network policy rule. Can be combined with `policy_id` to form a unique identifier. |
| `policy_id`      | [`text`]   | The ID the network policy the rule is part of. Corresponds to [`mz_network_policy_rules.id`](#mz_network_policy_rules).     |
| `action`         | [`text`]   | The action of the rule. `allow` is the only supported action.                                                    |
| `address`        | [`text`]   | The address the rule will take action on.                                                              |
| `direction`      | [`text`]   | The direction of traffic the rule applies to. `ingress` is the only supported direction. |

## `mz_show_network_policies`

The `mz_show_show_network_policies` view contains a row for each network policy in the system.

## `mz_show_all_privileges`

The `mz_show_all_privileges` view contains a row for each privilege granted
in the system on user objects to user roles.

<!-- RELATION_SPEC mz_internal.mz_show_all_privileges -->
- **`grantor`**: [`text`] | The role that granted the privilege.
- **`grantee`**: [`text`] | The role that the privilege was granted to.
- **`database`**: [`text`] | The name of the database containing the object.
- **`schema`**: [`text`] | The name of the schema containing the object.
- **`name`**: [`text`] | The name of the privilege target.
- **`object_type`**: [`text`] | The type of object the privilege is granted on.
- **`privilege_type`**: [`text`] | They type of privilege granted.


## `mz_show_cluster_privileges`

The `mz_show_cluster_privileges` view contains a row for each cluster privilege granted
in the system on user clusters to user roles.

<!-- RELATION_SPEC mz_internal.mz_show_cluster_privileges -->
| Field            | Type     | Meaning                                     |
|------------------|----------|---------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.        |
| `grantee`        | [`text`] | The role that the privilege was granted to. |
| `name`           | [`text`] | The name of the cluster.                    |
| `privilege_type` | [`text`] | They type of privilege granted.             |

## `mz_show_database_privileges`

The `mz_show_database_privileges` view contains a row for each database privilege granted
in the system on user databases to user roles.

<!-- RELATION_SPEC mz_internal.mz_show_database_privileges -->
| Field            | Type     | Meaning                                     |
|------------------|----------|---------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.        |
| `grantee`        | [`text`] | The role that the privilege was granted to. |
| `name`           | [`text`] | The name of the database.                   |
| `privilege_type` | [`text`] | They type of privilege granted.             |

## `mz_show_default_privileges`

The `mz_show_default_privileges` view contains a row for each default privilege granted
in the system in user databases and schemas to user roles.

<!-- RELATION_SPEC mz_internal.mz_show_default_privileges -->
- **`object_owner`**: [`text`] | Privileges described in this row will be granted on objects created by `object_owner`.
- **`database`**: [`text`] | Privileges described in this row will be granted only on objects created in `database` if non-null.
- **`schema`**: [`text`] | Privileges described in this row will be granted only on objects created in `schema` if non-null.
- **`object_type`**: [`text`] | Privileges described in this row will be granted only on objects of type `object_type`.
- **`grantee`**: [`text`] | Privileges described in this row will be granted to `grantee`.
- **`privilege_type`**: [`text`] | They type of privilege to be granted.

## `mz_show_object_privileges`

The `mz_show_object_privileges` view contains a row for each object privilege granted
in the system on user objects to user roles.

<!-- RELATION_SPEC mz_internal.mz_show_object_privileges -->
- **`grantor`**: [`text`] | The role that granted the privilege.
- **`grantee`**: [`text`] | The role that the privilege was granted to.
- **`database`**: [`text`] | The name of the database containing the object.
- **`schema`**: [`text`] | The name of the schema containing the object.
- **`name`**: [`text`] | The name of the object.
- **`object_type`**: [`text`] | The type of object the privilege is granted on.
- **`privilege_type`**: [`text`] | They type of privilege granted.

## `mz_show_role_members`

The `mz_show_role_members` view contains a row for each role membership in the system.

<!-- RELATION_SPEC mz_internal.mz_show_role_members -->
| Field     | Type     | Meaning                                                 |
|-----------|----------|---------------------------------------------------------|
| `role`    | [`text`] | The role that `member` is a member of.                  |
| `member`  | [`text`] | The role that is a member of `role`.                    |
| `grantor` | [`text`] | The role that granted membership of `member` to `role`. |

## `mz_show_schema_privileges`

The `mz_show_schema_privileges` view contains a row for each schema privilege granted
in the system on user schemas to user roles.

<!-- RELATION_SPEC mz_internal.mz_show_schema_privileges -->
| Field            | Type     | Meaning                                         |
|------------------|----------|-------------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.            |
| `grantee`        | [`text`] | The role that the privilege was granted to.     |
| `database`       | [`text`] | The name of the database containing the schema. |
| `name`           | [`text`] | The name of the schema.                         |
| `privilege_type` | [`text`] | They type of privilege granted.                 |

## `mz_show_system_privileges`

The `mz_show_system_privileges` view contains a row for each system privilege granted
in the system on to user roles.

<!-- RELATION_SPEC mz_internal.mz_show_system_privileges -->
| Field            | Type     | Meaning                                     |
|------------------|----------|---------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.        |
| `grantee`        | [`text`] | The role that the privilege was granted to. |
| `privilege_type` | [`text`] | They type of privilege granted.             |

## `mz_show_all_my_privileges`

The `mz_show_all_my_privileges` view is the same as
[`mz_show_all_privileges`](/sql/system-catalog/mz_internal/#mz_show_all_privileges), but
only includes rows where the current role is a direct or indirect member of `grantee`.

<!-- RELATION_SPEC mz_internal.mz_show_all_my_privileges -->
- **`grantor`**: [`text`] | The role that granted the privilege.
- **`grantee`**: [`text`] | The role that the privilege was granted to.
- **`database`**: [`text`] | The name of the database containing the object.
- **`schema`**: [`text`] | The name of the schema containing the object.
- **`name`**: [`text`] | The name of the privilege target.
- **`object_type`**: [`text`] | The type of object the privilege is granted on.
- **`privilege_type`**: [`text`] | They type of privilege granted.

## `mz_show_my_cluster_privileges`

The `mz_show_my_cluster_privileges` view is the same as
[`mz_show_cluster_privileges`](/sql/system-catalog/mz_internal/#mz_show_cluster_privileges), but
only includes rows where the current role is a direct or indirect member of `grantee`.

<!-- RELATION_SPEC mz_internal.mz_show_my_cluster_privileges -->
| Field            | Type     | Meaning                                     |
|------------------|----------|---------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.        |
| `grantee`        | [`text`] | The role that the privilege was granted to. |
| `name`           | [`text`] | The name of the cluster.                    |
| `privilege_type` | [`text`] | They type of privilege granted.             |

## `mz_show_my_database_privileges`

The `mz_show_my_database_privileges` view is the same as
[`mz_show_database_privileges`](/sql/system-catalog/mz_internal/#mz_show_database_privileges), but
only includes rows where the current role is a direct or indirect member of `grantee`.

<!-- RELATION_SPEC mz_internal.mz_show_my_database_privileges -->
| Field            | Type     | Meaning                                     |
|------------------|----------|---------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.        |
| `grantee`        | [`text`] | The role that the privilege was granted to. |
| `name`           | [`text`] | The name of the cluster.                    |
| `privilege_type` | [`text`] | They type of privilege granted.             |

## `mz_show_my_default_privileges`

The `mz_show_my_default_privileges` view is the same as
[`mz_show_default_privileges`](/sql/system-catalog/mz_internal/#mz_show_default_privileges), but
only includes rows where the current role is a direct or indirect member of `grantee`.

<!-- RELATION_SPEC mz_internal.mz_show_my_default_privileges -->
- **`object_owner`**: [`text`] | Privileges described in this row will be granted on objects created by `object_owner`.
- **`database`**: [`text`] | Privileges described in this row will be granted only on objects created in `database` if non-null.
- **`schema`**: [`text`] | Privileges described in this row will be granted only on objects created in `schema` if non-null.
- **`object_type`**: [`text`] | Privileges described in this row will be granted only on objects of type `object_type`.
- **`grantee`**: [`text`] | Privileges described in this row will be granted to `grantee`.
- **`privilege_type`**: [`text`] | They type of privilege to be granted.

## `mz_show_my_object_privileges`

The `mz_show_my_object_privileges` view is the same as
[`mz_show_object_privileges`](/sql/system-catalog/mz_internal/#mz_show_object_privileges), but
only includes rows where the current role is a direct or indirect member of `grantee`.

<!-- RELATION_SPEC mz_internal.mz_show_my_object_privileges -->
- **`grantor`**: [`text`] | The role that granted the privilege.
- **`grantee`**: [`text`] | The role that the privilege was granted to.
- **`database`**: [`text`] | The name of the database containing the object.
- **`schema`**: [`text`] | The name of the schema containing the object.
- **`name`**: [`text`] | The name of the object.
- **`object_type`**: [`text`] | The type of object the privilege is granted on.
- **`privilege_type`**: [`text`] | They type of privilege granted.

## `mz_show_my_role_members`

The `mz_show_my_role_members` view is the same as
[`mz_show_role_members`](/sql/system-catalog/mz_internal/#mz_show_role_members), but
only includes rows where the current role is a direct or indirect member of `member`.

<!-- RELATION_SPEC mz_internal.mz_show_my_role_members -->
| Field     | Type     | Meaning                                                 |
|-----------|----------|---------------------------------------------------------|
| `role`    | [`text`] | The role that `member` is a member of.                  |
| `member`  | [`text`] | The role that is a member of `role`.                    |
| `grantor` | [`text`] | The role that granted membership of `member` to `role`. |

## `mz_show_my_schema_privileges`

The `mz_show_my_schema_privileges` view is the same as
[`mz_show_schema_privileges`](/sql/system-catalog/mz_internal/#mz_show_schema_privileges), but
only includes rows where the current role is a direct or indirect member of `grantee`.

<!-- RELATION_SPEC mz_internal.mz_show_my_schema_privileges -->
| Field            | Type     | Meaning                                         |
|------------------|----------|-------------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.            |
| `grantee`        | [`text`] | The role that the privilege was granted to.     |
| `database`       | [`text`] | The name of the database containing the schema. |
| `name`           | [`text`] | The name of the schema.                         |
| `privilege_type` | [`text`] | They type of privilege granted.                 |

## `mz_show_my_system_privileges`

The `mz_show_my_system_privileges` view is the same as
[`mz_show_system_privileges`](/sql/system-catalog/mz_internal/#mz_show_system_privileges), but
only includes rows where the current role is a direct or indirect member of `grantee`.

<!-- RELATION_SPEC mz_internal.mz_show_my_system_privileges -->
| Field            | Type     | Meaning                                     |
|------------------|----------|---------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.        |
| `grantee`        | [`text`] | The role that the privilege was granted to. |
| `privilege_type` | [`text`] | They type of privilege granted.             |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_sink_statistics_raw -->

## `mz_sink_statistics`

The `mz_sink_statistics` view contains statistics about each sink.

### Counters
`messages_staged`, `messages_committed`, `bytes_staged`, and `bytes_committed` are all counters that monotonically increase. They are _only
useful for calculating rates_ to understand the general performance of your sink.

Note that:
- The non-rate values themselves are not directly comparable, because they are collected and aggregated across multiple threads/processes.

<!-- RELATION_SPEC mz_internal.mz_sink_statistics -->
- **`id`**: [`text`] | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks).
- **`replica_id`**: [`text`] | The ID of a replica running the sink. Corresponds to [`mz_catalog.mz_cluster_replicas.id`](../mz_catalog#mz_cluster_replicas).
- **`messages_staged`**: [`uint8`] | The number of messages staged but possibly not committed to the sink.
- **`messages_committed`**: [`uint8`] | The number of messages committed to the sink.
- **`bytes_staged`**: [`uint8`] | The number of bytes staged but possibly not committed to the sink. This counts both keys and values, if applicable.
- **`bytes_committed`**: [`uint8`] | The number of bytes committed to the sink. This counts both keys and values, if applicable.

## `mz_sink_statuses`

The `mz_sink_statuses` view provides the current state for each sink in the
system, including potential error messages and additional metadata helpful for
debugging.

<!-- RELATION_SPEC mz_internal.mz_sink_statuses -->
- **`id`**: [`text`] | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks).
- **`name`**: [`text`] | The name of the sink.
- **`type`**: [`text`] | The type of the sink.
- **`last_status_change_at`**: [`timestamp with time zone`] | Wall-clock timestamp of the sink status change.
- **`status`**: [`text`] | The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.
- **`error`**: [`text`] | If the sink is in an error state, the error message.
- **`details`**: [`jsonb`] | Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions.

## `mz_sink_status_history`

The `mz_sink_status_history` table contains rows describing the
history of changes to the status of each sink in the system, including potential error
messages and additional metadata helpful for debugging.

<!-- RELATION_SPEC mz_internal.mz_sink_status_history -->
- **`occurred_at`**: [`timestamp with time zone`] | Wall-clock timestamp of the sink status change.
- **`sink_id`**: [`text`] | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks).
- **`status`**: [`text`] | The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.
- **`error`**: [`text`] | If the sink is in an error state, the error message.
- **`details`**: [`jsonb`] | Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions.
- **`replica_id`**: [`text`] | The ID of the replica that an instance of a sink is running on.

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_source_statistics_raw -->

## `mz_source_statistics`

The `mz_source_statistics` view contains statistics about each source.

<!-- RELATION_SPEC mz_internal.mz_source_statistics -->
- **`id`**: [`text`] | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).
- **`replica_id`**: [`text`] | The ID of a replica running the source. Corresponds to [`mz_catalog.mz_cluster_replicas.id`](../mz_catalog#mz_cluster_replicas).
- **`messages_received`**: [`uint8`] | The number of messages the source has received from the external system. Messages are counted in a source type-specific manner. Messages do not correspond directly to updates: some messages produce multiple updates, while other messages may be coalesced into a single update.
- **`bytes_received`**: [`uint8`] | The number of bytes the source has read from the external system. Bytes are counted in a source type-specific manner and may or may not include protocol overhead.
- **`updates_staged`**: [`uint8`] | The number of updates (insertions plus deletions) the source has written but not yet committed to the storage layer.
- **`updates_committed`**: [`uint8`] | The number of updates (insertions plus deletions) the source has committed to the storage layer.
- **`records_indexed`**: [`uint8`] | The number of individual records indexed in the source envelope state.
- **`bytes_indexed`**: [`uint8`] | The number of bytes stored in the source's internal index, if any.
- **`rehydration_latency`**: [`interval`] | The amount of time it took for the source to rehydrate its internal index, if any, after the source last restarted.
- **`snapshot_records_known`**: [`uint8`] | The size of the source's snapshot, measured in number of records. See [below](#meaning-record) to learn what constitutes a record.
- **`snapshot_records_staged`**: [`uint8`] | The number of records in the source's snapshot that Materialize has read. See [below](#meaning-record) to learn what constitutes a record.
- **`snapshot_committed`**: [`boolean`] | Whether the source has committed the initial snapshot for a source.
- **`offset_known`**: [`uint8`] | The offset of the most recent data in the source's upstream service that Materialize knows about. See [below](#meaning-offset) to learn what constitutes an offset.
- **`offset_committed`**: [`uint8`] | The offset of the the data that Materialize has durably ingested. See [below](#meaning-offset) to learn what constitutes an offset.

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_source_statistics_with_history -->

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_source_references -->

### Counter metrics

`messages_received`, `bytes_received`, `updates_staged`, and `updates_committed`
are counter metrics that monotonically increase over time.

Counters are updated in a best-effort manner. An ill-timed restart of the source
may cause undercounting or overcounting. As a result, **counters are only useful
for calculating rates to understand the general performance of your source.**

For Postgres and MySQL sources, `messages_received` and `bytes_received` are
collected on the top-level source, and `updates_staged` and `updates_committed`
are collected on the source's tables.

### Gauge metrics

Gauge metrics reflect values that can increase or decrease over time. Gauge
metrics are eventually consistent. They may lag the true state of the source by
seconds or minutes, but if the source stops ingesting messages, the gauges will
eventually reflect the true state of the source.

`records_indexed` and `bytes_indexed` are the size (in records and bytes
respectively) of the index the source must maintain internally to efficiently
process incoming data. Currently, only sources that use the upsert and Debezium
envelopes must maintain an index. These gauges reset to 0 when the source is
restarted, as the index must be rehydrated.

`rehydration_latency` represents the amount of time it took for the source to
rehydrate its index after the latest restart. It is reset to `NULL` when a
source is restarted and is populated with a duration after hydration finishes.

When a source is first created, it must process an initial snapshot of data.
`snapshot_records_known` is the total number of records in the snapshot, and
`snapshot_records_staged` is how many of the records the source has read so far.

<a name="meaning-record"></a>

The meaning of record depends on the source:
- For Kafka sources, it's the total number of offsets in the snapshot.
- For Postgres and MySQL sources, it's the number of rows in the snapshot.

Note that when tables are added to Postgres or MySQL sources,
`snapshot_records_known` and `snapshot_records_staged` will reset as the source
snapshots those new tables. The metrics will also reset if the source is
restarted while the snapshot is in progress.

`snapshot_committed` becomes true when we have fully committed the snapshot for
the given source. <!-- TODO: when does this reset? -->

`offset_known` and `offset_committed` are used to represent the progress a
source is making relative to its upstream source. `offset_known` is the maximum
offset in the upstream system that Materialize knows about. `offset_committed`
is the offset that Materialize has durably ingested. These metrics will never
decrease over the lifetime of a source.

<a name="meaning-offset"></a>

The meaning of offset depends on the source:
- For Kafka sources, an offset is the Kafka message offset.
- For MySQL sources, an offset is the number of transactions committed across all servers in the cluster.
- For Postgres sources, an offset is a log sequence number (LSN).

## `mz_source_statuses`

The `mz_source_statuses` view provides the current state for each source in the
system, including potential error messages and additional metadata helpful for
debugging.

<!-- RELATION_SPEC mz_internal.mz_source_statuses -->
- **`id`**: [`text`] | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).
- **`name`**: [`text`] | The name of the source.
- **`type`**: [`text`] | The type of the source.
- **`last_status_change_at`**: [`timestamp with time zone`] | Wall-clock timestamp of the source status change.
- **`status`**: [`text`] | The status of the source: one of `created`, `starting`, `running`, `paused`, `stalled`, `failed`, or `dropped`.
- **`error`**: [`text`] | If the source is in an error state, the error message.
- **`details`**: [`jsonb`] | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions.

## `mz_source_status_history`

The `mz_source_status_history` table contains a row describing the status of the
historical state for each source in the system, including potential error
messages and additional metadata helpful for debugging.

<!-- RELATION_SPEC mz_internal.mz_source_status_history -->
- **`occurred_at`**: [`timestamp with time zone`] | Wall-clock timestamp of the source status change.
- **`source_id`**: [`text`] | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).
- **`status`**: [`text`] | The status of the source: one of `created`, `starting`, `running`, `paused`, `stalled`, `failed`, or `dropped`.
- **`error`**: [`text`] | If the source is in an error state, the error message.
- **`details`**: [`jsonb`] | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions.
- **`replica_id`**: [`text`] | The ID of the replica that an instance of a source is running on.

<!--
## `mz_statement_execution_history`

The `mz_statement_execution_history` table contains a row for each
statement executed, that the system decided to log. Entries older than
thirty days may be removed.

The system chooses to log statements randomly; the probability of
logging an execution is controlled by the
`statement_logging_sample_rate` configuration parameter. A value of 0 means
to log nothing; a value of 0.8 means to log approximately 80% of
statement executions. If `statement_logging_sample_rate` is higher
than `statement_logging_max_sample_rate` (which is set by Materialize
and cannot be changed by users), the latter is used instead.

- **`id`**: [`uuid`] | The ID of the execution event.
- **`prepared_statement_id`**: [`uuid`] | The ID of the prepared statement being executed. Corresponds to [`mz_prepared_statement_history.id`](#mz_prepared_statement_history).
- **`sample_rate`**: [`double precision`] | The sampling rate at the time the execution began.
- **`params`**: [`text list`] | The values of the prepared statement's parameters.
- **`began_at`**: [`timestamp with time zone`] | The time at which execution began.
- **`finished_at`**: [`timestamp with time zone`] | The time at which execution ended.
- **`finished_status`**: [`text`] | `'success'`, `'error'`, `'canceled'`, or `'aborted'`. `'aborted'` means that the database restarted (e.g., due to a crash or planned maintenance) before the query finished.
- **`error_message`**: [`text`] | The error returned when executing the statement, or `NULL` if it was successful, canceled or aborted.
- **`result_size`**: [`bigint`] | The size in bytes of the result, for statements that return rows.
- **`rows_returned`**: [`int8`] | The number of rows returned by the statement, if it finished successfully and was of a kind of statement that can return rows, or `NULL` otherwise.
- **`execution_strategy`**: [`text`] | `'standard'`, `'fast-path'` `'constant'`, or `NULL`. `'standard'` means a dataflow was built on a cluster to compute the result. `'fast-path'` means a cluster read the result from an existing arrangement. `'constant'` means the result was computed in the serving layer, without involving a cluster.
-->

## `mz_statement_lifecycle_history`

<!-- RELATION_SPEC mz_internal.mz_statement_lifecycle_history -->
| Field          | Type                         | Meaning                                                                                                                                                |
|----------------|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `statement_id` | [`uuid`]                     | The ID of the execution event. Corresponds to [`mz_recent_activity_log.execution_id`](#mz_recent_activity_log)                                         |
| `event_type`   | [`text`]                     | The type of lifecycle event, e.g. `'execution-began'`, `'storage-dependencies-finished'`, `'compute-dependencies-finished'`, or `'execution-finished'` |
| `occurred_at`  | [`timestamp with time zone`] | The time at which the event took place.                                                                                                                |

## `mz_subscriptions`

The `mz_subscriptions` table describes all active [`SUBSCRIBE`](/sql/subscribe)
operations in the system.

<!-- RELATION_SPEC mz_internal.mz_subscriptions -->
| Field                    | Type                         | Meaning                                                                                                                    |
| ------------------------ |------------------------------| --------                                                                                                                   |
| `id`                     | [`text`]                     | The ID of the subscription.                                                                                                |
| `session_id`             | [`uuid`]                     | The ID of the session that runs the subscription. Corresponds to [`mz_sessions.id`](#mz_sessions).                         |
| `cluster_id`             | [`text`]                     | The ID of the cluster on which the subscription is running. Corresponds to [`mz_clusters.id`](../mz_catalog/#mz_clusters). |
| `created_at`             | [`timestamp with time zone`] | The time at which the subscription was created.                                                                            |
| `referenced_object_ids`  | [`text list`]                | The IDs of objects referenced by the subscription. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects)             |

## `mz_wallclock_global_lag`

The `mz_wallclock_global_lag` view contains the most recent wallclock lag for tables, sources, indexes, materialized views, and sinks.
Wallclock lag measures how far behind real-world wall-clock time each
  object's data is, indicating the [freshness](/concepts/reaction-time/#freshness) of results when querying that object.

<!-- RELATION_SPEC mz_internal.mz_wallclock_global_lag -->
| Field         | Type         | Meaning
| --------------| -------------| --------
| `object_id`   | [`text`]     | The ID of the table, source, materialized view, index, or sink. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects).
| `lag`         | [`interval`] | The amount of time the object's write frontier lags behind wallclock time.

## `mz_wallclock_lag_history`

The `mz_wallclock_lag_history` table records the historical wallclock lag,
i.e., the [freshness](/concepts/reaction-time/#freshness), for each table, source, index, materialized view, and sink in the system.

<!-- RELATION_SPEC mz_internal.mz_wallclock_lag_history -->
| Field         | Type         | Meaning
| --------------| -------------| --------
| `object_id`   | [`text`]     | The ID of the table, source, materialized view, index, or sink. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects).
| `replica_id`  | [`text`]     | The ID of a replica computing the object, or `NULL` for persistent objects. Corresponds to [`mz_cluster_replicas.id`](../mz_catalog/#mz_cluster_replicas).
| `lag`         | [`interval`] | The amount of time the object's write frontier lags behind wallclock time.
| `occurred_at` | [`timestamp with time zone`] | Wall-clock timestamp at which the event occurred.

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_wallclock_global_lag_history -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_wallclock_global_lag_recent_history -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_wallclock_global_lag_histogram -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_wallclock_global_lag_histogram_raw -->

## `mz_webhook_sources`

The `mz_webhook_sources` table contains a row for each webhook source in the system.

<!-- RELATION_SPEC mz_internal.mz_webhook_sources -->
| Field          | Type        | Meaning                                                                                      |
| -------------- |-------------| --------                                                                                     |
| `id`           | [`text`]    | The ID of the webhook source. Corresponds to [`mz_sources.id`](../mz_catalog/#mz_sources).   |
| `name`         | [`text`]    | The name of the webhook source.                                                              |
| `url`          | [`text`]    | The URL which can be used to send events to the source.                                      |

[`bigint`]: /sql/types/bigint
[`boolean`]: /sql/types/boolean
[`bytea`]: /sql/types/bytea
[`double precision`]: /sql/types/double-precision
[`integer`]: /sql/types/integer
[`interval`]: /sql/types/interval
[`jsonb`]: /sql/types/jsonb
[`mz_timestamp`]: /sql/types/mz_timestamp
[`mz_aclitem`]: /sql/types/mz_aclitem
[`mz_aclitem array`]: /sql/types/mz_aclitem
[`numeric`]: /sql/types/numeric
[`oid`]: /sql/types/oid
[`text`]: /sql/types/text
[`text array`]: /sql/types/array
[`text list`]: /sql/types/list
[`uuid`]: /sql/types/uuid
[`uint4`]: /sql/types/uint4
[`uint8`]: /sql/types/uint8
[`uint8 list`]: /sql/types/list
[`timestamp with time zone`]: /sql/types/timestamp

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_activity_log_thinned -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_cluster_workload_classes -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_error_counts_raw_unified -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_continual_tasks -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_recent_activity_log_redacted -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_recent_activity_log_thinned -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_aggregates -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_prepared_statement_history -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_recent_sql_text -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_recent_sql_text_redacted -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_replacements -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_all_objects -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_clusters -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_cluster_replicas -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_columns -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_connections -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_continual_tasks -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_indexes -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_materialized_views -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_network_policies -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_roles -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_schemas -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_secrets -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_sinks -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_sources -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_tables -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_types -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_views -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_sql_text -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_sql_text_redacted -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_statement_execution_history -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_statement_execution_history_redacted -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_storage_shards -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_storage_usage_by_shard -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_type_pg_metadata -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_object_oid_alias -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_objects_id_namespace_types -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_console_cluster_utilization_overview -->


<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_class_all_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_type_all_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_namespace_all_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_description_all_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_attrdef_all_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_attribute_all_databases -->


---

## mz_introspection


The following sections describe the available objects in the `mz_introspection`
schema.

> **Warning:** 
The objects in the `mz_introspection` schema are not part of Materialize's stable interface.
Backwards-incompatible changes to these objects may be made at any time.


> **Warning:** 
`SELECT` statements may reference these objects, but creating views that
reference these objects is not allowed.


Introspection relations are maintained by independently collecting internal logging information within each of the replicas of a cluster.
Thus, in a multi-replica cluster, queries to these relations need to be directed to a specific replica by issuing the command `SET cluster_replica = <replica_name>`.
Note that once this command is issued, all subsequent `SELECT` queries, for introspection relations or not, will be directed to the targeted replica.
Replica targeting can be cancelled by issuing the command `RESET cluster_replica`.

For each of the below introspection relations, there exists also a variant with a `_per_worker` name suffix.
Per-worker relations expose the same data as their global counterparts, but have an extra `worker_id` column that splits the information by Timely Dataflow worker.

## `mz_active_peeks`

The `mz_active_peeks` view describes all read queries ("peeks") that are pending in the [dataflow] layer.

<!-- RELATION_SPEC mz_introspection.mz_active_peeks -->
| Field       | Type             | Meaning                                                                                                                                                                                                                                                                                                               |
|-------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `id`        | [`uuid`]         | The ID of the peek request.                                                                                                                                                                                                                                                                                           |
| `object_id` | [`text`]         | The ID of the collection the peek is targeting. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources), or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |
| `type`      | [`text`]         | The type of the corresponding peek: `index` if targeting an index or temporary dataflow; `persist` for a source, materialized view, or table.                                                                                                                                                                         |
| `time`      | [`mz_timestamp`] | The timestamp the peek has requested.                                                                                                                                                                                                                                                                                 |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_active_peeks_per_worker -->

## `mz_arrangement_sharing`

The `mz_arrangement_sharing` view describes how many times each [arrangement] in the system is used.

<!-- RELATION_SPEC mz_introspection.mz_arrangement_sharing -->
| Field          | Type       | Meaning                                                                                                                   |
| -------------- |------------| --------                                                                                                                  |
| `operator_id`  | [`uint8`]  | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `count`        | [`bigint`] | The number of operators that share the arrangement.                                                                       |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_sharing_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_sharing_raw -->

## `mz_arrangement_sizes`

The `mz_arrangement_sizes` view describes the size of each [arrangement] in the system.

The size, capacity, and allocations are an approximation, which may underestimate the actual size in memory.
Specifically, reductions can use more memory than we show here.

<!-- RELATION_SPEC mz_introspection.mz_arrangement_sizes -->
- **`operator_id`**: [`uint8`] | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
- **`records`**: [`bigint`] | The number of records in the arrangement.
- **`batches`**: [`bigint`] | The number of batches in the arrangement.
- **`size`**: [`bigint`] | The utilized size in bytes of the arrangement.
- **`capacity`**: [`bigint`] | The capacity in bytes of the arrangement. Can be larger than the size.
- **`allocations`**: [`bigint`] | The number of separate memory allocations backing the arrangement.

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_sizes_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_records_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_batcher_allocations_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_batcher_capacity_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_batcher_records_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_batcher_size_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_batches_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_heap_allocations_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_heap_capacity_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_heap_size_raw -->

## `mz_compute_error_counts`

The `mz_compute_error_counts` view describes the counts of errors in objects exported by [dataflows][dataflow] in the system.

Dataflow exports that don't have any errors are not included in this view.

<!-- RELATION_SPEC mz_introspection.mz_compute_error_counts -->
| Field        | Type        | Meaning                                                                                              |
| ------------ |-------------| --------                                                                                             |
| `export_id`  | [`text`]    | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `count`      | [`numeric`] | The count of errors present in this dataflow export.                                                 |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_error_counts_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_error_counts_raw -->

## `mz_compute_exports`

The `mz_compute_exports` view describes the objects exported by [dataflows][dataflow] in the system.

<!-- RELATION_SPEC mz_introspection.mz_compute_exports -->
| Field          | Type      | Meaning                                                                                                                                                                                                                                                                                        |
| -------------- |-----------| --------                                                                                                                                                                                                                                                                                       |
| `export_id`    | [`text`]  | The ID of the index, materialized view, or subscription exported by the dataflow. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), or [`mz_internal.mz_subscriptions`](../mz_internal#mz_subscriptions). |
| `dataflow_id`  | [`uint8`] | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).                                                                                                                                                                                                               |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_exports_per_worker -->

## `mz_compute_frontiers`

The `mz_compute_frontiers` view describes the frontier of each [dataflow] export in the system.
The frontier describes the earliest timestamp at which the output of the dataflow may change; data prior to that timestamp is sealed.

<!-- RELATION_SPEC mz_introspection.mz_compute_frontiers -->
| Field        | Type               | Meaning                                                                                              |
| ------------ | ------------------ | --------                                                                                             |
| `export_id`  | [`text`]           | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `time`       | [`mz_timestamp`]   | The next timestamp at which the dataflow output may change.                                          |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_frontiers_per_worker -->

## `mz_compute_import_frontiers`

The `mz_compute_import_frontiers` view describes the frontiers of each [dataflow] import in the system.
The frontier describes the earliest timestamp at which the input into the dataflow may change; data prior to that timestamp is sealed.

<!-- RELATION_SPEC mz_introspection.mz_compute_import_frontiers -->
| Field        | Type               | Meaning                                                                                                                                                                                                                |
| ------------ | ------------------ | --------                                                                                                                                                                                                               |
| `export_id`  | [`text`]           | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).                                                                                                                   |
| `import_id`  | [`text`]           | The ID of the dataflow import. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `time`       | [`mz_timestamp`]   | The next timestamp at which the dataflow input may change.                                                                                                                                                             |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_import_frontiers_per_worker -->

## `mz_compute_operator_durations_histogram`

The `mz_compute_operator_durations_histogram` view describes a histogram of the duration in nanoseconds of each invocation for each [dataflow] operator.

<!-- RELATION_SPEC mz_introspection.mz_compute_operator_durations_histogram -->
| Field          | Type        | Meaning                                                                                      |
| -------------- |-------------| --------                                                                                     |
| `id`           | [`uint8`]   | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `duration_ns`  | [`uint8`]   | The upper bound of the duration bucket in nanoseconds.                                       |
| `count`        | [`numeric`] | The (noncumulative) count of invocations in the bucket.                                      |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_operator_durations_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_operator_durations_histogram_raw -->

## `mz_dataflows`

The `mz_dataflows` view describes the [dataflows][dataflow] in the system.

<!-- RELATION_SPEC mz_introspection.mz_dataflows -->
| Field       | Type      | Meaning                                |
| ----------- |-----------| --------                               |
| `id`        | [`uint8`] | The ID of the dataflow.                |
| `name`      | [`text`]  | The internal name of the dataflow.     |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflows_per_worker -->

## `mz_dataflow_addresses`

The `mz_dataflow_addresses` view describes how the [dataflow] channels and operators in the system are nested into scopes.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_addresses -->
| Field        | Type            | Meaning                                                                                                                                                       |
| ------------ |-----------------| --------                                                                                                                                                      |
| `id`         | [`uint8`]       | The ID of the channel or operator. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels) or [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `address`    | [`bigint list`] | A list of scope-local indexes indicating the path from the root to this channel or operator.                                                                  |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_addresses_per_worker -->

## `mz_dataflow_arrangement_sizes`

The `mz_dataflow_arrangement_sizes` view describes the size of arrangements per
operators under each dataflow.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_arrangement_sizes -->
- **`id`**: [`uint8`] | The ID of the [dataflow]. Corresponds to [`mz_dataflows.id`](#mz_dataflows).
- **`name`**: [`text`] | The name of the [dataflow].
- **`records`**: [`bigint`] | The number of records in all arrangements in the dataflow.
- **`batches`**: [`bigint`] | The number of batches in all arrangements in the dataflow.
- **`size`**: [`bigint`] | The utilized size in bytes of the arrangements.
- **`capacity`**: [`bigint`] | The capacity in bytes of the arrangements. Can be larger than the size.
- **`allocations`**: [`bigint`] | The number of separate memory allocations backing the arrangements.

## `mz_dataflow_channels`

The `mz_dataflow_channels` view describes the communication channels between [dataflow] operators.
A communication channel connects one of the outputs of a source operator to one of the inputs of a target operator.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_channels -->
- **`id`**: [`uint8`] | The ID of the channel.
- **`from_index`**: [`uint8`] | The scope-local index of the source operator. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses).
- **`from_port`**: [`uint8`] | The source operator's output port.
- **`to_index`**: [`uint8`] | The scope-local index of the target operator. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses).
- **`to_port`**: [`uint8`] | The target operator's input port.
- **`type`**: [`text`] | The container type of the channel.

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_channels_per_worker -->

## `mz_dataflow_channel_operators`

The `mz_dataflow_channel_operators` view associates [dataflow] channels with the operators that are their endpoints.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_channel_operators -->
- **`id`**: [`uint8`] | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels).
- **`from_operator_id`**: [`uint8`] | The ID of the source of the channel. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
- **`from_operator_address`**: [`uint8 list`] | The address of the source of the channel. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses).
- **`to_operator_id`**: [`uint8`] | The ID of the target of the channel. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
- **`to_operator_address`**: [`uint8 list`] | The address of the target of the channel. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses).
- **`type`**: [`text`] | The container type of the channel.

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_channel_operators_per_worker -->

## `mz_dataflow_global_ids`

The `mz_dataflow_global_ids` view associates [dataflow] ids with global ids (ids of the form `u8` or `t5`).

<!-- RELATION_SPEC mz_introspection.mz_dataflow_global_ids -->

| Field        | Type      | Meaning                                    |
|------------- | -------   | --------                                   |
| `id`         | [`uint8`] | The dataflow ID.                           |
| `global_id`  | [`text`]  | A global ID associated with that dataflow. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_dataflow_global_ids_per_worker -->

## `mz_dataflow_operators`

The `mz_dataflow_operators` view describes the [dataflow] operators in the system.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_operators -->
| Field        | Type      | Meaning                            |
| ------------ |-----------| --------                           |
| `id`         | [`uint8`] | The ID of the operator.            |
| `name`       | [`text`]  | The internal name of the operator. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operators_per_worker -->

## `mz_dataflow_operator_dataflows`

The `mz_dataflow_operator_dataflows` view describes the [dataflow] to which each operator belongs.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_operator_dataflows -->
| Field            | Type      | Meaning                                                                                         |
| ---------------- |-----------| --------                                                                                        |
| `id`             | [`uint8`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).    |
| `name`           | [`text`]  | The internal name of the operator.                                                              |
| `dataflow_id`    | [`uint8`] | The ID of the dataflow hosting the operator. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `dataflow_name`  | [`text`]  | The internal name of the dataflow hosting the operator.                                         |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_dataflows_per_worker -->

## `mz_dataflow_operator_parents`

The `mz_dataflow_operator_parents` view describes how [dataflow] operators are nested into scopes, by relating operators to their parent operators.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_operator_parents -->
| Field        | Type      | Meaning                                                                                                        |
| ------------ |-----------| --------                                                                                                       |
| `id`         | [`uint8`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).                   |
| `parent_id`  | [`uint8`] | The ID of the operator's parent operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_parents_per_worker -->

## `mz_dataflow_shutdown_durations_histogram`

The `mz_dataflow_shutdown_durations_histogram` view describes a histogram of the time in nanoseconds required to fully shut down dropped [dataflows][dataflow].

<!-- RELATION_SPEC mz_introspection.mz_dataflow_shutdown_durations_histogram -->
| Field          | Type        | Meaning                                                |
| -------------- |-------------| --------                                               |
| `duration_ns`  | [`uint8`]   | The upper bound of the bucket in nanoseconds.          |
| `count`        | [`numeric`] | The (noncumulative) count of dataflows in this bucket. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_shutdown_durations_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_shutdown_durations_histogram_raw -->

## `mz_expected_group_size_advice`

The `mz_expected_group_size_advice` view provides advice on opportunities to set [query hints].
Query hints are applicable to dataflows maintaining [`MIN`], [`MAX`], or [Top K] query patterns.
The maintainance of these query patterns is implemented inside an operator scope, called a region,
through a hierarchical scheme for either aggregation or Top K computations.

<!-- RELATION_SPEC mz_introspection.mz_expected_group_size_advice -->
- **`dataflow_id`**: [`uint8`] | The ID of the [dataflow]. Corresponds to [`mz_dataflows.id`](#mz_dataflows).
- **`dataflow_name`**: [`text`] | The internal name of the dataflow hosting the min/max aggregation or Top K.
- **`region_id`**: [`uint8`] | The ID of the root operator scope. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
- **`region_name`**: [`text`] | The internal name of the root operator scope for the min/max aggregation or Top K.
- **`levels`**: [`bigint`] | The number of levels in the hierarchical scheme implemented by the region.
- **`to_cut`**: [`bigint`] | The number of levels that can be eliminated (cut) from the region's hierarchy.
- **`savings`**: [`numeric`] | A conservative estimate of the amount of memory in bytes to be saved by applying the hint.
- **`hint`**: [`double precision`] | The hint value that will eliminate `to_cut` levels from the region's hierarchy.

## `mz_mappable_objects`

The `mz_mappable_objects` identifies indexes (and their underlying views) and materialized views which can be debugged using the [`mz_lir_mapping`](#mz_lir_mapping) view.

<!-- RELATION_SPEC mz_introspection.mz_mappable_objects -->
| Field        | Type      | Meaning
| ------------ | --------  | -----------
| `name`       | [`text`]  | The name of the object.
| `global_id`  | [`text`]  | The global ID of the object.

See [Which part of my query runs slowly or uses a lot of memory?](/transform-data/troubleshooting/#which-part-of-my-query-runs-slowly-or-uses-a-lot-of-memory) for examples of debugging with `mz_mappable_objects` and `mz_lir_mapping`.

## `mz_lir_mapping`

The `mz_lir_mapping` view describes the low-level internal representation (LIR) plan that corresponds to global ids of indexes (and their underlying views) and materialized views.
You can find a list of all debuggable objects in [`mz_mappable_objects`](#mz_mappable_objects).
LIR is a higher-level representation than dataflows; this view is used for profiling and debugging indices and materialized views.
Note that LIR is not a stable interface and may change at any time.
In particular, you should not attempt to parse `operator` descriptions.
LIR nodes are implemented by zero or more dataflow operators with sequential ids.
We use the range `[operator_id_start, operator_id_end)` to record this information.
If an LIR node was implemented without any dataflow operators, `operator_id_start` will be equal to `operator_id_end`.

<!-- RELATION_SPEC mz_introspection.mz_lir_mapping -->
| Field             | Type      | Meaning
| ---------         | --------  | -----------
| global_id         | [`text`]  | The global ID.
| lir_id            | [`uint8`] | The LIR node ID.
| operator          | [`text`]  | The LIR operator, in the format `OperatorName INPUTS [OPTIONS]`.
| parent_lir_id     | [`uint8`] | The parent of this LIR node. May be `NULL`.
| nesting           | [`uint2`] | The nesting level of this LIR node.
| operator_id_start | [`uint8`] | The first dataflow operator ID implementing this LIR operator (inclusive).
| operator_id_end   | [`uint8`] | The first dataflow operator ID _after_ this LIR operator (exclusive).

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_lir_mapping_per_worker -->

## `mz_message_counts`

The `mz_message_counts` view describes the messages and message batches sent and received over the [dataflow] channels in the system.
It distinguishes between individual records (`sent`, `received`) and batches of records (`batch_sent`, `batch_sent`).

<!-- RELATION_SPEC mz_introspection.mz_message_counts -->
| Field              | Type        | Meaning                                                                                   |
| ------------------ |-------------| --------                                                                                  |
| `channel_id`       | [`uint8`]   | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels). |
| `sent`             | [`numeric`] | The number of messages sent.                                                              |
| `received`         | [`numeric`] | The number of messages received.                                                          |
| `batch_sent`       | [`numeric`] | The number of batches sent.                                                               |
| `batch_received`   | [`numeric`] | The number of batches received.                                                           |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_counts_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_batch_counts_received_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_batch_counts_sent_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_counts_received_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_counts_sent_raw -->

## `mz_peek_durations_histogram`

The `mz_peek_durations_histogram` view describes a histogram of the duration in nanoseconds of read queries ("peeks") in the [dataflow] layer.

<!-- RELATION_SPEC mz_introspection.mz_peek_durations_histogram -->
| Field         | Type        | Meaning                                            |
|---------------|-------------|----------------------------------------------------|
| `type`        | [`text`]    | The peek variant: `index` or `persist`.            |
| `duration_ns` | [`uint8`]   | The upper bound of the bucket in nanoseconds.      |
| `count`       | [`numeric`] | The (noncumulative) count of peeks in this bucket. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_peek_durations_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_peek_durations_histogram_raw -->

## `mz_records_per_dataflow`

The `mz_records_per_dataflow` view describes the number of records in each [dataflow].

<!-- RELATION_SPEC mz_introspection.mz_records_per_dataflow -->
- **`id`**: [`uint8`] | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).
- **`name`**: [`text`] | The internal name of the dataflow.
- **`records`**: [`bigint`] | The number of records in the dataflow.
- **`batches`**: [`bigint`] | The number of batches in the dataflow.
- **`size`**: [`bigint`] | The utilized size in bytes of the arrangements.
- **`capacity`**: [`bigint`] | The capacity in bytes of the arrangements. Can be larger than the size.
- **`allocations`**: [`bigint`] | The number of separate memory allocations backing the arrangements.

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_records_per_dataflow_per_worker -->

## `mz_records_per_dataflow_operator`

The `mz_records_per_dataflow_operator` view describes the number of records in each [dataflow] operator in the system.

<!-- RELATION_SPEC mz_introspection.mz_records_per_dataflow_operator -->
- **`id`**: [`uint8`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
- **`name`**: [`text`] | The internal name of the operator.
- **`dataflow_id`**: [`uint8`] | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).
- **`records`**: [`bigint`] | The number of records in the operator.
- **`batches`**: [`bigint`] | The number of batches in the dataflow.
- **`size`**: [`bigint`] | The utilized size in bytes of the arrangement.
- **`capacity`**: [`bigint`] | The capacity in bytes of the arrangement. Can be larger than the size.
- **`allocations`**: [`bigint`] | The number of separate memory allocations backing the arrangement.

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_records_per_dataflow_operator_per_worker -->

## `mz_scheduling_elapsed`

The `mz_scheduling_elapsed` view describes the total amount of time spent in each [dataflow] operator.

<!-- RELATION_SPEC mz_introspection.mz_scheduling_elapsed -->
| Field         | Type        | Meaning                                                                                      |
| ------------- |-------------| --------                                                                                     |
| `id`          | [`uint8`]   | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `elapsed_ns`  | [`numeric`] | The total elapsed time spent in the operator in nanoseconds.                                 |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_elapsed_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_elapsed_raw -->

## `mz_scheduling_parks_histogram`

The `mz_scheduling_parks_histogram` view describes a histogram of [dataflow] worker park events. A park event occurs when a worker has no outstanding work.

<!-- RELATION_SPEC mz_introspection.mz_scheduling_parks_histogram -->
| Field           | Type        | Meaning                                                  |
| --------------- |-------------| -------                                                  |
| `slept_for_ns`  | [`uint8`]   | The actual length of the park event in nanoseconds.      |
| `requested_ns`  | [`uint8`]   | The requested length of the park event in nanoseconds.   |
| `count`         | [`numeric`] | The (noncumulative) count of park events in this bucket. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_parks_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_parks_histogram_raw -->

[`bigint`]: /sql/types/bigint
[`bigint list`]: /sql/types/list
[`double precision`]: /sql/types/double-precision
[`mz_timestamp`]: /sql/types/mz_timestamp
[`numeric`]: /sql/types/numeric
[`text`]: /sql/types/text
[`uuid`]: /sql/types/uuid
[`uint2`]: /sql/types/uint2
[`uint8`]: /sql/types/uint8
[`uint8 list`]: /sql/types/list
[arrangement]: /get-started/arrangements/#arrangements
[dataflow]: /get-started/arrangements/#dataflows
[`MIN`]: /sql/functions/#min
[`MAX`]: /sql/functions/#max
[Top K]: /transform-data/patterns/top-k
[query hints]: /sql/select/#query-hints

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_hydration_times_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_operator_hydration_statuses_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_reachability -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_reachability_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_reachability_raw -->


---

## pg_catalog


Materialize has compatibility shims for the following relations from [PostgreSQL's
system catalog](https://www.postgresql.org/docs/current/catalogs.html):

  * [`pg_aggregate`](https://www.postgresql.org/docs/current/catalog-pg-aggregate.html)
  * [`pg_am`](https://www.postgresql.org/docs/current/catalog-pg-am.html)
  * [`pg_attribute`](https://www.postgresql.org/docs/current/catalog-pg-attribute.html)
  * [`pg_auth_members`](https://www.postgresql.org/docs/current/catalog-pg-auth-members.html)
  * [`pg_authid`](https://www.postgresql.org/docs/current/catalog-pg-authid.html)
  * [`pg_class`](https://www.postgresql.org/docs/current/catalog-pg-class.html)
  * [`pg_collation`](https://www.postgresql.org/docs/current/catalog-pg-collation.html)
  * [`pg_constraint`](https://www.postgresql.org/docs/current/catalog-pg-constraint.html)
  * [`pg_depend`](https://www.postgresql.org/docs/current/catalog-pg-depend.html)
  * [`pg_database`](https://www.postgresql.org/docs/current/catalog-pg-database.html)
  * [`pg_description`](https://www.postgresql.org/docs/current/catalog-pg-description.html)
  * [`pg_enum`](https://www.postgresql.org/docs/current/catalog-pg-enum.html)
  * [`pg_event_trigger`](https://www.postgresql.org/docs/current/catalog-pg-event-trigger.html)
  * [`pg_extension`](https://www.postgresql.org/docs/current/catalog-pg-extension.html)
  * [`pg_index`](https://www.postgresql.org/docs/current/catalog-pg-index.html)
  * [`pg_indexes`](https://www.postgresql.org/docs/current/view-pg-indexes.html)
  * [`pg_inherits`](https://www.postgresql.org/docs/current/catalog-pg-inherits.html)
  * [`pg_language`](https://www.postgresql.org/docs/current/catalog-pg-language.html)
  * [`pg_locks`](https://www.postgresql.org/docs/current/view-pg-locks.html)
  * [`pg_matviews`](https://www.postgresql.org/docs/current/view-pg-matviews.html)
  * [`pg_namespace`](https://www.postgresql.org/docs/current/catalog-pg-namespace.html)
  * [`pg_policy`](https://www.postgresql.org/docs/current/catalog-pg-policy.html)
  * [`pg_proc`](https://www.postgresql.org/docs/current/catalog-pg-proc.html)
  * [`pg_range`](https://www.postgresql.org/docs/current/catalog-pg-range.html)
  * [`pg_rewrite`](https://www.postgresql.org/docs/current/catalog-pg-rewrite.html)
  * [`pg_roles`](https://www.postgresql.org/docs/current/view-pg-roles.html)
  * [`pg_settings`](https://www.postgresql.org/docs/current/view-pg-settings.html)
  * [`pg_shdescription`](https://www.postgresql.org/docs/current/catalog-pg-shdescription.html)
  * [`pg_tables`](https://www.postgresql.org/docs/current/view-pg-tables.html)
  * [`pg_tablespace`](https://www.postgresql.org/docs/current/catalog-pg-tablespace.html)
  * [`pg_timezone_abbrevs`](https://www.postgresql.org/docs/current/view-pg-timezone-abbrevs.html)
  * [`pg_timezone_names`](https://www.postgresql.org/docs/current/view-pg-timezone-names.html)
  * [`pg_trigger`](https://www.postgresql.org/docs/current/catalog-pg-trigger.html)
  * [`pg_type`](https://www.postgresql.org/docs/current/catalog-pg-type.html)
  * [`pg_user`](https://www.postgresql.org/docs/current/view-pg-user.html)
  * [`pg_views`](https://www.postgresql.org/docs/current/view-pg-views.html)

These compatibility shims are largely incomplete. Most are lacking some columns
that are present in PostgreSQL, or if they do include the column the result set
its value may always be `NULL`. The precise nature of the incompleteness is
intentionally undocumented. New tools developed against Materialize should use
the documented [`mz_catalog`](../mz_catalog) API instead.

If you are having trouble making a PostgreSQL tool work with Materialize, please
[file a GitHub issue][gh-issue]. Many PostgreSQL tools can be made to work with
Materialize with minor changes to the `pg_catalog` compatibility shim.

[gh-issue]: https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration