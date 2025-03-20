---
title: "mz_catalog"
description: "mz_catalog is a system catalog that exposes metadata in Materialize's native format."
menu:
  main:
    parent: 'system-catalog'
    weight: 1
---

The following sections describe the available relations in the `mz_catalog`
schema. These relations contain metadata about objects in Materialize,
including descriptions of each database, schema, source, table, view, sink, and
index in the system.

{{< warning >}}
Views that directly reference these objects cannot include `NATURAL JOIN` or
`*` expressions. Instead, project the required columns and convert all `NATURAL JOIN`s
to `USING` joins.
{{< /warning >}}

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

{{< warn-if-unreleased "v0.118" >}}

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

{{< warning >}}
The values in this table may change at any time. You should not rely on them for
any kind of capacity planning.
{{< /warning >}}

<!-- RELATION_SPEC mz_catalog.mz_cluster_replica_sizes -->
| Field                  | Type        | Meaning                                                                                                                                                      |
|------------------------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `size`                 | [`text`]    | The human-readable replica size.                                                                                                                             |
| `processes`            | [`uint8`]   | The number of processes in the replica.                                                                                                                      |
| `workers`              | [`uint8`]   | The number of Timely Dataflow workers per process.                                                                                                           |
| `cpu_nano_cores`       | [`uint8`]   | The CPU allocation per process, in billionths of a vCPU core.                                                                                                |
| `memory_bytes`         | [`uint8`]   | The RAM allocation per process, in billionths of a vCPU core.                                                                                                |
| `disk_bytes`           | [`uint8`]   | The disk allocation per process.                                                                                                                             |
| `credits_per_hour`     | [`numeric`] | The number of compute credits consumed per hour.                                                                                                             |

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
| Field                | Type                 | Meaning                                                                                                                                  |
|----------------------|----------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `id`                      | [`text`]             | Materialize's unique ID for the cluster.                                                                                                 |
| `name`                    | [`text`]             | The name of the cluster.                                                                                                                 |
| `owner_id`                | [`text`]             | The role ID of the owner of the cluster. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).                       |
| `privileges`              | [`mz_aclitem array`] | The privileges belonging to the cluster.                                                                                                 |
| `managed`                 | [`boolean`]          | Whether the cluster is a [managed cluster](/sql/create-cluster/) with automatically managed replicas.                                    |
| `size`                    | [`text`]             | If the cluster is managed, the desired size of the cluster's replicas. `NULL` for unmanaged clusters.                                    |
| `replication_factor`      | [`uint4`]            | If the cluster is managed, the desired number of replicas of the cluster. `NULL` for unmanaged clusters.                                 |
| `disk`                    | [`boolean`]          | **Unstable** If the cluster is managed, `true` if the replicas have the `DISK` option . `NULL` for unmanaged clusters.                   |
| `availability_zones`      | [`text list`]        | **Unstable** If the cluster is managed, the list of availability zones specified in `AVAILABILITY ZONES`. `NULL` for unmanaged clusters. |
| `introspection_debugging` | [`boolean`]          | Whether introspection of the gathering of the introspection data is enabled.                                                             |
| `introspection_interval`  | [`interval`]         | The interval at which to collect introspection data.                                                                                     |

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

{{< warn-if-unreleased v0.115 >}}
The `mz_kafka_sources` table contains a row for each Kafka source in the system.

This table was previously in the `mz_internal` schema. All queries previously referencing
`mz_internal.mz_kafka_sources` should now reference `mz_catalog.mz_kafka_sources`.

<!-- RELATION_SPEC mz_catalog.mz_kafka_sources -->
| Field                  | Type           | Meaning                                                                                                   |
|------------------------|----------------|-----------------------------------------------------------------------------------------------------------|
| `id`                   | [`text`]       | The ID of the Kafka source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).        |
| `group_id_prefix`      | [`text`]       | The value of the `GROUP ID PREFIX` connection option.                                                     |
| `topic          `      | [`text`]       | The name of the Kafka topic the source is reading from.                                                              |


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

{{< warn-if-unreleased "v0.113" >}}

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

### `mz_role_members`

The `mz_role_members` table contains a row for each role membership in the
system.

<!-- RELATION_SPEC mz_catalog.mz_role_members -->
Field     | Type       | Meaning
----------|------------|--------
`role_id` | [`text`]   | The ID of the role the `member` is a member of. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`member`  | [`text`]   | The ID of the role that is a member of `role_id`. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`grantor` | [`text`]   | The ID of the role that granted membership of `member` to `role_id`. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).

### `mz_role_parameters`

The `mz_role_parameters` table contains a row for each configuration parameter
whose default value has been altered for a given role. See [`ALTER ROLE ... SET`](/sql/alter-role/#alter_role_set)
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

{{< warning >}}
This view is not indexed in the `mz_catalog_server` cluster. Querying this view
can be slow due to the amount of unindexed data that must be scanned.
{{< /warning >}}

The `mz_storage_usage` table describes the historical storage utilization of
each table, source, and materialized view in the system. Storage utilization is
assessed approximately every hour.

{{< if-released "v0.111" >}}
Consider querying
[`mz_catalog.mz_recent_storage_usage`](#mz_recent_storage_usage)
instead if you are interested in only the most recent storage usage information.
{{< /if-released >}}

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
