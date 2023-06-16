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

Field          | Type       | Meaning
---------------|------------|--------
`id`           | [`text`]   | The ID of the array type.
`element_id`   | [`text`]   | The ID of the array's element type.

### `mz_audit_events`

The `mz_audit_events` table records create, alter, and drop events for the
other objects in the system catalog.

Field           | Type                         | Meaning
----------------|------------------------------|--------
`id  `          | [`uint8`]                    | Materialize's unique, monotonically increasing ID for the event.
`event_type`    | [`text`]                     | The type of the event: `create`, `drop`, or `alter`.
`object_type`   | [`text`]                     | The type of the affected object: `cluster`, `cluster-replica`, `connection`, `database`, `function`, `index`, `materialized-view`, `role`, `schema`, `secret`, `sink`, `source`, `table`, `type`, or `view`.
`details`       | [`jsonb`]                    | Additional details about the event. The shape of the details varies based on `event_type` and `object_type`.
`user`          | [`text`]                     | The user who triggered the event, or `NULL` if triggered by the system.
`occurred_at`   | [`timestamp with time zone`] | The time at which the event occurred. Guaranteed to be in order of event creation. Events created in the same transaction will have identical values.

### `mz_aws_privatelink_connections`

The `mz_aws_privatelink_connections` table contains a row for each AWS PrivateLink connection in the system.

Field       | Type      | Meaning
------------|-----------|--------
`id`        | [`text`]  | The ID of the connection.
`principal` | [`text`]  | The AWS Principal that Materialize will use to connect to the VPC endpoint.

### `mz_base_types`

The `mz_base_types` table contains a row for each base type in the system.

Field          | Type       | Meaning
---------------|------------|----------
`id`           | [`text`]   | The ID of the type.

### `mz_cluster_replicas`

The `mz_cluster_replicas` table contains a row for each cluster replica in the system.

Field               | Type      | Meaning
--------------------|-----------|--------
`id`                | [`text`]  | Materialize's unique ID for the cluster replica.
`name`              | [`text`]  | The name of the cluster replica.
`cluster_id`        | [`text`]  | The ID of the cluster to which the replica belongs. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters).
`size`              | [`text`]  | The cluster replica's size, selected during creation.
`availability_zone` | [`text`]  | The availability zone in which the cluster is running.
`owner_id`          | [`text`]  | The role ID of the owner of the cluster replica. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).

### `mz_clusters`

The `mz_clusters` table contains a row for each cluster in the system.

| Field                | Type                 | Meaning                                                                                                            |
|----------------------|----------------------|--------------------------------------------------------------------------------------------------------------------|
| `id`                 | [`text`]             | Materialize's unique ID for the cluster.                                                                           |
| `name`               | [`text`]             | The name of the cluster.                                                                                           |
| `owner_id`           | [`text`]             | The role ID of the owner of the cluster. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges`         | [`mz_aclitem array`] | The privileges belonging to the cluster.                                                                           |
| `managed`            | [`boolean`]          | Whether the cluster has automatically managed replicas.                                                            |
| `size`               | [`text`]             | If the cluster is managed, the desired size of the cluster's replicas. If the cluster is unmanaged, `NULL`.        |
| `replication_factor` | [`uint4`]            | If the cluster is managed, the desired number of replicas of the cluster. If the cluster is unmanaged, `NULL`.     |

### `mz_columns`

The `mz_columns` contains a row for each column in each table, source, and view
in the system.

Field            | Type        | Meaning
-----------------|-------------|--------
`id`             | [`uint8`]   | The unique ID of the table, source, or view containing the column.
`name`           | [`text`]    | The name of the column.
`position`       | [`uint8`]   | The 1-indexed position of the column in its containing table, source, or view.
`nullable`       | [`boolean`] | Can the column contain a `NULL` value?
`type`           | [`text`]    | The data type of the column.
`default`        | [`text`]    | The default expression of the column.
`type_oid`       | [`oid`]     | The OID of the type of the column (references `mz_types`).

### `mz_connections`

The `mz_connections` table contains a row for each connection in the system.

Field        | Type                 | Meaning
-------------|----------------------|--------
`id`         | [`text`]             | The unique ID of the connection.
`oid`        | [`oid`]              | A [PostgreSQL-compatible OID][oid] for the connection.
`schema_id`  | [`uint8`]            | The ID of the schema to which the connection belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`       | [`text`]             | The name of the connection.
`type`       | [`text`]             | The type of the connection: `confluent-schema-registry`, `kafka`, `postgres`, or `ssh-tunnel`.
`owner_id`   | [`text`]             | The role ID of the owner of the connection. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges` | [`mz_aclitem array`] | The privileges belonging to the connection.

### `mz_databases`

The `mz_databases` table contains a row for each database in the system.

Field       | Type                 | Meaning
------------|----------------------|--------
`id`        | [`uint8`]            | Materialize's unique ID for the database.
`oid`       | [`oid`]              | A [PostgreSQL-compatible OID][oid] for the database.
`name`      | [`text`]             | The name of the database.
`owner_id`  | [`text`]             | The role ID of the owner of the database. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`| [`mz_aclitem array`] | The privileges belonging to the database.

### `mz_default_privileges`

The `mz_default_privileges` table contains information on default privileges
that will be applied to new objects when they are created.

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

Field       | Type     | Meaning
------------|----------|--------
`egress_ip` | [`text`] | The IP address.

### `mz_functions`

The `mz_functions` table contains a row for each function in the system.

Field                       | Type           | Meaning
----------------------------|----------------|--------
`id`                        | [`text`]       | Materialize's unique ID for the function.
`oid`                       | [`oid`]        | A [PostgreSQL-compatible OID][oid] for the function.
`schema_id`                 | [`uint8`]      | The ID of the schema to which the function belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`                      | [`text`]       | The name of the function.
`argument_type_ids`         | [`text array`] | The ID of each argument's type. Each entry refers to `mz_types.id`.
`variadic_argument_type_id` | [`text`]       | The ID of the variadic argument's type, or `NULL` if the function does not have a variadic argument. Refers to `mz_types.id`.
`return_type_id`            | [`text`]       | The returned value's type, or `NULL` if the function does not return a value. Refers to `mz_types.id`. Note that for table functions with > 1 column, this type corresponds to [`record`].
`returns_set`               | [`boolean`]    | Whether the function returns a set, i.e. the function is a table function.
`owner_id`                  | [`text`]       | The role ID of the owner of the function. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).

### `mz_indexes`

The `mz_indexes` table contains a row for each index in the system.

Field        | Type        | Meaning
-------------|-------------|--------
`id`         | [`text`]    | Materialize's unique ID for the index.
`oid`        | [`oid`]     | A [PostgreSQL-compatible OID][oid] for the index.
`name`       | [`text`]    | The name of the index.
`on_id`      | [`text`]    | The ID of the relation on which the index is built.
`owner_id`   | [`text`]    | The role ID of the owner of the index. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).

### `mz_index_columns`

The `mz_index_columns` table contains a row for each column in each index in the
system. For example, an index on `(a, b + 1)` would have two rows in this table,
one for each of the two columns in the index.

For a given row, if `field_number` is null then `expression` will be nonnull, or
vice-versa.

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

Field                 | Type           | Meaning
----------------------|----------------|--------
`id`                  | [`text`]       | The ID of the connection.
`brokers`             | [`text array`] | The addresses of the Kafka brokers to connect to.
`sink_progress_topic` | [`text`]       | The name of the Kafka topic where any sinks associated with this connection will track their progress information and other metadata. The contents of this topic are unspecified.

### `mz_kafka_sinks`

The `mz_kafka_sinks` table contains a row for each Kafka sink in the system.

Field                | Type     | Meaning
---------------------|----------|--------
`id`                 | [`text`] | The ID of the sink.
`topic`              | [`text`] | The name of the Kafka topic into which the sink is writing.

### `mz_list_types`

The `mz_list_types` table contains a row for each list type in the system.

Field        | Type     | Meaning
-------------|----------|--------
`id`         | [`text`] | The ID of the list type.
`element_id` | [`text`] | The IID of the list's element type.

### `mz_map_types`

The `mz_map_types` table contains a row for each map type in the system.

Field          | Type       | Meaning
---------------|------------|----------
`id`           | [`text`]   | The ID of the map type.
`key_id `      | [`text`]   | The ID of the map's key type.
`value_id`     | [`text`]   | The ID of the map's value type.

### `mz_materialized_views`

The `mz_materialized_views` table contains a row for each materialized view in the system.

Field          | Type                 | Meaning
---------------|----------------------|----------
`id`           | [`text`]             | Materialize's unique ID for the materialized view.
`oid`          | [`oid`]              | A [PostgreSQL-compatible OID][oid] for the materialized view.
`schema_id`    | [`uint8`]            | The ID of the schema to which the materialized view belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`         | [`text`]             | The name of the materialized view.
`cluster_id`   | [`text`]             | The ID of the cluster maintaining the materialized view. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters).
`definition`   | [`text`]             | The materialized view definition (a `SELECT` query).
`owner_id`     | [`text`]             | The role ID of the owner of the materialized view. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`   | [`mz_aclitem array`] | The privileges belonging to the materialized view.

### `mz_objects`

The `mz_objects` view contains a row for each table, source, view, materialized view, sink,
index, connection, secret, type, and function in the system.

IDs for all objects represented in `mz_objects` share a namespace. If there is a view
with ID u1, there will never be a table, source, view, materialized view, sink, index,
connection, secret, type, or function with ID u1.

Field       | Type                 | Meaning
------------|----------------------|--------
`id`        | [`text`]             | Materialize's unique ID for the object.
`oid`       | [`oid`]              | A [PostgreSQL-compatible OID][oid] for the object.
`schema_id` | [`uint8`]            | The ID of the schema to which the object belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`      | [`text`]             | The name of the object.
`type`      | [`text`]             | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.
`owner_id`  | [`text`]             | The role ID of the owner of the object. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`| [`mz_aclitem array`] | The privileges belonging to the object.

### `mz_pseudo_types`

The `mz_pseudo_types` table contains a row for each pseudo type in the system.

Field          | Type       | Meaning
---------------|------------|----------
`id`           | [`text`]   | The ID of the type.

### `mz_relations`

The `mz_relations` view contains a row for each table, source, view, and
materialized view in the system.

Field       | Type                 | Meaning
------------|----------------------|--------
`id`        | [`text`]             | Materialize's unique ID for the relation.
`oid`       | [`oid`]              | A [PostgreSQL-compatible OID][oid] for the relation.
`schema_id` | [`uint8`]            | The ID of the schema to which the relation belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`      | [`text`]             | The name of the relation.
`type`      | [`text`]             | The type of the relation: either `table`, `source`, `view`, or `materialized view`.
`owner_id`  | [`text`]             | The role ID of the owner of the relation. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`| [`mz_aclitem array`] | The privileges belonging to the relation.

### `mz_roles`

The `mz_roles` table contains a row for each role in the system.

Field            | Type       | Meaning
-----------------|------------|--------
`id`             | [`text`]   | Materialize's unique ID for the role.
`oid`            | [`oid`]    | A [PostgreSQL-compatible OID][oid] for the role.
`name`           | [`text`]   | The name of the role.
`inherit`        | [`bool`]   | Indicates whether the role has inheritance of privileges.
`create_role`    | [`bool`]   | Indicates whether the role is allowed to create, alter, drop, grant, and revoke roles.
`create_db`      | [`bool`]   | Indicates whether the role is allowed to create databases.
`create_cluster` | [`bool`]   | Indicates whether the role is allowed to create clusters.

### `mz_role_members`

The `mz_role_members` table contains a row for role membership in the system.

Field     | Type       | Meaning
----------|------------|--------
`role_id` | [`text`]   | The role id of the role that `member` is a meber of. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`member`  | [`text`]   | The role id that is a member of `role_id`. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`grantor` | [`text`]   | The role id that granted membership of `member` to `role_id`. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).

### `mz_schemas`

The `mz_schemas` table contains a row for each schema in the system.

Field         | Type                 | Meaning
--------------|----------------------|--------
`id`          | [`uint8`]            | Materialize's unique ID for the schema.
`oid`         | [`oid`]              | A [PostgreSQL-compatible oid][oid] for the schema.
`database_id` | [`uint8`]            | The ID of the database containing the schema. Corresponds to [`mz_databases.id`](/sql/system-catalog/mz_catalog/#mz_databases).
`name`        | [`text`]             | The name of the schema.
`owner_id`    | [`text`]             | The role ID of the owner of the schema. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`  | [`mz_aclitem array`] | The privileges belonging to the schema.

### `mz_secrets`

The `mz_secrets` table contains a row for each connection in the system.

Field            | Type                 | Meaning
-----------------|----------------------|--------
`id`             | [`text`]             | The unique ID of the secret.
`schema_id`      | [`uint8`]            | The ID of the schema to which the secret belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`           | [`text`]             | The name of the secret.
`owner_id`       | [`text`]             | The role ID of the owner of the secret. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`     | [`mz_aclitem array`] | The privileges belonging to the secret.

### `mz_ssh_tunnel_connections`

The `mz_ssh_tunnel_connections` table contains a row for each SSH tunnel
connection in the system.

Field                 | Type           | Meaning
----------------------|----------------|--------
`id`                  | [`text`]       | The ID of the connection.
`public_key_1`        | [`text`]       | The first public key associated with the SSH tunnel.
`public_key_2`        | [`text`]       | The second public key associated with the SSH tunnel.

### `mz_sinks`

The `mz_sinks` table contains a row for each sink in the system.

Field            | Type        | Meaning
-----------------|-------------|--------
`id`             | [`text`]    | Materialize's unique ID for the sink.
`oid`            | [`oid`]     | A [PostgreSQL-compatible OID][oid] for the sink.
`schema_id`      | [`uint8`]   | The ID of the schema to which the sink belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`           | [`text`]    | The name of the sink.
`type`           | [`text`]    | The type of the sink: `kafka`.
`connection_id`  | [`text`]    | The ID of the connection associated with the sink, if any. Corresponds to [`mz_connections.id`](/sql/system-catalog/mz_catalog/#mz_connections).
`size`           | [`text`]    | The size of the sink.
`envelope_type`  | [`text`]    | The [envelope](/sql/create-sink/#envelopes) of the sink: `upsert`, or `debezium`.
`cluster_id`     | [`text`]    | The ID of the cluster maintaining the sink. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters).
`owner_id`       | [`text`]    | The role ID of the owner of the sink. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).

### `mz_sources`

The `mz_sources` table contains a row for each source in the system.

Field            | Type                 | Meaning
-----------------|----------------------|----------
`id`             | [`text`]             | Materialize's unique ID for the source.
`oid`            | [`oid`]              | A [PostgreSQL-compatible OID][oid] for the source.
`schema_id`      | [`uint8`]            | The ID of the schema to which the source belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`           | [`text`]             | The name of the source.
`type`           | [`text`]             | The type of the source: `kafka`, `postgres`, `load-generator`, `progress`, or `subsource`.
`connection_id`  | [`text`]             | The ID of the connection associated with the source, if any. Corresponds to [`mz_connections.id`](/sql/system-catalog/mz_catalog/#mz_connections).
`size`           | [`text`]             | The [size](/sql/create-source/#sizing-a-source) of the source.
`envelope_type`  | [`text`]             | The [envelope](/sql/create-source/#envelopes) of the source: `none`, `upsert`, or `debezium`.
`cluster_id`     | [`text`]             | The ID of the cluster maintaining the source. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters).
`owner_id`       | [`text`]             | The role ID of the owner of the source. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`     | [`mz_aclitem array`] | The privileges belonging to the source.

### `mz_storage_usage`

The `mz_storage_usage` table describes the storage utilization of each
table, source, and materialized view in the system. Storage utilization is
assessed approximately every hour.

Field                  | Type                         | Meaning
---------------------- | ---------------------------- | -----------------------------------------------------------
`object_id`            | [`text`]                     | The ID of the table, source, or materialized view.
`size_bytes`           | [`uint8`]                    | The number of storage bytes used by the object.
`collection_timestamp` | [`timestamp with time zone`] | The time at which storage usage of the object was assessed.

### `mz_tables`

The `mz_tables` table contains a row for each table in the system.

Field        | Type                 | Meaning
-------------|----------------------|----------
`id`         | [`text`]             | Materialize's unique ID for the table.
`oid`        | [`oid`]              | A [PostgreSQL-compatible OID][oid] for the table.
`schema_id`  | [`uint8`]            | The ID of the schema to which the table belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`       | [`text`]             | The name of the table.
`owner_id`   | [`text`]             | The role ID of the owner of the table. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges` | [`mz_aclitem array`] | The privileges belonging to the table.

### `mz_types`

The `mz_types` table contains a row for each type in the system.

Field          | Type                 | Meaning
---------------|----------------------|----------
`id`           | [`text`]             | Materialize's unique ID for the type.
`oid`          | [`oid`]              | A [PostgreSQL-compatible OID][oid] for the type.
`schema_id`    | [`uint8`]            | The ID of the schema to which the type belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`         | [`text`]             | The name of the type.
`owner_id`     | [`text`]             | The role ID of the owner of the type. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`   | [`mz_aclitem array`] | The privileges belonging to the type.

### `mz_views`

The `mz_views` table contains a row for each view in the system.

Field          | Type                 | Meaning
---------------|----------------------|----------
`id`           | [`text`]             | Materialize's unique ID for the view.
`oid`          | [`oid`]              | A [PostgreSQL-compatible OID][oid] for the view.
`schema_id`    | [`uint8`]            | The ID of the schema to which the view belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).
`name`         | [`text`]             | The name of the view.
`definition`   | [`text`]             | The view definition (a `SELECT` query).
`owner_id`     | [`text`]             | The role ID of the owner of the view. Corresponds to [`mz_roles.id`](/sql/system-catalog/mz_catalog/#mz_roles).
`privileges`   | [`mz_aclitem array`] | The privileges belonging to the view.

[`bigint`]: /sql/types/bigint
[`boolean`]: /sql/types/boolean
[`jsonb`]: /sql/types/jsonb
[`oid`]: /sql/types/oid
[`text`]: /sql/types/text
[`timestamp with time zone`]: /sql/types/timestamp
[oid]: /sql/types/oid
[`text array`]: /sql/types/array
[`record`]: /sql/types/record
[`uint8`]: /sql/types/uint8
