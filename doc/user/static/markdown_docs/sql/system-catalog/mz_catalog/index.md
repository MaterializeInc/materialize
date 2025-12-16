<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)  /  [System
catalog](/docs/self-managed/v25.2/sql/system-catalog/)

</div>

# mz_catalog

The following sections describe the available relations in the
`mz_catalog` schema. These relations contain metadata about objects in
Materialize, including descriptions of each database, schema, source,
table, view, sink, and index in the system.

<div class="warning">

**WARNING!** Views that directly reference these objects cannot include
`NATURAL JOIN` or `*` expressions. Instead, project the required columns
and convert all `NATURAL JOIN`s to `USING` joins.

</div>

### `mz_array_types`

The `mz_array_types` table contains a row for each array type in the
system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the array type. |
| `element_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the array’s element type. |

### `mz_audit_events`

The `mz_audit_events` table records create, alter, and drop events for
the other objects in the system catalog.

| Field | Type | Meaning |
|----|----|----|
| `id ` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | Materialize’s unique, monotonically increasing ID for the event. |
| `event_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the event: `create`, `drop`, or `alter`. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the affected object: `cluster`, `cluster-replica`, `connection`, `database`, `function`, `index`, `materialized-view`, `role`, `schema`, `secret`, `sink`, `source`, `table`, `type`, or `view`. |
| `details` | [`jsonb`](/docs/self-managed/v25.2/sql/types/jsonb) | Additional details about the event. The shape of the details varies based on `event_type` and `object_type`. |
| `user` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The user who triggered the event, or `NULL` if triggered by the system. |
| `occurred_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the event occurred. Guaranteed to be in order of event creation. Events created in the same transaction will have identical values. |

### `mz_aws_privatelink_connections`

The `mz_aws_privatelink_connections` table contains a row for each AWS
PrivateLink connection in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the connection. |
| `principal` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The AWS Principal that Materialize will use to connect to the VPC endpoint. |

### `mz_base_types`

The `mz_base_types` table contains a row for each base type in the
system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the type. |

### `mz_cluster_replica_frontiers`

The `mz_cluster_replica_frontiers` table describes the per-replica
frontiers of sources, sinks, materialized views, indexes, and
subscriptions in the system, as observed from the coordinator.

[`mz_compute_frontiers`](../mz_introspection/#mz_compute_frontiers) is
similar to `mz_cluster_replica_frontiers`, but `mz_compute_frontiers`
reports the frontiers known to the active compute replica, while
`mz_cluster_replica_frontiers` reports the frontiers of all replicas.
Note also that `mz_compute_frontiers` is restricted to compute objects
(indexes, materialized views, and subscriptions) while
`mz_cluster_replica_frontiers` contains storage objects that are
installed on replicas (sources, sinks) as well.

At this time, we do not make any guarantees about the freshness of these
numbers.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the source, sink, index, materialized view, or subscription. |
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. |
| `write_frontier` | [`mz_timestamp`](/docs/self-managed/v25.2/sql/types/mz_timestamp) | The next timestamp at which the output may change. |

### `mz_cluster_replica_sizes`

The `mz_cluster_replica_sizes` table contains a mapping of logical sizes
(e.g. `100cc`) to physical sizes (number of processes, and CPU and
memory allocations per process).

This table was previously in the `mz_internal` schema. All queries
previously referencing `mz_internal.mz_cluster_replica_sizes` should now
reference `mz_catalog.mz_cluster_replica_sizes`.

<div class="warning">

**WARNING!** The values in this table may change at any time. You should
not rely on them for any kind of capacity planning.

</div>

| Field | Type | Meaning |
|----|----|----|
| `size` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The human-readable replica size. |
| `processes` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of processes in the replica. |
| `workers` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of Timely Dataflow workers per process. |
| `cpu_nano_cores` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The CPU allocation per process, in billionths of a vCPU core. |
| `memory_bytes` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The RAM allocation per process, in billionths of a vCPU core. |
| `disk_bytes` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The disk allocation per process. |
| `credits_per_hour` | [`numeric`](/docs/self-managed/v25.2/sql/types/numeric/) | The number of compute credits consumed per hour. |

### `mz_cluster_replicas`

The `mz_cluster_replicas` table contains a row for each cluster replica
in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the cluster replica. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the cluster replica. |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster to which the replica belongs. Corresponds to [`mz_clusters.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_clusters). |
| `size` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The cluster replica’s size, selected during creation. |
| `availability_zone` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The availability zone in which the cluster is running. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the cluster replica. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `disk` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | If the replica has a local disk. |

### `mz_clusters`

The `mz_clusters` table contains a row for each cluster in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the cluster. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the cluster. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the cluster. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the cluster. |
| `managed` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Whether the cluster is a [managed cluster](/docs/self-managed/v25.2/sql/create-cluster/) with automatically managed replicas. |
| `size` | [`text`](/docs/self-managed/v25.2/sql/types/text) | If the cluster is managed, the desired size of the cluster’s replicas. `NULL` for unmanaged clusters. |
| `replication_factor` | [`uint4`](/docs/self-managed/v25.2/sql/types/uint4) | If the cluster is managed, the desired number of replicas of the cluster. `NULL` for unmanaged clusters. |
| `disk` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | **Unstable** If the cluster is managed, `true` if the replicas have the `DISK` option . `NULL` for unmanaged clusters. |
| `availability_zones` | [`text list`](/docs/self-managed/v25.2/sql/types/list/) | **Unstable** If the cluster is managed, the list of availability zones specified in `AVAILABILITY ZONES`. `NULL` for unmanaged clusters. |
| `introspection_debugging` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Whether introspection of the gathering of the introspection data is enabled. |
| `introspection_interval` | [`interval`](/docs/self-managed/v25.2/sql/types/interval) | The interval at which to collect introspection data. |

### `mz_columns`

The `mz_columns` contains a row for each column in each table, source,
and view in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The unique ID of the table, source, or view containing the column. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the column. |
| `position` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The 1-indexed position of the column in its containing table, source, or view. |
| `nullable` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Can the column contain a `NULL` value? |
| `type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The data type of the column. |
| `default` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The default expression of the column. |
| `type_oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | The OID of the type of the column (references `mz_types`). |
| `type_mod` | [`integer`](/docs/self-managed/v25.2/sql/types/integer/) | The packed type identifier of the column. |

### `mz_connections`

The `mz_connections` table contains a row for each connection in the
system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The unique ID of the connection. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the connection. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the connection belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the connection. |
| `type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the connection: `confluent-schema-registry`, `kafka`, `postgres`, or `ssh-tunnel`. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the connection. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the connection. |
| `create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The `CREATE` SQL statement for the connection. |
| `redacted_create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The redacted `CREATE` SQL statement for the connection. |

### `mz_databases`

The `mz_databases` table contains a row for each database in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the database. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the database. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the database. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the database. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the database. |

### `mz_default_privileges`

The `mz_default_privileges` table contains information on default
privileges that will be applied to new objects when they are created.

| Field | Type | Meaning |
|----|----|----|
| `role_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted on objects created by `role_id`. The role ID `p` stands for the `PUBLIC` pseudo-role and applies to all roles. |
| `database_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted only on objects in the database identified by `database_id` if non-null. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted only on objects in the schema identified by `schema_id` if non-null. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted only on objects of type `object_type`. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted to `grantee`. The role ID `p` stands for the `PUBLIC` pseudo-role and applies to all roles. |
| `privileges` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The set of privileges that will be granted. |

### `mz_egress_ips`

The `mz_egress_ips` table contains a row for each potential IP address
that the system may connect to external systems from.

| Field | Type | Meaning |
|----|----|----|
| `egress_ip` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The start of the range of IP addresses. |
| `prefix_length` | [`integer`](/docs/self-managed/v25.2/sql/types/integer/) | The number of leading bits in the CIDR netmask. |
| `cidr` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The CIDR representation. |

### `mz_functions`

The `mz_functions` table contains a row for each function in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the function. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the function. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the function belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the function. |
| `argument_type_ids` | [`text array`](/docs/self-managed/v25.2/sql/types/array) | The ID of each argument’s type. Each entry refers to `mz_types.id`. |
| `variadic_argument_type_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the variadic argument’s type, or `NULL` if the function does not have a variadic argument. Refers to `mz_types.id`. |
| `return_type_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The returned value’s type, or `NULL` if the function does not return a value. Refers to `mz_types.id`. Note that for table functions with \> 1 column, this type corresponds to [`record`](/docs/self-managed/v25.2/sql/types/record). |
| `returns_set` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Whether the function returns a set, i.e. the function is a table function. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the function. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |

### `mz_indexes`

The `mz_indexes` table contains a row for each index in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the index. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the index. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the index. |
| `on_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the relation on which the index is built. |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster in which the index is built. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the index. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The `CREATE` SQL statement for the index. |
| `redacted_create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The redacted `CREATE` SQL statement for the index. |

### `mz_index_columns`

The `mz_index_columns` table contains a row for each column in each
index in the system. For example, an index on `(a, b + 1)` would have
two rows in this table, one for each of the two columns in the index.

For a given row, if `field_number` is null then `expression` will be
nonnull, or vice-versa.

| Field | Type | Meaning |
|----|----|----|
| `index_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the index which contains this column. Corresponds to [`mz_indexes.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_indexes). |
| `index_position` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The 1-indexed position of this column within the index. (The order of columns in an index does not necessarily match the order of columns in the relation on which the index is built.) |
| `on_position` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | If not `NULL`, specifies the 1-indexed position of a column in the relation on which this index is built that determines the value of this index column. |
| `on_expression` | [`text`](/docs/self-managed/v25.2/sql/types/text) | If not `NULL`, specifies a SQL expression that is evaluated to compute the value of this index column. The expression may contain references to any of the columns of the relation. |
| `nullable` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Can this column of the index evaluate to `NULL`? |

### `mz_kafka_connections`

The `mz_kafka_connections` table contains a row for each Kafka
connection in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the connection. |
| `brokers` | [`text array`](/docs/self-managed/v25.2/sql/types/array) | The addresses of the Kafka brokers to connect to. |
| `sink_progress_topic` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the Kafka topic where any sinks associated with this connection will track their progress information and other metadata. The contents of this topic are unspecified. |

### `mz_kafka_sinks`

The `mz_kafka_sinks` table contains a row for each Kafka sink in the
system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the sink. |
| `topic` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the Kafka topic into which the sink is writing. |

### `mz_kafka_sources`

The `mz_kafka_sources` table contains a row for each Kafka source in the
system.

This table was previously in the `mz_internal` schema. All queries
previously referencing `mz_internal.mz_kafka_sources` should now
reference `mz_catalog.mz_kafka_sources`.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the Kafka source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources). |
| `group_id_prefix` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The value of the `GROUP ID PREFIX` connection option. |
| `topic ` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the Kafka topic the source is reading from. |

### `mz_list_types`

The `mz_list_types` table contains a row for each list type in the
system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the list type. |
| `element_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The IID of the list’s element type. |
| `element_modifiers` | [`uint8 list`](/docs/self-managed/v25.2/sql/types/list) | The element type modifiers, or `NULL` if none. |

### `mz_map_types`

The `mz_map_types` table contains a row for each map type in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the map type. |
| `key_id ` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the map’s key type. |
| `value_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the map’s value type. |
| `key_modifiers` | [`uint8 list`](/docs/self-managed/v25.2/sql/types/list) | The key type modifiers, or `NULL` if none. |
| `value_modifiers` | [`uint8 list`](/docs/self-managed/v25.2/sql/types/list) | The value type modifiers, or `NULL` if none. |

### `mz_materialized_views`

The `mz_materialized_views` table contains a row for each materialized
view in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the materialized view. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the materialized view. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the materialized view belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the materialized view. |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster maintaining the materialized view. Corresponds to [`mz_clusters.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_clusters). |
| `definition` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The materialized view definition (a `SELECT` query). |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the materialized view. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the materialized view. |
| `create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The `CREATE` SQL statement for the materialized view. |
| `redacted_create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The redacted `CREATE` SQL statement for the materialized view. |

### `mz_objects`

The `mz_objects` view contains a row for each table, source, view,
materialized view, sink, index, connection, secret, type, and function
in the system.

IDs for all objects represented in `mz_objects` share a namespace. If
there is a view with ID `u1`, there will never be a table, source, view,
materialized view, sink, index, connection, secret, type, or function
with ID `u1`.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the object. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the object. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the object belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the object. |
| `type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the object: one of `table`, `source`, `view`, `materialized-view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the object. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to [`mz_clusters.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_clusters). `NULL` for other object types. |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the object. |

### `mz_pseudo_types`

The `mz_pseudo_types` table contains a row for each pseudo type in the
system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the type. |

### `mz_relations`

The `mz_relations` view contains a row for each table, source, view, and
materialized view in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the relation. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the relation. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the relation belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the relation. |
| `type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the relation: either `table`, `source`, `view`, or `materialized view`. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the relation. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to [`mz_clusters.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_clusters). `NULL` for other object types. |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the relation. |

### `mz_recent_storage_usage`

The `mz_recent_storage_usage` table describes the storage utilization of
each table, source, and materialized view in the system in the most
recent storage utilization assessment. Storage utilization assessments
occur approximately every hour.

See [`mz_storage_usage`](../mz_catalog#mz_storage_usage) for historical
storage usage information.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the table, source, or materialized view. |
| `size_bytes` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of storage bytes used by the object in the most recent assessment. |

### `mz_roles`

The `mz_roles` table contains a row for each role in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the role. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the role. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the role. |
| `inherit` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Indicates whether the role has inheritance of privileges. |

### `mz_role_members`

The `mz_role_members` table contains a row for each role membership in
the system.

| Field | Type | Meaning |
|----|----|----|
| `role_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the role the `member` is a member of. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `member` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the role that is a member of `role_id`. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the role that granted membership of `member` to `role_id`. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |

### `mz_role_parameters`

The `mz_role_parameters` table contains a row for each configuration
parameter whose default value has been altered for a given role. See
[`ALTER ROLE ... SET`](/docs/self-managed/v25.2/sql/alter-role/#alter_role_set)
on setting default configuration parameter values per role.

| Field | Type | Meaning |
|----|----|----|
| `role_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the role whose configuration parameter default is set. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `parameter_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The configuration parameter name. One of the supported [configuration parameters](/docs/self-managed/v25.2/sql/set/#key-configuration-parameters). |
| `parameter_value` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The default value of the parameter for the given role. Can be either a single value, or a comma-separated list of values for configuration parameters that accept a list. |

### `mz_schemas`

The `mz_schemas` table contains a row for each schema in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the schema. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible oid](/docs/self-managed/v25.2/sql/types/oid) for the schema. |
| `database_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the database containing the schema. Corresponds to [`mz_databases.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_databases). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the schema. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the schema. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the schema. |

### `mz_secrets`

The `mz_secrets` table contains a row for each connection in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The unique ID of the secret. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible oid](/docs/self-managed/v25.2/sql/types/oid) for the secret. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the secret belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the secret. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the secret. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the secret. |

### `mz_ssh_tunnel_connections`

The `mz_ssh_tunnel_connections` table contains a row for each SSH tunnel
connection in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the connection. |
| `public_key_1` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The first public key associated with the SSH tunnel. |
| `public_key_2` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The second public key associated with the SSH tunnel. |

### `mz_sinks`

The `mz_sinks` table contains a row for each sink in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the sink. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the sink. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the sink belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the sink. |
| `type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the sink: `kafka`. |
| `connection_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the connection associated with the sink, if any. Corresponds to [`mz_connections.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_connections). |
| `size` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The size of the sink. |
| `envelope_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The [envelope](/docs/self-managed/v25.2/sql/create-sink/kafka/#envelopes) of the sink: `upsert`, or `debezium`. |
| `format` | [`text`](/docs/self-managed/v25.2/sql/types/text) | *Deprecated* The [format](/docs/self-managed/v25.2/sql/create-sink/kafka/#formats) of the Kafka messages produced by the sink: `avro`, `json`, `text`, or `bytes`. |
| `key_format` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The [format](/docs/self-managed/v25.2/sql/create-sink/kafka/#formats) of the Kafka message key for messages produced by the sink: `avro`, `json`, `bytes`, `text`, or `NULL`. |
| `value_format` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The [format](/docs/self-managed/v25.2/sql/create-sink/kafka/#formats) of the Kafka message value for messages produced by the sink: `avro`, `json`, `text`, or `bytes`. |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster maintaining the sink. Corresponds to [`mz_clusters.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_clusters). |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the sink. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The `CREATE` SQL statement for the sink. |
| `redacted_create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The redacted `CREATE` SQL statement for the sink. |

### `mz_sources`

The `mz_sources` table contains a row for each source in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the source. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the source. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the source belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the source. |
| `type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the source: `kafka`, `mysql`, `postgres`, `load-generator`, `progress`, or `subsource`. |
| `connection_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the connection associated with the source, if any. Corresponds to [`mz_connections.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_connections). |
| `size` | [`text`](/docs/self-managed/v25.2/sql/types/text) | *Deprecated* The [size](/docs/self-managed/v25.2/sql/create-source/#sizing-a-source) of the source. |
| `envelope_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | For Kafka sources, the [envelope](/docs/self-managed/v25.2/sql/create-source/#envelopes) type: `none`, `upsert`, or `debezium`. `NULL` for other source types. |
| `key_format` | [`text`](/docs/self-managed/v25.2/sql/types/text) | For Kafka sources, the [format](/docs/self-managed/v25.2/sql/create-source/#formats) of the Kafka message key: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`, or `NULL`. |
| `value_format` | [`text`](/docs/self-managed/v25.2/sql/types/text) | For Kafka sources, the [format](/docs/self-managed/v25.2/sql/create-source/#formats) of the Kafka message value: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`. `NULL` for other source types. |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster maintaining the source. Corresponds to [`mz_clusters.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_clusters). |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the source. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges granted on the source. |
| `create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The `CREATE` SQL statement for the source. |
| `redacted_create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The redacted `CREATE` SQL statement for the source. |

### `mz_storage_usage`

<div class="warning">

**WARNING!** This view is not indexed in the `mz_catalog_server`
cluster. Querying this view can be slow due to the amount of unindexed
data that must be scanned.

</div>

The `mz_storage_usage` table describes the historical storage
utilization of each table, source, and materialized view in the system.
Storage utilization is assessed approximately every hour.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the table, source, or materialized view. |
| `size_bytes` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of storage bytes used by the object. |
| `collection_timestamp` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which storage usage of the object was assessed. |

### `mz_system_privileges`

The `mz_system_privileges` table contains information on system
privileges.

| Field | Type | Meaning |
|----|----|----|
| `privileges` | [`mz_aclitem`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the system. |

### `mz_tables`

The `mz_tables` table contains a row for each table in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the table. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the table. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the table belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the table. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the table. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the table. |
| `create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The `CREATE` SQL statement for the table. |
| `redacted_create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The redacted `CREATE` SQL statement for the table. |
| `source_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the source associated with the table, if any. Corresponds to [`mz_sources.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_sources). |

### `mz_timezone_abbreviations`

The `mz_timezone_abbreviations` view contains a row for each supported
timezone abbreviation. A “fixed” abbreviation does not change its offset
or daylight status based on the current time. A non-“fixed” abbreviation
is dependent on the current time for its offset, and must use the
[`timezone_offset`](/docs/self-managed/v25.2/sql/functions/#timezone_offset)
function to find its properties. These correspond to the
`pg_catalog.pg_timezone_abbrevs` table, but can be materialized as they
do not depend on the current time.

| Field | Type | Meaning |
|----|----|----|
| `abbreviation` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The timezone abbreviation. |
| `utc_offset` | [`interval`](/docs/self-managed/v25.2/sql/types/interval) | The UTC offset of the timezone or `NULL` if fixed. |
| `dst` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Whether the timezone is in daylight savings or `NULL` if fixed. |
| `timezone_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The full name of the non-fixed timezone or `NULL` if not fixed. |

### `mz_timezone_names`

The `mz_timezone_names` view contains a row for each supported timezone.
Use the
[`timezone_offset`](/docs/self-managed/v25.2/sql/functions/#timezone_offset)
function for properties of a timezone at a certain timestamp. These
correspond to the `pg_catalog.pg_timezone_names` table, but can be
materialized as they do not depend on the current time.

| Field | Type | Meaning |
|----|----|----|
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The timezone name. |

### `mz_types`

The `mz_types` table contains a row for each type in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the type. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the type. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the type belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the type. |
| `category` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The category of the type. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the type. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the type. |
| `create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The `CREATE` SQL statement for the type. |
| `redacted_create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The redacted `CREATE` SQL statement for the type. |

### `mz_views`

The `mz_views` table contains a row for each view in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the view. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the view. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the view belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the view. |
| `definition` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The view definition (a `SELECT` query). |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the view. Corresponds to [`mz_roles.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the view. |
| `create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The `CREATE` SQL statement for the view. |
| `redacted_create_sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The redacted `CREATE` SQL statement for the view. |

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/system-catalog/mz_catalog.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
