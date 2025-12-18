---
title: "mz_internal"
description: "mz_internal is a system catalog schema which exposes internal metadata about Materialize. This schema is not part of Materialize's stable interface."
menu:
  main:
    parent: 'system-catalog'
    weight: 4
---

The following sections describe the available objects in the `mz_internal`
schema.

{{< warning >}}
The objects in the `mz_internal` schema are not part of Materialize's stable interface.
Backwards-incompatible changes to these objects may be made at any time.
{{< /warning >}}

{{< warning >}}
`SELECT` statements may reference these objects, but creating views that
reference these objects is not allowed.
{{< /warning >}}

## `mz_object_global_ids`

The `mz_object_global_ids` table maps Materialize catalog item IDs to global IDs.

<!-- RELATION_SPEC mz_internal.mz_object_global_ids -->
| Field        | Type     | Meaning                                                                                             |
|--------------|----------|-----------------------------------------------------------------------------------------------------|
| `id`         | [`text`] | The ID of the object. Corresponds to [`mz_objects.id`](/sql/system-catalog/mz_catalog/#mz_objects). |
| `global_id`  | [`text`] | The global ID of the object.                                                                        |

## `mz_recent_activity_log`

{{< public-preview />}}

{{< warning >}}
Do not rely on all statements being logged in this view. Materialize
controls the maximum rate at which statements are sampled, and may change
this rate at any time.
{{< /warning >}}

{{< warning >}}
Entries in this view may be cleared on restart (e.g., during Materialize maintenance windows).
{{< /warning >}}

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
| Field                      | Type                         | Meaning                                                                                                                                                                                                                                                                       |
|----------------------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `execution_id`             | [`uuid`]                     | An ID that is unique for each executed statement.                                                                                                                                                                                                                             |
| `sample_rate`              | [`double precision`]         | The actual rate at which the statement was sampled.                                                                                                                                                                                                                           |
| `cluster_id`               | [`text`]                     | The ID of the cluster the statement execution was directed to. Corresponds to [mz_clusters.id](/sql/system-catalog/mz_catalog/#mz_clusters).                                                                                                      |
| `application_name`         | [`text`]                     | The value of the `application_name` configuration parameter at execution time.                                                                                                                                                                                                |
| `cluster_name`             | [`text`]                     | The name of the cluster with ID `cluster_id` at execution time.                                                                                                                                                                                                               |
| `database_name`            | [`text`]                     | The value of the `database` configuration parameter at execution time.                                                                                                                                                                                                        |
| `search_path`              | [`text list`]                | The value of the `search_path` configuration parameter at execution time.                                                                                                                                                                                                     |
| `transaction_isolation`    | [`text`]                     | The value of the `transaction_isolation` configuration parameter at execution time.                                                                                                                                                                                           |
| `execution_timestamp`      | [`uint8`]                    | The logical timestamp at which execution was scheduled.                                                                                                                                                                                                                       |
| `transient_index_id`       | [`text`]                     | The internal index of the compute dataflow created for the query, if any.                                                                                                                                                                                                     |
| `params`                   | [`text array`]               | The parameters with which the statement was executed.                                                                                                                                                                                                                         |
| `mz_version`               | [`text`]                     | The version of Materialize that was running when the statement was executed.                                                                                                                                                                                                  |
| `began_at`                 | [`timestamp with time zone`] | The wall-clock time at which the statement began executing.                                                                                                                                                                                                                   |
| `finished_at`              | [`timestamp with time zone`] | The wall-clock time at which the statement finished executing.                                                                                                                                                                                                                |
| `finished_status`          | [`text`]                     | The final status of the statement (e.g., `success`, `canceled`, `error`, or `aborted`). `aborted` means that Materialize exited before the statement finished executing.                                                                                                   |
| `error_message`            | [`text`]                     | The error message, if the statement failed.                                                                                                                                                                                                                                   |
| `result_size`              | [`bigint`]                   | The size in bytes of the result, for statements that return rows.                                                                                                                                                                                                                 |
| `rows_returned`            | [`bigint`]                   | The number of rows returned, for statements that return rows.                                                                                                                                                                                                                 |
| `execution_strategy`       | [`text`]                     | For `SELECT` queries, the strategy for executing the query. `constant` means computed in the control plane without the involvement of a cluster, `fast-path` means read by a cluster directly from an in-memory index, and `standard` means computed by a temporary dataflow. |
| `transaction_id`           | [`uint8`]                    | The ID of the transaction that the statement was part of. Note that transaction IDs are only unique per session.                                                                                                                                                              |
| `prepared_statement_id`    | [`uuid`]                     | An ID that is unique for each prepared statement. For example, if a statement is prepared once and then executed multiple times, all executions will have the same value for this column (but different values for `execution_id`).                                           |
| `sql_hash`                 | [`bytea`]                    | An opaque value uniquely identifying the text of the query.                                                                                                                                                                                                                   |
| `prepared_statement_name`  | [`text`]                     | The name given by the client library to the prepared statement.                                                                                                                                                                                                               |
| `session_id`               | [`uuid`]                     | An ID that is unique for each session. Corresponds to [mz_sessions.id](#mz_sessions). |
| `prepared_at`              | [`timestamp with time zone`] | The time at which the statement was prepared.                                                                                                                                                                                                                                 |
| `statement_type`           | [`text`]                     | The _type_ of the statement, e.g. `select` for a `SELECT` query, or `NULL` if the statement was empty.                                                                                                                                                                        |
| `throttled_count`          | [`uint8`]                    | The number of statement executions that were dropped due to throttling before the current one was seen. If you have a very high volume of queries and need to log them without throttling, [contact our team](/support/).                                   |
| `connected_at`       | [`timestamp with time zone`]                     | The time at which the session was established.                                                                                                                                                                                                                   |
| `initial_application_name` | [`text`]                     | The initial value of `application_name` at the beginning of the session.                                                                                                                                                                                                      |
| `authenticated_user`       | [`text`]                     | The name of the user for which the session was established.                                                                                                                                                                                                                   |
| `sql`                      | [`text`]                     | The SQL text of the statement.                                                                                                                                                                                                                                                |


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

{{< warn-if-unreleased v0.116 >}}
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

{{< warn-if-unreleased v0.116 >}}
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

{{< warn-if-unreleased v0.116 >}}
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
| Field                 | Type                         | Meaning                                                                                                                                   |
|-----------------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `replica_id`          | [`text`]                     | The ID of a cluster replica.                                                                                                              |
| `size`                | [`text`]                     | The size of the cluster replica. Corresponds to [`mz_cluster_replica_sizes.size`](../mz_catalog#mz_cluster_replica_sizes).                             |
| `cluster_id`        | [`text`]                     | The ID of the cluster associated with the replica.                                                                                      |
| `cluster_name`        | [`text`]                     | The name of the cluster associated with the replica.                                                                                      |
| `replica_name`        | [`text`]                     | The name of the replica.                                                                                                                  |
| `created_at`          | [`timestamp with time zone`] | The time at which the replica was created.                                                                                                |
| `dropped_at`          | [`timestamp with time zone`] | The time at which the replica was dropped, or `NULL` if it still exists.                                                                  |
| `credits_per_hour`    | [`numeric`]                  | The number of compute credits consumed per hour. Corresponds to [`mz_cluster_replica_sizes.credits_per_hour`](../mz_catalog#mz_cluster_replica_sizes). |

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

{{< warning >}}
Following the advice in this view might not always yield resource usage
optimizations. You should test any changes in a development environment
before deploying the changes to production.
{{< /warning >}}

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
| Field           | Type         | Meaning                                                                                                                                                                                         |
| --------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------                                                |
| `id`            | [`text`]     | Materialize's unique ID for the object.                                                                                                                                                         |
| `name`          | [`text`]     | The name of the object.                                                                                                                                                                         |
| `object_type`   | [`text`]     | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.                                                  |
| `schema_id`     | [`text`]     | The ID of the schema to which the object belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).                                                                 |
| `schema_name`   | [`text`]     | The name of the schema to which the object belongs. Corresponds to [`mz_schemas.name`](/sql/system-catalog/mz_catalog/#mz_schemas).                                                             |
| `database_id`   | [`text`]     | The ID of the database to which the object belongs. Corresponds to [`mz_databases.id`](/sql/system-catalog/mz_catalog/#mz_schemas).                                                             |
| `database_name` | [`text`]     | The name of the database to which the object belongs. Corresponds to [`mz_databases.name`](/sql/system-catalog/mz_catalog/#mz_databases).                                                       |
| `cluster_id`    | [`text`]     | The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters). `NULL` for other object types. |

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

{{< public-preview />}}

The `mz_notices` view contains a list of currently active notices emitted by the
system. The view can be accessed by Materialize _superusers_.

<!-- RELATION_SPEC mz_internal.mz_notices -->
| Field                   | Type                         | Meaning                                                                                                                                           |
| ----------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                    | [`text`]                     | Materialize's unique ID for this notice.                                                                                                          |
| `notice_type`           | [`text`]                     | The notice type.                                                                                                                                  |
| `message`               | [`text`]                     | A brief description of the issue highlighted by this notice.                                                                                      |
| `hint`                  | [`text`]                     | A high-level hint that tells the user what can be improved.                                                                                       |
| `action`                | [`text`]                     | A concrete action that will resolve the notice.                                                                                                   |
| `redacted_message`      | [`text`]                     | A redacted version of the `message` column. `NULL` if no redaction is needed.                                                                     |
| `redacted_hint`         | [`text`]                     | A redacted version of the `hint` column. `NULL` if no redaction is needed.                                                                        |
| `redacted_action`       | [`text`]                     | A redacted version of the `action` column. `NULL` if no redaction is needed.                                                                      |
| `action_type`           | [`text`]                     | The type of the `action` string (`sql_statements` for a valid SQL string or `plain_text` for plain text).                                         |
| `object_id`             | [`text`]                     | The ID of the materialized view or index. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). For global notices, this column is `NULL`. |
| `created_at`            | [`timestamp with time zone`] | The time at which the notice was created. Note that some notices are re-created on `environmentd` restart.                                        |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_optimizer_notices -->

## `mz_notices_redacted`

{{< public-preview />}}

The `mz_notices_redacted` view contains a redacted list of currently active
optimizer notices emitted by the system. The view can be accessed by Materialize
_superusers_ and Materialize support.

<!-- RELATION_SPEC mz_internal.mz_notices_redacted -->
| Field                   | Type                         | Meaning                                                                                                                                           |
| ----------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                    | [`text`]                     | Materialize's unique ID for this notice.                                                                                                          |
| `notice_type`           | [`text`]                     | The notice type.                                                                                                                                  |
| `message`               | [`text`]                     | A redacted brief description of the issue highlighted by this notice.                                                                             |
| `hint`                  | [`text`]                     | A redacted high-level hint that tells the user what can be improved.                                                                              |
| `action`                | [`text`]                     | A redacted concrete action that will resolve the notice.                                                                                          |
| `action_type`           | [`text`]                     | The type of the `action` string (`sql_statements` for a valid SQL string or `plain_text` for plain text).                                         |
| `object_id`             | [`text`]                     | The ID of the materialized view or index. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). For global notices, this column is `NULL`. |
| `created_at`            | [`timestamp with time zone`] | The time at which the notice was created. Note that some notices are re-created on `environmentd` restart.                                        |

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

{{< warning >}}
Do not rely on all sessions being logged in this view. Materialize
controls the maximum rate at which statements are sampled, and may change
this rate at any time.
{{< /warning >}}

<!-- RELATION_SPEC mz_internal.mz_session_history -->
| Field                | Type                         | Meaning                                                                                                                           |
|----------------------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `session_id`         | [`uuid`]                     | The globally unique ID of the session. Corresponds to [`mz_sessions.id`](#mz_sessions).                                           |
| `connected_at`       | [`timestamp with time zone`] | The time at which the session was established.                                                                                    |
| `initial_application_name`   | [`text`]                     | The `application_name` session metadata field.                                                                                    |
| `authenticated_user` | [`text`]                     | The name of the user for which the session was established.                                                                       |

{{< if-unreleased "v0.113" >}}
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
{{< /if-unreleased >}}

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
| Field            | Type     | Meaning                                         |
|------------------|----------|-------------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.            |
| `grantee`        | [`text`] | The role that the privilege was granted to.     |
| `database`       | [`text`] | The name of the database containing the object. |
| `schema`         | [`text`] | The name of the schema containing the object.   |
| `name`           | [`text`] | The name of the privilege target.               |
| `object_type`    | [`text`] | The type of object the privilege is granted on. |
| `privilege_type` | [`text`] | They type of privilege granted.                 |


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
| Field            | Type     | Meaning                                                                                             |
|------------------|----------|-----------------------------------------------------------------------------------------------------|
| `object_owner`   | [`text`] | Privileges described in this row will be granted on objects created by `object_owner`.              |
| `database`       | [`text`] | Privileges described in this row will be granted only on objects created in `database` if non-null. |
| `schema`         | [`text`] | Privileges described in this row will be granted only on objects created in `schema` if non-null.   |
| `object_type`    | [`text`] | Privileges described in this row will be granted only on objects of type `object_type`.             |
| `grantee`        | [`text`] | Privileges described in this row will be granted to `grantee`.                                      |
| `privilege_type` | [`text`] | They type of privilege to be granted.                                                               |

## `mz_show_object_privileges`

The `mz_show_object_privileges` view contains a row for each object privilege granted
in the system on user objects to user roles.

<!-- RELATION_SPEC mz_internal.mz_show_object_privileges -->
| Field            | Type     | Meaning                                         |
|------------------|----------|-------------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.            |
| `grantee`        | [`text`] | The role that the privilege was granted to.     |
| `database`       | [`text`] | The name of the database containing the object. |
| `schema`         | [`text`] | The name of the schema containing the object.   |
| `name`           | [`text`] | The name of the object.                         |
| `object_type`    | [`text`] | The type of object the privilege is granted on. |
| `privilege_type` | [`text`] | They type of privilege granted.                 |

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
| Field            | Type     | Meaning                                         |
|------------------|----------|-------------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.            |
| `grantee`        | [`text`] | The role that the privilege was granted to.     |
| `database`       | [`text`] | The name of the database containing the object. |
| `schema`         | [`text`] | The name of the schema containing the object.   |
| `name`           | [`text`] | The name of the privilege target.               |
| `object_type`    | [`text`] | The type of object the privilege is granted on. |
| `privilege_type` | [`text`] | They type of privilege granted.                 |

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
| Field            | Type     | Meaning                                                                                             |
|------------------|----------|-----------------------------------------------------------------------------------------------------|
| `object_owner`   | [`text`] | Privileges described in this row will be granted on objects created by `object_owner`.              |
| `database`       | [`text`] | Privileges described in this row will be granted only on objects created in `database` if non-null. |
| `schema`         | [`text`] | Privileges described in this row will be granted only on objects created in `schema` if non-null.   |
| `object_type`    | [`text`] | Privileges described in this row will be granted only on objects of type `object_type`.             |
| `grantee`        | [`text`] | Privileges described in this row will be granted to `grantee`.                                      |
| `privilege_type` | [`text`] | They type of privilege to be granted.                                                               |

## `mz_show_my_object_privileges`

The `mz_show_my_object_privileges` view is the same as
[`mz_show_object_privileges`](/sql/system-catalog/mz_internal/#mz_show_object_privileges), but
only includes rows where the current role is a direct or indirect member of `grantee`.

<!-- RELATION_SPEC mz_internal.mz_show_my_object_privileges -->
| Field            | Type     | Meaning                                         |
|------------------|----------|-------------------------------------------------|
| `grantor`        | [`text`] | The role that granted the privilege.            |
| `grantee`        | [`text`] | The role that the privilege was granted to.     |
| `database`       | [`text`] | The name of the database containing the object. |
| `schema`         | [`text`] | The name of the schema containing the object.   |
| `name`           | [`text`] | The name of the object.                         |
| `object_type`    | [`text`] | The type of object the privilege is granted on. |
| `privilege_type` | [`text`] | They type of privilege granted.                 |

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
| Field                | Type      | Meaning                                                                                                                        |
|----------------------|-----------| --------                                                                                                                       |
| `id`                 | [`text`]  | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks).                                       |
| `replica_id`         | [`text`]  | The ID of a replica running the sink. Corresponds to [`mz_catalog.mz_cluster_replicas.id`](../mz_catalog#mz_cluster_replicas). |
| `messages_staged`    | [`uint8`] | The number of messages staged but possibly not committed to the sink.                                                          |
| `messages_committed` | [`uint8`] | The number of messages committed to the sink.                                                                                  |
| `bytes_staged`       | [`uint8`] | The number of bytes staged but possibly not committed to the sink. This counts both keys and values, if applicable.            |
| `bytes_committed`    | [`uint8`] | The number of bytes committed to the sink. This counts both keys and values, if applicable.                                    |

## `mz_sink_statuses`

The `mz_sink_statuses` view provides the current state for each sink in the
system, including potential error messages and additional metadata helpful for
debugging.

<!-- RELATION_SPEC mz_internal.mz_sink_statuses -->
| Field                    | Type                            | Meaning                                                                                                          |
| ------------------------ | ------------------------------- | --------                                                                                                         |
| `id`                     | [`text`]                        | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks).                           |
| `name`                   | [`text`]                        | The name of the sink.                                                                                            |
| `type`                   | [`text`]                        | The type of the sink.                                                                                            |
| `last_status_change_at`  | [`timestamp with time zone`]    | Wall-clock timestamp of the sink status change.                                                                  |
| `status`                 | [`text`]                        | The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.              |
| `error`                  | [`text`]                        | If the sink is in an error state, the error message.                                                             |
| `details`                | [`jsonb`]                       | Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions. |

## `mz_sink_status_history`

The `mz_sink_status_history` table contains rows describing the
history of changes to the status of each sink in the system, including potential error
messages and additional metadata helpful for debugging.

<!-- RELATION_SPEC mz_internal.mz_sink_status_history -->
| Field          | Type                            | Meaning                                                                                                          |
| -------------- | ------------------------------- | --------                                                                                                         |
| `occurred_at`  | [`timestamp with time zone`]    | Wall-clock timestamp of the sink status change.                                                                  |
| `sink_id`      | [`text`]                        | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks).                           |
| `status`       | [`text`]                        | The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.              |
| `error`        | [`text`]                        | If the sink is in an error state, the error message.                                                             |
| `details`      | [`jsonb`]                       | Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions. |
| `replica_id`   | [`text`]                        | The ID of the replica that an instance of a sink is running on.                                                  |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_source_statistics_raw -->

## `mz_source_statistics`

The `mz_source_statistics` view contains statistics about each source.

<!-- RELATION_SPEC mz_internal.mz_source_statistics -->
| Field                     | Type         | Meaning |
| --------------------------|--------------|---------|
| `id`                      | [`text`]     | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources). |
| `replica_id`              | [`text`]     | The ID of a replica running the source. Corresponds to [`mz_catalog.mz_cluster_replicas.id`](../mz_catalog#mz_cluster_replicas). |
| `messages_received`       | [`uint8`]    | The number of messages the source has received from the external system. Messages are counted in a source type-specific manner. Messages do not correspond directly to updates: some messages produce multiple updates, while other messages may be coalesced into a single update. |
| `bytes_received`          | [`uint8`]    | The number of bytes the source has read from the external system. Bytes are counted in a source type-specific manner and may or may not include protocol overhead. |
| `updates_staged`          | [`uint8`]    | The number of updates (insertions plus deletions) the source has written but not yet committed to the storage layer. |
| `updates_committed`       | [`uint8`]    | The number of updates (insertions plus deletions) the source has committed to the storage layer. |
| `records_indexed`         | [`uint8`]    | The number of individual records indexed in the source envelope state. |
| `bytes_indexed`           | [`uint8`]    | The number of bytes stored in the source's internal index, if any. |
| `rehydration_latency`     | [`interval`] | The amount of time it took for the source to rehydrate its internal index, if any, after the source last restarted. |
| `snapshot_records_known`  | [`uint8`]    | The size of the source's snapshot, measured in number of records. See [below](#meaning-record) to learn what constitutes a record. |
| `snapshot_records_staged` | [`uint8`]    | The number of records in the source's snapshot that Materialize has read. See [below](#meaning-record) to learn what constitutes a record. |
| `snapshot_committed`      | [`boolean`]  | Whether the source has committed the initial snapshot for a source. |
| `offset_known`            | [`uint8`]    | The offset of the most recent data in the source's upstream service that Materialize knows about. See [below](#meaning-offset) to learn what constitutes an offset. |
| `offset_committed`        | [`uint8`]    | The offset of the the data that Materialize has durably ingested. See [below](#meaning-offset) to learn what constitutes an offset. |

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
| Field                    | Type                            | Meaning                                                                                                            |
| ------------------------ | ------------------------------- | --------                                                                                                           |
| `id`                     | [`text`]                        | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                       |
| `name`                   | [`text`]                        | The name of the source.                                                                                            |
| `type`                   | [`text`]                        | The type of the source.                                                                                            |
| `last_status_change_at`  | [`timestamp with time zone`]    | Wall-clock timestamp of the source status change.                                                                  |
| `status`                 | [`text`]                        | The status of the source: one of `created`, `starting`, `running`, `paused`, `stalled`, `failed`, or `dropped`.    |
| `error`                  | [`text`]                        | If the source is in an error state, the error message.                                                             |
| `details`                | [`jsonb`]                       | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions. |

## `mz_source_status_history`

The `mz_source_status_history` table contains a row describing the status of the
historical state for each source in the system, including potential error
messages and additional metadata helpful for debugging.

<!-- RELATION_SPEC mz_internal.mz_source_status_history -->
| Field          | Type                            | Meaning                                                                                                            |
| -------------- | ------------------------------- | --------                                                                                                           |
| `occurred_at`  | [`timestamp with time zone`]    | Wall-clock timestamp of the source status change.                                                                  |
| `source_id`    | [`text`]                        | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                       |
| `status`       | [`text`]                        | The status of the source: one of `created`, `starting`, `running`, `paused`, `stalled`, `failed`, or `dropped`.    |
| `error`        | [`text`]                        | If the source is in an error state, the error message.                                                             |
| `details`      | [`jsonb`]                       | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions. |
| `replica_id`   | [`text`]                        | The ID of the replica that an instance of a source is running on.                                                  |

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

| Field                   | Type                         | Meaning                                                                                                                                                                                                                                                                                                    |
|-------------------------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `id`                    | [`uuid`]                     | The ID of the execution event.                                                                                                                                                                                                                                                                             |
| `prepared_statement_id` | [`uuid`]                     | The ID of the prepared statement being executed. Corresponds to [`mz_prepared_statement_history.id`](#mz_prepared_statement_history).                                                                                                                                                                      |
| `sample_rate`           | [`double precision`]         | The sampling rate at the time the execution began.                                                                                                                                                                                                                                                         |
| `params`                | [`text list`]                | The values of the prepared statement's parameters.                                                                                                                                                                                                                                                         |
| `began_at`              | [`timestamp with time zone`] | The time at which execution began.                                                                                                                                                                                                                                                                         |
| `finished_at`           | [`timestamp with time zone`] | The time at which execution ended.                                                                                                                                                                                                                                                                         |
| `finished_status`       | [`text`]                     | `'success'`, `'error'`, `'canceled'`, or `'aborted'`. `'aborted'` means that the database restarted (e.g., due to a crash or planned maintenance) before the query finished.                                                                                                                               |
| `error_message`         | [`text`]                     | The error returned when executing the statement, or `NULL` if it was successful, canceled or aborted.                                                                                                                                                                                                      |
| `result_size`           | [`bigint`]                   | The size in bytes of the result, for statements that return rows.                                                                                                                                                                                                                 |
| `rows_returned`         | [`int8`]                     | The number of rows returned by the statement, if it finished successfully and was of a kind of statement that can return rows, or `NULL` otherwise.                                                                                                                                                        |
| `execution_strategy`    | [`text`]                     | `'standard'`, `'fast-path'` `'constant'`, or `NULL`. `'standard'` means a dataflow was built on a cluster to compute the result. `'fast-path'` means a cluster read the result from an existing arrangement. `'constant'` means the result was computed in the serving layer, without involving a cluster. |
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
