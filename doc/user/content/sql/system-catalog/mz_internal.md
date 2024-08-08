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
that have been issued to Materialize in the last three days, along
with various metadata about them.

Entries in this log may be sampled. The sampling rate is controlled by
the configuration parameter `statement_logging_sample_rate`, which may be set
to any value between 0 and 1. For example, to disable statement
logging entirely for a session, execute `SET
statement_logging_sample_rate TO 0`. Materialize may apply a lower
sampling rate than the one set in this parameter.

The view can be accessed by Materialize _superusers_ or users that have been
granted the [`mz_monitor` role](/manage/access-control/manage-roles#builtin-roles).

<!-- RELATION_SPEC mz_internal.mz_recent_activity_log -->
| Field                      | Type                         | Meaning                                                                                                                                                                                                                                                                       |
|----------------------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `execution_id`             | [`uuid`]                     | An ID that is unique for each executed statement.                                                                                                                                                                                                                             |
| `sample_rate`              | [`double precision`]         | The actual rate at which the statement was sampled.                                                                                                                                                                                                                           |
| `cluster_id`               | [`text`]                     | The ID of the cluster the statement execution was directed to. Corresponds to [mz_clusters.id](https://materialize.com/docs/sql/system-catalog/mz_catalog/#mz_clusters).                                                                                                      |
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
| `rows_returned`            | [`bigint`]                   | The number of rows returned, for statements that return rows.                                                                                                                                                                                                                 |
| `execution_strategy`       | [`text`]                     | For `SELECT` queries, the strategy for executing the query. `constant` means computed in the control plane without the involvement of a cluster, `fast-path` means read by a cluster directly from an in-memory index, and `standard` means computed by a temporary dataflow. |
| `transaction_id`           | [`uint8`]                    | The ID of the transaction that the statement was part of. Note that transaction IDs are only unique per session.                                                                                                                                                              |
| `prepared_statement_id`    | [`uuid`]                     | An ID that is unique for each prepared statement. For example, if a statement is prepared once and then executed multiple times, all executions will have the same value for this column (but different values for `execution_id`).                                           |
| `sql_hash`                 | [`bytea`]                    | An opaque value uniquely identifying the text of the query.                                                                                                                                                                                                                   |
| `prepared_statement_name`  | [`text`]                     | The name given by the client library to the prepared statement.                                                                                                                                                                                                               |
| `session_id`               | [`uuid`]                     | An ID that is unique for each session. Corresponds to [mz_sessions.id](#mz_sessions). |
| `prepared_at`              | [`timestamp with time zone`] | The time at which the statement was prepared.                                                                                                                                                                                                                                 |
| `statement_type`           | [`text`]                     | The _type_ of the statement, e.g. `select` for a `SELECT` query, or `NULL` if the statement was empty.                                                                                                                                                                        |
| `throttled_count`          | [`uint8`]                    | The number of statements that were dropped due to throttling before the current one was seen. If you have a very high volume of queries and need to log them without throttling, [contact our team](https://materialize.com/docs/support/).                                   |
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

## `mz_cluster_schedules`

The `mz_cluster_schedules` table shows the `SCHEDULE` option specified for each cluster.

<!-- RELATION_SPEC mz_internal.mz_cluster_schedules -->
| Field                               | Type         | Meaning                                                        |
|-------------------------------------|--------------|----------------------------------------------------------------|
| `cluster_id`                        | [`text`]     | The ID of the cluster. Corresponds to [`mz_clusters.id`](../mz_catalog/#mz_clusters). |
| `type`                              | [`text`]     | `on-refresh`, or `manual`. Default: `manual`                   |
| `refresh_hydration_time_estimate`   | [`interval`] | The interval given in the `HYDRATION TIME ESTIMATE` option.    |

## `mz_cluster_replica_frontiers`

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

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_frontiers -->
| Field            | Type             | Meaning                                                                |
| -----------------| ---------------- | --------                                                               |
| `object_id`      | [`text`]         | The ID of the source, sink, index, materialized view, or subscription. |
| `replica_id`     | [`text`]         | The ID of a cluster replica.                                           |
| `write_frontier` | [`mz_timestamp`] | The next timestamp at which the output may change.                     |

## `mz_cluster_replica_metrics`

The `mz_cluster_replica_metrics` table gives the last known CPU and RAM utilization statistics
for all processes of all extant cluster replicas.

At this time, we do not make any guarantees about the exactness or freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_metrics -->
| Field               | Type         | Meaning                                                                                                                                                      |
| ------------------- | ------------ | --------                                                                                                                                                     |
| `replica_id`        | [`text`]     | The ID of a cluster replica.                                                                                                                                 |
| `process_id`        | [`uint8`]    | An identifier of a compute process within a replica.                                                                                                         |
| `cpu_nano_cores`    | [`uint8`]    | Approximate CPU usage, in billionths of a vCPU core.                                                                                                         |
| `memory_bytes`      | [`uint8`]    | Approximate RAM usage, in bytes.                                                                                                                             |
| `disk_bytes`        | [`uint8`]    | Approximate disk usage in bytes.                                                                                                                             |

## `mz_cluster_replica_statuses`

The `mz_cluster_replica_statuses` table contains a row describing the status
of each process in each cluster replica in the system.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_statuses -->
| Field        | Type                         | Meaning                                                                                                 |
|--------------|------------------------------|---------------------------------------------------------------------------------------------------------|
| `replica_id` | [`text`]                     | Materialize's unique ID for the cluster replica.                                                        |
| `process_id` | [`uint8`]                    | The ID of the process within the cluster replica.                                                       |
| `status`     | [`text`]                     | The status of the cluster replica: `ready` or `not-ready`.                                              |
| `reason`     | [`text`]                     | If the cluster replica is in a `not-ready` state, the reason (if available). For example, `oom-killed`. |
| `updated_at` | [`timestamp with time zone`] | The time at which the status was last updated.                                                          |

## `mz_cluster_replica_utilization`

The `mz_cluster_replica_utilization` view gives the last known CPU and RAM utilization statistics
for all processes of all extant cluster replicas, as a percentage of the total resource allocation.

At this time, we do not make any guarantees about the exactness or freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_utilization -->
| Field            | Type                 | Meaning                                                                                                                                                                                |
|------------------|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `replica_id`     | [`text`]             | The ID of a cluster replica.                                                                                                                                                           |
| `process_id`     | [`uint8`]            | An identifier of a compute process within a replica.                                                                                                                                   |
| `cpu_percent`    | [`double precision`] | Approximate CPU usage in percent of the total allocation.                                                                                                                              |
| `memory_percent` | [`double precision`] | Approximate RAM usage in percent of the total allocation.                                                                                                                              |
| `disk_percent`   | [`double precision`] | Approximate disk usage in percent of the total allocation.                                                                                                                             |

## `mz_cluster_replica_history`

The `mz_cluster_replica_history` view contains information about the timespan of
each replica, including the times at which it was created and dropped
(if applicable).

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_history -->
| Field                 | Type                         | Meaning                                                                                                                                   |
|-----------------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `replica_id`          | [`text`]                     | The ID of a cluster replica.                                                                                                              |
| `size`                | [`text`]                     | The size of the cluster replica. Corresponds to [`mz_cluster_replica_sizes.size`](../mz_catalog#mz_cluster_replica_sizes).                             |
| `cluster_name`        | [`text`]                     | The name of the cluster associated with the replica.                                                                                      |
| `replica_name`        | [`text`]                     | The name of the replica.                                                                                                                  |
| `created_at`          | [`timestamp with time zone`] | The time at which the replica was created.                                                                                                |
| `dropped_at`          | [`timestamp with time zone`] | The time at which the replica was dropped, or `NULL` if it still exists.                                                                  |
| `credits_per_hour`    | [`numeric`]                  | The number of compute credits consumed per hour. Corresponds to [`mz_cluster_replica_sizes.credits_per_hour`](../mz_catalog#mz_cluster_replica_sizes). |

## `mz_internal_cluster_replicas`

The `mz_internal_cluster_replicas` table lists the replicas that are created and maintained by Materialize support.

<!-- RELATION_SPEC mz_internal.mz_internal_cluster_replicas -->
| Field      | Type     | Meaning                                                                                                     |
|------------|----------|-------------------------------------------------------------------------------------------------------------|
| id         | [`text`] | The ID of a cluster replica. Corresponds to [`mz_cluster_replicas.id`](../mz_catalog/#mz_cluster_replicas). |

## `mz_unstable_cluster_replicas`

The `mz_unstable_cluster_replicas` table lists the replicas that were created during managed cluster alter statement that has not yet finished. The configurations of these replica may differ from the cluster's configuration.

<!-- RELATION_SPEC mz_internal.mz_unstable_cluster_replicas -->
| Field      | Type     | Meaning                                                                                                     |
|------------|----------|-------------------------------------------------------------------------------------------------------------|
| id         | [`text`] | The ID of a cluster replica. Corresponds to [`mz_cluster_replicas.id`](../mz_catalog/#mz_cluster_replicas). |

## `mz_comments`

The `mz_comments` table stores optional comments (descriptions) for objects in the database.

<!-- RELATION_SPEC mz_internal.mz_comments -->
| Field          | Type        | Meaning                                                                                      |
| -------------- |-------------| --------                                                                                     |
| `id`           | [`text`]    | The ID of the object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects).           |
| `object_type`  | [`text`]    | The type of object the comment is associated with.                                           |
| `object_sub_id`| [`integer`] | For a comment on a column of a relation, this is the column number. For all other object types this column is `NULL`. |
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
| `object_id`             | [`text`]    | The ID of a compute object. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes) or [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views). |
| `physical_plan_node_id` | [`uint8`]   | The ID of a node in the physical plan of the compute object. Corresponds to a `node_id` displayed in the output of `EXPLAIN PHYSICAL PLAN WITH (node identifiers)`. |
| `replica_id`            | [`text`]    | The ID of a cluster replica. |
| `hydrated`              | [`boolean`] | Whether the node is hydrated on the replica. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_operator_hydration_statuses_per_worker -->

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

## `mz_kafka_sources`

The `mz_kafka_sources` table contains a row for each Kafka source in the system.

<!-- RELATION_SPEC mz_internal.mz_kafka_sources -->
| Field                  | Type           | Meaning                                                                                                   |
|------------------------|----------------|-----------------------------------------------------------------------------------------------------------|
| `id`                   | [`text`]       | The ID of the Kafka source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).        |
| `group_id_prefix`      | [`text`]       | The value of the `GROUP ID PREFIX` connection option.                                                     |
| `topic          `      | [`text`]       | The name of the Kafka topic the source is reading from.                                                              |

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
| Field          | Type       | Meaning                                                                                                                                        |
| ---------------|------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `id`           | [`text`]   | Materialize's unique ID for the object.                                                                                                        |
| `name`         | [`text`]   | The name of the object.                                                                                                                        |
| `object_type`  | [`text`]   | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`. |
| `schema_id`    | [`text`]   | The ID of the schema to which the object belongs. Corresponds to [`mz_schemas.id`](/sql/system-catalog/mz_catalog/#mz_schemas).                |
| `schema_name`  | [`text`]   | The name of the schema to which the object belongs. Corresponds to [`mz_schemas.name`](/sql/system-catalog/mz_catalog/#mz_schemas).            |
| `database_id`  | [`text`]   | The ID of the database to which the object belongs. Corresponds to [`mz_databases.id`](/sql/system-catalog/mz_catalog/#mz_schemas).             |
| `database_name`| [`text`]   | The name of the database to which the object belongs. Corresponds to [`mz_databases.name`](/sql/system-catalog/mz_catalog/#mz_databases).      |

## `mz_object_lifetimes`

The `mz_object_lifetimes` view enriches the [`mz_catalog.mz_objects`](/sql/system-catalog/mz_catalog/#mz_objects) view with information about the last lifetime event that occurred for each object in the system.

<!-- RELATION_SPEC mz_internal.mz_object_lifetimes -->
| Field          | Type                         | Meaning                                          |
| ---------------|------------------------------|------------------------------------------------- |
| `id`           | [`text`]                     | Materialize's unique ID for the object.          |
| `previous_id`  | [`text`]                     | The object's previous ID, if one exists.          |
| `object_type`  | [`text`]                     | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.                                                                              |
| `event_type`   | [`text`]                     | The lifetime event, either `create` or `drop`.   |
| `occurred_at`  | [`timestamp with time zone`] | Wall-clock timestamp of when the event occurred. |

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

The `mz_postgres_source_tables` table contains the mapping between each
subsource and the corresponding upstream PostgreSQL table being ingested.

<!-- RELATION_SPEC mz_internal.mz_postgres_source_tables -->
| Field               | Type             | Meaning                                                                                                        |
| ------------------- | ---------------- | --------                                                                                                       |
| `id`                | [`text`]         | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                   |
| `schema_name`       | [`text`]         | The schema of the upstream table being ingested. |
| `table_name`        | [`text`]         | The name of the upstream table being ingested.   |

## `mz_mysql_source_tables`

The `mz_mysql_source_tables` table contains the mapping between each
subsource and the corresponding upstream MySQL table being ingested.

<!-- RELATION_SPEC mz_internal.mz_mysql_source_tables -->
| Field               | Type             | Meaning                                                                                                        |
| ------------------- | ---------------- | --------                                                                                                       |
| `id`                | [`text`]         | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                   |
| `schema_name`       | [`text`]         | The schema ([or, database](https://dev.mysql.com/doc/refman/8.0/en/glossary.html#glos_schema)) of the upstream table being ingested. |
| `table_name`        | [`text`]         | The name of the upstream table being ingested. |

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

<!--
## `mz_session_history`

The `mz_session_history` table contains all the sessions that have
been established in the last 30 days, or (even if older) that are
referenced from
[`mz_prepared_statement_history`](#mz_prepared_statement_history).

| Field                | Type                         | Meaning                                                                                                                           |
|----------------------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `session_id`         | [`uuid`]                     | The globally unique ID of the session. Corresponds to [`mz_sessions.id`](#mz_sessions).                                           |
| `connected_at`       | [`timestamp with time zone`] | The time at which the session was established.                                                                                    |
| `application_name`   | [`text`]                     | The `application_name` session metadata field.                                                                                    |
| `authenticated_user` | [`text`]                     | The name of the user for which the session was established.                                                                       |
-->

### `mz_recent_storage_usage`

{{< warn-if-unreleased "v0.111" >}}

The `mz_recent_storage_usage` table describes the storage utilization of each
table, source, and materialized view in the system in the most recent storage
utilization assessment. Storage utilization assessments occur approximately
every hour.

See [`mz_storage_usage`](../mz_catalog#mz_storage_usage) for historical storage
usage information.

<!-- RELATION_SPEC mz_internal.mz_recent_storage_usage -->
Field                  | Type                         | Meaning
---------------------- | ---------------------------- | -----------------------------------------------------------
`object_id`            | [`text`]                     | The ID of the table, source, or materialized view.
`size_bytes`           | [`uint8`]                    | The number of storage bytes used by the object in the most recent assessment.

## `mz_sessions`

The `mz_sessions` table contains a row for each active session in the system.

<!-- RELATION_SPEC mz_internal.mz_sessions -->
| Field           | Type                           | Meaning                                                                                                                   |
| --------------- | ------------------------------ | --------                                                                                                                  |
| `id`            | [`uuid`]                       | The globally unique ID of the session. |
| `connection_id` | [`uint4`]                      | The connection ID of the session. Unique only for active sessions and can be recycled. Corresponds to [`pg_backend_pid()`](/sql/functions/#pg_backend_pid). |
| `role_id`       | [`text`]                       | The role ID of the role that the session is logged in as. Corresponds to [`mz_catalog.mz_roles`](../mz_catalog#mz_roles). |
| `connected_at`  | [`timestamp with time zone`]   | The time at which the session connected to the system.                                                                    |

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
| Field                | Type      | Meaning                                                                                                             |
|----------------------|-----------| --------                                                                                                            |
| `id`                 | [`text`]  | The ID of the sink. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sinks).                            |
| `messages_staged`    | [`uint8`] | The number of messages staged but possibly not committed to the sink.                                               |
| `messages_committed` | [`uint8`] | The number of messages committed to the sink.                                                                       |
| `bytes_staged`       | [`uint8`] | The number of bytes staged but possibly not committed to the sink. This counts both keys and values, if applicable. |
| `bytes_committed`    | [`uint8`] | The number of bytes committed to the sink. This counts both keys and values, if applicable.                         |

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

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_source_statistics_raw -->

## `mz_source_statistics`

The `mz_source_statistics` view contains statistics about each source.

<!-- RELATION_SPEC mz_internal.mz_source_statistics -->
| Field                    | Type        | Meaning                                                                                                                                                                                                                                                                             |
| -------------------------|-------------| --------                                                                                                                                                                                                                                                                            |
| `id`                     | [`text`]     | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                                                                                                                                                                                        |
| `messages_received`       | [`uint8`]    | The number of messages the worker has received from the external system. Messages are counted in a source type-specific manner. Messages do not correspond directly to updates: some messages produce multiple updates, while other messages may be coalesced into a single update. |
| `bytes_received`          | [`uint8`]    | The number of bytes the worker has read from the external system. Bytes are counted in a source type-specific manner and may or may not include protocol overhead.                                                                                                                  |
| `updates_staged`          | [`uint8`]    | The number of updates (insertions plus deletions) the worker has written but not yet committed to the storage layer.                                                                                                                                                                |
| `updates_committed`       | [`uint8`]    | The number of updates (insertions plus deletions) the worker has committed to the storage layer.                                                                                                                                                                                    |
| `records_indexed`         | [`uint8`]    | The number of individual records indexed in the source envelope state.                                                                                                                                                                                                              |
| `bytes_indexed`           | [`uint8`]    | The number of bytes indexed in the source envelope state.                                                                                                                                                                                                                           |
| `rehydration_latency`     | [`interval`] | The amount of time it took for the worker to rehydrate the source envelope state.                                                                                                                                                                                                   |
| `snapshot_records_known`  | [`uint8`]    | The size of the source's snapshot. See above for its unit. |
| `snapshot_records_staged` | [`uint8`]    | The amount of the source's snapshot Materialize has read. See above for its unit. |
| `snapshot_committed`      | [`boolean`]  | Whether the worker has committed the initial snapshot for a source.                                                                                                                                                                                                                 |
| `offset_known`            | [`uint8`]    | The offset of the most recent data in the source's upstream service that Materialize knows about. See above for its unit. |
| `offset_committed`        | [`uint8`]    | The offset of the source's upstream service Materialize has fully committed. See above for its unit. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_source_statistics_with_history -->

### Counters
`messages_received`, `messages_staged`, `updates_staged`, and `updates_committed` are all counters that monotonically increase. They are _only
useful for calculating rates_, to understand the general performance of your source.

Note that:
- For Postgres and MySQL sources, currently, the former 2 are collected on the top-level source, and the latter 2 on the source's tables.
- The non-rate values themselves are not directly comparable, because they are collected and aggregated across multiple threads/processes.

### Resetting gauges
Resetting Gauges generally increase, but can periodically be reset to 0 or other numbers.

---
**Indexed records**

`records_indexed` and `bytes_indexed` are the size (in records and bytes respectively) of the data the given source _indexes_. Currently, this is only
`UPSERT` and `DEBEZIUM` sources. These reset to 0 when sources are restarted and must re-index their data.

---
**Rehydration latency**

`rehydration_latency` is reset to `NULL` when sources are restarted, and is populated with a duration after rehydration finishes. This is typically
the time it takes `UPSERT` and `DEBEZIUM` sources to re-index their data.

---
**Snapshot progress**

When a source is first created, it must process and initial snapshot of data. `snapshot_records_known` is the full size of that snapshot, and `snapshot_records_staged`
is how much of that snapshot the source has read so far.

The _size_ of the snapshot has a source-defined unit:
- For Kafka sources, its the total number of offsets in the snapshot.
- For Postgres and MySQL sources, its the number of rows in the snapshot.

Note that when tables are added to Postgres or MySQL sources, this statistics will reset as we snapshot those new tables.

### Gauges
Gauges never decrease/reset.

---
**Snapshot Completion**
`snapshot_committed` becomes true when we have fully _committed_ the snapshot for the given source.

---
**Steady-state progress**

`offset_known` and `offset_committed` are used to represent the _progress_ a source is making,
in comparison to its upstream source. They are designed to be turned into _rates_, and compared.

`offset_known` is the maximum offset of upstream data knows about. `offset_committed` is the offset that Materialize has committed data up to.

These statistics have a source-defined unit:
- For Kafka sources, its the number of offsets.
- For MySQL sources, its the number of transactions.
- For Postgres sources, its the number of bytes in its replication stream.

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
| `status`                 | [`text`]                        | The status of the source: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.              |
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
| `status`       | [`text`]                        | The status of the source: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.              |
| `error`        | [`text`]                        | If the source is in an error state, the error message.                                                             |
| `details`      | [`jsonb`]                       | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions. |

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
[`numeric`]: /sql/types/numeric
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
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_recent_activity_log_redacted -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_recent_activity_log_thinned -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_aggregates -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_prepared_statement_history -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_recent_sql_text -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_recent_sql_text_redacted -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_session_history -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_cluster_replicas -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_indexes -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_materialized_views -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_sinks -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_sources -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_sql_text -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_sql_text_redacted -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_statement_execution_history -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_statement_execution_history_redacted -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_storage_shards -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_storage_usage_by_shard -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_type_pg_metadata -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_object_oid_alias -->

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_class_all_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_type_all_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_namespace_all_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_description_all_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_attrdef_all_databases -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.pg_attribute_all_databases -->
