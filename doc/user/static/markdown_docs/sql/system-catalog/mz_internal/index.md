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

# mz_internal

The following sections describe the available objects in the
`mz_internal` schema.

<div class="warning">

**WARNING!** The objects in the `mz_internal` schema are not part of
Materialize’s stable interface. Backwards-incompatible changes to these
objects may be made at any time.

</div>

<div class="warning">

**WARNING!** `SELECT` statements may reference these objects, but
creating views that reference these objects is not allowed.

</div>

## `mz_recent_activity_log`

<div class="public-preview">

**PREVIEW** This feature is in **[public
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.

</div>

<div class="warning">

**WARNING!** Do not rely on all statements being logged in this view.
Materialize controls the maximum rate at which statements are sampled,
and may change this rate at any time.

</div>

<div class="warning">

**WARNING!** Entries in this view may be cleared on restart (e.g.,
during Materialize maintenance windows).

</div>

The `mz_recent_activity_log` view contains a log of the SQL statements
that have been issued to Materialize in the last 24 hours, along with
various metadata about them.

Entries in this log may be sampled. The sampling rate is controlled by
the configuration parameter `statement_logging_sample_rate`, which may
be set to any value between 0 and 1. For example, to disable statement
logging entirely for a session, execute
`SET statement_logging_sample_rate TO 0`. Materialize may apply a lower
sampling rate than the one set in this parameter.

The view can be accessed by Materialize *superusers* or users that have
been granted the [`mz_monitor`
role](/docs/self-managed/v25.2/manage/access-control/appendix-built-in-roles/#system-catalog-roles).

| Field | Type | Meaning |
|----|----|----|
| `execution_id` | [`uuid`](/docs/self-managed/v25.2/sql/types/uuid) | An ID that is unique for each executed statement. |
| `sample_rate` | [`double precision`](/docs/self-managed/v25.2/sql/types/double-precision) | The actual rate at which the statement was sampled. |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster the statement execution was directed to. Corresponds to [mz_clusters.id](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_clusters). |
| `application_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The value of the `application_name` configuration parameter at execution time. |
| `cluster_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the cluster with ID `cluster_id` at execution time. |
| `database_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The value of the `database` configuration parameter at execution time. |
| `search_path` | [`text list`](/docs/self-managed/v25.2/sql/types/list) | The value of the `search_path` configuration parameter at execution time. |
| `transaction_isolation` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The value of the `transaction_isolation` configuration parameter at execution time. |
| `execution_timestamp` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The logical timestamp at which execution was scheduled. |
| `transient_index_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The internal index of the compute dataflow created for the query, if any. |
| `params` | [`text array`](/docs/self-managed/v25.2/sql/types/array) | The parameters with which the statement was executed. |
| `mz_version` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The version of Materialize that was running when the statement was executed. |
| `began_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The wall-clock time at which the statement began executing. |
| `finished_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The wall-clock time at which the statement finished executing. |
| `finished_status` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The final status of the statement (e.g., `success`, `canceled`, `error`, or `aborted`). `aborted` means that Materialize exited before the statement finished executing. |
| `error_message` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The error message, if the statement failed. |
| `result_size` | [`bigint`](/docs/self-managed/v25.2/sql/types/bigint) | The size in bytes of the result, for statements that return rows. |
| `rows_returned` | [`bigint`](/docs/self-managed/v25.2/sql/types/bigint) | The number of rows returned, for statements that return rows. |
| `execution_strategy` | [`text`](/docs/self-managed/v25.2/sql/types/text) | For `SELECT` queries, the strategy for executing the query. `constant` means computed in the control plane without the involvement of a cluster, `fast-path` means read by a cluster directly from an in-memory index, and `standard` means computed by a temporary dataflow. |
| `transaction_id` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The ID of the transaction that the statement was part of. Note that transaction IDs are only unique per session. |
| `prepared_statement_id` | [`uuid`](/docs/self-managed/v25.2/sql/types/uuid) | An ID that is unique for each prepared statement. For example, if a statement is prepared once and then executed multiple times, all executions will have the same value for this column (but different values for `execution_id`). |
| `sql_hash` | [`bytea`](/docs/self-managed/v25.2/sql/types/bytea) | An opaque value uniquely identifying the text of the query. |
| `prepared_statement_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name given by the client library to the prepared statement. |
| `session_id` | [`uuid`](/docs/self-managed/v25.2/sql/types/uuid) | An ID that is unique for each session. Corresponds to [mz_sessions.id](#mz_sessions). |
| `prepared_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the statement was prepared. |
| `statement_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The *type* of the statement, e.g. `select` for a `SELECT` query, or `NULL` if the statement was empty. |
| `throttled_count` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of statements that were dropped due to throttling before the current one was seen. If you have a very high volume of queries and need to log them without throttling, [contact our team](/docs/self-managed/v25.2/support/). |
| `initial_application_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The initial value of `application_name` at the beginning of the session. |
| `authenticated_user` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the user for which the session was established. |
| `sql` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The SQL text of the statement. |

## `mz_aws_connections`

The `mz_aws_connections` table contains a row for each AWS connection in
the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the connection. |
| `endpoint` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The value of the `ENDPOINT` option, if set. |
| `region` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The value of the `REGION` option, if set. |
| `access_key_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The value of the `ACCESS KEY ID` option, if provided in line. |
| `access_key_id_secret_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the secret referenced by the `ACCESS KEY ID` option, if provided via a secret. |
| `secret_access_key_secret_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the secret referenced by the `SECRET ACCESS KEY` option, if set. |
| `session_token` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The value of the `SESSION TOKEN` option, if provided in line. |
| `session_token_secret_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the secret referenced by the `SESSION TOKEN` option, if provided via a secret. |
| `assume_role_arn` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The value of the `ASSUME ROLE ARN` option, if set. |
| `assume_role_session_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The value of the `ASSUME ROLE SESSION NAME` option, if set. |
| `principal` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ARN of the AWS principal Materialize will use when assuming the provided role, if the connection is configured to use role assumption. |
| `external_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The external ID Materialize will use when assuming the provided role, if the connection is configured to use role assumption. |
| `example_trust_policy` | [`jsonb`](/docs/self-managed/v25.2/sql/types/jsonb) | An example of an IAM role trust policy that allows this connection’s principal and external ID to assume the role. |

## `mz_aws_privatelink_connection_status_history`

The `mz_aws_privatelink_connection_status_history` table contains a row
describing the historical status for each AWS PrivateLink connection in
the system.

| Field | Type | Meaning |
|----|----|----|
| `occurred_at` | `timestamp with time zone` | Wall-clock timestamp of the status change. |
| `connection_id` | `text` | The unique identifier of the AWS PrivateLink connection. Corresponds to [`mz_catalog.mz_connections.id`](../mz_catalog#mz_connections). |
| `status` | `text` | The status of the connection: one of `pending-service-discovery`, `creating-endpoint`, `recreating-endpoint`, `updating-endpoint`, `available`, `deleted`, `deleting`, `expired`, `failed`, `pending`, `pending-acceptance`, `rejected`, or `unknown`. |

## `mz_aws_privatelink_connection_statuses`

The `mz_aws_privatelink_connection_statuses` table contains a row
describing the most recent status for each AWS PrivateLink connection in
the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the connection. Corresponds to [`mz_catalog.mz_connections.id`](../mz_catalog#mz_sinks). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the connection. |
| `last_status_change_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp of the connection status change. |
| `status` | [`text`](/docs/self-managed/v25.2/sql/types/text) |  |

## `mz_cluster_deployment_lineage`

The `mz_cluster_deployment_lineage` table shows the blue/green
deployment lineage of all clusters in
[`mz_clusters`](../mz_catalog/#mz_clusters). It determines all cluster
IDs that are logically the same cluster.

| Field | Type | Meaning |
|----|----|----|
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster. Corresponds to [`mz_clusters.id`](../mz_catalog/#mz_clusters) (though the cluster may no longer exist). |
| `current_deployment_cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The cluster ID of the last cluster in `cluster_id`’s blue/green lineage (the cluster is guaranteed to exist). |
| `cluster_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the cluster |

## `mz_cluster_schedules`

The `mz_cluster_schedules` table shows the `SCHEDULE` option specified
for each cluster.

| Field | Type | Meaning |
|----|----|----|
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster. Corresponds to [`mz_clusters.id`](../mz_catalog/#mz_clusters). |
| `type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | `on-refresh`, or `manual`. Default: `manual` |
| `refresh_hydration_time_estimate` | [`interval`](/docs/self-managed/v25.2/sql/types/interval) | The interval given in the `HYDRATION TIME ESTIMATE` option. |

## `mz_cluster_replica_metrics`

The `mz_cluster_replica_metrics` table gives the last known CPU and RAM
utilization statistics for all processes of all extant cluster replicas.

At this time, we do not make any guarantees about the exactness or
freshness of these numbers.

| Field | Type | Meaning |
|----|----|----|
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. |
| `process_id` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The ID of a process within the replica. |
| `cpu_nano_cores` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | Approximate CPU usage, in billionths of a vCPU core. |
| `memory_bytes` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | Approximate RAM usage, in bytes. |
| `disk_bytes` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | Approximate disk usage in bytes. |

## `mz_cluster_replica_metrics_history`

The `mz_cluster_replica_metrics_history` table records resource
utilization metrics for all processes of all extant cluster replicas.

At this time, we do not make any guarantees about the exactness or
freshness of these numbers.

| Field | Type | Meaning |
|----|----|----|
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. |
| `process_id` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The ID of a process within the replica. |
| `cpu_nano_cores` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | Approximate CPU usage in billionths of a vCPU core. |
| `memory_bytes` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | Approximate memory usage in bytes. |
| `disk_bytes` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | Approximate disk usage in bytes. |
| `occurred_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp at which the event occurred. |

## `mz_cluster_replica_statuses`

The `mz_cluster_replica_statuses` table contains a row describing the
status of each process in each cluster replica in the system.

| Field | Type | Meaning |
|----|----|----|
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the cluster replica. |
| `process_id` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The ID of the process within the cluster replica. |
| `status` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The status of the cluster replica: `online` or `offline`. |
| `reason` | [`text`](/docs/self-managed/v25.2/sql/types/text) | If the cluster replica is in a `offline` state, the reason (if available). For example, `oom-killed`. |
| `updated_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the status was last updated. |

## `mz_cluster_replica_status_history`

The `mz_cluster_replica_status_history` table records status changes for
all processes of all extant cluster replicas.

| Field | Type | Meaning |
|----|----|----|
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. |
| `process_id` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The ID of a process within the replica. |
| `status` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The status of the cluster replica: `online` or `offline`. |
| `reason` | [`text`](/docs/self-managed/v25.2/sql/types/text) | If the cluster replica is in an `offline` state, the reason (if available). For example, `oom-killed`. |
| `occurred_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp at which the event occurred. |

## `mz_cluster_replica_utilization`

The `mz_cluster_replica_utilization` view gives the last known CPU and
RAM utilization statistics for all processes of all extant cluster
replicas, as a percentage of the total resource allocation.

At this time, we do not make any guarantees about the exactness or
freshness of these numbers.

| Field | Type | Meaning |
|----|----|----|
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. |
| `process_id` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The ID of a process within the replica. |
| `cpu_percent` | [`double precision`](/docs/self-managed/v25.2/sql/types/double-precision) | Approximate CPU usage in percent of the total allocation. |
| `memory_percent` | [`double precision`](/docs/self-managed/v25.2/sql/types/double-precision) | Approximate RAM usage in percent of the total allocation. |
| `disk_percent` | [`double precision`](/docs/self-managed/v25.2/sql/types/double-precision) | Approximate disk usage in percent of the total allocation. |

## `mz_cluster_replica_utilization_history`

The `mz_cluster_replica_utilization_history` view records resource
utilization metrics for all processes of all extant cluster replicas, as
a percentage of the total resource allocation.

At this time, we do not make any guarantees about the exactness or
freshness of these numbers.

| Field | Type | Meaning |
|----|----|----|
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. |
| `process_id` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The ID of a process within the replica. |
| `cpu_percent` | [`double precision`](/docs/self-managed/v25.2/sql/types/double-precision) | Approximate CPU usage in percent of the total allocation. |
| `memory_percent` | [`double precision`](/docs/self-managed/v25.2/sql/types/double-precision) | Approximate RAM usage in percent of the total allocation. |
| `disk_percent` | [`double precision`](/docs/self-managed/v25.2/sql/types/double-precision) | Approximate disk usage in percent of the total allocation. |
| `occurred_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp at which the event occurred. |

## `mz_cluster_replica_history`

The `mz_cluster_replica_history` view contains information about the
timespan of each replica, including the times at which it was created
and dropped (if applicable).

| Field | Type | Meaning |
|----|----|----|
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. |
| `size` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The size of the cluster replica. Corresponds to [`mz_cluster_replica_sizes.size`](../mz_catalog#mz_cluster_replica_sizes). |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster associated with the replica. |
| `cluster_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the cluster associated with the replica. |
| `replica_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the replica. |
| `created_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the replica was created. |
| `dropped_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the replica was dropped, or `NULL` if it still exists. |
| `credits_per_hour` | [`numeric`](/docs/self-managed/v25.2/sql/types/numeric) | The number of compute credits consumed per hour. Corresponds to [`mz_cluster_replica_sizes.credits_per_hour`](../mz_catalog#mz_cluster_replica_sizes). |

## `mz_cluster_replica_name_history`

The `mz_cluster_replica_name_history` view contains historical
information about names of each cluster replica.

| Field | Type | Meaning |
|----|----|----|
| `occurred_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the cluster replica was created or renamed. `NULL` if it’s a built in system cluster replica. |
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster replica. |
| `previous_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The previous name of the cluster replica. `NULL` if there was no previous name. |
| `new_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The new name of the cluster replica. |

## `mz_internal_cluster_replicas`

The `mz_internal_cluster_replicas` table lists the replicas that are
created and maintained by Materialize support.

| Field | Type | Meaning |
|----|----|----|
| id | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. Corresponds to [`mz_cluster_replicas.id`](../mz_catalog/#mz_cluster_replicas). |

## `mz_pending_cluster_replicas`

The `mz_pending_cluster_replicas` table lists the replicas that were
created during managed cluster alter statement that has not yet
finished. The configurations of these replica may differ from the
cluster’s configuration.

| Field | Type | Meaning |
|----|----|----|
| id | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. Corresponds to [`mz_cluster_replicas.id`](../mz_catalog/#mz_cluster_replicas). |

## `mz_comments`

The `mz_comments` table stores optional comments (i.e., descriptions)
for objects in the database.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of object the comment is associated with. |
| `object_sub_id` | [`integer`](/docs/self-managed/v25.2/sql/types/integer) | For a comment on a column of a relation, the column number. `NULL` for other object types. |
| `comment` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The comment itself. |

## `mz_compute_dependencies`

The `mz_compute_dependencies` table describes the dependency structure
between each compute object (index, materialized view, or subscription)
and the sources of its data.

In contrast to [`mz_object_dependencies`](#mz_object_dependencies), this
table only lists dependencies in the compute layer. SQL objects that
don’t exist in the compute layer (such as views) are omitted.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a compute object. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), or [`mz_internal.mz_subscriptions`](#mz_subscriptions). |
| `dependency_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a compute dependency. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources), or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |

## `mz_compute_hydration_statuses`

The `mz_compute_hydration_statuses` view describes the per-replica
hydration status of each compute object (index, materialized view).

A compute object is hydrated on a given replica when it has fully
processed the initial snapshot of data available in its inputs.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a compute object. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes) or [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views) |
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. |
| `hydrated` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Whether the compute object is hydrated on the replica. |
| `hydration_time` | [`interval`](/docs/self-managed/v25.2/sql/types/interval) | The amount of time it took for the replica to hydrate the compute object. |

## `mz_compute_operator_hydration_statuses`

The `mz_compute_operator_hydration_statuses` table describes the
dataflow operator hydration status of compute objects (indexes or
materialized views).

A dataflow operator is hydrated on a given replica when it has fully
processed the initial snapshot of data available in its inputs.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a compute object. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes) or [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views). |
| `physical_plan_node_id` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The ID of a node in the physical plan of the compute object. Corresponds to a `node_id` displayed in the output of `EXPLAIN PHYSICAL PLAN WITH (node identifiers)`. |
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. |
| `hydrated` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Whether the node is hydrated on the replica. |

## `mz_frontiers`

The `mz_frontiers` table describes the frontiers of each source, sink,
table, materialized view, index, and subscription in the system, as
observed from the coordinator.

At this time, we do not make any guarantees about the freshness of these
numbers.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the source, sink, table, index, materialized view, or subscription. |
| `read_frontier` | [`mz_timestamp`](/docs/self-managed/v25.2/sql/types/mz_timestamp) | The earliest timestamp at which the output is still readable. |
| `write_frontier` | [`mz_timestamp`](/docs/self-managed/v25.2/sql/types/mz_timestamp) | The next timestamp at which the output may change. |

## `mz_history_retention_strategies`

The `mz_history_retention_strategies` describes the history retention
strategies for tables, sources, indexes, materialized views that are
configured with a [history retention
period](/docs/self-managed/v25.2/transform-data/patterns/durable-subscriptions/#history-retention-period).

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the object. |
| `strategy` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The strategy. `FOR` is the only strategy, and means the object’s compaction window is the duration of the `value` field. |
| `value` | [`jsonb`](/docs/self-managed/v25.2/sql/types/jsonb) | The value of the strategy. For `FOR`, is a number of milliseconds. |

## `mz_hydration_statuses`

The `mz_hydration_statuses` view describes the per-replica hydration
status of each object powered by a dataflow.

A dataflow-powered object is hydrated on a given replica when the
respective dataflow has fully processed the initial snapshot of data
available in its inputs.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a dataflow-powered object. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), [`mz_internal.mz_subscriptions`](#mz_subscriptions), [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources), or [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks). |
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a cluster replica. |
| `hydrated` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Whether the object is hydrated on the replica. |

## `mz_index_advice`

<div class="warning">

**WARNING!** Following the advice in this view might not always yield
resource usage optimizations. You should test any changes in a
development environment before deploying the changes to production.

</div>

The `mz_index_advice` view provides advice on opportunities to optimize
resource usage (memory and CPU) in Materialize. The advice provided
suggests either creating indexes or materialized views to precompute
intermediate results that can be reused across several objects, or
removing unnecessary indexes or materialized views.

### Known limitations

The suggestions are based on the graph of dependencies between objects
and do not take into account other important factors, like the actual
usage patterns and execution plans. This means that following the advice
in this view **might not always lead to resource usage optimizations**.
In some cases, the provided advice might lead to suboptimal execution
plans or even increased resource usage. For example:

- If a materialized view or an index has been created for direct
  querying, the dependency graph will not reflect this nuance and
  `mz_index_advice` might recommend using an unindexed view instead. In
  this case, you should refer to the reference documentation for [query
  optimization](/docs/self-managed/v25.2/transform-data/optimization/#indexes)
  instead.
- If a view is depended on by multiple objects that use very selective
  filters, or multiple projections that can be pushed into or even
  beyond the view, adding an index may increase resource usage.
- If an index has been created to [enable delta
  joins](/docs/self-managed/v25.2/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins),
  removing it may lead to lower memory utilization, but the delta join
  optimization will no longer be used in the join implementation.

To guarantee that there are no regressions given your specific usage
patterns, it’s important to test any changes in a development
environment before deploying the changes to production.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the object. Corresponds to [mz_objects.id](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_objects). |
| `hint` | [`text`](/docs/self-managed/v25.2/sql/types/text) | A suggestion to either change the object (e.g. create an index, turn a materialized view into an indexed view) or keep the object unchanged. |
| `details` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Additional details on why the `hint` was proposed based on the dependencies of the object. |
| `referenced_object_ids` | \[`list`\] | The IDs of objects referenced by `details`. Corresponds to [mz_objects.id](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_objects). |

## `mz_materialization_dependencies`

The `mz_materialization_dependencies` view describes the dependency
structure between each materialization (materialized view, index, or
sink) and the sources of its data.

In contrast to [`mz_object_dependencies`](#mz_object_dependencies), this
view only lists dependencies in the dataflow layer. SQL objects that
don’t exist in the dataflow layer (such as views) are omitted.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a materialization. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), or [`mz_catalog.mz_sinks.id`](#mz_subscriptions). |
| `dependency_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a dataflow dependency. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources), or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |

## `mz_materialization_lag`

The `mz_materialization_lag` view describes the difference between the
input frontiers and the output frontier for each materialized view,
index, and sink in the system. For hydrated dataflows, this lag roughly
corresponds to the time it takes for updates at the inputs to be
reflected in the output.

At this time, we do not make any guarantees about the freshness of these
numbers.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the materialized view, index, or sink. |
| `local_lag` | [`interval`](/docs/self-managed/v25.2/sql/types/interval) | The amount of time the materialization lags behind its direct inputs. |
| `global_lag` | [`interval`](/docs/self-managed/v25.2/sql/types/interval) | The amount of time the materialization lags behind its root inputs (sources and tables). |
| `slowest_local_input_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the slowest direct input. |
| `slowest_global_input_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the slowest root input. |

## `mz_materialized_view_refresh_strategies`

The `mz_materialized_view_refresh_strategies` table shows the refresh
strategies specified for materialized views. If a materialized view has
multiple refresh strategies, a row will exist for each.

| Field | Type | Meaning |
|----|----|----|
| `materialized_view_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the materialized view. Corresponds to [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views) |
| `type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | `at`, `every`, or `on-commit`. Default: `on-commit` |
| `interval` | [`interval`](/docs/self-managed/v25.2/sql/types/interval) | The refresh interval of a `REFRESH EVERY` option, or `NULL` if the `type` is not `every`. |
| `aligned_to` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The `ALIGNED TO` option of a `REFRESH EVERY` option, or `NULL` if the `type` is not `every`. |
| `at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time of a `REFRESH AT`, or `NULL` if the `type` is not `at`. |

## `mz_materialized_view_refreshes`

The `mz_materialized_view_refreshes` table shows the time of the last
successfully completed refresh and the time of the next scheduled
refresh for each materialized view with a refresh strategy other than
`on-commit`.

| Field | Type | Meaning |
|----|----|----|
| `materialized_view_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the materialized view. Corresponds to [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views) |
| `last_completed_refresh` | [`mz_timestamp`](/docs/self-managed/v25.2/sql/types/mz_timestamp) | The time of the last successfully completed refresh. `NULL` if the materialized view hasn’t completed any refreshes yet. |
| `next_refresh` | [`mz_timestamp`](/docs/self-managed/v25.2/sql/types/mz_timestamp) | The time of the next scheduled refresh. `NULL` if the materialized view has no future scheduled refreshes. |

## `mz_object_dependencies`

The `mz_object_dependencies` table describes the dependency structure
between all database objects in the system.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the dependent object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |
| `referenced_object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the referenced object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |

## `mz_object_fully_qualified_names`

The `mz_object_fully_qualified_names` view enriches the
[`mz_catalog.mz_objects`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_objects)
view with namespace information.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the object. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the object. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`. |
| `schema_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the schema to which the object belongs. Corresponds to [`mz_schemas.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `schema_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the schema to which the object belongs. Corresponds to [`mz_schemas.name`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `database_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the database to which the object belongs. Corresponds to [`mz_databases.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_schemas). |
| `database_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the database to which the object belongs. Corresponds to [`mz_databases.name`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_databases). |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to [`mz_clusters.id`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_clusters). `NULL` for other object types. |

## `mz_object_lifetimes`

The `mz_object_lifetimes` view enriches the
[`mz_catalog.mz_objects`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_objects)
view with information about the last lifetime event that occurred for
each object in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the object. |
| `previous_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The object’s previous ID, if one exists. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`. |
| `event_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The lifetime event, either `create` or `drop`. |
| `occurred_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp of when the event occurred. |

## `mz_object_history`

The `mz_object_history` view enriches the
[`mz_catalog.mz_objects`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_objects)
view with historical information about each object in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for the object. |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The object’s cluster ID. `NULL` if the object has no associated cluster. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`. |
| `created_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp of when the object was created. `NULL` for built in system objects. |
| `dropped_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp of when the object was dropped. `NULL` for built in system objects or if the object hasn’t been dropped. |

## `mz_object_transitive_dependencies`

The `mz_object_transitive_dependencies` view describes the transitive
dependency structure between all database objects in the system. The
view is defined as the transitive closure of
[`mz_object_dependencies`](#mz_object_dependencies).

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the dependent object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |
| `referenced_object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the (possibly transitively) referenced object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |

## `mz_notices`

<div class="public-preview">

**PREVIEW** This feature is in **[public
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.

</div>

The `mz_notices` view contains a list of currently active notices
emitted by the system. The view can be accessed by Materialize
*superusers*.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for this notice. |
| `notice_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The notice type. |
| `message` | [`text`](/docs/self-managed/v25.2/sql/types/text) | A brief description of the issue highlighted by this notice. |
| `hint` | [`text`](/docs/self-managed/v25.2/sql/types/text) | A high-level hint that tells the user what can be improved. |
| `action` | [`text`](/docs/self-managed/v25.2/sql/types/text) | A concrete action that will resolve the notice. |
| `redacted_message` | [`text`](/docs/self-managed/v25.2/sql/types/text) | A redacted version of the `message` column. `NULL` if no redaction is needed. |
| `redacted_hint` | [`text`](/docs/self-managed/v25.2/sql/types/text) | A redacted version of the `hint` column. `NULL` if no redaction is needed. |
| `redacted_action` | [`text`](/docs/self-managed/v25.2/sql/types/text) | A redacted version of the `action` column. `NULL` if no redaction is needed. |
| `action_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the `action` string (`sql_statements` for a valid SQL string or `plain_text` for plain text). |
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the materialized view or index. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). For global notices, this column is `NULL`. |
| `created_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the notice was created. Note that some notices are re-created on `environmentd` restart. |

## `mz_notices_redacted`

<div class="public-preview">

**PREVIEW** This feature is in **[public
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.

</div>

The `mz_notices_redacted` view contains a redacted list of currently
active optimizer notices emitted by the system. The view can be accessed
by Materialize *superusers* and Materialize support.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Materialize’s unique ID for this notice. |
| `notice_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The notice type. |
| `message` | [`text`](/docs/self-managed/v25.2/sql/types/text) | A redacted brief description of the issue highlighted by this notice. |
| `hint` | [`text`](/docs/self-managed/v25.2/sql/types/text) | A redacted high-level hint that tells the user what can be improved. |
| `action` | [`text`](/docs/self-managed/v25.2/sql/types/text) | A redacted concrete action that will resolve the notice. |
| `action_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the `action` string (`sql_statements` for a valid SQL string or `plain_text` for plain text). |
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the materialized view or index. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). For global notices, this column is `NULL`. |
| `created_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the notice was created. Note that some notices are re-created on `environmentd` restart. |

## `mz_postgres_sources`

The `mz_postgres_sources` table contains a row for each PostgreSQL
source in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources). |
| `replication_slot` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the replication slot in the PostgreSQL database that Materialize will create and stream data from. |
| `timeline_id` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The PostgreSQL [timeline ID](https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-TIMELINES) determined on source creation. |

## `mz_postgres_source_tables`

The `mz_postgres_source_tables` table contains the mapping between each
Materialize subsource or table and the corresponding upstream PostgreSQL
table being ingested.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the subsource or table. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |
| `schema_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The schema of the upstream table being ingested. |
| `table_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the upstream table being ingested. |

## `mz_mysql_source_tables`

The `mz_mysql_source_tables` table contains the mapping between each
Materialize subsource or table and the corresponding upstream MySQL
table being ingested.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the subsource or table. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |
| `schema_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The schema ([or, database](https://dev.mysql.com/doc/refman/8.0/en/glossary.html#glos_schema)) of the upstream table being ingested. |
| `table_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the upstream table being ingested. |

## `mz_sql_server_source_tables`

The `mz_sql_server_source_tables` table contains the mapping between
each Materialize subsource or table and the corresponding upstream SQL
Server table being ingested.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the subsource or table. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |
| `schema_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The schema of the upstream table being ingested. |
| `table_name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the upstream table being ingested. |

## `mz_kafka_source_tables`

The `mz_kafka_source_tables` table contains the mapping between each
Materialize table and the corresponding upstream Kafka topic being
ingested.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the table. Corresponds to [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |
| `topic` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The topic being ingested. |
| `envelope_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The [envelope](/docs/self-managed/v25.2/sql/create-source/#envelopes) type: `none`, `upsert`, or `debezium`. `NULL` for other source types. |
| `key_format` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The [format](/docs/self-managed/v25.2/sql/create-source/#formats) of the Kafka message key: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`, or `NULL`. |
| `value_format` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The [format](/docs/self-managed/v25.2/sql/create-source/#formats) of the Kafka message value: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`. `NULL` for other source types. |

### `mz_recent_storage_usage`

The `mz_recent_storage_usage` table describes the storage utilization of
each table, source, and materialized view in the system in the most
recent storage utilization assessment. Storage utilization assessments
occur approximately every hour.

See [`mz_storage_usage`](../mz_catalog#mz_storage_usage) for historical
storage usage information.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | \[`text`\] | The ID of the table, source, or materialized view. |
| `size_bytes` | \[`uint8`\] | The number of storage bytes used by the object in the most recent assessment. |

## `mz_sessions`

The `mz_sessions` table contains a row for each active session in the
system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uuid`](/docs/self-managed/v25.2/sql/types/uuid) | The globally unique ID of the session. |
| `connection_id` | [`uint4`](/docs/self-managed/v25.2/sql/types/uint4) | The connection ID of the session. Unique only for active sessions and can be recycled. Corresponds to [`pg_backend_pid()`](/docs/self-managed/v25.2/sql/functions/#pg_backend_pid). |
| `role_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the role that the session is logged in as. Corresponds to [`mz_catalog.mz_roles`](../mz_catalog#mz_roles). |
| `client_ip` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The IP address of the client that initiated the session. |
| `connected_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the session connected to the system. |

## `mz_network_policies`

The `mz_network_policies` table contains a row for each network policy
in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the network policy. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the network policy. |
| `owner_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role ID of the owner of the network policy. Corresponds to [`mz_catalog.mz_roles.id`](../mz_catalog#mz_roles). |
| `privileges` | [`mz_aclitem array`](/docs/self-managed/v25.2/sql/types/mz_aclitem) | The privileges belonging to the network policy. |
| `oid` | [`oid`](/docs/self-managed/v25.2/sql/types/oid) | A [PostgreSQL-compatible OID](/docs/self-managed/v25.2/sql/types/oid) for the network policy. |

## `mz_network_policy_rules`

The `mz_network_policy_rules` table contains a row for each network
policy rule in the system.

| Field | Type | Meaning |
|----|----|----|
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the network policy rule. Can be combined with `policy_id` to form a unique identifier. |
| `policy_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID the network policy the rule is part of. Corresponds to [`mz_network_policy_rules.id`](#mz_network_policy_rules). |
| `action` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The action of the rule. `allow` is the only supported action. |
| `address` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The address the rule will take action on. |
| `direction` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The direction of traffic the rule applies to. `ingress` is the only supported direction. |

## `mz_show_network_policies`

The `mz_show_show_network_policies` view contains a row for each network
policy in the system.

## `mz_show_all_privileges`

The `mz_show_all_privileges` view contains a row for each privilege
granted in the system on user objects to user roles.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `database` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the database containing the object. |
| `schema` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the schema containing the object. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the privilege target. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of object the privilege is granted on. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_cluster_privileges`

The `mz_show_cluster_privileges` view contains a row for each cluster
privilege granted in the system on user clusters to user roles.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the cluster. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_database_privileges`

The `mz_show_database_privileges` view contains a row for each database
privilege granted in the system on user databases to user roles.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the database. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_default_privileges`

The `mz_show_default_privileges` view contains a row for each default
privilege granted in the system in user databases and schemas to user
roles.

| Field | Type | Meaning |
|----|----|----|
| `object_owner` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted on objects created by `object_owner`. |
| `database` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted only on objects created in `database` if non-null. |
| `schema` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted only on objects created in `schema` if non-null. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted only on objects of type `object_type`. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted to `grantee`. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege to be granted. |

## `mz_show_object_privileges`

The `mz_show_object_privileges` view contains a row for each object
privilege granted in the system on user objects to user roles.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `database` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the database containing the object. |
| `schema` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the schema containing the object. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the object. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of object the privilege is granted on. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_role_members`

The `mz_show_role_members` view contains a row for each role membership
in the system.

| Field | Type | Meaning |
|----|----|----|
| `role` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that `member` is a member of. |
| `member` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that is a member of `role`. |
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted membership of `member` to `role`. |

## `mz_show_schema_privileges`

The `mz_show_schema_privileges` view contains a row for each schema
privilege granted in the system on user schemas to user roles.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `database` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the database containing the schema. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the schema. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_system_privileges`

The `mz_show_system_privileges` view contains a row for each system
privilege granted in the system on to user roles.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_all_my_privileges`

The `mz_show_all_my_privileges` view is the same as
[`mz_show_all_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_all_privileges),
but only includes rows where the current role is a direct or indirect
member of `grantee`.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `database` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the database containing the object. |
| `schema` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the schema containing the object. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the privilege target. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of object the privilege is granted on. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_my_cluster_privileges`

The `mz_show_my_cluster_privileges` view is the same as
[`mz_show_cluster_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_cluster_privileges),
but only includes rows where the current role is a direct or indirect
member of `grantee`.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the cluster. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_my_database_privileges`

The `mz_show_my_database_privileges` view is the same as
[`mz_show_database_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_database_privileges),
but only includes rows where the current role is a direct or indirect
member of `grantee`.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the cluster. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_my_default_privileges`

The `mz_show_my_default_privileges` view is the same as
[`mz_show_default_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_default_privileges),
but only includes rows where the current role is a direct or indirect
member of `grantee`.

| Field | Type | Meaning |
|----|----|----|
| `object_owner` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted on objects created by `object_owner`. |
| `database` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted only on objects created in `database` if non-null. |
| `schema` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted only on objects created in `schema` if non-null. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted only on objects of type `object_type`. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | Privileges described in this row will be granted to `grantee`. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege to be granted. |

## `mz_show_my_object_privileges`

The `mz_show_my_object_privileges` view is the same as
[`mz_show_object_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_object_privileges),
but only includes rows where the current role is a direct or indirect
member of `grantee`.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `database` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the database containing the object. |
| `schema` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the schema containing the object. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the object. |
| `object_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of object the privilege is granted on. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_my_role_members`

The `mz_show_my_role_members` view is the same as
[`mz_show_role_members`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_role_members),
but only includes rows where the current role is a direct or indirect
member of `member`.

| Field | Type | Meaning |
|----|----|----|
| `role` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that `member` is a member of. |
| `member` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that is a member of `role`. |
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted membership of `member` to `role`. |

## `mz_show_my_schema_privileges`

The `mz_show_my_schema_privileges` view is the same as
[`mz_show_schema_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_schema_privileges),
but only includes rows where the current role is a direct or indirect
member of `grantee`.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `database` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the database containing the schema. |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the schema. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_show_my_system_privileges`

The `mz_show_my_system_privileges` view is the same as
[`mz_show_system_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_system_privileges),
but only includes rows where the current role is a direct or indirect
member of `grantee`.

| Field | Type | Meaning |
|----|----|----|
| `grantor` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that granted the privilege. |
| `grantee` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The role that the privilege was granted to. |
| `privilege_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | They type of privilege granted. |

## `mz_sink_statistics`

The `mz_sink_statistics` view contains statistics about each sink.

### Counters

`messages_staged`, `messages_committed`, `bytes_staged`, and
`bytes_committed` are all counters that monotonically increase. They are
*only useful for calculating rates* to understand the general
performance of your sink.

Note that:

- The non-rate values themselves are not directly comparable, because
  they are collected and aggregated across multiple threads/processes.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the sink. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sinks). |
| `messages_staged` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of messages staged but possibly not committed to the sink. |
| `messages_committed` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of messages committed to the sink. |
| `bytes_staged` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of bytes staged but possibly not committed to the sink. This counts both keys and values, if applicable. |
| `bytes_committed` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of bytes committed to the sink. This counts both keys and values, if applicable. |

## `mz_sink_statuses`

The `mz_sink_statuses` view provides the current state for each sink in
the system, including potential error messages and additional metadata
helpful for debugging.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the sink. |
| `type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the sink. |
| `last_status_change_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp of the sink status change. |
| `status` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`. |
| `error` | [`text`](/docs/self-managed/v25.2/sql/types/text) | If the sink is in an error state, the error message. |
| `details` | [`jsonb`](/docs/self-managed/v25.2/sql/types/jsonb) | Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions. |

## `mz_sink_status_history`

The `mz_sink_status_history` table contains rows describing the history
of changes to the status of each sink in the system, including potential
error messages and additional metadata helpful for debugging.

| Field | Type | Meaning |
|----|----|----|
| `occurred_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp of the sink status change. |
| `sink_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks). |
| `status` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`. |
| `error` | [`text`](/docs/self-managed/v25.2/sql/types/text) | If the sink is in an error state, the error message. |
| `details` | [`jsonb`](/docs/self-managed/v25.2/sql/types/jsonb) | Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions. |
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the replica that an instance of a sink is running on. |

## `mz_source_statistics`

The `mz_source_statistics` view contains statistics about each source.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources). |
| `messages_received` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of messages the source has received from the external system. Messages are counted in a source type-specific manner. Messages do not correspond directly to updates: some messages produce multiple updates, while other messages may be coalesced into a single update. |
| `bytes_received` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of bytes the source has read from the external system. Bytes are counted in a source type-specific manner and may or may not include protocol overhead. |
| `updates_staged` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of updates (insertions plus deletions) the source has written but not yet committed to the storage layer. |
| `updates_committed` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of updates (insertions plus deletions) the source has committed to the storage layer. |
| `records_indexed` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of individual records indexed in the source envelope state. |
| `bytes_indexed` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of bytes stored in the source’s internal index, if any. |
| `rehydration_latency` | [`interval`](/docs/self-managed/v25.2/sql/types/interval) | The amount of time it took for the source to rehydrate its internal index, if any, after the source last restarted. |
| `snapshot_records_known` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The size of the source’s snapshot, measured in number of records. See [below](#meaning-record) to learn what constitutes a record. |
| `snapshot_records_staged` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The number of records in the source’s snapshot that Materialize has read. See [below](#meaning-record) to learn what constitutes a record. |
| `snapshot_committed` | [`boolean`](/docs/self-managed/v25.2/sql/types/boolean) | Whether the source has committed the initial snapshot for a source. |
| `offset_known` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The offset of the most recent data in the source’s upstream service that Materialize knows about. See [below](#meaning-offset) to learn what constitutes an offset. |
| `offset_committed` | [`uint8`](/docs/self-managed/v25.2/sql/types/uint8) | The offset of the the data that Materialize has durably ingested. See [below](#meaning-offset) to learn what constitutes an offset. |

### Counter metrics

`messages_received`, `bytes_received`, `updates_staged`, and
`updates_committed` are counter metrics that monotonically increase over
time.

Counters are updated in a best-effort manner. An ill-timed restart of
the source may cause undercounting or overcounting. As a result,
**counters are only useful for calculating rates to understand the
general performance of your source.**

For Postgres and MySQL sources, `messages_received` and `bytes_received`
are collected on the top-level source, and `updates_staged` and
`updates_committed` are collected on the source’s tables.

### Gauge metrics

Gauge metrics reflect values that can increase or decrease over time.
Gauge metrics are eventually consistent. They may lag the true state of
the source by seconds or minutes, but if the source stops ingesting
messages, the gauges will eventually reflect the true state of the
source.

`records_indexed` and `bytes_indexed` are the size (in records and bytes
respectively) of the index the source must maintain internally to
efficiently process incoming data. Currently, only sources that use the
upsert and Debezium envelopes must maintain an index. These gauges reset
to 0 when the source is restarted, as the index must be rehydrated.

`rehydration_latency` represents the amount of time it took for the
source to rehydrate its index after the latest restart. It is reset to
`NULL` when a source is restarted and is populated with a duration after
hydration finishes.

When a source is first created, it must process an initial snapshot of
data. `snapshot_records_known` is the total number of records in the
snapshot, and `snapshot_records_staged` is how many of the records the
source has read so far.

<span id="meaning-record"></span>

The meaning of record depends on the source:

- For Kafka sources, it’s the total number of offsets in the snapshot.
- For Postgres and MySQL sources, it’s the number of rows in the
  snapshot.

Note that when tables are added to Postgres or MySQL sources,
`snapshot_records_known` and `snapshot_records_staged` will reset as the
source snapshots those new tables. The metrics will also reset if the
source is restarted while the snapshot is in progress.

`snapshot_committed` becomes true when we have fully committed the
snapshot for the given source.

`offset_known` and `offset_committed` are used to represent the progress
a source is making relative to its upstream source. `offset_known` is
the maximum offset in the upstream system that Materialize knows about.
`offset_committed` is the offset that Materialize has durably ingested.
These metrics will never decrease over the lifetime of a source.

<span id="meaning-offset"></span>

The meaning of offset depends on the source:

- For Kafka sources, an offset is the Kafka message offset.
- For MySQL sources, an offset is the number of transactions committed
  across all servers in the cluster.
- For Postgres sources, an offset is a log sequence number (LSN).

## `mz_source_statuses`

The `mz_source_statuses` view provides the current state for each source
in the system, including potential error messages and additional
metadata helpful for debugging.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the source. |
| `type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of the source. |
| `last_status_change_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp of the source status change. |
| `status` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The status of the source: one of `created`, `starting`, `running`, `paused`, `stalled`, `failed`, or `dropped`. |
| `error` | [`text`](/docs/self-managed/v25.2/sql/types/text) | If the source is in an error state, the error message. |
| `details` | [`jsonb`](/docs/self-managed/v25.2/sql/types/jsonb) | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions. |

## `mz_source_status_history`

The `mz_source_status_history` table contains a row describing the
status of the historical state for each source in the system, including
potential error messages and additional metadata helpful for debugging.

| Field | Type | Meaning |
|----|----|----|
| `occurred_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp of the source status change. |
| `source_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources). |
| `status` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The status of the source: one of `created`, `starting`, `running`, `paused`, `stalled`, `failed`, or `dropped`. |
| `error` | [`text`](/docs/self-managed/v25.2/sql/types/text) | If the source is in an error state, the error message. |
| `details` | [`jsonb`](/docs/self-managed/v25.2/sql/types/jsonb) | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions. |
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the replica that an instance of a source is running on. |

## `mz_statement_lifecycle_history`

| Field | Type | Meaning |
|----|----|----|
| `statement_id` | [`uuid`](/docs/self-managed/v25.2/sql/types/uuid) | The ID of the execution event. Corresponds to [`mz_recent_activity_log.execution_id`](#mz_recent_activity_log) |
| `event_type` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The type of lifecycle event, e.g. `'execution-began'`, `'storage-dependencies-finished'`, `'compute-dependencies-finished'`, or `'execution-finished'` |
| `occurred_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the event took place. |

## `mz_subscriptions`

The `mz_subscriptions` table describes all active
[`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe) operations in the
system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the subscription. |
| `session_id` | [`uuid`](/docs/self-managed/v25.2/sql/types/uuid) | The ID of the session that runs the subscription. Corresponds to [`mz_sessions.id`](#mz_sessions). |
| `cluster_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the cluster on which the subscription is running. Corresponds to [`mz_clusters.id`](../mz_catalog/#mz_clusters). |
| `created_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | The time at which the subscription was created. |
| `referenced_object_ids` | [`text list`](/docs/self-managed/v25.2/sql/types/list) | The IDs of objects referenced by the subscription. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects) |

## `mz_wallclock_global_lag`

The `mz_wallclock_global_lag` view contains the most recently recorded
wallclock lag for each table, source, index, materialized view, and sink
in the system.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the table, source, materialized view, index, or sink. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |
| `lag` | [`interval`](/docs/self-managed/v25.2/sql/types/interval) | The amount of time the object’s write frontier lags behind wallclock time. |

## `mz_wallclock_lag_history`

The `mz_wallclock_lag_history` table records the historical wallclock
lag, i.e., the difference between the write frontier and the current
wallclock time, for each table, source, index, materialized view, and
sink in the system.

| Field | Type | Meaning |
|----|----|----|
| `object_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the table, source, materialized view, index, or sink. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |
| `replica_id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of a replica computing the object, or `NULL` for persistent objects. Corresponds to [`mz_cluster_replicas.id`](../mz_catalog/#mz_cluster_replicas). |
| `lag` | [`interval`](/docs/self-managed/v25.2/sql/types/interval) | The amount of time the object’s write frontier lags behind wallclock time. |
| `occurred_at` | [`timestamp with time zone`](/docs/self-managed/v25.2/sql/types/timestamp) | Wall-clock timestamp at which the event occurred. |

## `mz_webhook_sources`

The `mz_webhook_sources` table contains a row for each webhook source in
the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The ID of the webhook source. Corresponds to [`mz_sources.id`](../mz_catalog/#mz_sources). |
| `name` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The name of the webhook source. |
| `url` | [`text`](/docs/self-managed/v25.2/sql/types/text) | The URL which can be used to send events to the source. |

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/system-catalog/mz_internal.md"
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
