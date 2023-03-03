---
title: "SET"
description: "`SET` a run-time configuration option in Materialize."
menu:
  main:
    parent: 'commands'

---

The `SET` command modifies a session variable value. Session variables store information about the user, application state, or preferences like target cluster, search path, or transaction isolation during a session lifetime.


{{< diagram "set-session-variable.svg" >}}

## Variables

Name                                        | Default Value | Description   |
--------------------------------------------|---------------|---------------|
application_name                            | Empty string  | The application name to be reported in statistics and logs.|
client_encoding                             | UTF8          | The client's character set encoding.
client_min_messages                         | `Notice`      | The message levels that are sent to the client. <br/><br/> **Accepted values:** `Error`, `Warning`, `Notice`, `Log`, `Debug1`, `Debug2`, `Debug3`, `Debug4`, `Debug5`
cluster                                     | `default`     | The current cluster.
cluster_replica                             | Empty string  | The target cluster replica for SELECT queries.
database                                    | `materialize` | The current database.
datestyle                                   | `ISO, MDY`      | The display format for date and time values
emit_timestamp_notice                       | `false`         | Boolean flag indicating whether to send a NOTICE specifying query timestamps.
extra_float_digits                          | `3`           | Adjusts the number of digits displayed for floating-point values.
failpoints                                  | Empty string    | Allows failpoints to be dynamically activated.
integer_datetimes                           | `true`        | Reports whether the server uses 64-bit-integer dates and times.
intervalstyle                               | `postgres`             | The display format for interval values.
search_path                                 | `public`      | The schema search order for names that are not schema-qualified.
server_version                              | Version-dependent             | The server version.
server_version_num                          | Version-dependent             | The server version as an integer.
sql_safe_updates                            | `false`             | Prohibits SQL statements that may be overly destructive.
standard_conforming_strings                 | `true`             | Causes `'...'` strings to treat backslashes literally.
transaction_isolation                       | `STRICT SERIALIZABLE`             | The current transaction's isolation level.


### Examples

#### `SET` Cluster

```sql
SET CLUSTER = 'primary';
```

#### `SET` transaction isolation

```sql
SET TRANSACTION_ISOLATION TO 'STRICT SERIALIZABLE';
```

<!-- #### `SET` Statement timeout

Setting statements a one minute timeout:

```sql
SET statement_timeout = '1min';
``` -->

### Resetting a value

Use the `RESET` command to revert a session variable to its initial value.

#### Example:
```sql
-- Resets the cluster session variable value
RESET CLUSTER;
```


<!-- Do we support statement_timeout? We can change the value but has no effect. -->

<!-- statement_timeout                           | `10 seconds`             | The maximum allowed duration of `INSERT`, `SELECT`, `UPDATE`, and `DELETE` operations.
idle_in_transaction_session_timeout         | `120 seconds`             | The maximum allowed duration that a session can sit idle in a transaction before being terminated. A value of zero disables the timeout. -->

<!-- We only support UTC value -->
<!-- timezone                                    | `UTC`             | The time zone for displaying and interpreting time stamps. -->
<!--?build_info                                 | Text          | -             | Returns the value of the `mz_version` configuration parameter. | -->
<!--?real_time_recency                          | Text          | -             | Feature flag indicating whether real time recency is enabled (Materialize) -->

<!--
emit_timestamp_notice                       | Text          | -             | Boolean flag indicating whether to send a NOTICE specifying query timestamps (Materialize).
emit_trace_id_notice                        | Text          | -             | Boolean flag indicating whether to send a NOTICE specifying the trace id when available (Materialize).
mock_audit_event_timestamp                  | Text          | -             | Mocked timestamp to use for audit events for testing purposes
?max_aws_privatelink_connections            | Text          | -             | The maximum number of AWS PrivateLink connections in the region, across all schemas (Materialize).
max_tables                                  | Text          | -             | The maximum number of tables in the region, across all schemas (Materialize).
max_sources                                 | Text          | -             | The maximum number of sources in the region, across all schemas (Materialize).
max_sinks                                   | Text          | -             | The maximum number of sinks in the region, across all schemas (Materialize).
max_materialized_views                      | Text          | -             | The maximum number of sinks in the region, across all schemas (Materialize).
max_clusters                                | Text          | -             | The maximum number of clusters in the region (Materialize).
max_replicas_per_cluster                    | Text          | -             | The maximum number of replicas of a single cluster (Materialize).
max_databases                               | Text          | -             | The maximum number of databases in the region (Materialize).
max_schemas_per_database                    | Text          | -             | The maximum number of schemas in a database (Materialize).
max_objects_per_schema                      | Text          | -             | The maximum number of objects in a schema (Materialize).
max_secrets                                 | Text          | -             | The maximum number of objects in a schema (Materialize).
max_roles                                   | Text          | -             | The maximum number of roles in the region (Materialize).
max_result_size                             | Text          | -             | The maximum size in bytes for a single query's result (Materialize).
metrics_retention                           | Text          | -             | The time to retain cluster utilization metrics (Materialize).
allowed_cluster_replica_sizes               | Text          | -             | The allowed sizes when creating a new cluster replica (Materialize).
persist_blob_target_size                    | Text          | -             | A target maximum size of persist blob payloads in bytes (Materialize).
persist_compaction_minimum_timeout          | Text          | -             | The minimum amount of time to allow a persist compaction request to run before timing it out.
crdb_connect_timeout                        | Text          | -             | The connection timeout to Cockroach used by persist.
-->