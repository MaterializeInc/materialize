---
title: "SHOW"
description: "`SHOW` a session variable value in Materialize."
menu:
  main:
    parent: 'commands'

---

The `SHOW` command displays a session variable value.

## Syntax
{{< diagram "show-session-variable.svg" >}}

Field | Use
------|-----
_variable&lowbar;name_ | The name of the session variable to display. For the available session variable names, see [the following variables table](#variables).


## Variables

Name                                        | Default Value             | Description   |
--------------------------------------------|---------------------------|---------------|
application_name                            | Empty string              | The application name to be reported in statistics and logs.
client_encoding                             | `UTF8`                    | The client's character set encoding.
client_min_messages                         | `Notice`                  | The message levels that are sent to the client. <br/><br/> Accepted values: `Error`, `Warning`, `Notice`, `Log`, `Debug1`, `Debug2`, `Debug3`, `Debug4`, `Debug5`
cluster                                     | `default`                 | The current cluster.
cluster_replica                             | Empty string              | The target cluster replica for SELECT queries.
database                                    | `materialize`             | The current database.
datestyle                                   | `ISO, MDY`                | The display format for date and time values
emit_timestamp_notice                       | `false`                   | Boolean flag indicating whether to send a `NOTICE` specifying query timestamps.
emit_trace_id_notice                        | `false`                   | Boolean flag indicating whether to send a `NOTICE` specifying the trace id when available
extra_float_digits                          | `3`                       | Adjusts the number of digits displayed for floating-point values.
failpoints                                  | Empty string              | Allows failpoints to be dynamically activated.
integer_datetimes                           | `true`                    | Reports whether the server uses 64-bit-integer dates and times.
intervalstyle                               | `postgres`                | The display format for interval values.
search_path                                 | `public`                  | The schema search order for names that are not schema-qualified.
server_version                              | Version-dependent         | The server version.
server_version_num                          | Version-dependent         | The server version as an integer.
sql_safe_updates                            | `false`                   | Prohibits SQL statements that may be overly destructive.
standard_conforming_strings                 | `true`                    | Causes `'...'` strings to treat backslashes literally.
statement_timeout                           | `10 seconds`              | The maximum allowed duration of `INSERT`, `SELECT`, `UPDATE`, and `DELETE` operations. <br/> This session variable **has no effect** and its sole purpose is to maintain compatibility with Postgres.
idle_in_transaction_session_timeout         | `120 seconds`             | The maximum allowed duration that a session can sit idle in a transaction before being terminated. A value of zero disables the timeout. <br/> This session variable **has no effect** and its sole purpose is to maintain compatibility with Postgres.
transaction_isolation                       | `STRICT SERIALIZABLE`     | The current transaction's isolation level.
timezone                                    | `UTC`                     | The time zone for displaying and interpreting time stamps. <br/> This session variable **has no effect** and its sole purpose is to maintain compatibility with Postgres.


#### Interval variables

The following variables are available but cannot be modified, hence the 'SET' command will not recognize them. These variables store information about internal constraints, like the maximum amount of creable tables or sinks.

Name                                        | Default Value                                                         | Description   |
--------------------------------------------|-----------------------------------------------------------------------|---------------|
allowed_cluster_replica_sizes               | `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`, `xlarge`  | The allowed sizes when creating a new cluster replica.
mock_audit_event_timestamp                  | -                                                                     | Mocked timestamp to use for audit events for testing purposes
max_aws_privatelink_connections             | -                                                                     | The maximum number of AWS PrivateLink connections in the region, across all schemas.
max_tables                                  | `25`                                                                  | The maximum number of tables in the region, across all schemas.
max_sources                                 | `25`                                                                  | The maximum number of sources in the region, across all schemas.
max_sinks                                   | `25`                                                                  | The maximum number of sinks in the region, across all schemas.
max_materialized_views                      | `100`                                                                 | The maximum number of sinks in the region, across all schemas.
max_clusters                                | `10`                                                                  | The maximum number of clusters in the region.
max_replicas_per_cluster                    | `5`                                                                   | The maximum number of replicas of a single cluster.
max_databases                               | `1000`                                                                | The maximum number of databases in the region.
max_schemas_per_database                    | `1000`                                                                | The maximum number of schemas in a database.
max_objects_per_schema                      | `1000`                                                                | The maximum number of objects in a schema.
max_secrets                                 | `100`                                                                 | The maximum number of objects in a schema.
max_roles                                   | `1000`                                                                | The maximum number of roles in the region.
max_result_size                             | `1 GiB`                                                               | The maximum size in bytes for a single query's result.
metrics_retention                           | `30 days`                                                             | The time to retain cluster utilization metrics.
persist_blob_target_size                    | `128MB`                                                               | A target maximum size of persist blob payloads in bytes.
persist_compaction_minimum_timeout          | `90 seconds`                                                          | The minimum amount of time to allow a persist compaction request to run before timing it out.
crdb_connect_timeout                        | `5 seconds`                                                           | The connection timeout to Cockroach used by persist.
dataflow_max_inflight_bytes                 | `5 seconds`                                                           | The maximum number of in-flight bytes emitted by persist_sources feeding dataflows.
real_time_recency                           | `false`                                                               | Feature flag indicating whether real time recency is enabled


### Examples

#### `SHOW` Cluster

```sql
SHOW CLUSTER;
```
```
 cluster
---------
 default
(1 row)
```

#### `SHOW` transaction isolation

```sql
SHOW TRANSACTION_ISOLATION;
```
```
 transaction_isolation
-----------------------
 strict serializable
(1 row)
```
