---
title: "SHOW"
description: "Display the value of a session or system variable."
menu:
  main:
    parent: 'commands'

---

`SHOW` displays the value of a session or system variable.

## Syntax

{{< diagram "show-variable.svg" >}}

Field                  | Use
-----------------------|-----
_variable&lowbar;name_ | The name of the session or system variable to display.
**ALL**                | Display the values of all session and system variables.

{{% session-variables %}}

## System variables

Materialize reserves system variables for region-wide configuration. Although it's possible to `SHOW` system variables, you must [contact us](https://materialize.com/contact/) to change their value (e.g. increasing the maximum number of AWS PrivateLink connections in your region).

Name                                        | Default value                                                         | Description   |
--------------------------------------------|-----------------------------------------------------------------------|---------------|
allowed_cluster_replica_sizes               | `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`, `xlarge`  | The allowed sizes when creating a new cluster replica.
enable_rbac_checks                          | `false`                                                               | Boolean flag indicating whether to apply RBAC checks before executing statements. Setting this variable requires _superuser_ privileges.
max_aws_privatelink_connections             | `0`                                                                   | The maximum number of AWS PrivateLink connections in the region, across all schemas.
max_clusters                                | `10`                                                                  | The maximum number of clusters in the region.
max_credits_per_hour                        | `64`                                                                  | The maximum number of compute credits per hour in the region.
max_databases                               | `1000`                                                                | The maximum number of databases in the region.
max_objects_per_schema                      | `1000`                                                                | The maximum number of objects in a schema.
max_replicas_per_cluster                    | `5`                                                                   | The maximum number of replicas of a single cluster.
max_result_size                             | `1 GiB`                                                               | The maximum size in bytes for a single query's result.
max_schemas_per_database                    | `1000`                                                                | The maximum number of schemas in a database.
max_secrets                                 | `100`                                                                 | The maximum number of secrets in the region, across all schemas.
max_sources                                 | `25`                                                                  | The maximum number of sources in the region, across all schemas.
max_sinks                                   | `25`                                                                  | The maximum number of sinks in the region, across all schemas.
max_tables                                  | `25`                                                                  | The maximum number of tables in the region, across all schemas.

## Examples

### Show active cluster

```sql
SHOW cluster;
```
```
 cluster
---------
 default
```

### Show transaction isolation level

```sql
SHOW transaction_isolation;
```
```
 transaction_isolation
-----------------------
 strict serializable
```

## Related pages

- [`RESET`](../reset)
- [`SET`](../set)
