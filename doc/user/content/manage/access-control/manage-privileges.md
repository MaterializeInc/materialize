---
title: "Manage privileges"
description: "Assign and manage role privileges in Materialize"
menu:
  main:
    parent: access-control
    weight: 20
---

This page outlines how to assign and manage role privileges.

## Grant privileges

To grant privileges to a role, use the [`GRANT PRIVILEGE`](/sql/grant-privilege/) statement with the
object you want to grant privileges to:

```mzsql
GRANT USAGE ON <OBJECT_TYPE> <object_name> TO <role_name>;
```

Materialize objects allow for the following privileges:

| Object Type         | Privileges                                |
|---------------------|-------------------------------------------|
| `SYSTEM`            | `CREATEROLE`, `CREATEDB`, `CREATECLUSTER` |
| `DATABASE`          | `USAGE`, `CREATE`                         |
| `SCHEMA`            | `USAGE`, `CREATE`                         |
| `TABLE`             | `INSERT`, `SELECT`, `UPDATE`, `DELETE`    |
| `VIEW`              | `SELECT`                                  |
| `MATERIALIZED VIEW` | `SELECT`                                  |
| `TYPE`              | `USAGE`                                   |
| `SOURCE`            | `SELECT`                                  |
| `CONNECTION`        | `USAGE`                                   |
| `SECRET`            | `USAGE`                                   |
| `CLUSTER`           | `USAGE`, `CREATE`                         |

Materialize object access is also dependent on cluster privileges.
Roles that need access to an object that use compute resources must also have
the same level of access to the cluster. Materialize objects that use compute
resources are:

* Replicas
* Sources
* Sinks
* Indexes
* Materialized views

For example, to allow a role to create a materialized view, you would
give that role `CREATE` privileges on the cluster and the schema because the
materialized view will be namespaced by the schema.

```mzsql
GRANT CREATE ON CLUSTER <cluster_name> to <role_name>;
GRANT CREATE ON <schema_name> to <role_name>;
```

## Revoke privileges

To remove privileges from a role, use the [`REVOKE`](/sql/revoke-privilege/) statement:

```mzsql
REVOKE USAGE ON <OBJECT_TYPE> <object_name> FROM <role_name>;
```
