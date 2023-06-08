---
title: "Manage privileges"
description: "Assign and manage role privileges in Materialize"
menu:
  main:
    parent: user-management
    weight: 20
---

{{< alpha />}}

This page outlines how to assign and manage role privileges.

## Grant privileges

To grant privileges to a role, use the [`GRANT PRIVILEGE`](https://materialize.com/docs/sql/grant-privilege/) statement with the
object you want to grant privileges to:

```sql
GRANT USAGE ON <OBJECT_TYPE> <object_name> TO <role_name>;
```

Materialize objects allow for the following privileges:

| Object Type          | Privileges                          |
|----------------------|-------------------------------------|
| `DATABASE`           | `USAGE` `CREATE`                    |
| `SCHEMA`             | `USAGE` `CREATE`                    |
| `TABLE`              | `INSERT` `SELECT` `UPDATE` `DELETE` |
| `VIEW`               | `SELECT`                            |
| `MATERIALIZED  VIEW` | `SELECT`                            |
| `TYPE`               | `USAGE`                             |
| `SOURCE`             | `SELECT`                            |
| `CONNECTION`         | `USAGE`                             |
| `SECRET`             | `USAGE`                             |
| `CLUSTER`            | `USAGE` `CREATE`                    |

## Revoke privileges

To remove privileges from a role, use the [`REVOKE`](https://materialize.com/docs/sql/revoke-privilege/) statement:

```sql
REVOKE USAGE ON <OBJECT_TYPE> <object_name> FROM <role_name>;
```
