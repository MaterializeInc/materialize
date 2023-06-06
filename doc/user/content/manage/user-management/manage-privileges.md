---
title: "Manage privileges"
description: "Assign and manage role privileges in Materialize"
menu:
  main:
    parent: user-management
    weight: 20 
---

This page outlines how to assign and manage role privileges.

## Grant privileges

To grant privileges to a role, use the `GRANT PRIVILEGE` statement with the
object you want to grant privileges to:

```sql
GRANT USAGE ON DATABASE <database> TO <new_role>;
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

To remove privileges from a role, use the `REVOKE` statement:

```sql
REVOKE USAGE ON DATABASE <database> FROM <new_role>;
```
