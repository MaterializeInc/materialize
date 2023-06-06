---
title: "Manage roles"
description: "Create and manage roles in Materialize"
menu:
  main:
    parent: user-management
    weight: 15
---

This page outlines how to create new roles in Materialize.

## Create a role

To create a new role, use the `CREATE ROLE` statement:

```sql
CREATE ROLE <new_role>;
```

## Grant a role to a user

To grant a role assignment to a user, use the `GRANT` statement:

```sql
GRANT <new_role> to <new_user>;
```

## Alter a role's attributes

To change a role's attributes, use the `ALTER ROLE` statement:

```sql
ALTER ROLE <new_role> WITH CREATEROLE;
```

Materialize roles have the following available attributes:

| Name            | Description                                                                     |
|-----------------|---------------------------------------------------------------------------------|
| `CREATEDB`      | Can create a database.                                                          |
| `CREATEROLE`    | Can create, alter, drop, grant membership to, and revoke membership from roles. |
| `INHERIT`       | **Read-only.** Can inherit the privileges of roles that it is a member of. On by default.      |
| `CREATECLUSTER` | Can create a cluster.                                                           |


## Drop a role

To remove a role, use the `DROP ROLE` statement:

```sql
DROP ROLE <new_role>;
```
