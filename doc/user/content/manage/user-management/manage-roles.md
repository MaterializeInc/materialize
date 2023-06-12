---
title: "Manage roles"
description: "Create and manage roles in Materialize"
menu:
  main:
    parent: user-management
    weight: 15
---

{{< alpha />}}

This page outlines how to create and manage roles in Materialize.

## Create a role

To create a new role, use the [`CREATE ROLE`](https://materialize.com/docs/sql/create-role/) statement:

```sql
CREATE ROLE <role_name> WITH <role_attribute>;
```

Materialize roles have the following available attributes:

| Name              | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `CREATEDB`        | Can create a database.                                                      |
| `CREATEROLE`      | Can create, alter, delete roles and can grant and revoke role membership.   |
| `INHERIT`         | **Read-only.** Can inherit privileges of other roles.                       |
| `CREATECLUSTER`   | Can create a cluster.                                                       |
| `NOCREATEDB`      | Denies the role the ability to create databases.                            |
| `NOCREATEROLE`    | Denies the role the ability to create, alter, delete roles or grant/revoke. |
| `NOCREATECLUSTER` | Denies the role the ability to create clusters.                             |

## Alter a role's attributes

To change a role's attributes, use the [`ALTER ROLE`](https://materialize.com/docs/sql/alter-role/) statement:

```sql
ALTER ROLE <role_name> WITH <ATTRIBUTE>;
```

## Grant a role to a user

To grant a role assignment to a user, use the [`GRANT`](https://materialize.com/docs/sql/grant-role/) statement:

```sql
GRANT <role_name> to <user_name>;
```

## Remove a user from a role

To remove a user from a role, use the [`REVOKE`](https://materialize.com/docs/sql/revoke-role/) statement:

```sql
REVOKE <role_name> FROM <user_name>;
```

## Drop a role

To remove a role, use the [`DROP ROLE`](https://materialize.com/docs/sql/drop-role/) statement:

```sql
DROP ROLE <role_name>;
```
