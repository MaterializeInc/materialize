---
title: "Manage roles"
description: "Create and manage roles in Materialize"
menu:
  main:
    parent: access-control
    weight: 20
aliases:
  - /sql/builtin-roles/
---

This page outlines how to create and manage roles in Materialize.

## Create a role

To create a new role, use the [`CREATE ROLE`](https://materialize.com/docs/sql/create-role/) statement:

```mzsql
CREATE ROLE <role_name> WITH <role_attribute>;
```

Materialize roles have the following available attributes:

| Name              | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `INHERIT`         | **Read-only.** Can inherit privileges of other roles.                       |

## Alter a role's attributes

To change a role's attributes, use the [`ALTER ROLE`](https://materialize.com/docs/sql/alter-role/) statement:

```mzsql
ALTER ROLE <role_name> WITH <ATTRIBUTE>;
```

## Grant a role to a user

To grant a role assignment to a user, use the [`GRANT`](https://materialize.com/docs/sql/grant-role/) statement:

```mzsql
GRANT <role_name> to <user_name>;
```

## Remove a user from a role

To remove a user from a role, use the [`REVOKE`](https://materialize.com/docs/sql/revoke-role/) statement:

```mzsql
REVOKE <role_name> FROM <user_name>;
```

## Drop a role

To remove a role, use the [`DROP ROLE`](https://materialize.com/docs/sql/drop-role/) statement:

```mzsql
DROP ROLE <role_name>;
```

## Builtin roles
Certain internal objects may only be queried by superusers or by users
belonging to a particular builtin role, which superusers may
[grant](/sql/grant-role). These include the following:

| Name                  | Description                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mz_monitor`          | Grants access to objects that reveal actions taken by other users, in particular, SQL statements they have issued. Includes [`mz_recent_activity_log`](/sql/system-catalog/mz_internal#mz_recent_activity_log) and [`mz_notices`](/sql/system-catalog/mz_internal#mz_notices).                                                                                                                                    |
| `mz_monitor_redacted` | Grants access to objects that reveal less sensitive information about actions taken by other users, for example, SQL statements they have issued with constant values redacted. Includes `mz_recent_activity_log_redacted`, [`mz_notices_redacted`](/sql/system-catalog/mz_internal#mz_notices_redacted), and [`mz_statement_lifecycle_history`](/sql/system-catalog/mz_internal#mz_statement_lifecycle_history). |
|                       |
