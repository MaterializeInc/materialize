---
title: "ALTER ROLE"
description: "`ALTER ROLE` alters the attributes of an existing role."
menu:
    main:
        parent: commands
---

`ALTER ROLE` alters the attributes of an existing role.

## Syntax

```mzsql
ALTER ROLE <role_name> [WITH [SUPERUSER | NOSUPERUSER ]
    [ LOGIN | NOLOGIN ]
    [ INHERIT | NOINHERIT ]
    [ PASSWORD <text> ]] [SET <name> TO <value> ]
```

| Field       | Use                  |
| ----------- | -------------------- |
| _role_name_ | A name for the role. |

#### `alter_role_attributes`

| Field         | Use                                                                                                                             |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `INHERIT`     | If specified, grants the role the ability to inherit privileges of other roles. (Default)                                       |
| `LOGIN`       | If specified, allows a role to login via the PostgreSQL or web endpoints                                                        |
| `NOLOGIN`     | If specified, prevents a role from logging in. This is the default behavior if `LOGIN` is not specified.                        |
| `SUPERUSER`   | If specified, grants the role superuser privileges.                                                                             |
| `NOSUPERUSER` | If specified, prevents the role from having superuser privileges. This is the default behavior if `SUPERUSER` is not specified. |
| `PASSWORD`    | ***Public Preview*** This feature may have minor stability issues. If specified, allows you to set a password for the role. Setting the password to `NULL` removes the password.                   |

#### `alter_role_set`

| Field       | Use                                                                                                                                  |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| _name_      | The name of the configuration parameter to modify.                                                                                   |
| _value_     | The value to assign to the configuration parameter.                                                                                  |
| **DEFAULT** | Reset the value of the configuration parameter for the specified role to the system's default. Equivalent to `ALTER ROLE ... RESET`. |

## Details

Unlike PostgreSQL, Materialize does not currently support the `NOINHERIT` attribute and the `SET
ROLE` command.

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `ALTER ROLE ... INHERIT INHERIT`.

With the exception of the `SUPERUSER` attribute and unlike PostgreSQL, Materialize does not use role attributes to determine a roles ability to create
top level objects such as databases and other roles. Instead, Materialize uses system level
privileges. See [GRANT PRIVILEGE](../grant-privilege) for more details.

Like PostgreSQL, altering the configuration parameter for a role only affects **new sessions**.
Also like PostgreSQL, role configuration parameters are **not inherited**. To view the
current configuration parameter defaults for a role, see [`mz_role_parameters`](/sql/system-catalog/mz_catalog#mz_role_parameters).

## Examples

#### Altering the attributes of a role

```mzsql
ALTER ROLE rj INHERIT;
```

```mzsql
SELECT name, inherit FROM mz_roles WHERE name = 'rj';
```

```nofmt
rj  true
```

#### Setting configuration parameters for a role

```mzsql
SHOW cluster;
quickstart

ALTER ROLE rj SET cluster TO rj_compute;

-- Role parameters only take effect for new sessions.
SHOW cluster;
quickstart

-- Start a new SQL session with the role 'rj'.
SHOW cluster;
rj_compute

-- In a new SQL session with a role that is not 'rj'.
SHOW cluster;
quickstart
```

##### Non-inheritance

```mzsql
CREATE ROLE team;
CREATE ROLE member;

ALTER ROLE team SET cluster = 'team_compute';
GRANT team TO member;

-- Start a new SQL session with the Role 'member'.
SHOW cluster;
quickstart
```

##### Making a role a superuser

Unlike regular roles, superusers have unrestricted access to all objects in the system and can perform any action on them.

```mzsql
ALTER ROLE rj SUPERUSER;
```

To verify that the role has superuser privileges, you can query the `pg_authid` system catalog:

```mzsql
SELECT name, rolsuper FROM pg_authid WHERE rolname = 'rj';
```

```nofmt
rj  t
```

##### Removing the superuser attribute from a role

NOSUPERUSER will remove the superuser attribute from a role, preventing it from having unrestricted access to all objects in the system.

```mzsql
ALTER ROLE rj NOSUPERUSER;
```

```mzsql
SELECT name, rolsuper FROM pg_authid WHERE rolname = 'rj';
```

```nofmt
rj  f
```

##### Removing a role's password

{{< warning >}}
Setting a NULL password removes the password.
{{< /warning >}}

```mzsql
ALTER ROLE rj PASSWORD NULL;
```

##### Changing a role's password

```mzsql
ALTER ROLE rj PASSWORD 'new_password';
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-role.md" >}}

## Related pages

-   [`CREATE ROLE`](../create-role)
-   [`DROP ROLE`](../drop-role)
-   [`DROP USER`](../drop-user)
-   [`GRANT ROLE`](../grant-role)
-   [`REVOKE ROLE`](../revoke-role)
-   [`ALTER OWNER`](../alter-owner)
-   [`GRANT PRIVILEGE`](../grant-privilege)
-   [`REVOKE PRIVILEGE`](../revoke-privilege)
