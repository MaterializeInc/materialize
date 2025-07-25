---
title: "CREATE ROLE"
description: "`CREATE ROLE` creates a new role."
menu:
    main:
        parent: commands
---

`CREATE ROLE` creates a new role, which is a user account in Materialize.

When you connect to Materialize, you must specify the name of a valid role in
the system.

## Syntax

```mzsql
CREATE ROLE _role_name_ [WITH [SUPERUSER | NOSUPERUSER ]
    [ LOGIN | NOLOGIN ]
    [ INHERIT ]
    [ PASSWORD <text> ]]
```

| Field                           | Use                    |
| ------------------------------- | ---------------------- |
| _role_name_                     | A name for the role.   |
| **WITH (** _options list_ **)** | See below for options. |

## Details

Materialize's support for `CREATE ROLE` is similar to that of PostgreSQL, with
the following options exceptions:

| Option        | Description                                                                                                                     |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `INHERIT`     | If specified, grants the role the ability to inherit privileges of other roles. (Default)                                       |
| `LOGIN`       | If specified, allows a role to login via the PostgreSQL or web endpoints                                                        |
| `NOLOGIN`     | If specified, prevents a role from logging in. This is the default behavior if `LOGIN` is not specified.                        |
| `SUPERUSER`   | If specified, grants the role superuser privileges.                                                                             |
| `NOSUPERUSER` | If specified, prevents the role from having superuser privileges. This is the default behavior if `SUPERUSER` is not specified. |
| `PASSWORD`    | ***Public Preview*** This feature may have minor stability issues. If specified, allows you to set a password for the role.                                                                        |

{{< note >}}

With the exception of the `SUPERUSER` attribute, Materialize does not use role attributes to determine a role's ability to create
top level objects such as databases and other roles. Instead, Materialize uses
system level privileges. See [GRANT PRIVILEGE](../grant-privilege) for more
details.

{{</ note >}}

### Restrictions

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... INHERIT INHERIT`.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-role.md" >}}

## Examples

### Create a role with login and password

```mzsql
CREATE ROLE db_reader WITH LOGIN PASSWORD 'password';
```

You can verify that the role was created by querying the `mz_roles` system catalog:

```mzsql
SELECT name FROM mz_roles;
```

```nofmt
 db_reader
 mz_system
 mz_support
```

### Create a superuser role

Unlike regular roles, superusers have unrestricted access to all objects in the system and can perform any action on them.

```mzsql
CREATE ROLE super_user WITH SUPERUSER LOGIN PASSWORD 'password';
```

You can verify that the superuser role was created by querying the `mz_roles` system catalog:

```mzsql
SELECT name FROM mz_roles;
```

```nofmt
 db_reader
 mz_system
 mz_support
 super_user
```

You can also verify that the role has superuser privileges by checking the `pg_authid` system catalog:

```mzsql
SELECT rolsuper FROM pg_authid WHERE rolname = 'super_user';
```

```nofmt
 true
```

## Related pages

-   [`ALTER ROLE`](../alter-role)
-   [`DROP ROLE`](../drop-role)
-   [`DROP USER`](../drop-user)
-   [`GRANT ROLE`](../grant-role)
-   [`REVOKE ROLE`](../revoke-role)
-   [`ALTER OWNER`](../alter-owner)
-   [`GRANT PRIVILEGE`](../grant-privilege)
-   [`REVOKE PRIVILEGE`](../revoke-privilege)
