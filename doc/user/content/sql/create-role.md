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

```mzsql

```

## Syntax

```mzsql
CREATE ROLE _role_name_ [WITH [SUPERUSER | NOSUPERUSER ]
    [ LOGIN | NOLOGIN ]
    [ INHERIT | NOINHERIT ]
    [ PASSWORD <text> ]]
```

| Field                           | Use                    |
| ------------------------------- | ---------------------- |
| _role_name_                     | A name for the role.   |
| **WITH (** _options list_ **)** | See below for options. |

## Details

Materialize's support for `CREATE ROLE` is similar to that of PostgreSQL, with
the following options exceptions:

| Option        | Description                                                                                                                                              |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `INHERIT`     | Materialize implicitly uses `INHERIT` for the `CREATE ROLE` command. That is, `CREATE ROLE <name>` and `CREATE ROLE <name> WITH INHERIT` are equivalent. |
| `NOINHERIT`   | Materialize does not support the `NOINHERIT` option for `CREATE ROLE`.                                                                                   |
| `LOGIN`       | The `LOGIN` attribute allows a role to login via the postgresql or web endpoints                                                                         |
| `NOLOGIN`     | The `NOLOGIN` attribute prevents a role from logging in. This is the default behavior if `LOGIN` is not specified.                                       |
| `SUPERUSER`   | The `SUPERUSER` attribute grants the role superuser privileges.                                                                                          |
| `NOSUPERUSER` | The `NOSUPERUSER` attribute prevents the role from having superuser privileges. This is the default behavior if `SUPERUSER` is not specified.            |
| `PASSWORD`    | The `PASSWORD` attribute allows you to set a password for the role.                                                                                      |

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

-   `CREATEROLE` privileges on the system.

## Examples

```mzsql
CREATE ROLE db_reader WITH LOGIN PASSWORD 'password';
```

```mzsql
SELECT name FROM mz_roles;
```

```nofmt
 db_reader
 mz_system
 mz_support
```

```mzsql
CREATE ROLE super_user WITH SUPERUSER LOGIN PASSWORD 'password';
```

```mzsql
SELECT name FROM mz_roles;
```

```nofmt
 db_reader
 mz_system
 mz_support
 super_user
```

```mzsql
SELECT rolsuper FROM pg_authid WHERE rolname = 'super_user';
```

```nofmt
 t
```

## Related pages

-   [ALTER ROLE](../alter-role)
-   [DROP ROLE](../drop-role)
-   [DROP USER](../drop-user)
-   [GRANT ROLE](../grant-role)
-   [REVOKE ROLE](../revoke-role)
-   [ALTER OWNER](../alter-owner)
-   [GRANT PRIVILEGE](../grant-privilege)
-   [REVOKE PRIVILEGE](../revoke-privilege)
