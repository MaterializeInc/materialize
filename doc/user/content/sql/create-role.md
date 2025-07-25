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

{{< diagram "create-role.svg" >}}

Field               | Use
--------------------|-------------------------------------------------------------------------
_role_name_         | A name for the role.
**INHERIT**         | Grants the role the ability to inherit privileges of other roles.

## Details

Materialize's support for `CREATE ROLE` is similar to that of PostgreSQL, with
the following options exceptions:

| Option   | Description                                                                                                                                                                                            |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `INHERIT`   | Materialize implicitly uses `INHERIT` for the `CREATE ROLE` command. That is, `CREATE ROLE <name>` and `CREATE ROLE <name> WITH INHERIT` are equivalent.                                                   |
| `NOINHERIT` | Materialize does not support the `NOINHERIT` option for `CREATE ROLE`.                                                                                                                                  |
| `LOGIN`     | Materialize does not support the `LOGIN` option for `CREATE ROLE`.<ul><li>Instead, Materialize derives the `LOGIN` option for a role during authentication every time that role tries to connect.</li><li>Materialize does not support the `CREATE USER` command as the command implies a `LOGIN` attribute for the role.</li></ul>|
| `SUPERUSER` | Materialize does not support the `SUPERUSER` option for `CREATE ROLE`.<ul><li>Instead, Materialize derives the `SUPERUSER` option for a role during authentication every time that role tries to connect.</li></ul>|

{{< note >}}

Materialize does not use role attributes to determine a role's ability to create
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

```mzsql
CREATE ROLE db_reader;
```
```mzsql
SELECT name FROM mz_roles;
```
```nofmt
 db_reader
 mz_system
 mz_support
```

## Related pages

- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](../alter-owner)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
