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

{{< private-preview />}}

## Syntax

{{< diagram "create-role.svg" >}}

Field               | Use
--------------------|-------------------------------------------------------------------------
_role_name_         | A name for the role.
**INHERIT**         | Grants the role the ability to inheritance of privileges of other roles.

## Details

Unlike PostgreSQL, Materialize derives the `LOGIN` and `SUPERUSER`
attributes for a role during authentication, every time that role tries
to connect. Therefore, you cannot specify either
attribute when creating a new role. Additionally, we do not support the
`CREATE USER` command, because it implies a `LOGIN` attribute for the role.

Unlike PostgreSQL, Materialize does not currently support `NOINHERIT`.

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... INHERIT INHERIT`.

Unlike PostgreSQL, Materialize does not use role attributes to determine a roles ability to create
top level objects such as databases and other roles. Instead, Materialize uses system level
privileges. See [GRANT PRIVILEGE](../grant-privilege) for more details.

When RBAC is enabled a role must have the `CREATEROLE` system privilege to create another role.

## Examples

```sql
CREATE ROLE db_reader;
```
```sql
SELECT name FROM mz_roles;
```
```nofmt
 db_reader
 mz_system
 mz_introspection
```

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.

## Related pages

- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [ALTER OWNER](../alter-owner)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
