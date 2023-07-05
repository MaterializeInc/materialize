---
title: "ALTER ROLE"
description: "`ALTER ROLE` alters the attributes of an existing role."
menu:
  main:
    parent: commands
---

`ALTER ROLE` alters the attributes of an existing role.

{{< alpha />}}

## Syntax

{{< diagram "alter-role.svg" >}}

Field               | Use
--------------------|-------------------------------------------------------------------------
_role_name_         | A name for the role.
**INHERIT**         | Grants the role the ability to inheritance of privileges of other roles.

## Details

Unlike PostgreSQL, materialize derives the `LOGIN` and `SUPERUSER`
attributes for a role during authentication, every time that role tries
to connect to Materialize. Therefore, you cannot specify either
attribute when altering an existing role.

Unlike PostgreSQL, materialize does not currently support the `NOINHERIT` attribute and the `SET
ROLE` command.

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `ALTER ROLE ... INHERIT INHERIT`.

Unlike PostgreSQL, Materialize does not use role attributes to determine a roles ability to create
top level objects such as databases and other roles. Instead, Materialize uses system level
privileges. See [GRANT PRIVILEGE](../grant-privilege) for more details.

When RBAC is enabled a role must have the `CREATEROLE` system privilege to alter another role.

## Examples

```sql
ALTER ROLE rj INHERIT;
```
```sql
SELECT name, inherit FROM mz_roles WHERE name = 'rj';
```
```nofmt
rj  true
```

## Privileges

{{< alpha />}}

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.

## Related pages

- [CREATE ROLE](../create-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [ALTER OWNER](../alter-owner)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
