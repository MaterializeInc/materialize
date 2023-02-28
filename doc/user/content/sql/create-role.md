---
title: "CREATE ROLE"
description: "`CREATE ROLE` creates a new role."
menu:
  main:
    parent: commands
---

`CREATE ROLE` creates a new role.

## Conceptual framework

A role is a user account in a Materialize instance.

When you [connect to a Materialize instance](/integrations/psql), you must specify
the name of a valid role in the system.

{{< warning >}}
Roles in Materialize are currently limited in functionality. In the future they
will be used for role-based access control. See GitHub issue {{% gh 677 %}}
for details.
{{< /warning >}}

## Syntax

{{< diagram "create-role.svg" >}}

Field | Use
------|-----
_role_name_ | A name for the role.

## Details

Materialize does not currently support any role attributes.

Unlike PostgreSQL, materialize derives the `LOGIN` and `SUPERUSER`
attributes for a role during authentication, every time that role tries
to connect to Materialize. Therefore, you cannot specify either
attribute when creating a new role. Additionally, we do not support
`CREATE USER` because it implies a `LOGIN` attribute for the role.

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... INHERIT NOINHERIT` because
the `INHERIT` and `NOINHERIT` options conflict.

## Examples

```sql
CREATE ROLE rj;
```
```sql
SELECT name FROM mz_roles;
```
```nofmt
materialize
rj
```

## Related pages

- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
