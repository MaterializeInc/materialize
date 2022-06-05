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
**LOGIN** | Grants the user the ability to log in.
**NOLOGIN** | Denies the user the ability to log in.
**SUPERUSER** | Grants the user superuser permission, i.e., unrestricted access to the system.
**NOSUPERUSER** | Denies the user superuser permission.
_role_name_ | A name for the role.

## Details

Materialize only permits creating user accounts with both the `LOGIN` and
`SUPERUSER` options specified.

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... LOGIN NOLOGIN` because
the `LOGIN` and `NOLOGIN` options conflict.

## Examples

```sql
CREATE ROLE rj LOGIN SUPERUSER;
```
```sql
SELECT name FROM mz_roles;
```
```nofmt
materialize
rj
```

## Related pages

- [CREATE USER](../create-user)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
