---
title: "ALTER ROLE"
description: "`ALTER ROLE` alters the attributes of an existing role."
menu:
  main:
    parent: commands
---

`ALTER ROLE` alters the attributes of an existing role.

{{< warning >}}
Roles in Materialize are currently limited in functionality. In the future they
will be used for role-based access control. See GitHub issue {{% gh 677 %}}
for details.
{{< /warning >}}

## Syntax

{{< diagram "alter-role.svg" >}}

Field | Use
------|-----
**LOGIN** | Grants the user the ability to log in.
**NOLOGIN** | Denies the user the ability to log in.
**SUPERUSER** | Grants the user superuser permission, i.e., unrestricted access to the system.
**NOSUPERUSER** | Denies the user superuser permission.
_role_name_ | A name for the role.

## Details

Materialize only permits user accounts with both the `LOGIN` and
`SUPERUSER` options specified.

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `ALTER ROLE ... LOGIN NOLOGIN` because
the `LOGIN` and `NOLOGIN` options conflict.

## Examples

```sql
ALTER ROLE rj LOGIN SUPERUSER;
```
```sql
SELECT name, can_login, super_user FROM mz_roles WHERE name = 'rj';
```
```nofmt
rj  true  true
```

## Related pages

- [CREATE ROLE](../create-role)
- [CREATE USER](../create-user)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
