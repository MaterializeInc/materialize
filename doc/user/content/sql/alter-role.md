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
**CREATEROLE**      | Grants the role the ability to create, alter, delete roles and the ability to grant and revoke role membership. This attribute is very powerful. It allows roles to grant and revoke membership in other roles, even if it doesn't have explicit membership in those roles. As a consequence, any role with this attribute can obtain the privileges of any other role in the system.
**NOCREATEROLE**    | Denies the role the ability to create, alter, delete roles or grant and revoke role membership.
**CREATEDB**        | Grants the role the ability to create databases.
**NOCREATEDB**      | Denies the role the ability to create databases.
**CREATECLUSTER**   | Grants the role the ability to create clusters.
**NOCREATECLUSTER** | Denies the role the ability to create clusters.

## Details

Unlike PostgreSQL, materialize derives the `LOGIN` and `SUPERUSER`
attributes for a role during authentication, every time that role tries
to connect to Materialize. Therefore, you cannot specify either
attribute when altering an existing role.

Unlike PostgreSQL, materialize does not currently support `NOINHERIT`.

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `ALTER ROLE ... CREATEDB NOCREATEDB` because
the `CREATEDB` and `NOCREATEDB` options conflict.

When RBAC is enabled a role must have the `CREATEROLE` attribute to alter another role.
Additionally, no role can grant another role an attribute that the altering role doesn't
have itself.

## Examples

```sql
ALTER ROLE rj CREATEDB NOCREATECLUSTER;
```
```sql
SELECT name, create_db, create_cluster FROM mz_roles WHERE name = 'rj';
```
```nofmt
rj  true  false
```

## Related pages

- [CREATE ROLE](../create-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [ALTER OWNER](../alter-owner)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
