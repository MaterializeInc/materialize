---
title: "ALTER ROLE"
description: "`ALTER ROLE` alters the attributes of an existing role."
menu:
  main:
    parent: commands
---

`ALTER ROLE` alters the attributes of an existing role.

{{< warning >}}
Role-Based Access Control is under development {{% gh 11579 %}}. Currently, no
role attributes or privileges are considered when executing `CREATE ROLE`
statements, but these attributes are saved and will be considered in a future
release.
{{< /warning >}}

## Syntax

{{< diagram "alter-role.svg" >}}

Field               | Use
--------------------|-------------------------------------------------------------------------
_role_name_         | A name for the role.
**INHERIT**         | Grants the role the ability to inheritance of privileges of other roles.
**CREATEROLE**      | Grants the role the ability to create, alter, and delete roles.
**NOCREATEROLE**    | Denies the role the ability to create, alter, and delete roles.
**CREATEDB**        | Grants the role the ability to create databases.
**NOCREATEDB**      | Denies the role the ability to create databases.
**CREATECLUSTER**   | Grants the role the ability to create clusters.
**NOCREATECLUSTER** | Denies the role the ability to create clusters.

## Details

Unlike PostgreSQL, materialize derives the `LOGIN` and `SUPERUSER`
attributes for a role during authentication, every time that role tries
to connect to Materialize. Therefore, you cannot specify either
attribute when creating a new role. Additionally, we do not support
`CREATE USER` because it implies a `LOGIN` attribute for the role.

Unlike PostgreSQL, materialize does not currently support `NOINHERIT`.

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... CREATEDB NOCREATEDB` because
the `CREATEDB` and `NOCREATEDB` options conflict.

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
