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

{{< warning >}}
Role-Based Access Control is under development {{% gh 11579 %}}. Currently, no
role attributes or privileges are considered when executing `CREATE ROLE`
statements, but these attributes are saved and will be considered in a future
release.
{{< /warning >}}

## Syntax

{{< diagram "create-role.svg" >}}

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

Unlike PostgreSQL, Materialize derives the `LOGIN` and `SUPERUSER`
attributes for a role during authentication, every time that role tries
to connect. Therefore, you cannot specify either
attribute when creating a new role. Additionally, we do not support the
`CREATE USER` command, because it implies a `LOGIN` attribute for the role.

Unlike PostgreSQL, Materialize does not currently support `NOINHERIT`.

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... CREATEDB NOCREATEDB` because
the `CREATEDB` and `NOCREATEDB` options conflict.

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

## Related pages

- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
