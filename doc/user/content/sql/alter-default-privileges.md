---
title: "ALTER DEFAULT PRIVILEGES"
description: "`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied to objects created in the future."
menu:
  main:
    parent: commands
---

Use `ALTER DEFAULT PRIVILEGES` to:

- Define default privileges that will be applied to objects created in the
future. It does not affect any existing objects.

- Revoke previously created default privileges on objects created in the future.

All new environments are created with a single default privilege, `USAGE` is
granted on all `TYPES` to the `PUBLIC` role. This can be revoked like any other
default privilege.

## Syntax

{{< tabs >}}
{{< tab "GRANT" >}}
### GRANT

`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied to
objects created by a role in the future. It does not affect any existing
objects.

Default privileges are specified for a certain object type and can be applied to
all objects of that type, all objects of that type created within a specific set
of databases, or all objects of that type created within a specific set of
schemas. Default privileges are also specified for objects created by a certain
set of roles or by all roles.

{{% include-syntax file="examples/alter_default_privileges" example="syntax-grant" %}}

{{< /tab >}}
{{< tab "REVOKE" >}}
### REVOKE

{{< note >}}
`ALTER DEFAULT PRIVILEGES` cannot be used to revoke the default owner privileges
on objects. Those privileges must be revoked manually after the object is
created. Though owners can always re-grant themselves any privilege on an object
that they own.
{{< /note >}}

The `REVOKE` variant of `ALTER DEFAULT PRIVILEGES` is used to revoke previously
created default privileges on objects created in the future. It will not revoke
any privileges on objects that have already been created. When revoking a
default privilege, all the fields in the revoke statement (`creator_role`,
`schema_name`, `database_name`, `privilege`, `target_role`) must exactly match
an existing default privilege. The existing default privileges can easily be
viewed by the following query: `SELECT * FROM
mz_internal.mz_show_default_privileges`.

{{% include-syntax file="examples/alter_default_privileges" example="syntax-revoke" %}}

{{< /tab >}}
{{< /tabs >}}

## Details

### Available privileges

{{< tabs >}}
{{< tab "By Privilege" >}}
{{< yaml-table data="rbac/privileges_objects" >}}
{{</ tab >}}
{{< tab "By Object" >}}
{{< yaml-table data="rbac/object_privileges" >}}
{{</ tab >}}
{{</ tabs >}}


### Compatibility

For PostgreSQL compatibility reasons, you must specify `TABLES` as the object
type for sources, views, and materialized views.

## Examples

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE mike GRANT SELECT ON TABLES TO joe;
```

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE interns IN DATABASE dev GRANT ALL PRIVILEGES ON TABLES TO intern_managers;
```

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE developers REVOKE USAGE ON SECRETS FROM project_managers;
```

```mzsql
ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT ON TABLES TO managers;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-default-privileges.md" >}}

## Useful views

- [`mz_internal.mz_show_default_privileges`](/sql/system-catalog/mz_internal/#mz_show_default_privileges)
- [`mz_internal.mz_show_my_default_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_default_privileges)

## Related pages

- [`SHOW DEFAULT PRIVILEGES`](../show-default-privileges)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
