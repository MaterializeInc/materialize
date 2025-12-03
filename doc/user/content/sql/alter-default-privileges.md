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

```mzsql
ALTER DEFAULT PRIVILEGES
  FOR ROLE <object_creator> [, ...] | ALL ROLES
  [IN SCHEMA <schema_name> [, ...] | IN DATABASE <database_name> [, ...]]
  GRANT [<privilege> [, ...] | ALL [PRIVILEGES]]
  ON TABLES | TYPES | SECRETS | CONNECTIONS | DATABASES | SCHEMAS | CLUSTERS
  TO <target_role> [, ...]
;
```

Syntax element | Description
---------------|------------
`<object_creator>` | The default privilege will apply to objects created by this role. Use the `PUBLIC` pseudo-role to target objects created by all roles.
**ALL ROLES** | The default privilege will apply to objects created by all roles. This is shorthand for specifying `PUBLIC` as the target role.
**IN SCHEMA** `<schema_name>` | Optional. The default privilege will apply only to objects created in this schema.
**IN DATABASE** `<database_name>` | Optional. The default privilege will apply only to objects created in this database.
`<privilege>` | A specific privilege (e.g., `SELECT`, `USAGE`, `CREATE`). See [Available privileges](#available-privileges).
**ALL [PRIVILEGES]** | All applicable privileges for the provided object type.
**TO** `<target_role>` | The role who will be granted the default privilege. Use the `PUBLIC` pseudo-role to grant privileges to all roles.

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


```mzsql
ALTER DEFAULT PRIVILEGES
  FOR ROLE <creator_role> [, ...] | ALL ROLES
  [IN SCHEMA <schema_name> [, ...] | IN DATABASE <database_name> [, ...]]
  REVOKE [<privilege> [, ...] | ALL [PRIVILEGES]]
  ON TABLES | TYPES | SECRETS | CONNECTIONS | DATABASES | SCHEMAS | CLUSTERS
  FROM <target_role> [, ...]
;
```

Syntax element | Description
---------------|------------
`<creator_role>` | The default privileges for objects created by this role. Use the `PUBLIC` pseudo-role to specify objects created by all roles.
**ALL ROLES** | The default privilege for objects created by all roles. This is shorthand for specifying `PUBLIC` as the target role.
**IN SCHEMA** `<schema_name>` | Optional. The default privileges for objects created in this schema.
**IN DATABASE** `<database_name>` | Optional. The default privilege for objects created in this database.
`<privilege>` | A specific privilege (e.g., `SELECT`, `USAGE`, `CREATE`). See [Available privileges](#available-privileges).
**ALL [PRIVILEGES]** | All applicable privileges for the provided object type.
**FROM** `<target_role>` | The role from whom to remove the default privilege. Use the `PUBLIC` pseudo-role to remove default privileges previously granted to `PUBLIC`.

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
