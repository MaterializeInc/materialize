---
title: "ALTER DEFAULT PRIVILEGES"
description: "`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied to objects created in the future."
menu:
  main:
    parent: commands
---

`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied to objects created in
the future. It does not affect any existing objects.

Default privileges are specified for a certain object type and can be applied to all objects of
that type, all objects of that type created within a specific set of databases, or all objects of
that type created within a specific set of schemas. Default privileges are also specified for
objects created by a certain set of roles or by all roles.

`ALTER DEFAULT PRIVILEGES` cannot be used to revoke the default owner privileges on objects. Those
privileges must be revoked manually after the object is created. Though owners can always re-grant
themselves any privilege on an object that they own.

The `REVOKE` variant of `ALTER DEFAULT PRIVILEGES` is used to revoke previously created default
privileges on objects created in the future. It will not revoke any privileges on objects that have
already been created. When revoking a default privilege, all the fields in the revoke statement
(`target_role`, `schema_name`, `database_name`, `privilege`, `grantee`) must exactly match an
existing default privilege. The existing default privileges can easily be viewed by the following
query: `SELECT * FROM mz_internal.mz_show_default_privileges`.

All new environments are created with a single default privilege, `USAGE` is granted on all `TYPES`
to the `PUBLIC` role. This can be revoked like any other default privilege.

## Syntax

{{< diagram "alter-default-privileges.svg" >}}

### `abreviated_grant`

{{< diagram "abbreviated-grant.svg" >}}

### `abbreviated_revoke`

{{< diagram "abbreviated-revoke.svg" >}}

### `privilege`

{{< diagram "privilege.svg" >}}

Field              | Use
-------------------|--------------------------------------------------
_target_role_      | The default privilege will apply to objects created by this role. Use the `PUBLIC` pseudo-role to target objects created by all roles.
**ALL ROLES**      | The default privilege will apply to objects created by all roles. This is shorthand for specifying `PUBLIC` as the _target_role_.
_schema_name_      | The default privilege will apply only to objects created in this schema, if specified.
_database_name_    | The default privilege will apply only to objects created in this database, if specified.
**SELECT**         | Allows reading rows from an object. The abbreviation for this privilege is 'r' (read).
**INSERT**         | Allows inserting into an object. The abbreviation for this privilege is 'a' (append).
**UPDATE**         | Allows updating an object (requires **SELECT** if a read is necessary). The abbreviation for this privilege is 'w' (write).
**DELETE**         | Allows deleting from an object (requires **SELECT** if a read is necessary). The abbreviation for this privilege is 'd'.
**CREATE**         | Allows creating a new object within another object. The abbreviation for this privilege is 'C'.
**USAGE**          | Allows using an object or looking up members of an object. The abbreviation for this privilege is 'U'.
**ALL PRIVILEGES** | All applicable privileges for the provided object type.
_grantee_          | The role name that will gain the default privilege. Use the `PUBLIC` pseudo-role to grant privileges to all roles.
_revokee_          | The role name that will not gain the default privilege. Use the `PUBLIC` pseudo-role to remove default privileges previously granted to `PUBLIC`.

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
- [`ALTER OWNER`](../alter-owner)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
