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
objects created by a certain set of roles, if no roles are specified then it is assumed to be the
current role.

`ALTER DEFAULT PRIVILEGES` cannot be used to revoke the default owner privileges on objects. Those
privileges must be revoked manually after the object is created. Though owners can always re-grant
themselves any privilege on an object that they own.

The `REVOKE` variant of `ALTER DEFAULT PRIVILEGES` is used to revoke previously created default
privileges on objects created in the future. It will not revoke any privileges on objects that have
already been created.

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
_target_role_      | The default privilege will apply to objects created by this role. If this is left blank, then the current role is assumed. Use the `PUBLIC` pseudo-role to target objects created by all roles.
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

```sql
ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO joe;
```

```sql
ALTER DEFAULT PRIVILEGES FOR ROLE interns IN DATABASE dev GRANT ALL PRIVILEGES ON TABLES TO intern_managers;
```

```sql
ALTER DEFAULT PRIVILEGES FOR ROLE developers REVOKE USAGE ON SECRETS FROM project_managers;
```

```sql
ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT ON TABLES TO managers;
```

## Related pages

- [CREATE ROLE](../create-role)
- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [ALTER OWNER](../alter-owner)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
