---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-default-privileges/
complexity: advanced
description: '`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied
  to objects created in the future.'
doc_type: reference
keywords:
- 'Note:'
- ALTER DEFAULT
- ALTER DEFAULT PRIVILEGES
- ALL ROLES
product_area: Indexes
status: stable
title: ALTER DEFAULT PRIVILEGES
---

# ALTER DEFAULT PRIVILEGES

## Purpose
`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied to objects created in the future.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied to objects created in the future.


Use `ALTER DEFAULT PRIVILEGES` to:

- Define default privileges that will be applied to objects created in the
future. It does not affect any existing objects.

- Revoke previously created default privileges on objects created in the future.

All new environments are created with a single default privilege, `USAGE` is
granted on all `TYPES` to the `PUBLIC` role. This can be revoked like any other
default privilege.

## Syntax

This section covers syntax.

#### GRANT

### GRANT

`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied to
objects created by a role in the future. It does not affect any existing
objects.

Default privileges are specified for a certain object type and can be applied to
all objects of that type, all objects of that type created within a specific set
of databases, or all objects of that type created within a specific set of
schemas. Default privileges are also specified for objects created by a certain
set of roles or by all roles.

<!-- Syntax example: examples/alter_default_privileges / syntax-grant -->

#### REVOKE

### REVOKE

> **Note:** 
`ALTER DEFAULT PRIVILEGES` cannot be used to revoke the default owner privileges
on objects. Those privileges must be revoked manually after the object is
created. Though owners can always re-grant themselves any privilege on an object
that they own.


The `REVOKE` variant of `ALTER DEFAULT PRIVILEGES` is used to revoke previously
created default privileges on objects created in the future. It will not revoke
any privileges on objects that have already been created. When revoking a
default privilege, all the fields in the revoke statement (`creator_role`,
`schema_name`, `database_name`, `privilege`, `target_role`) must exactly match
an existing default privilege. The existing default privileges can easily be
viewed by the following query: `SELECT * FROM
mz_internal.mz_show_default_privileges`.

<!-- Syntax example: examples/alter_default_privileges / syntax-revoke -->

## Details

This section covers details.

### Available privileges


#### By Privilege

<!-- Dynamic table: rbac/privileges_objects - see original docs -->

#### By Object

<!-- Dynamic table: rbac/object_privileges - see original docs -->


### Compatibility

For PostgreSQL compatibility reasons, you must specify `TABLES` as the object
type for sources, views, and materialized views.

## Examples

This section covers examples.

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE mike GRANT SELECT ON TABLES TO joe;
```text

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE interns IN DATABASE dev GRANT ALL PRIVILEGES ON TABLES TO intern_managers;
```text

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE developers REVOKE USAGE ON SECRETS FROM project_managers;
```text

```mzsql
ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT ON TABLES TO managers;
```

## Privileges

The privileges required to execute this statement are:

- Role membership in `role_name`.
- `USAGE` privileges on the containing database if `database_name` is specified.
- `USAGE` privileges on the containing schema if `schema_name` is specified.
- _superuser_ status if the _target_role_ is `PUBLIC` or **ALL ROLES** is
  specified.


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