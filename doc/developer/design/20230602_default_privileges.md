# Default Privileges

- Associated: [RBAC](20230216_role_based_access_control.md)

## Context

Role based access control comprises a set of features that allow customers to control the
privileges of individual users. Default privileges allow users to configure what privileges an
object will have by default when it's first created.

PostgreSQL briefly describes default privilege in
their [privilege documentation](https://www.postgresql.org/docs/15/ddl-priv.html). Default
privileges for objects created by specified roles can be altered via
the [`ALTER DEFAULT PRIVILEGES`](https://www.postgresql.org/docs/15/sql-alterdefaultprivileges.html)
command. In PostgreSQL, default privileges are stored in
the [`pg_default_acl`](https://www.postgresql.org/docs/15/catalog-pg-default-acl.html) catalog
table.

What follows is brief description of default privileges in PostgreSQL, for a full understanding
please read the linked documentation.

- PostgreSQL has system default privileges that always exist in the system and have no corresponding
  entries in the `pg_default_acl` catalog table. Some examples of these are, the object owner is
  granted all privileges and the PUBLIC pseudo role is granted `USAGE` privileges on all types.
- The `ALTER DEFAULT PRIVILEGES` command only affects objects created by the roles specified in the
  command, or the current user if no roles are specified. There is no way to alter the default
  privileges for all roles.
- The `ALTER DEFAULT PRIVILEGES` command can either grant privileges or revoke privileges.
- There is a global variant of `ALTER DEFAULT PRIVILEGES` that affects the default privileges of all
  objects of a certain type within the current database.
- There is a non-global variant of `ALTER DEFAULT PRIVILEGES` that affects the default privileges of
  all objects of a certain type within on or more schemas.
- System default privileges can be overriden by a global `ALTER DEFAULT PRIVILEGES` command, but not
  by a non-global variant.
- If a revoke `ALTER DEFAULT PRIVILEGES` command matches a grant entry in the `pg_default_acl`
  table, then it will update the entry from the table and not add any other entries. If the entry
  ends up with an empty privilege `aclitem`, then it is removed from the table.
- If a revoke `ALTER DEFAULT PRIVILEGES` command is a global variant and matches a system default
  privilege, then it will add an entry in the `pg_default_acl` table that matches the system
  default, without the part that was revoked.
- If a revoke `ALTER DEFAULT PRIVILEGES` command does not match either of the previous two
  scenarios, then it has no affect.
- When an object is first created, the acl column for that object is `NULL`. This indicates that the
  privileges for this object are equivalent to the system default privileges for that object type.
  It does not indicate that the privileges are equivalent to the default privileges stored
  in `pg_default_acl`. Once the privileges of an object are modified, either through an
  explicit `GRANT`/`REVOKE` or through a configured default privilege in `pg_default_acl`, then the
  acl column for that object is filled in.

## Goals

- Allow users to configure default privileges on objects of different types, in a simple and easy to
  understand way (preferably simpler the PostgreSQL's model).

## Overview

We will build a default privileges framework based off of PostgreSQL, but simplified.

## Detailed description

The `mz_default_privileges` table will store default privileges and have the following columns:

- `id text` (maybe `uint4`): The id of the default privilege.
- `role_id text`: The id of the role this default privilege applies to. A special ID will be
  reserved for all roles.
- `schema_id text`: The id of the schema this default privilege applies to or NULL.
- `database_id text`: The id of the database this default privilege applies to or NULL.
- `object_type char`: Type of object this default privilege applies to.
- `privileges mz_aclitem[]`: The default privileges to apply.

If both `schema_id` and `database_id` are NULL, then the default privilege applies to all objects of
a certain type in all databases and schemas.

`object_type` can be one of the following characters:

- `n`: schema
- `d`: database
- `C`: cluster
- `r`: relation
- `T`: type
- `s`: secret
- `c`: connection

`mz_default_privileges` will be pre-populated with certain system default privileges. Currently, the
only one is:

- `(<id>, <reserved-role-id-for-all-roles>, NULL, NULL, T, {=U/mz_system})`. i.e. `PUBLIC` is
  granted `USAGE` on every type.

Roles cannot be deleted while it is referenced in the `mz_default_privilges` table.

`ALTER DEFAULT PRIVILEGES` is a command that will have the following syntax (formatted the same way
that PostgreSQL formats SQL syntax in their documentation):

`ALTER DEFAULT PRIVILEGES [ FOR <role_specification> ] [ { IN SCHEMA <schema_name> [, ...] | IN DATABASE <database_name> [, ...] } ] <abbreviated_grant_or_revoke>`

`<role_specification>: FOR { { ROLE | USER } <target_role> [, ...] | ALL ROLES }`

`<abbreviated_grant_or_revoke>`: `{ <grant> | <revoke> }`

`<grant>`: `GRANT { <privilege> [, ...] | ALL [ PRIVILEGES ] } ON <object_type> TO [ GROUP ] <role_name> [, ...]`

`<revoke>`: `REVOKE { <privilege> [, ...] | ALL [ PRIVILEGES ] } ON <object_type> FROM [ GROUP ] <role_name> [, ...]`

The grant variant will add rows to `mz_default_privileges`, such that:

- `role_id` is filled in from `<role_specification>`.
- `schema_id` is filled in from `<schema_name>`.
- `oject_type` is filled in from `<object_type>`.
- `privilege` is filled in from `<privilege>` and `<role_name>`.

If a matching row already exists, then the `mz_aclitem` will be updated instead of having a new row
added.

The revoke variant will update an existing row, if one exists. Otherwise, it will have no affect. If
a row ends up with an empty `mz_aclitem` then it will be removed.

Users must be a superuser to specify the `ALL ROLES` role specification. Users must be a member of
both `<target_role>` and `<role_name>`.

When an object is first created materialize will do the following in order:

1. Grant the owner all privileges on that object.
2. Grant any privileges that match a privilege in the `mz_default_privileges` table on that object.

### Differences from PostgreSQL

This section will highlight the differences between PostgreSQL default privileges and the proposed
default privileges for Materialize.

- This proposal has no special system privileges that are hard-coded into the system. Instead, we
  pre-populate the user default privileges table with some initial default privileges.
- This proposal allows defining default privileges for all current and future roles.
- This proposal allows defining default privileges for multiple databases at once.
- This proposal does not differentiate between global and non-global default privilege revokes.
- This proposal has no way to override default owner privileges.

## Alternatives

- Match PostgreSQL exactly.

## Open questions

- Should we allow users to modify the default owner privileges on an object?
- `mz_aclitem` might not be the best type for the `privilege` column, because we won't know the
  actual grantor until the privilege is applied to some object.
