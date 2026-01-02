---
audience: developer
canonical_url: https://materialize.com/docs/sql/create-role/
complexity: intermediate
description: '`CREATE ROLE` creates a new role.'
doc_type: reference
keywords:
- CREATE USER
- 'Note:'
- CREATE ROLE
product_area: Indexes
status: stable
title: CREATE ROLE
---

# CREATE ROLE

## Purpose
`CREATE ROLE` creates a new role.

If you need to understand the syntax and options for this command, you're in the right place.


`CREATE ROLE` creates a new role.


`CREATE ROLE` creates a new role, which is a user account in Materialize.[^1]

When you connect to Materialize, you must specify the name of a valid role in
the system.

[^1]: Materialize does not support the `CREATE USER` command.

## Syntax

This section covers syntax.

#### Cloud

### Cloud
<!-- Unresolved shortcode: {{% include-example file="examples/rbac-cloud/crea... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-cloud/crea... -->

**Note:**
<!-- Unresolved shortcode: {{% include-example file="examples/rbac-cloud/crea... -->

#### Self-Managed

### Self-Managed
<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/create_... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/create_... -->

**Note:**
<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/create_... -->

## Restrictions

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... INHERIT INHERIT`.

## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.


## Examples

This section covers examples.

### Create a functional role

In Materialize Cloud and Self-Managed, you can create a functional role:

```mzsql
CREATE ROLE db_reader;
```bash

### Create a role with login and password (Self-Managed)

```mzsql
CREATE ROLE db_reader WITH LOGIN PASSWORD 'password';
```text

You can verify that the role was created by querying the `mz_roles` system catalog:

```mzsql
SELECT name FROM mz_roles;
```text

```nofmt
 db_reader
 mz_system
 mz_support
```bash

### Create a superuser role (Self-Managed)

Unlike regular roles, superusers have unrestricted access to all objects in the system and can perform any action on them.

```mzsql
CREATE ROLE super_user WITH SUPERUSER LOGIN PASSWORD 'password';
```text

You can verify that the superuser role was created by querying the `mz_roles` system catalog:

```mzsql
SELECT name FROM mz_roles;
```text

```nofmt
 db_reader
 mz_system
 mz_support
 super_user
```text

You can also verify that the role has superuser privileges by checking the `pg_authid` system catalog:

```mzsql
SELECT rolsuper FROM pg_authid WHERE rolname = 'super_user';
```text

```nofmt
 true
```


## Related pages

- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)