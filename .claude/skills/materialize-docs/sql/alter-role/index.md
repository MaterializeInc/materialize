---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-role/
complexity: beginner
description: '`ALTER ROLE` alters the attributes of an existing role.'
doc_type: reference
keywords:
- 'Note:'
- ALTER ROLE
- 'Warning:'
product_area: Indexes
status: stable
title: ALTER ROLE
---

# ALTER ROLE

## Purpose
`ALTER ROLE` alters the attributes of an existing role.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER ROLE` alters the attributes of an existing role.


`ALTER ROLE` alters the attributes of an existing role.[^1]

[^1]: Materialize does not support the `SET ROLE` command.

## Syntax


This section covers syntax.

#### Cloud

### Cloud

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-cloud/alte... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-cloud/alte... -->

**Note:**
<!-- Unresolved shortcode: {{% include-example file="examples/rbac-cloud/alte... -->

#### Self-Managed

### Self-Managed

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_r... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_r... -->

**Note:**
<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_r... -->

## Restrictions

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `ALTER ROLE ... INHERIT INHERIT`.

## Examples

This section covers examples.

#### Altering the attributes of a role

```mzsql
ALTER ROLE rj INHERIT;
```text
```mzsql
SELECT name, inherit FROM mz_roles WHERE name = 'rj';
```text
```nofmt
rj  true
```bash

#### Setting configuration parameters for a role

```mzsql
SHOW cluster;
quickstart

ALTER ROLE rj SET cluster TO rj_compute;

-- Role parameters only take effect for new sessions.
SHOW cluster;
quickstart

-- Start a new SQL session with the role 'rj'.
SHOW cluster;
rj_compute

-- In a new SQL session with a role that is not 'rj'.
SHOW cluster;
quickstart
```bash


#### Making a role a superuser  (Self-Managed)

Unlike regular roles, superusers have unrestricted access to all objects in the system and can perform any action on them.

```mzsql
ALTER ROLE rj SUPERUSER;
```text

To verify that the role has superuser privileges, you can query the `pg_authid` system catalog:

```mzsql
SELECT name, rolsuper FROM pg_authid WHERE rolname = 'rj';
```text

```nofmt
rj  t
```bash

#### Removing the superuser attribute from a role (Self-Managed)

NOSUPERUSER will remove the superuser attribute from a role, preventing it from having unrestricted access to all objects in the system.

```mzsql
ALTER ROLE rj NOSUPERUSER;
```text

```mzsql
SELECT name, rolsuper FROM pg_authid WHERE rolname = 'rj';
```text

```nofmt
rj  f
```bash

#### Removing a role's password (Self-Managed)

> **Warning:** 
Setting a NULL password removes the password.


```mzsql
ALTER ROLE rj PASSWORD NULL;
```bash

#### Changing a role's password (Self-Managed)

```mzsql
ALTER ROLE rj PASSWORD 'new_password';
```
## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.


## Related pages

- [`CREATE ROLE`](../create-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)