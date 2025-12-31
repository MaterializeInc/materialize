---
audience: developer
canonical_url: https://materialize.com/docs/sql/grant-role/
complexity: advanced
description: '`GRANT` grants membership of one role to another role.'
doc_type: reference
keywords:
- SHOW ROLE
- DROP USER
- CREATE ROLE
- GRANT ROLE
- ALTER ROLE
- DROP ROLE
product_area: Indexes
status: stable
title: GRANT ROLE
---

# GRANT ROLE

## Purpose
`GRANT` grants membership of one role to another role.

If you need to understand the syntax and options for this command, you're in the right place.


`GRANT` grants membership of one role to another role.



`GRANT` grants membership of one role to another role. Roles can be members of
other roles, as well as inherit all the privileges of those roles.

## Syntax

[See diagram: grant-role.svg]

Field         | Use
--------------|--------------------------------------------------
_role_name_   | The role name to add _member_name_ as a member.
_member_name_ | The role name to add to _role_name_ as a member.

## Examples

This section covers examples.

```mzsql
GRANT data_scientist TO joe;
```text

```mzsql
GRANT data_scientist TO joe, mike;
```

## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.


## Useful views

- [`mz_internal.mz_show_role_members`](/sql/system-catalog/mz_internal/#mz_show_role_members)
- [`mz_internal.mz_show_my_role_members`](/sql/system-catalog/mz_internal/#mz_show_my_role_members)

## Related pages

- [`SHOW ROLE MEMBERSHIP`](../show-role-membership)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)

