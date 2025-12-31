---
audience: developer
canonical_url: https://materialize.com/docs/sql/reassign-owned/
complexity: intermediate
description: '`REASSIGN OWNED` reassigns the owner of all the objects that are owned
  by one of the specified roles.'
doc_type: reference
keywords:
- REASSIGN OWNED
- ALTER OWNER
- DROP OWNED
- 'Note:'
product_area: Indexes
status: stable
title: REASSIGN OWNED
---

# REASSIGN OWNED

## Purpose
`REASSIGN OWNED` reassigns the owner of all the objects that are owned by one of the specified roles.

If you need to understand the syntax and options for this command, you're in the right place.


`REASSIGN OWNED` reassigns the owner of all the objects that are owned by one of the specified roles.



`REASSIGN OWNED` reassigns the owner of all the objects that are owned by one of the specified roles.

> **Note:** 
Unlike [PostgreSQL](https://www.postgresql.org/docs/current/sql-drop-owned.html), Materialize reassigns
all objects across all databases, including the databases themselves.


## Syntax

This section covers syntax.

```mzsql
REASSIGN OWNED BY <current_owner> [, ...] TO <new_owner>;
```text

Syntax element | Description
---------------|------------
`<current_owner>` | The role whose objects are to be reassigned.
`<new_owner>` | The role name of the new owner of these objects.

## Examples

This section covers examples.

```mzsql
REASSIGN OWNED BY joe TO mike;
```text

```mzsql
REASSIGN OWNED BY joe, george TO mike;
```

## Privileges

The privileges required to execute this statement are:

- Role membership in `old_role` and `new_role`.


## Related pages

- [`ALTER OWNER`](/sql/#rbac)
- [`DROP OWNED`](../drop-owned)

