---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-owned/
complexity: intermediate
description: '`DROP OWNED` drops all the objects that are owned by one of the specified
  roles.'
doc_type: reference
keywords:
- DROP OWNED
- DROP ANYTHING
- RESTRICT
- 'Note:'
- CASCADE
product_area: Indexes
status: stable
title: DROP OWNED
---

# DROP OWNED

## Purpose
`DROP OWNED` drops all the objects that are owned by one of the specified roles.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP OWNED` drops all the objects that are owned by one of the specified roles.



`DROP OWNED` drops all the objects that are owned by one of the specified roles.
Any privileges granted to the given roles on objects will also be revoked.

> **Note:** 
Unlike [PostgreSQL](https://www.postgresql.org/docs/current/sql-drop-owned.html), Materialize drops
all objects across all databases, including the database itself.


## Syntax

This section covers syntax.

```mzsql
DROP OWNED BY <role_name> [, ...] [RESTRICT|CASCADE];
```text

Syntax element | Description
---------------|------------
`<role_name>`   | The role name whose owned objects will be dropped.
**CASCADE** | Optional. If specified, remove all dependent objects.
**RESTRICT**  | Optional. Do not drop anything if any non-index objects depencies exist. _(Default.)_

## Examples

This section covers examples.

```mzsql
DROP OWNED BY joe;
```text

```mzsql
DROP OWNED BY joe, george CASCADE;
```

## Privileges

The privileges required to execute this statement are:

- Role membership in `role_name`.


## Related pages

- [`REVOKE PRIVILEGE`](../revoke-privilege)
- [`CREATE ROLE`](../create-role)
- [`REASSIGN OWNED`](../reassign-owned)
- [`DROP CLUSTER`](../drop-cluster)
- [`DROP CLUSTER REPLICA`](../drop-cluster-replica)
- [`DROP CONNECTION`](../drop-connection)
- [`DROP DATABASE`](../drop-database)
- [`DROP INDEX`](../drop-index)
- [`DROP MATERIALIZED VIEW`](../drop-materialized-view)
- [`DROP SCHEMA`](../drop-schema)
- [`DROP SECRET`](../drop-secret)
- [`DROP SINK`](../drop-sink)
- [`DROP SOURCE`](../drop-source)
- [`DROP TABLE`](../drop-table)
- [`DROP TYPE`](../drop-type)
- [`DROP VIEW`](../drop-view)

