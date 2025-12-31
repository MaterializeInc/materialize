---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-schema/
complexity: intermediate
description: '`ALTER SCHEMA` change properties of a schema'
doc_type: reference
keywords:
- ALTER SCHEMA
- CREATE TABLE
- CREATE SCHEMA
product_area: Indexes
status: stable
title: ALTER SCHEMA
---

# ALTER SCHEMA

## Purpose
`ALTER SCHEMA` change properties of a schema

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER SCHEMA` change properties of a schema



Use `ALTER SCHEMA` to:
- Swap the name of a schema with that of another schema.
- Rename a schema.
- Change owner of a schema.


## Syntax

This section covers syntax.

#### Swap with

### Swap with

To swap the name of a schema with that of another schema:

<!-- Syntax example: examples/alter_schema / syntax-swap-with -->

#### Rename schema

### Rename schema

To rename a schema:

<!-- Syntax example: examples/alter_schema / syntax-rename -->

#### Change owner to

### Change owner to

To change the owner of a schema:

<!-- Syntax example: examples/alter_schema / syntax-change-owner -->


## Examples

This section covers examples.

### Swap schema names

Swapping two schemas is useful for a blue/green deployment. The following swaps
the names of the `blue` and `green` schemas.

```mzsql
CREATE SCHEMA blue;
CREATE TABLE blue.numbers (n int);

CREATE SCHEMA green;
CREATE TABLE green.tags (tag text);

ALTER SCHEMA blue SWAP WITH green;

-- The schema which was previously named 'green' is now named 'blue'.
SELECT * FROM blue.tags;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the schema.
- In addition,
  - To swap with another schema:
    - Ownership of the other schema
  - To change owners:
    - Role membership in `new_owner`.
    - `CREATE` privileges on the containing database.


## See also

- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW SECRETS`](/sql/show-secrets)
- [`SHOW SINKS`](/sql/show-sinks)

