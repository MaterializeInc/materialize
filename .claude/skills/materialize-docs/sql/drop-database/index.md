---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-database/
complexity: intermediate
description: '`DROP DATABASE` removes a database from Materialize.'
doc_type: reference
keywords:
- 'Warning:'
- IF EXISTS
- DROP DATABASE
- RESTRICT
- CASCADE
product_area: Indexes
status: stable
title: DROP DATABASE
---

# DROP DATABASE

## Purpose
`DROP DATABASE` removes a database from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP DATABASE` removes a database from Materialize.



`DROP DATABASE` removes a database from Materialize.

> **Warning:**  `DROP DATABASE` immediately removes all objects within the
database without confirmation. Use with care! 

## Syntax

This section covers syntax.

```mzsql
DROP DATABASE [IF EXISTS] <database_name> [CASCADE|RESTRICT];
```text

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional.  If specified, do not return an error if the specified database does not exist.
`<database_name>` | The database you want to drop. For available databases, see [`SHOW DATABASES`](../show-databases).
**CASCADE** | Optional. Remove the database and its dependent objects. _(Default)_
**RESTRICT** | Optional. If specified, do not remove this database if it contains any schemas.

## Example

This section covers example.

### Remove a database containing schemas
You can use either of the following commands:

- ```mzsql
  DROP DATABASE my_db;
  ```text
- ```mzsql
  DROP DATABASE my_db CASCADE;
  ```bash

### Remove a database only if it contains no schemas
```mzsql
DROP DATABASE my_db RESTRICT;
```bash

### Do not issue an error if attempting to remove a nonexistent database
```mzsql
DROP DATABASE IF EXISTS my_db;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped database.


## Related pages

- [`DROP OWNED`](../drop-owned)

