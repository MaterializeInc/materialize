---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-database/
complexity: intermediate
description: '`ALTER DATABASE` changes properties of a database.'
doc_type: reference
keywords:
- ALTER DATABASE
product_area: Indexes
status: stable
title: ALTER DATABASE
---

# ALTER DATABASE

## Purpose
`ALTER DATABASE` changes properties of a database.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER DATABASE` changes properties of a database.



Use `ALTER DATABASE` to:
- Rename a database.
- Change owner of a database.

## Syntax

This section covers syntax.

#### Rename

### Rename

To rename a database:

<!-- Syntax example: examples/alter_database / syntax-rename -->

#### Change owner

### Change owner

To change the owner of a database:

<!-- Syntax example: examples/alter_database / syntax-change-owner -->

## Privileges

The privileges required to execute this statement are:

- Ownership of the database.
- In addition, to change owners:
  - Role membership in `new_owner`.


