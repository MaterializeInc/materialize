---
title: "Namespaces"
description: "SQL namespaces let you create a taxonomy of objects in your Materialize instances."
weight: 40
menu:
  main:
    parent: 'reference'
    name: 'SQL namespaces'
    weight: 126
---

Materialize mimics SQL standard's namespace hierarchy, which is:

- Databases (highest level)
- Schemas
- Tables, views, sources
- Columns (lowest level)

Each layer in the hierarchy can contain elements directly beneath it. For
example, databases can contain schemas.

## Details

- All namespaces must adhere to [identifier rules](../identifiers).

### Reaching namespaces

Namespaces are specified using a format like `parent.child`.

For example, in a situation where a statement expects a source name, you could
use:

- `schema.source` to reach a source in another schema
- `database.schema.source` to reach a source in another database.

### Database details

- Unlike PostgreSQL, Materialize allows cross-database queries.
- By default, Materialize instances have a database named `materialize`.
- By default, each database has a schema called `public`.
- You can specify which database you connect to either when you connect (e.g.
  `psql -d my_db ...`) or within SQL using `SET` (e.g. `SET DATABASE = my_db`).
