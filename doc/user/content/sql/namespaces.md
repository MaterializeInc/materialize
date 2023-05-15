---
title: "Namespaces"
description: "SQL namespaces let you create a taxonomy of objects in Materialize."
weight: 40
menu:
  main:
    parent: 'reference'
    name: 'SQL namespaces'
    weight: 126
---

Materialize follows SQL standard's namespace hierarchy for most objects. The
Materialize structure is:

- Databases (highest level)
- Schemas
- Tables
- Sources
- Views
- Materialized views
- Sinks
- Indexes
- Types
- Functions
- Secrets
- Columns (lowest level)

Each layer in the hierarchy can contain elements directly beneath it. For
example, databases can contain schemas.

## Independent Objects

The Materialzie objects that exist outside of the standard namespace hierarchy
are:

- Clusters
- Cluster replicas
- Roles

These objects are independent of other objects and are not referenced by the
standard SQL namespace.

For example, to create a materialized view in a specific cluster, your SQL
statement would be:

```sql
CREATE MATERIALIZED VIEW mv IN CLUSTER cluster1 AS ... 
```

Replicas are referenced as `<cluster-name>.<replica-name>`.

For example, to delete replica `r1` in cluster `cluster1`, your SQL statement
would be:

```sql
DROP CLUSTER REPLICA cluster1.r1
```

Roles are referenced by their name. For example, to alter the `manager` role, your SQL statement would be:

```sql
ALTER ROLE manager ...
```

No two clusters or roles can have the same name. Replicas can have the same name
if they are in different clusters.

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
- By default, Materialize regions have a database named `materialize`.
- By default, each database has a schema called `public`.
- You can specify which database you connect to either when you connect (e.g.
  `psql -d my_db ...`) or within SQL using `SET` (e.g. `SET DATABASE = my_db`).
