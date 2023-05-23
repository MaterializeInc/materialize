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

Namespaces are a logical way to organize Materialize objects. In
organizations with multiple objects, they help avoid naming conflicts and make
it easier to manage objects. Namespaces allow you to refer to objects by a fully
qualified name like `database.schema.object_name`.

## Namespace hierarchy

Materialize follows SQL standard's namespace hierarchy for most objects. The
Materialize structure is:

- Databases (highest level)
- Schemas
    - Tables
    - Views
    - Materialized views
    - Connections
    - Sources
    - Sinks
    - Indexes
    - Types
    - Functions
    - Secrets
- Columns (lowest level)

The root layer in the hierarchy can contain elements directly beneath it. For
example, databases can contain schemas.

Objects within the schema layer are not in a hierarchy and do not necessarily
share a relationship with one another.

## Independent objects

The Materialize objects that exist outside the standard namespace hierarchy
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

Two clusters or two roles cannot have the same name, however, a cluster and a
role can share the same name. Replicas can have the same name
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
