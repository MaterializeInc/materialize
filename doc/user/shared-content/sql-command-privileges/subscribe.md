- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
  execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
    granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.
