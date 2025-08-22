- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster.
- `USAGE` privileges on all types used in the materialized view definition.
- `USAGE` privileges on the schemas for the types used in the statement.
- Ownership of the existing view if replacing an existing
  view with the same name (i.e., `OR REPLACE` is specified in `CREATE
  MATERIALIZED VIEW` command).
