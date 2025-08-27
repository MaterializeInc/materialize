- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the view definition.
- `USAGE` privileges on the schemas for the types in the statement.
- Ownership of the existing view if replacing an existing
  view with the same name (i.e., `OR REPLACE` is specified in `CREATE VIEW` command).
