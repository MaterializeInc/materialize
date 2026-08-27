---
headless: true
---
- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the table definition.
- `USAGE` privileges on the schemas that all types in the statement are
  contained in.
- For `CREATE TABLE ... FROM SOURCE`, `SELECT` privileges on the source and
  `USAGE` privileges on its schema. `SELECT` on a source permits attaching any
  reference that source ingests, so grant it only to roles that should be able
  to read all of the source's data.
