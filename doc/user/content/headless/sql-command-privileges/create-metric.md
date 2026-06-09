---
headless: true
---
- `CREATE` privileges on the containing schema.
- `USAGE` privileges on the API the metric is bound to.
- `SELECT` privileges on the relation named in `VALUES FROM`.
  - NOTE: if the relation is a materialized view, then the view owner must
    also have the necessary privileges to execute the view definition.
