---
headless: true
---
- `SELECT` privileges on all **directly** referenced relations in the query. If
  the directly referenced relation is a view or materialized view: {{% include-headless "/headless/rbac-cloud/select-views-privileges" %}}

- `USAGE` privileges on the schemas that contain the relations in the query.
- `USAGE` privileges on the active cluster.
