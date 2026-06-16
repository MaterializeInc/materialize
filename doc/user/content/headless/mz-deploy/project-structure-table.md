---
headless: true
---
| Directory | Scope | Contains |
|-----------|-------|----------|
| `models/` | Per database and schema | Schema-scoped objects ([views](/sql/create-view/), [materialized views](/sql/create-materialized-view/), [sinks](/sql/create-sink/), [tables](/sql/create-table/), [sources](/sql/create-source/), [connections](/sql/create-connection/), and [secrets](/sql/create-secret/)) organized under `models/<database>/<schema>/`. **Note**: You do not create the database and schema explicitly; `mz-deploy` [derives them from the file path](/manage/mz-deploy/develop/project-structure/#file-path-to-object-name-mapping) and automatically creates them during deployment. |
| `clusters/` | Global | [Cluster](/sql/create-cluster/) definitions. |
| `roles/` | Global | [Role](/sql/create-role/) definitions. |
| `network-policies/` | Global | [Network policy](/sql/create-network-policy/) definitions. |
