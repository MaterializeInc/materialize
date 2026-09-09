---
headless: true
---
- Ownership of the connection.
- In addition, to set, reset, or drop connection options:
  - `USAGE` privileges on all connections and secrets referenced by the
    resulting connection definition.
  - `USAGE` privileges on the schemas that contain those connections and
    secrets.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the connection is namespaced
  by a schema.
