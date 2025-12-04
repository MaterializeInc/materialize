- Ownership of the materialized view.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the materialized view is
  namespaced by a schema.
