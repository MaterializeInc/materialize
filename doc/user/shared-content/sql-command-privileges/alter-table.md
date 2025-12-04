- Ownership of the table being altered.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the table is namespaced by
  a schema.
