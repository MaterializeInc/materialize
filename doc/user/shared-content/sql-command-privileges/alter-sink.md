- Ownership of the sink being altered.
- In addition,
  - To change the sink from relation:
    - `SELECT` privileges on the new relation being written out to an external system.
    - `CREATE` privileges on the cluster maintaining the sink.
    - `USAGE` privileges on all connections and secrets used in the sink definition.
    - `USAGE` privileges on the schemas that all connections and secrets in the
      statement are contained in.
  - To change owners:
    - Role membership in `new_owner`.
    - `CREATE` privileges on the containing schema if the sink is namespaced
  by a schema.
