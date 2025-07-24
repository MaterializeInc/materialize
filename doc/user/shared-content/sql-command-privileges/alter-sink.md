- Ownership of the sink being altered.
- `SELECT` privileges on the new relation being written out to an external system.
- `CREATE` privileges on the cluster maintaining the sink.
- `USAGE` privileges on all connections and secrets used in the sink definition.
- `USAGE` privileges on the schemas that all connections and secrets in the
  statement are contained in.
