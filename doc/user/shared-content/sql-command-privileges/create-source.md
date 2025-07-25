- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster if the source is created in an existing cluster.
- `CREATECLUSTER` privileges on the system if the source is not created in an existing cluster.
- `USAGE` privileges on all connections and secrets used in the source definition.
- `USAGE` privileges on the schemas that all connections and secrets in the
  statement are contained in.
