- A Kafka or Redpanda cluster. Materialize and the destination system must
  both connect to this cluster.

- A materialized view, source, or table to export. A sink cannot read
  from a plain view.

- A cluster to run the sink. Name this cluster with `IN CLUSTER`. See
  [`CREATE CLUSTER`](/sql/create-cluster/). When a sink starts, it loads a
  full snapshot of the relation into memory. Size the cluster for the
  snapshot, not for the steady-state rate of change.

- The [Kafka ACLs](/sql/create-sink/kafka/#required-kafka-acls) that the sink
  needs.

You also need these privileges in Materialize:

{{% include-headless "/headless/sql-command-privileges/create-sink" %}}
