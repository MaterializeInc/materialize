# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Sink the contents of the mz_catalog.mz_objects system relation to Kafka.
# A sink cannot read a catalog object directly, so we go through a
# user-created materialized view layered over mz_objects.

$ set-sql-timeout duration=60s

$ set-arg-default default-storage-size=scale=1,workers=1

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

# Create a known object so the sinked data is deterministic.
> CREATE TABLE sink_marker_table (a int);

# Materialize a slice of mz_objects, filtered to the object we just created so
# the output does not depend on the rest of the catalog.
> CREATE MATERIALIZED VIEW mz_objects_mv AS
  SELECT name, type FROM mz_catalog.mz_objects WHERE name = 'sink_marker_table';

> CREATE CLUSTER objects_sink_cluster SIZE '${arg.default-storage-size}';

> CREATE SINK mz_objects_sink
  IN CLUSTER objects_sink_cluster
  FROM mz_objects_mv
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-mz-objects-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.mz_objects_sink key=false
{"before": null, "after": {"name": "sink_marker_table", "type": "table"}}

# Dropping the object retracts the row through the sink.
> DROP TABLE sink_marker_table;

$ kafka-verify-data format=json sink=materialize.public.mz_objects_sink key=false
{"before": {"name": "sink_marker_table", "type": "table"}, "after": null}
