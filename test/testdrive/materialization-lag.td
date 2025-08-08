# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the contents of `mz_materialization_lag`.
#
# These tests rely on testdrive's retry feature, as they query introspection
# relations whose data might not be immediately available.
#
# Note that when running under cloudtest, it's quite common to see lags of over
# 1 second even when all replicas are healthy. So we need to build some
# tolerances into these checks that verify whether induced lag is present. A
# threshold value of 2 seconds seems to work fine.

$ set-sql-timeout duration=60s

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_clusters = 15

> CREATE CLUSTER source SIZE 'scale=1,workers=1'
> CREATE CLUSTER compute SIZE 'scale=1,workers=1'
> CREATE CLUSTER sink SIZE 'scale=1,workers=1'

# Set up a bunch of sources and materializations that depend on each other.
# Put them on different clusters so we can spin some down to create lag.

> CREATE SOURCE src
  IN CLUSTER source
  FROM LOAD GENERATOR counter

> CREATE TABLE tbl (a int)

> CREATE VIEW src_plus_tbl
  AS SELECT counter + a AS a FROM src, tbl

> CREATE INDEX idx
  IN CLUSTER compute
  ON src_plus_tbl (a)

> CREATE MATERIALIZED VIEW mv
  IN CLUSTER compute
  AS SELECT * FROM src_plus_tbl

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
> CREATE CONNECTION csr_conn
  TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}')
> CREATE SINK snk
  IN CLUSTER sink
  FROM mv
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

# Make sure things are hydrated.

> INSERT INTO tbl VALUES (1)
> SELECT count(*) > 0 FROM mv
true

# When all clusters are running, there should be no/little visible lag.

> SELECT name
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects ON (id = object_id)
  WHERE
    local_lag > INTERVAL '5s' OR
    global_lag > INTERVAL '5s'

# When the source is down, there should be no visible lag either, as lag is
# relative to the source frontiers.

> ALTER CLUSTER source SET (REPLICATION FACTOR 0)

> SELECT name
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects ON (id = object_id)
  WHERE
    local_lag > INTERVAL '5s' OR
    global_lag > INTERVAL '5s'

> ALTER CLUSTER source SET (REPLICATION FACTOR 1)

# Bring down the compute cluster and observe resulting lag.

> ALTER CLUSTER compute SET (REPLICATION FACTOR 0)

> SELECT name
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects o ON (id = object_id)
  WHERE local_lag > INTERVAL '5s'
idx

> SELECT name
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects ON (id = object_id)
  WHERE global_lag > INTERVAL '5s'
idx
mv
snk

# Bringing up the compute cluster again should remove the lag.

> ALTER CLUSTER compute SET (REPLICATION FACTOR 1)

> SELECT name
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects ON (id = object_id)
  WHERE
    local_lag > INTERVAL '5s' OR
    global_lag > INTERVAL '5s'

# Bring down the sink cluster and observe resulting lag.

> ALTER CLUSTER sink SET (REPLICATION FACTOR 0)

> SELECT name
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects ON (id = object_id)
  WHERE local_lag > INTERVAL '5s'
snk

> SELECT name
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects ON (id = object_id)
  WHERE global_lag > INTERVAL '5s'
snk

# Bringing up the sink cluster again should remove the lag.

> ALTER CLUSTER sink SET (REPLICATION FACTOR 1)

> SELECT name
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects ON (id = object_id)
  WHERE
    local_lag > INTERVAL '5s' OR
    global_lag > INTERVAL '5s'

# If a source has an empty frontier we can't compute a lag value anymore, so
# the lag of dependant collections shows up as NULL instead.

> ALTER CLUSTER compute SET (REPLICATION FACTOR 0)

> CREATE MATERIALIZED VIEW mv_empty
  IN CLUSTER source
  AS SELECT 1;
> CREATE MATERIALIZED VIEW mv_behind_empty
  IN CLUSTER compute
  AS SELECT * FROM mv_empty;

> SELECT local_lag, global_lag
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects ON (id = object_id)
  WHERE name = 'mv_behind_empty'
<null> <null>

# Once the dependant collection manages to catch up, the lag should become 0.

> ALTER CLUSTER compute SET (REPLICATION FACTOR 1)

> SELECT local_lag, global_lag
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects ON (id = object_id)
  WHERE name = 'mv_behind_empty'
00:00:00 00:00:00

# Test that when there are multiple inputs to a materialization the right one
# is reported as the "slowest".

> ALTER CLUSTER source SET (REPLICATION FACTOR 0)

> SELECT o.name, d.name
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects o ON (o.id = object_id)
  JOIN mz_objects d ON (d.id = slowest_local_input_id)
  WHERE o.name = 'idx'
idx src

> SELECT o.name, d.name
  FROM mz_internal.mz_materialization_lag
  JOIN mz_objects o ON (o.id = object_id)
  JOIN mz_objects d ON (d.id = slowest_global_input_id)
  WHERE o.name in ('idx', 'mv', 'snk')
idx src
mv  src
snk src

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET max_clusters
