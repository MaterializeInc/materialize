# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Set up a storage cluster to host the sink
> DROP CLUSTER IF EXISTS storage;
> CREATE CLUSTER storage REPLICAS (r1 (SIZE '1'))

# Set up a replica with two workers:
> DROP CLUSTER IF EXISTS test;
> CREATE CLUSTER test REPLICAS (r (SIZE '2'));
> SET CLUSTER = test;

# Create a materialized view reading from an index:
> CREATE TABLE t (a int);
> CREATE DEFAULT INDEX ON t;
> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;

# Feed a bunch of data into the MV:
> INSERT INTO t SELECT * FROM generate_series(1, 10);

> CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SINK mv_sink IN CLUSTER storage FROM mv INTO KAFKA CONNECTION kafka_conn (TOPIC 'mv-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM

# Wait for the initial data to be written
$ kafka-verify-data format=json topic=mv-${testdrive.seed} key=false sort-messages=true
{"before": null, "after": {"a": 1}}
{"before": null, "after": {"a": 10}}
{"before": null, "after": {"a": 2}}
{"before": null, "after": {"a": 3}}
{"before": null, "after": {"a": 4}}
{"before": null, "after": {"a": 5}}
{"before": null, "after": {"a": 6}}
{"before": null, "after": {"a": 7}}
{"before": null, "after": {"a": 8}}
{"before": null, "after": {"a": 9}}

# This sends a drop command but it is ignored, simulating a delay.
> DROP SINK mv_sink;

# This drops the token in worker 1 but not worker 0:
#   [1] dropping tokens for collection u3
> DROP MATERIALIZED VIEW mv;

# Give some time for clusterds to partially drop their tokens
> SELECT mz_internal.mz_sleep(0.5);
<null>

# # Insert more data into the table:
# Observe that worker 0 continues to write and append (incomplete) batches even though worker 1 has dropped the dataflow.
#   [0] writing 5 updates
#   appending 1
> INSERT INTO t SELECT * FROM generate_series(1, 10);

# Observe partial data written out to kafka
$ kafka-verify-data format=json topic=mv-${testdrive.seed} key=false sort-messages=true
{"before": null, "after": {"a": 1}}
{"before": null, "after": {"a": 10}}
{"before": null, "after": {"a": 2}}
{"before": null, "after": {"a": 3}}
{"before": null, "after": {"a": 4}}
{"before": null, "after": {"a": 5}}
{"before": null, "after": {"a": 6}}
{"before": null, "after": {"a": 7}}
{"before": null, "after": {"a": 8}}
{"before": null, "after": {"a": 9}}
