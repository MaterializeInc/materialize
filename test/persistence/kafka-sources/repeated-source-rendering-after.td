# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Run parts of repeated-source-rendering-before.td post-restart

> SELECT COUNT(*) FROM a_view;
10

> SELECT SUM(CAST(statistics->'topics'->'testdrive-re-created-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) FROM mz_kafka_source_statistics;
0

# Verify that we cannot create multiple materializations of a persisted source, even after restart.
! CREATE MATERIALIZED VIEW a_view_second_materialization AS SELECT * FROM re_created;
contains:Cannot re-materialize source re_created

> DROP VIEW a_view;

> CREATE MATERIALIZED VIEW a_view AS SELECT * FROM re_created;

> SELECT COUNT(*) FROM a_view;
10

# Re-creating the source should result in no messages being read from Kafka, because we still have the persisted data and offsets.
> SELECT SUM(CAST(statistics->'topics'->'testdrive-re-created-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) FROM mz_kafka_source_statistics;
0

# Same with DROP INDEX

> DROP INDEX a_view_primary_idx;

> CREATE DEFAULT INDEX ON a_view;

> SELECT COUNT(*) FROM a_view;
10

> SELECT SUM(CAST(statistics->'topics'->'testdrive-re-created-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) FROM mz_kafka_source_statistics;
0
