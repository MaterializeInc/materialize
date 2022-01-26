# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Run parts of repeated-source-rendering-before.td post-restart

> SELECT COUNT(*) = 10 FROM a_view;
true

> SELECT SUM(CAST(statistics->'topics'->'testdrive-re-created-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) = 0 FROM mz_kafka_source_statistics;
true

> DROP VIEW a_view;

> CREATE MATERIALIZED VIEW a_view AS SELECT * FROM re_created;

> SELECT COUNT(*) = 10 FROM a_view;
true

# Re-creating the source should result in no messages being read from Kafka, because we still have the persisted data and offsets.
> SELECT SUM(CAST(statistics->'topics'->'testdrive-re-created-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) = 0 FROM mz_kafka_source_statistics;
true

# Same with DROP INDEX

> DROP INDEX a_view_primary_idx;

> CREATE DEFAULT INDEX ON a_view;

> SELECT COUNT(*) = 10 FROM a_view;
true

> SELECT SUM(CAST(statistics->'topics'->'testdrive-re-created-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) = 0 FROM mz_kafka_source_statistics;
true
