# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Setup for performing load tests for persistent upsert sources.
#

$ kafka-create-topic topic=load-test partitions=4

# Mirror mz_source_info and use to keep track of how much data has been ingested
# at all times.
> CREATE MATERIALIZED VIEW records_ingested AS
  SELECT SUM("offset") FROM mz_source_info WHERE
  source_name = 'testdrive-load-test-${testdrive.seed}';

# Alter the underlying index to keep track of historical data.
> ALTER INDEX records_ingested_primary_idx SET(logical_compaction_window="off");

# Sleep enough so that no one asks for times not already present in the index.
> SELECT mz_internal.mz_sleep(2)
<null>

> CREATE SOURCE load_test
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-load-test-${testdrive.seed}'
  FORMAT BYTES
  INCLUDE PARTITION AS kafka_partition, OFFSET AS mz_offset
  ENVELOPE ${arg.envelope}

# Create an intermediate view that applies meaningless filters (in the sense that
# no data can get filtered out) which defeat the demand optimization that gets
# pushed into non-persistent upsert.
#
# TODO: remove this once the optimization is pushed down into persistent upsert
# as well.
> CREATE VIEW intermediate AS SELECT data, kafka_partition, mz_offset FROM load_test
  WHERE
  data != '\\x0' AND
  kafka_partition != 10000 AND
  mz_offset != -1

# Render a dataflow that uses the source, but does a minimal amount of
# work and keeps a minimal amount of data in memory.
#
# This view can be also be used to track how many records have been ingested when
# the data is append only (ie no duplicate keys).
> CREATE MATERIALIZED VIEW load_test_count AS SELECT
  COUNT(*) FROM intermediate;

# Create a view so we can easily query the time that has fully been
# closed in the load test.
> CREATE MATERIALIZED VIEW load_test_materialization_frontier AS
  SELECT frontiers.time FROM
  mz_materialization_frontiers frontiers, mz_indexes indexes WHERE
  frontiers.global_id = indexes.id AND
  indexes.name = 'load_test_count_primary_idx';
