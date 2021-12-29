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

> CREATE SOURCE load_test
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-load-test-${testdrive.seed}'
  FORMAT BYTES
  INCLUDE PARTITION AS kafka_partition, OFFSET AS mz_offset
  ENVELOPE UPSERT

# Keep track of the maximum offset afor each partition that has been ingested and
# made definite, and try do so with as little overhead as possible.
> CREATE MATERIALIZED VIEW max_offsets AS SELECT
  kafka_partition,
  MAX(mz_offset) as max_offset FROM load_test
  GROUP BY kafka_partition OPTION(expected_group_size = 50000000);

# The total number of messages ingested from the topic is <= the sum of the max
# moffsets at each partition (Kafka can occaisonally use some offsets for storing
# metadata but its rare in this workload).
#
# TODO: we could encode the # of generated and inserted messages in the payload of
# the generated messages, but it would require some annoying bit maninpulation to
# extract it out, and unclear if it would save as that much CPU/RAM because we
# still would need to compute a max over that value.
> CREATE MATERIALIZED VIEW num_records_ingested AS
  SELECT SUM(max_offset) FROM max_offsets;
