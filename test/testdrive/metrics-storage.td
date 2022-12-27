# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=metrics-test partitions=1
$ kafka-ingest topic=metrics-test format=bytes
jack,jill
goofus,gallant

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SOURCE metrics_test_source (a, b)
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-metrics-test-${testdrive.seed}')
  FORMAT CSV WITH 2 COLUMNS
  INCLUDE OFFSET

> CREATE SINK metrics_test_sink FROM metrics_test_source
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-metrics-test-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

# NOTE: These queries are slow to succeed because the default metrics scraping
# interval is 20 seconds.

> SELECT
      s.name,
      u.cpu_percent >= 0.0,
      u.cpu_percent_normalized >= 0.0,
      u.memory_percent > 0.0
  FROM
      mz_sources AS s
          JOIN
              mz_internal.mz_source_utilization AS u
              ON s.id = u.source_id
  WHERE s.name IN ( 'metrics_test_source' )
  ORDER BY s.name
metrics_test_source true true true

> SELECT
      s.name,
      u.cpu_percent >= 0.0,
      u.cpu_percent_normalized >= 0.0,
      u.memory_percent > 0.0
  FROM
      mz_sinks AS s
          JOIN
              mz_internal.mz_sink_utilization AS u
              ON s.id = u.sink_id
  WHERE s.name IN ( 'metrics_test_sink' )
  ORDER BY s.name
metrics_test_sink true true true

> DROP SINK metrics_test_sink
> DROP SOURCE metrics_test_source
