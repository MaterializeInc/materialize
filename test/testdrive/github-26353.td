# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# We will create a topic with 100 partitions and we create two sinks that
# publishe a single record 4 times, each time with a different Avro schema id
# due to changed comments.
#
# With the v0 partitioning scheme this will almost certainly lead to at
# least two different partitions being used, which is the issue we're trying to
# avoid. The chance of the v0 partitioning scheme correctly assigning the
# same partition four times in a row is one in 100 million, so while flaky by
# design this is low enough to be negligible.
#
# With the v1 partitioning scheme this is never lead to two different
# partitions being used.
$ kafka-create-topic topic=data partitions=100

$ set-arg-default default-storage-size=1
$ set-arg-default single-replica-cluster=quickstart

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_comment = true;

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

# This is the row that will be published with the v0 scheme
> CREATE TABLE data (key text, value text);
> INSERT INTO data VALUES('v0', NULL);

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET default_sink_partition_strategy = 'v0';

# v0 Scheme - Execution 1
> COMMENT ON COLUMN data.key IS 'v01';
> CREATE SINK v0
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
$ kafka-verify-data format=avro sink=materialize.public.v0
{"key": {"string": "v0"}} {"key": {"string": "v0"}, "value": null}
> DROP SINK v0;

# v0 Scheme - Execution 2
> COMMENT ON COLUMN data.key IS 'v02';
> CREATE SINK v0
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
$ kafka-verify-data format=avro sink=materialize.public.v0
{"key": {"string": "v0"}} {"key": {"string": "v0"}, "value": null}
> DROP SINK v0;

# v0 Scheme - Execution 3
> COMMENT ON COLUMN data.key IS 'v03';
> CREATE SINK v0
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
$ kafka-verify-data format=avro sink=materialize.public.v0
{"key": {"string": "v0"}} {"key": {"string": "v0"}, "value": null}
> DROP SINK v0;

# v0 Scheme - Execution 4
> COMMENT ON COLUMN data.key IS 'v04';
> CREATE SINK v0
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
$ kafka-verify-data format=avro sink=materialize.public.v0
{"key": {"string": "v0"}} {"key": {"string": "v0"}, "value": null}
> DROP SINK v0;

# This is the row that will be published with the v0 scheme
> DROP TABLE data;
> CREATE TABLE data (key text, value text);
> INSERT INTO data VALUES('v1', NULL);

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET default_sink_partition_strategy = 'v1';

# v1 Scheme - Execution 1
> COMMENT ON COLUMN data.key IS 'v11';
> CREATE SINK v1
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
$ kafka-verify-data format=avro sink=materialize.public.v1
{"key": {"string": "v1"}} {"key": {"string": "v1"}, "value": null}
> DROP SINK v1;

# v1 Scheme - Execution 2
> COMMENT ON COLUMN data.key IS 'v12';
> CREATE SINK v1
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
$ kafka-verify-data format=avro sink=materialize.public.v1
{"key": {"string": "v1"}} {"key": {"string": "v1"}, "value": null}
> DROP SINK v1;

# v1 Scheme - Execution 3
> COMMENT ON COLUMN data.key IS 'v13';
> CREATE SINK v1
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
$ kafka-verify-data format=avro sink=materialize.public.v1
{"key": {"string": "v1"}} {"key": {"string": "v1"}, "value": null}
> DROP SINK v1;

# v1 Scheme - Execution 4
> COMMENT ON COLUMN data.key IS 'v14';
> CREATE SINK v1
  FROM data
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
$ kafka-verify-data format=avro sink=materialize.public.v1
{"key": {"string": "v1"}} {"key": {"string": "v1"}, "value": null}
> DROP SINK v1;

# Now we will ingest the raw data and confirm that the v0 scheme moved
# records around and that the v1 scheme used a single partition.
> CREATE SOURCE sink_verify
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE PARTITION
  ENVELOPE NONE;

> SELECT key, COUNT(DISTINCT partition) = 1 FROM sink_verify GROUP BY key
v0 false
v1 true
