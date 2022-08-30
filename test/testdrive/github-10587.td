# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/materialize/issues/10587
#
# This file uses the old create source syntax.

$ set-sql-timeout duration=300s

$ set keyschema={"type": "record", "name": "Key", "fields": [ { "name": "f1", "type": "long" } ] }

$ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }

$ set count=100000

# Create sources and fill them with data and render one dataflow that uses everything just to
# stress the system.

$ kafka-create-topic topic=multi-topic-0

$ kafka-ingest format=avro topic=multi-topic-0 schema=${schema} repeat=${count}
{"f2": ${kafka-ingest.iteration} }

$ kafka-create-topic topic=multi-topic-1

$ kafka-ingest format=avro topic=multi-topic-1 schema=${schema} repeat=${count}
{"f2": ${kafka-ingest.iteration} }

$ kafka-create-topic topic=multi-topic-2

$ kafka-ingest format=avro topic=multi-topic-2 schema=${schema} repeat=${count}
{"f2": ${kafka-ingest.iteration} }

$ kafka-create-topic topic=multi-topic-3

$ kafka-ingest format=avro topic=multi-topic-3 schema=${schema} repeat=${count}
{"f2": ${kafka-ingest.iteration} }

$ kafka-create-topic topic=multi-topic-4

$ kafka-ingest format=avro topic=multi-topic-4 schema=${schema} repeat=${count}
{"f2": ${kafka-ingest.iteration} }

$ kafka-create-topic topic=multi-topic-5

$ kafka-ingest format=avro topic=multi-topic-5 schema=${schema} repeat=${count}
{"f2": ${kafka-ingest.iteration} }

$ kafka-create-topic topic=multi-topic-6

$ kafka-ingest format=avro topic=multi-topic-6 schema=${schema} repeat=${count}
{"f2": ${kafka-ingest.iteration} }

$ kafka-create-topic topic=multi-topic-7

$ kafka-ingest format=avro topic=multi-topic-7 schema=${schema} repeat=${count}
{"f2": ${kafka-ingest.iteration} }

$ kafka-create-topic topic=multi-topic-8

$ kafka-ingest format=avro topic=multi-topic-8 schema=${schema} repeat=${count}
{"f2": ${kafka-ingest.iteration} }

$ kafka-create-topic topic=multi-topic-9

$ kafka-ingest format=avro topic=multi-topic-9 schema=${schema} repeat=${count}
{"f2": ${kafka-ingest.iteration} }

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE s0 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-0-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;

> CREATE SOURCE s1 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-1-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;

> CREATE SOURCE s2 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-2-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;

> CREATE SOURCE s3 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-3-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;

> CREATE SOURCE s4 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-4-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;

> CREATE SOURCE s5 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-5-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;

> CREATE SOURCE s6 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-6-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;

> CREATE SOURCE s7 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-7-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;

> CREATE SOURCE s8 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-8-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;

> CREATE SOURCE s9 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-9-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;

> CREATE MATERIALIZED VIEW v1 AS
  SELECT SUM(f1) AS f1 FROM
  (SELECT
    COUNT(*) AS f1 FROM s0
      UNION ALL SELECT COUNT(*) AS f1 FROM s1
      UNION ALL SELECT COUNT(*) AS f1 FROM s2
      UNION ALL SELECT COUNT(*) AS f1 FROM s3
      UNION ALL SELECT COUNT(*) AS f1 FROM s4
      UNION ALL SELECT COUNT(*) AS f1 FROM s5
      UNION ALL SELECT COUNT(*) AS f1 FROM s6
      UNION ALL SELECT COUNT(*) AS f1 FROM s7
      UNION ALL SELECT COUNT(*) AS f1 FROM s8
      UNION ALL SELECT COUNT(*) AS f1 FROM s9);

# Make sure that s1 has been fully timestamped
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

# Now disable retries, and verify that we get the exact same result multiple times
# in a row. Obviously, this test will not always catch the issue since the original
# bug was nondeterministic, but this is a good best-effort smake test.

$ set-max-tries max-tries=1

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000
