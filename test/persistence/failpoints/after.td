# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reading all that data after starting up can take longer than the default timeout.
$ set-sql-timeout duration=60s

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"string"}
        ]
    }

> SELECT COUNT(*), COUNT(DISTINCT f1), COUNT(DISTINCT f2) FROM failpoint;
100000 100000 100000

# We expect that we have successfully read and persisted some messages before the failpoint was activated
> SELECT SUM(CAST(statistics->'topics'->'testdrive-failpoint-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) < 100000 FROM mz_kafka_source_statistics;
true

# Make sure that ingestion can continue

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "k${kafka-ingest.iteration}"} {"f2": "k${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "l${kafka-ingest.iteration}"} {"f2": "l${kafka-ingest.iteration}"}

> SELECT COUNT(*), COUNT(DISTINCT f1), COUNT(DISTINCT f2) FROM failpoint;
120000 120000 120000


# Validate the output of the sink as well

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE failpoint_from_sink
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-failpoint-sink-${testdrive.seed}'
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE KEY AS f1
  ENVELOPE UPSERT

> SELECT COUNT(*), COUNT(DISTINCT f1), COUNT(DISTINCT after) FROM failpoint_from_sink
120000 120000 120000

# Delete some values inserted pre-restart
$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "a${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=failpoint key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": "b${kafka-ingest.iteration}"}

# And validate again
> SELECT COUNT(*), COUNT(DISTINCT f1), COUNT(DISTINCT f2) FROM failpoint;
100000 100000 100000

> SELECT COUNT(*) FROM failpoint_from_sink;
120000
