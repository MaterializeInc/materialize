# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Ingest wide data
#

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "long"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"long"}
        ]
    }

#
# Generate the wide data via a convoluted mechanism so that we do not have to push a multi-MB file in
# the repository.
# 1. Create a topic + source that outputs numbers 0 to 9
# 2. Create a materialized view that outputs 10 rows worth of wide data
# 3. Have that view produce a new Kafka topic that has the final data we want to ingest
#

$ kafka-create-topic topic=wide-data-ten

$ kafka-ingest format=avro topic=wide-data-ten key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}

> CREATE CONNECTION IF NOT EXISTS kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE wide_data_ten
  FROM KAFKA CONNECTION kafka_conn TOPIC 'testdrive-wide-data-ten-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE;

> CREATE MATERIALIZED VIEW wide_data_view AS SELECT wide_data_ten.f2 AS key, REPEAT('x', 512 * 1024) AS value FROM wide_data_ten;

> CREATE SINK wide_data_sink FROM wide_data_view
  INTO KAFKA CONNECTION kafka_conn TOPIC 'testdrive-wide-data-${testdrive.seed}'
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

> CREATE SOURCE wide_data_source
  FROM KAFKA CONNECTION kafka_conn TOPIC 'testdrive-wide-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE KEY AS key2
  ENVELOPE UPSERT;

> SELECT COUNT(*) FROM wide_data_source;
10
