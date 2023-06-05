# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test creating a Kafka/CSR source using SSH when both the Kafka broker and
# CSR server use SSL.

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=avroavro

$ kafka-ingest format=avro topic=avroavro schema=${schema}
{"f1": "fish", "f2": 1000}

> CREATE SECRET ssl_key_kafka AS '${arg.materialized-kafka-key}'
> CREATE SECRET ssl_key_csr AS '${arg.materialized-schema-registry-key}'
> CREATE SECRET password_csr AS 'sekurity'

> CREATE CONNECTION kafka_conn
  TO KAFKA (
    BROKER '${testdrive.kafka-addr}' USING SSH TUNNEL thancred,
    SSL KEY = SECRET ssl_key_kafka,
    SSL CERTIFICATE = '${arg.materialized-kafka-crt}',
    SSL CERTIFICATE AUTHORITY = '${arg.ca-crt}'
  );

> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}',
    SSH TUNNEL thancred,
    SSL KEY = SECRET ssl_key_csr,
    SSL CERTIFICATE = '${arg.materialized-schema-registry-crt}',
    SSL CERTIFICATE AUTHORITY = '${arg.ca-crt}',
    USERNAME = 'materialize',
    PASSWORD = SECRET password_csr
  );

> CREATE SOURCE csr_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-avroavro-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> SELECT * FROM csr_source
f1    f2
----------
fish  1000
