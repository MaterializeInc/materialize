# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
{
  "name": "kafka",
  "listen": "0.0.0.0:9093",
  "upstream": "kafka:9092"
}

$ kafka-create-topic topic=input
$ kafka-ingest topic=input format=bytes
Rochester,NY,14618
New York,NY,10004

> CREATE CONNECTION IF NOT EXISTS kafka_conn1 TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
> CREATE CONNECTION IF NOT EXISTS kafka_conn2 TO KAFKA (BROKER 'toxiproxy:9093', SECURITY PROTOCOL PLAINTEXT);

# The source intentionally does not go through toxiproxy.
> BEGIN
> CREATE SOURCE input
  FROM KAFKA CONNECTION kafka_conn1 (TOPIC 'testdrive-input-${testdrive.seed}')

> CREATE TABLE input_tbl (city, state, zip) FROM SOURCE input (REFERENCE "testdrive-input-${testdrive.seed}")
  FORMAT CSV WITH 3 COLUMNS
  INCLUDE OFFSET
> COMMIT

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SINK output
  IN CLUSTER quickstart
  FROM input_tbl
  INTO KAFKA CONNECTION kafka_conn2 (TOPIC 'output-byo-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
