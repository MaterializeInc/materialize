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

$ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
{
  "name": "csr",
  "listen": "0.0.0.0:8081",
  "upstream": "schema-registry:8081"
}

$ kafka-create-topic topic=input
$ kafka-ingest topic=input format=bytes repeat=1000000000
Rochester,NY,14618
New York,NY,10004

$ kafka-create-topic topic=input2
$ kafka-ingest topic=input2 format=bytes repeat=1000000000
Rochester,NY,14618,0
New York,NY,10004,0

> CREATE CONNECTION IF NOT EXISTS kafka_conn1 TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS kafka_conn2 TO KAFKA (BROKER 'toxiproxy:9093', SECURITY PROTOCOL PLAINTEXT);

# The source intentionally does not go through toxiproxy.
> CREATE SOURCE input (city, state, zip)
  FROM KAFKA CONNECTION kafka_conn1 (TOPIC 'testdrive-input-${testdrive.seed}')
  FORMAT CSV WITH 3 COLUMNS
  INCLUDE OFFSET

> CREATE SOURCE input2 (city, state, zip, x)
  FROM KAFKA CONNECTION kafka_conn1 (TOPIC 'testdrive-input2-${testdrive.seed}')
  FORMAT CSV WITH 3 COLUMNS
  INCLUDE OFFSET

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://toxiproxy:8081/'
  );

> CREATE SINK output2 FROM input2
  INTO KAFKA CONNECTION kafka_conn2 (TOPIC 'output-byo-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE SINK output FROM input
  INTO KAFKA CONNECTION kafka_conn2 (TOPIC 'output-byo-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
