# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=largeinput
$ kafka-ingest topic=largeinput format=bytes
A,B,0

# The test intentionally does not go through toxiproxy.
> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE largeinput
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-largeinput-${testdrive.seed}')

> CREATE TABLE largeinput_tbl (city, state, zip) FROM SOURCE largeinput (REFERENCE "testdrive-largeinput-${testdrive.seed}")
  FORMAT CSV WITH 3 COLUMNS
  INCLUDE OFFSET

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );
