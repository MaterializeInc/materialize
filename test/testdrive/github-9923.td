# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for database-issues#9923.

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  )

> CREATE TABLE t (a int)
> CREATE VIEW v AS SELECT a, 1 FROM t

! CREATE SINK sink
  FROM v
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-alter-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM
contains:v is a view, which cannot be exported as a sink

> CREATE SINK sink
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-alter-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM

! ALTER SINK sink SET FROM v
contains:v is a view, which cannot be exported as a sink
