# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SET transaction_isolation = 'strict serializable'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_transaction_timeout = '10min';

> CREATE TABLE t (c1 TEXT, c2 INT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> INSERT INTO t VALUES ('A', 1);

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SINK output
  IN CLUSTER quickstart
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'table-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> INSERT INTO t VALUES ('B', 2);

$ kafka-verify-data format=avro sink=materialize.public.output sort-messages=true
{"before": null, "after": {"row": {"c1": {"string": "A"}, "c2": {"int": 1}}}}
{"before": null, "after": {"row": {"c1": {"string": "B"}, "c2": {"int": 2}}}}
