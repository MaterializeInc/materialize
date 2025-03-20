# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the MONEY type is replicated correctly
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE money_type (f1 MONEY);
ALTER TABLE money_type REPLICA IDENTITY FULL;
INSERT INTO money_type VALUES (NULL), (12.34);

# waiting for the subject fails with "subject not found"
$ skip-if
SELECT true

$ schema-registry-wait topic=postgres.public.money_type

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE money_type
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.money_type');

> CREATE TABLE money_type_tbl FROM SOURCE money_type (REFERENCE "postgres.public.money_type")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

! SELECT * FROM money_type_tbl;
contains:decimals with precision greater than 38 are not supported
