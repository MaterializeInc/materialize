# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# ARRAY columns are supported
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE array_type (f1 text[], f2 INTEGER PRIMARY KEY);
INSERT INTO array_type VALUES ('{foo, null}', 0);

$ schema-registry-wait topic=postgres.public.array_type

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE array_type
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.array_type')

> CREATE TABLE array_type_tbl FROM SOURCE array_type (REFERENCE "postgres.public.array_type")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
> COMMIT

# Cast to `text` to work around TD's lack of support for PG arrays
> SELECT f1::text FROM array_type_tbl
f1
---
{foo,NULL}
