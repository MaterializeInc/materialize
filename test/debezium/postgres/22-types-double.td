# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the double type is replicated correctly
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE double_type (f1 DOUBLE PRECISION, f2 INTEGER PRIMARY KEY);
ALTER TABLE double_type REPLICA IDENTITY FULL;
INSERT INTO double_type VALUES (NULL, 0), ('Infinity', 1),('-Infinity', 2), ('NaN', 3);

$ schema-registry-wait subject=postgres.public.double_type-value

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE CONNECTION IF NOT EXISTS kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SOURCE double_type
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.double_type')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT f1 FROM double_type;
<null>
inf
-inf
NaN

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE double_type SET f1 = 123 WHERE f1 = 'Infinity';
UPDATE double_type SET f1 = -123 WHERE f1 = '-Infinity';
UPDATE double_type SET f1 = NULL WHERE f1 = 'NaN';

> SELECT f1 FROM double_type;
<null>
<null>
123
-123
