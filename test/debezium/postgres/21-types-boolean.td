# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the boolean type is replicated correctly
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE boolean_type (pk_col BOOLEAN PRIMARY KEY, nopk_col BOOLEAN);
ALTER TABLE boolean_type REPLICA IDENTITY FULL;
INSERT INTO boolean_type VALUES (TRUE, TRUE);
INSERT INTO boolean_type VALUES (FALSE, FALSE);

$ schema-registry-wait-schema schema=postgres.public.boolean_type-value

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE boolean_type
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.boolean_type'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT * FROM boolean_type;
true true
false false

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE boolean_type SET nopk_col = NULL WHERE pk_col = TRUE;
DELETE FROM boolean_type WHERE pk_col = FALSE;

> SELECT * FROM boolean_type;
true <null>
