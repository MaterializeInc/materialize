# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the BYTEA type is replicated correctly
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE bytea_type (f1 BYTEA);
ALTER TABLE bytea_type REPLICA IDENTITY FULL;
INSERT INTO bytea_type VALUES (NULL), (''), (E'\\x00'), (E'\\xABCDEF1234');

$ schema-registry-wait-schema schema=postgres.public.bytea_type-value

> CREATE SOURCE bytea_type
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.bytea_type'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

# There is \\x124 below instead of \x1234 but apparently this is outside of Mz
# as Mz returns the correct length and the correct output in the psql client
> SELECT f1, length(f1), f1 = E'\\xABCDEF1234' FROM bytea_type;
"" 0 false
<null> <null> <null>
"\\x00" 1 false
"\\xab\\xcd\\xef\\x124" 5 true

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE bytea_type SET f1 = E'\\xFFFF' WHERE f1 IS NULL;
UPDATE bytea_type SET f1 = NULL WHERE f1 = E'\\xABCDEF1234';
UPDATE bytea_type SET f1 = E'\\x0000' WHERE f1 = E'\\x00';

> SELECT f1, length(f1) FROM bytea_type;
"" "0"
<null> <null>
"\\x00\\x00" 2
"\\xff\\xff" 2
