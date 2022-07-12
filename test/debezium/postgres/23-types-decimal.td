# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the decimal type is replicated correctly
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE decimal_type (f1 DECIMAL(5,3));
ALTER TABLE decimal_type REPLICA IDENTITY FULL;
INSERT INTO decimal_type VALUES (NULL), (NULL), (12.345), ('NaN');

$ schema-registry-wait-schema schema=postgres.public.decimal_type-value

> CREATE SOURCE decimal_type
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.decimal_type'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

# NaN arrives as NULL

> SELECT * FROM decimal_type;
12.345
<null>
<null>
<null>

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE decimal_type SET f1 = NULL WHERE f1 = 'NaN';
UPDATE decimal_type SET f1 = 0.123 WHERE f1 = 12.345;
UPDATE decimal_type SET f1 = 'NaN' WHERE f1 IS NULL;

> SELECT * FROM decimal_type;
<null>
<null>
<null>
0.123
