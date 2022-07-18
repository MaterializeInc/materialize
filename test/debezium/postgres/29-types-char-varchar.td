# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that CHAR/VARCHAR is replicated correctly
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE char_varchar_type (char_col CHAR(5), varchar_col VARCHAR(5));
ALTER TABLE char_varchar_type REPLICA IDENTITY FULL;
INSERT INTO char_varchar_type VALUES ('a ', 'a ');

$ schema-registry-wait-schema schema=postgres.public.char_varchar_type-value

> CREATE SOURCE char_varchar_type
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.char_varchar_type'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

# Type information is not carried over as the AVRO protocol does not have CHAR/VARCHAR
> SELECT pg_typeof(char_col), pg_typeof(varchar_col) FROM char_varchar_type;
text text

> SELECT char_col, varchar_col FROM char_varchar_type;
"a    " "a "

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE char_varchar_type SET char_col = 'a  ', varchar_col = 'a  ';

> SELECT char_col, varchar_col FROM char_varchar_type;
"a    " "a  "
