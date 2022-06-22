# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that TOAST-ed values are handled correctly.
#
# TOAST-ed values are values larger than around 8Kb
# see https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-toasted-values
# and https://www.postgresql.org/docs/current/storage-toast.html
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE toasted_full (f1 text);
ALTER TABLE toasted_full REPLICA IDENTITY FULL;
INSERT INTO toasted_full VALUES (NULL), (REPEAT('a', 32 * 1024) || 'b');

$ schema-registry-wait-schema schema=postgres.public.toasted_full-value

> CREATE MATERIALIZED SOURCE toasted_full
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.toasted_full'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

> SELECT length(f1) = (32 * 1024) + 1, substr(f1, 1, 2), substr(f1, 32 * 1024, 2) FROM toasted_full;
<null> <null> <null>
true aa ab

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE toasted_full SET f1 = (REPEAT('c', 32 * 1024) || 'd') WHERE f1 IS NULL;
UPDATE toasted_full SET f1 = (REPEAT('e', 32 * 1024) || 'f') WHERE f1 = REPEAT('a', 32 * 1024) || 'b';

> SELECT length(f1), substr(f1, 1, 2), substr(f1, 32 * 1024, 2) FROM toasted_full;
32769 cc cd
32769 ee ef

# The documentation says that we do not support REPLICA IDENTITY DEFAULT, which is the more interesting
# case, so we are not testing that here.
