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
# see https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-toasteds
# and https://www.postgresql.org/docs/current/storage-toast.html
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE toasted_full (f1 text, f2 INTEGER PRIMARY KEY);
ALTER TABLE toasted_full REPLICA IDENTITY FULL;
INSERT INTO toasted_full VALUES (NULL, 0), (REPEAT('a', 32 * 1024) || 'b', 1);

$ schema-registry-wait topic=postgres.public.toasted_full

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE toasted_full
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.toasted_full');

> CREATE TABLE toasted_full_tbl FROM SOURCE toasted_full (REFERENCE "postgres.public.toasted_full")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT length(f1) = (32 * 1024) + 1, substr(f1, 1, 2), substr(f1, 32 * 1024, 2) FROM toasted_full_tbl;
<null> <null> <null>
true aa ab

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE toasted_full SET f1 = (REPEAT('c', 32 * 1024) || 'd') WHERE f1 IS NULL;
UPDATE toasted_full SET f1 = (REPEAT('e', 32 * 1024) || 'f') WHERE f1 = REPEAT('a', 32 * 1024) || 'b';

> SELECT length(f1), substr(f1, 1, 2), substr(f1, 32 * 1024, 2) FROM toasted_full_tbl;
32769 cc cd
32769 ee ef

# The documentation says that we do not support REPLICA IDENTITY DEFAULT, which is the more interesting
# case, so we are not testing that here.
