# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Change a column from DATE to TIMESTAMP is not allowed
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE alter_change_date_timestamp (f1 DATE);
ALTER TABLE alter_change_date_timestamp REPLICA IDENTITY FULL;
INSERT INTO alter_change_date_timestamp VALUES ('2011-11-11'),('2012-12-12');

$ schema-registry-wait-schema schema=postgres.public.alter_change_date_timestamp-value

> CREATE SOURCE alter_change_date_timestamp
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.alter_change_date_timestamp'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

> SELECT * FROM alter_change_date_timestamp;
2011-11-11
2012-12-12

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_change_date_timestamp ALTER COLUMN f1 TYPE TIMESTAMP;
INSERT INTO alter_change_date_timestamp VALUES ('2011-11-11 11:11:11');
UPDATE alter_change_date_timestamp SET f1 = '2012-12-12 12:12:12' WHERE f1 = '2012-12-12';
DELETE FROM alter_change_date_timestamp WHERE f1 = '2011-11-11';

! SELECT * FROM alter_change_date_timestamp;
contains:Failed to match writer union variant `TimestampMicro` against any variant in the reader for field `postgres.public.alter_change_date_timestamp.Value.f1`
