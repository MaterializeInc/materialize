# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=1

$ kafka-create-topic topic=missing_keys_or_values partitions=1

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CLUSTER missing_keys_or_values_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE missing_keys_or_values
  IN CLUSTER missing_keys_or_values_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-missing_keys_or_values-${testdrive.seed}')

> CREATE TABLE missing_keys_or_values_tbl FROM SOURCE missing_keys_or_values (REFERENCE "testdrive-missing_keys_or_values-${testdrive.seed}")
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY
  ENVELOPE NONE

# make sure a record with both key and value goes through

$ kafka-ingest topic=missing_keys_or_values format=bytes key-format=bytes key-terminator=:
hello:world

> SELECT * FROM missing_keys_or_values_tbl
key   text
-----------
hello world


# send a value without a key. key columns should be null

$ kafka-ingest topic=missing_keys_or_values format=bytes omit-key=true
foo

> SELECT * FROM missing_keys_or_values_tbl
key    text
-------------
hello  world
<null> foo


# send an empty record with neither key nor value, should be skipped

$ kafka-ingest topic=missing_keys_or_values format=bytes omit-value=true omit-key=true

> SELECT * FROM missing_keys_or_values_tbl
key    text
-------------
hello  world
<null> foo


# send a key without a value, should error

$ kafka-ingest topic=missing_keys_or_values key-format=bytes format=bytes omit-value=true
bar

! SELECT * FROM missing_keys_or_values_tbl
contains: Envelope error: Flat: Value not present for message
