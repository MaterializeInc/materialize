# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=upsert

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT

> CREATE CLUSTER kept_cluster SIZE 'scale=1,workers=1'

> CREATE SOURCE kept_upsert
  IN CLUSTER kept_cluster
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )

> CREATE TABLE kept_upsert_tbl FROM SOURCE kept_upsert (REFERENCE "testdrive-upsert-${testdrive.seed}")
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT

> CREATE CLUSTER dropped_cluster SIZE 'scale=1,workers=1'

> CREATE SOURCE dropped_upsert
  IN CLUSTER dropped_cluster
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )

> CREATE TABLE dropped_upsert_tbl FROM SOURCE dropped_upsert (REFERENCE "testdrive-upsert-${testdrive.seed}")
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT

$ kafka-ingest format=bytes topic=upsert key-format=bytes key-terminator=:
fish:fish
bird:goose
animal:whale

> SELECT count(*) from dropped_upsert_tbl;
3

> SELECT count(*) from kept_upsert_tbl;
3
